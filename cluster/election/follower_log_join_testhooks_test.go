//go:build testhooks

package election_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

const (
	followerLogJoinTickNs  = 10_000_000 // 10 ms simulated time per tick
	followerLogJoinMaxTick = 500
)

// multiMemberNode is one fully wired election participant for the completion
// harness: real Agent, HostAdapter, SbePublisher, and SubscriptionLogJoinSource.
type multiMemberNode struct {
	id           int32
	agent        *consensus.Agent
	election     *election.Election
	logPublisher *consensus.LogPublisher
	logAdapter   *consensus.LogAdapter
	observer     *recordingStateObserver
}

// TestMultiMemberElectionCompletion drives cold-start elections through real
// production join wiring at one, two, and three members, plus a three-member
// negative control with an empty log-join subscription.
func TestMultiMemberElectionCompletion(t *testing.T) {
	t.Parallel()

	for _, n := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("members=%d", n), func(t *testing.T) {
			t.Parallel()
			runMultiMemberElectionCompletion(t, n, true)
		})
	}

	t.Run("members=3 empty join source", func(t *testing.T) {
		t.Parallel()
		runMultiMemberElectionCompletion(t, 3, false)
	})
}

func runMultiMemberElectionCompletion(t *testing.T, n int, seedJoinImages bool) {
	t.Helper()
	require.GreaterOrEqual(t, n, 1)

	clock := &FakeClock{nowNs: 1_000_000_000}
	epochClock := &FakeEpochClock{nowMs: 1_000}

	// Build log publishers first so join images can carry every session id.
	logPublishers := make([]*consensus.LogPublisher, n)
	for i := range n {
		lp := consensus.NewLogPublisher()
		lp.SetPublication(aeron.NewTestExclusivePublicationForHooks(t))
		logPublishers[i] = lp
	}

	ingresses := make([]*consensus.QueueConsensusIngress, n)
	for i := range n {
		ingresses[i] = &consensus.QueueConsensusIngress{}
	}

	nodes := make([]*multiMemberNode, n)
	for i := range n {
		id := int32(i)
		memberPtrs := hostAdapterMembers(n)
		thisMember := memberPtrs[i]

		nodeState, err := election.OpenNodeStateFile(t.TempDir(), true, recordinglog.FileSyncNone)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, nodeState.Close()) })

		logAdapter := consensus.NewLogAdapter(0)
		logPublisher := logPublishers[i]

		sub := aeron.NewTestSubscriptionForHooks(t)
		if seedJoinImages {
			// Seed every publisher session so any elected leader is joinable.
			// Dedup session ids: test exclusive publications share one session.
			seen := make(map[int32]struct{}, n)
			corr := int64(id+1) * 1000
			for _, lp := range logPublishers {
				sessionID := lp.SessionID()
				if _, ok := seen[sessionID]; ok {
					continue
				}
				seen[sessionID] = struct{}{}
				corr++
				image := aeron.NewTestImageForSubscriptionHooksWithSessionID(
					t,
					sub,
					sessionID,
					corr,
					0,
				)
				require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
			}
		}
		joinSource := consensus.NewSubscriptionLogJoinSource(sub)

		cfg := multiMemberElectionConfig(id)
		agent := consensus.NewAgent(consensus.AgentConfig{
			Commit:                    new(atomic.Int64),
			LeaderHeartbeatIntervalNs: cfg.LeaderHeartbeatIntervalNs,
			LogPublisher:              logPublisher,
			LogAdapter:                logAdapter,
			LogJoinSource:             joinSource,
			ThisMemberID:              id,
			LeaderMemberID:            election.NullMemberID,
			Role:                      consensus.RoleFollower,
		})
		agent.SetAppendSource(consensus.NewLogAppendPosition(agent))
		agent.SetIngress(ingresses[i])

		pub := &election.SbePublisher{}
		pub.SentHandler = func(msg election.SentMessage) {
			dst := int(msg.Dst.MemberID)
			if dst < 0 || dst >= n {
				return
			}
			wire := append([]byte(nil), msg.Wire...)
			ingresses[dst].Enqueue(wire)
		}

		host := election.NewHostAdapter(election.HostAdapterConfig{
			Agent:                    agent,
			Publisher:                pub,
			Members:                  memberPtrs,
			ThisMember:               thisMember,
			LeaderHeartbeatTimeoutNs: cfg.LeaderHeartbeatTimeoutNs,
			NanoClock:                clock.TimeNs,
			LeaderMemberID:           election.NullMemberID,
		})
		agent.SetPositions(host)
		agent.SetEgress(host)

		observer := &recordingStateObserver{}
		e := election.NewElection(cfg, election.ElectionDeps{
			Handler:    agent,
			Host:       host,
			Publisher:  pub,
			NanoClock:  clock.TimeNs,
			EpochClock: epochClock.TimeMs,
			// Member 0 nominates immediately; others wait nearly half an election
			// timeout so RequestVote lands before a competing candidacy.
			NominationRand: func() float64 {
				if id == 0 {
					return 0
				}
				return 0.99
			},
			NodeState:     nodeState,
			Members:       memberPtrs,
			ThisMember:    thisMember,
			StateObserver: observer,
		})
		agent.SetElection(e)

		nodes[i] = &multiMemberNode{
			id:           id,
			agent:        agent,
			election:     e,
			logPublisher: logPublisher,
			logAdapter:   logAdapter,
			observer:     observer,
		}
	}

	completed := false
	for range followerLogJoinMaxTick {
		nowNs := clock.nowNs
		for _, node := range nodes {
			node.agent.DoWork(nowNs)
		}
		if seedJoinImages && allElectionsComplete(nodes) {
			completed = true
			break
		}
		advanceFakeClocks(clock, epochClock, followerLogJoinTickNs)
	}

	if !seedJoinImages {
		requireMultiMemberElectionStalled(t, nodes)
		return
	}

	require.True(t, completed, "election did not complete within %d ticks", followerLogJoinMaxTick)
	requireMultiMemberElectionComplete(t, nodes)
}

func multiMemberElectionConfig(memberID int32) election.ElectionConfig {
	cfg := election.ElectionTestConfig()
	cfg.MemberID = memberID
	cfg.IsNodeStartup = true
	cfg.LogPosition = 0
	cfg.AppendPosition = 0
	cfg.LeadershipTermID = 0
	// Tight but staggered: member 0 nominates at once; peers wait ~99ms of the
	// half-timeout before competing. Join wait stays multi-tick.
	cfg.ElectionTimeoutNs = 200_000_000
	cfg.ElectionStatusIntervalNs = 10_000_000
	cfg.LeaderHeartbeatIntervalNs = 20_000_000
	cfg.LeaderHeartbeatTimeoutNs = 1_000_000_000
	cfg.StartupCanvassTimeoutNs = 200_000_000
	return cfg
}

func allElectionsComplete(nodes []*multiMemberNode) bool {
	for _, node := range nodes {
		if node.agent.Election() != nil {
			return false
		}
	}
	return true
}

func requireMultiMemberElectionComplete(t *testing.T, nodes []*multiMemberNode) {
	t.Helper()

	var leader *multiMemberNode
	for _, node := range nodes {
		require.Nil(t, node.agent.Election(), "member %d still electing", node.id)
		require.Contains(
			t,
			node.observer.states,
			election.ElectionStateClosed,
			"member %d never reached CLOSED",
			node.id,
		)

		switch node.agent.Role() {
		case consensus.RoleLeader:
			if leader != nil {
				require.Failf(t, "two leaders", "%d and %d", leader.id, node.id)
			}
			leader = node
		case consensus.RoleFollower:
			// expected for non-leaders
		default:
			require.Failf(t, "unexpected role", "member %d role %v", node.id, node.agent.Role())
		}
	}
	require.NotNil(t, leader, "no leader elected")

	require.Equal(t, election.NullMemberID, leader.agent.LeaderMemberID())
	require.Positive(t, leader.logPublisher.Position())

	for _, node := range nodes {
		if node == leader {
			continue
		}
		require.Equal(t, consensus.RoleFollower, node.agent.Role())
		require.Equal(t, leader.agent.ThisMemberID(), node.agent.LeaderMemberID())
		require.NotNil(t, node.logAdapter.Image(), "member %d log image unbound", node.id)
	}
}

func requireMultiMemberElectionStalled(t *testing.T, nodes []*multiMemberNode) {
	t.Helper()

	for _, node := range nodes {
		require.NotNil(t, node.agent.Election(), "member %d completed with empty join source", node.id)
	}

	var leader *multiMemberNode
	for _, node := range nodes {
		if node.agent.Role() == consensus.RoleLeader {
			require.Nil(t, leader, "two leaders with empty join source")
			leader = node
		}
	}
	require.NotNil(t, leader, "expected a leader stuck in leader-ready")
	require.Equal(t, election.ElectionStateLeaderReady, leader.election.State())
}
