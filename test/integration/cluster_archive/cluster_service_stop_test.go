//go:build integration && archive_live

package cluster_archive_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/client"
	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/test/integration/harness"
)

// TestClusterService_stopEndsMemberAndReElects closes a leader or follower
// service under session traffic.
func TestClusterService_stopEndsMemberAndReElects(t *testing.T) {
	t.Parallel()

	trafficPayload := []byte("service-stop-message")
	postPayload := []byte("service-stop-post-message")

	for _, stopLeader := range []bool{true, false} {
		name := "follower"
		if stopLeader {
			name = "leader"
		}

		t.Run(name, func(t *testing.T) {
			moduleErrors := harness.NewModuleErrors()
			sc := startServiceCluster(
				t,
				harness.ClusterServiceStopPortBase,
				harness.ClusterServiceStopLogEndpoint,
				func(cfg cluster.Config) cluster.Config {
					return moduleErrors.AmendConfig(amendServiceMember(cfg))
				},
			)
			set := sc.set

			for i := range sc.members {
				serviceAgent := sc.members[i].container.Agent()
				err := set.Module(i).Agent().SetServiceLiveness(0, serviceAgent)
				require.NoError(t, err)
			}

			requireServicesJoined(t, sc)

			leader := requireSingleLeader(t, set, "")
			pump, session := openTransferSession(
				t,
				sc,
				leader,
				harness.ClientEgressPort(harness.ClusterServiceStopPortBase),
				&client.EgressHandlers{},
			)
			sessionID := session.ClusterSessionID()
			require.GreaterOrEqual(t, sessionID, int64(0))

			requireSessionMessageCommitted(t, sc, pump, leader, sessionID, nil, trafficPayload)

			if !leader.IsLeader() {
				require.FailNow(t, "leader lost before stop", sc.dump())
			}

			victimIndex := int(leader.ThisMemberID())
			if !stopLeader {
				victimIndex = firstNonLeaderIndex(set, leader.ThisMemberID())
				if victimIndex < 0 {
					require.FailNow(t, "no follower", sc.dump())
				}
			}

			preTerm := leader.LeadershipTermID()
			preCommit := leader.CommitPosition()
			floors := snapshotServiceLogPositions(sc)
			electionCounts := snapshotElectionCounts(sc)
			t.Logf(
				"stop member %d leader=%v term=%d commit=%d",
				victimIndex,
				stopLeader,
				preTerm,
				preCommit,
			)

			requireServiceStopEndsMember(t, sc, pump, victimIndex, moduleErrors)

			// The ended leader stops sending heartbeats. A survivor starts an
			// election on the leader heartbeat timeout.
			if stopLeader {
				awaitHeartbeatElection(t, sc, pump, victimIndex, electionCounts)
			}

			require.NoError(t, set.StopMember(victimIndex))
			require.Nil(t, set.Module(victimIndex))

			live := leader
			var onTick func()
			if stopLeader {
				live = requireTransferSessionRecovery(
					t,
					sc,
					pump,
					session,
					victimIndex,
					preTerm,
					preCommit,
					floors,
				)
			} else {
				require.Equal(t, leader, requireSingleLeader(t, set, "after follower stop"))
				onTick = func() { failIfFurtherElection(t, set, preTerm, victimIndex) }
			}

			requireSessionMessageCommitted(
				t,
				sc,
				pump,
				live,
				sessionID,
				onTick,
				trafficPayload,
				postPayload,
			)
			failIfServiceTransferStall(t, sc, victimIndex)
		})
	}
}

// requireServiceStopEndsMember closes the service container at victimIndex.
// It then asserts that the member ends with ErrServiceStopped.
func requireServiceStopEndsMember(
	t testing.TB,
	sc serviceCluster,
	pump *transferPump,
	victimIndex int,
	moduleErrors *harness.ModuleErrors,
) {
	t.Helper()

	victim := sc.set.Module(victimIndex)
	closeServiceTransferContainer(t, sc, victimIndex)
	requireServiceAwaitTick(
		t,
		sc,
		"member ended",
		durableWorkBound,
		pump.sessionPump(func() bool {
			return victim.Agent().TerminationError() != nil
		}),
	)

	termErr := victim.Agent().TerminationError()
	require.ErrorIs(t, termErr, consensus.ErrTerminated)
	require.ErrorIs(t, termErr, consensus.ErrServiceStopped)
	require.True(
		t,
		strings.HasSuffix(termErr.Error(), "service 0 closed"),
		"termination %q",
		termErr,
	)
	require.Equal(t, 1, moduleErrors.Count(victim.ThisMemberID(), consensus.ErrServiceStopped))

	for i, m := range sc.set.Modules() {
		if i == victimIndex || m == nil {
			continue
		}

		require.Zero(t, moduleErrors.Count(m.ThisMemberID(), consensus.ErrServiceStopped))
	}

	require.Equal(t, consensus.ModuleStateClosed, victim.ModuleState())
	require.Zero(t, victim.Agent().DoWork(victim.NanoClock()()))
}

// snapshotElectionCounts copies each module ElectionCount in member-index
// order.
func snapshotElectionCounts(sc serviceCluster) []int64 {
	modules := sc.set.Modules()
	counts := make([]int64, len(modules))

	for i, m := range modules {
		if m != nil {
			counts[i] = m.Agent().ElectionCount()
		}
	}

	return counts
}

// awaitHeartbeatElection awaits an election that the leader heartbeat timeout
// starts on a member other than skipIndex. The election count must pass its
// electionCounts entry.
func awaitHeartbeatElection(
	t testing.TB,
	sc serviceCluster,
	pump *transferPump,
	skipIndex int,
	electionCounts []int64,
) {
	t.Helper()

	requireServiceAwaitTick(
		t,
		sc,
		"heartbeat election",
		durableReElectBound,
		pump.sessionPump(func() bool {
			for i, m := range sc.set.Modules() {
				if i == skipIndex || m == nil {
					continue
				}

				agent := m.Agent()
				if agent.ElectionCount() > electionCounts[i] &&
					agent.LastElectionTrigger() == consensus.ElectionTriggerLeaderHeartbeatTimeout {
					return true
				}
			}

			return false
		}),
	)
}

// requireSessionMessageCommitted offers the last of payloads on the session.
// It awaits a leader commit past the pre-offer position. Every live service
// must then hold the session open, followed by payloads in order.
func requireSessionMessageCommitted(
	t testing.TB,
	sc serviceCluster,
	pump *transferPump,
	leader *cluster.ConsensusModule,
	sessionID int64,
	onTick func(),
	payloads ...[]byte,
) {
	t.Helper()

	floor := leader.CommitPosition()
	offerTransferSessionMessage(t, sc.set, pump, payloads[len(payloads)-1], sc.dump)
	requireServiceAwaitTick(
		t,
		sc,
		"session message committed",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfRecorderSequenceCorrupt(t, sc, -1, sessionID, nil, payloads...)

			if onTick != nil {
				onTick()
			}

			return leader.CommitPosition() > floor &&
				survivorServicesHoldSequence(sc, -1, sessionID, payloads...)
		}),
	)
}
