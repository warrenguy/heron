//go:build rapid

package election_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func runElectionSafetyPBT(t *testing.T, crashWeighted bool) {
	t.Helper()
	checks := 50
	steps := 30
	if testing.Short() {
		checks = 10

		// Repeat halves steps under testing.Short(); 20 -> ~10 effective.
		steps = 20
	}

	rapidCheckSteps(t, checks, steps, func(rt *rapid.T) {
		c := newElectionRapidCluster(t, rapidClusterSize)
		defer c.closeStores()
		c.crashWeighted = crashWeighted
		c.checkInvariants(rt)
		c.runActions(rt)
	})
}

// TestElectionSafety_clusterStateMachine_rapid exercises multi-node bag-delivered
// election safety.
//
// After every action:
//   - at most one self-leader win per leadership term
//   - no live split-brain via election state
//   - candidate term non-decreasing after init load
//   - durable candidate term matches the recorded write oracle
//   - each counted ballot vote has a ledger grant
//
// The send hook checks each TRUE vote:
//   - one grant per voter per candidate term
//   - the vote names the voter as follower
//   - the vote answers the RequestVote in delivery
//   - the voter log is not ahead of the request log
//
// A self-win also checks that leadership term equals candidate term, that
// each counted vote has a grant, and quorum. A canvass above the election
// term must move a leader awaiting followers out of its state. Any other
// canvass must leave the state unchanged. Crash and restart exercise durable
// store reuse across lives.
func TestElectionSafety_clusterStateMachine_rapid(t *testing.T) {
	runElectionSafetyPBT(t, false)
}

// TestElectionSafety_crashPreservesCandidateTerm_rapid biases crash/restart and
// checks candidate-term monotonicity plus durable propose-max.
func TestElectionSafety_crashPreservesCandidateTerm_rapid(t *testing.T) {
	runElectionSafetyPBT(t, true)
}

const (
	rejoinLeaderID int32 = 0
	rejoinMemberID int32 = 1
	rejoinVoterID  int32 = 2
)

// divergentRejoin is one drawn rejoin shape. The returning member holds log
// term missedTermID-1. Its log runs divergentTail past missedBase. The leader
// and the voter hold the missed term from missedBase to missedBase plus
// missedLength.
type divergentRejoin struct {
	missedTermID   int64
	missedBase     int64
	missedLength   int64
	divergentTail  int64
	missedTermOpen bool
}

func (d divergentRejoin) missedEnd() int64 { return d.missedBase + d.missedLength }

func (d divergentRejoin) memberLogTermID() int64 { return d.missedTermID - 1 }

func drawDivergentRejoin(rt *rapid.T) divergentRejoin {
	rt.Helper()
	zeroOrLength := rapid.OneOf(rapid.Just(int64(0)), rapid.Int64Range(1, 1<<12))
	return divergentRejoin{
		missedTermID:   rapid.Int64Range(1, 32).Draw(rt, "missedTermID"),
		missedBase:     rapid.Int64Range(0, 1<<16).Draw(rt, "missedBase"),
		missedLength:   zeroOrLength.Draw(rt, "missedLength"),
		divergentTail:  zeroOrLength.Draw(rt, "divergentTail"),
		missedTermOpen: rapid.Bool().Draw(rt, "missedTermOpen"),
	}
}

// newDivergentRejoinCluster builds the leader and the voter on the missed term
// and the returning member on the term before it. The returning member starts
// down.
func newDivergentRejoinCluster(
	t *testing.T,
	d divergentRejoin,
) (*electionRapidCluster, *rapidRecordingLog) {
	t.Helper()

	c := newRapidClusterShell(t, rapidClusterSize)

	missedEnd := d.missedEnd()
	if d.missedTermOpen {
		missedEnd = election.NullPosition
	}
	for _, id := range []int32{rejoinLeaderID, rejoinVoterID} {
		log := &rapidRecordingLog{entries: []recordinglog.Entry{
			{LeadershipTermID: d.memberLogTermID(), LogPosition: d.missedBase},
			{
				LeadershipTermID:    d.missedTermID,
				TermBaseLogPosition: d.missedBase,
				LogPosition:         missedEnd,
			},
		}}
		cfg := rapidNodeConfig(id, false, d.missedTermID, d.missedEnd(), d.missedEnd())
		c.nodes[id] = c.constructNodeWithConfig(
			id,
			rapidClusterSize,
			cfg,
			withRapidRecordingLog(log),
		)
	}

	memberLog := &rapidRecordingLog{entries: []recordinglog.Entry{
		{LeadershipTermID: d.memberLogTermID(), LogPosition: election.NullPosition},
	}}
	memberCfg := rapidNodeConfig(
		rejoinMemberID,
		false,
		d.memberLogTermID(),
		d.missedBase,
		d.missedBase+d.divergentTail,
	)
	member := c.constructNodeWithConfig(
		rejoinMemberID,
		rapidClusterSize,
		memberCfg,
		withRapidRecordingLog(memberLog),
	)
	member.live = false
	c.nodes[rejoinMemberID] = member

	c.clockDeltaGen = rapidClockDeltaGen(memberCfg)
	return c, memberLog
}

func (c *electionRapidCluster) tickUntil(
	rt *rapid.T,
	n *rapidNode,
	want election.ElectionState,
	maxTicks int,
) {
	rt.Helper()
	for range maxTicks {
		if n.election.State() == want {
			return
		}
		c.tickOne(n)
	}
	require.Equal(rt, want, n.election.State(), "member=%d", n.id)
}

// electLeaderOnVoterThenCrashVoter drives the leader to LEADER_READY on the
// voter's vote. The voter crashes before it learns the new term.
func (c *electionRapidCluster) electLeaderOnVoterThenCrashVoter(rt *rapid.T) {
	rt.Helper()
	leader := c.nodes[rejoinLeaderID]
	voter := c.nodes[rejoinVoterID]
	cfg := leader.cfg

	// Leave INIT, then exchange canvass positions.
	c.tickAll()
	c.tickAll()
	c.deliverAllHolding()

	c.advanceClock(cfg.LeaderHeartbeatTimeoutNs)
	c.tickUntil(rt, leader, election.ElectionStateCandidateBallot, 4)
	c.tickOne(leader)

	// Deliver the RequestVote, then the vote.
	c.deliverAllHolding()
	require.Equal(rt, election.ElectionStateFollowerBallot, voter.election.State())
	c.deliverAllHolding()
	c.crashNode(voter)

	c.advanceClock(cfg.ElectionTimeoutNs)
	c.tickUntil(rt, leader, election.ElectionStateLeaderReady, 8)
}

// publishSettledAppendPositions stands in for the settled follower agent. Each
// live follower in CLOSED sends its AppendPosition to its leader.
func (c *electionRapidCluster) publishSettledAppendPositions() {
	for _, n := range c.nodes {
		if !n.live || n.election.State() != election.ElectionStateClosed {
			continue
		}
		leader := n.election.Leader()
		if leader == nil || leader.ID == n.id {
			continue
		}
		n.publisher.AppendPosition(
			election.PublicationTarget{MemberID: leader.ID},
			codecs.AppendPositionFields{
				LeadershipTermID: n.election.LeadershipTermID(),
				LogPosition:      n.election.AppendPosition(),
				FollowerMemberID: n.id,
			},
		)
	}
}

func (c *electionRapidCluster) runRejoinPrefix(rt *rapid.T) {
	rt.Helper()
	steps := rapid.IntRange(0, 64).Draw(rt, "prefixSteps")
	for range steps {
		switch rapid.IntRange(0, 4).Draw(rt, "prefixAction") {
		case 0:
			c.tickAll()
		case 1:
			id := rapid.IntRange(0, len(c.nodes)-1).Draw(rt, "prefixTickNode")
			c.tickOne(c.nodes[id])
		case 2:
			c.deliverDrawn(rt, "prefixBagIdx")
		case 3:
			c.deliverAllHolding()
		default:
			c.advanceClock(c.drawClockDelta(rt))
		}
	}
}

// TestElection_divergentExLeaderRejoin_rapid checks a member that returns on
// an older log term to a leader waiting in LEADER_READY.
//
// The leader wins on one vote. The voter crashes before it joins. The member
// returns with a drawn divergent tail past the missed term's base. A drawn
// prefix of ticks, deliveries and clock steps runs, then a drain with no
// partition, crash or restart.
func TestElection_divergentExLeaderRejoin_rapid(t *testing.T) {
	checks := 100
	if testing.Short() {
		checks = 20
	}

	rapidCheck(t, checks, func(rt *rapid.T) {
		d := drawDivergentRejoin(rt)
		c, memberLog := newDivergentRejoinCluster(t, d)
		defer c.closeStores()

		leader := c.nodes[rejoinLeaderID]
		member := c.nodes[rejoinMemberID]
		c.electLeaderOnVoterThenCrashVoter(rt)
		leaderTermID := leader.election.LeadershipTermID()
		require.Greater(rt, leaderTermID, d.missedTermID)

		member.live = true
		c.runRejoinPrefix(rt)

		// The member rejoins when the missed term holds no log. Otherwise the
		// member stops at the replication request: the host builds no
		// LogReplication.
		rejoins := d.missedLength == 0
		done := func() bool {
			if rejoins {
				return leader.election.State() == election.ElectionStateClosed &&
					member.election.State() == election.ElectionStateClosed
			}
			return len(member.host.newLogReplicationCalls) > 0
		}

		// 400 status intervals span four leader heartbeat timeouts.
		for range 400 {
			if done() {
				break
			}
			c.publishSettledAppendPositions()
			c.tickAll()
			c.deliverAllHolding()
			c.advanceClock(member.cfg.ElectionStatusIntervalNs)
			c.checkInvariants(rt)
		}
		c.checkInvariants(rt)
		require.True(rt, done(), "member state %v", member.election.State())

		if d.divergentTail > 0 {
			require.Equal(rt, []truncateCall{{
				leadershipTermID: d.memberLogTermID(),
				logPosition:      d.missedBase,
			}}, member.host.truncateCalls)
			require.Equal(
				rt,
				d.missedBase,
				memberLog.FindTermEntry(d.memberLogTermID()).LogPosition,
			)
		} else {
			require.Empty(rt, member.host.truncateCalls)
		}
		require.Equal(rt, d.missedBase, member.election.AppendPosition())

		if rejoins {
			require.Equal(rt, leaderTermID, leader.election.LeadershipTermID())
			require.Equal(rt, leaderTermID, member.election.LeadershipTermID())
			return
		}
		for _, call := range member.host.newLogReplicationCalls {
			require.Equal(rt, d.missedEnd(), call.stopPosition)
		}
	})
}

// TestElectionRapidCluster_crashRestartHelpers covers crash/restart fixed paths
// that the multi-step bag-delivered sequence only hits indirectly.
func TestElectionRapidCluster_crashRestartHelpers(t *testing.T) {
	c := newElectionRapidCluster(t, rapidClusterSize)
	n := c.nodes[0]
	store := n.nodeState
	require.NotNil(t, store)
	require.True(t, n.live)
	require.True(t, n.initLoaded)

	// Seed a durable propose so proposeCalls and store leave NullValue.
	const proposedID int64 = 42
	got, err := store.ProposeMaxCandidateTermID(proposedID, rapidSeedLogPos, 1)
	require.NoError(t, err)
	require.Equal(t, proposedID, got)
	require.Equal(t, proposedID, store.expectedCandidateTerm())
	ct, err := store.CandidateTerm()
	require.NoError(t, err)
	require.Equal(t, proposedID, ct.CandidateTermID())

	// Diverge log leadership term from leadership term before crash.
	const divergedLogTerm int64 = rapidSeedTerm + 7
	election.SetLogLeadershipTermIDForTest(n.election, divergedLogTerm)
	require.Equal(t, divergedLogTerm, election.LogLeadershipTermIDForTest(n.election))
	require.NotEqual(t, divergedLogTerm, n.election.LeadershipTermID())

	proposeCallsBefore := len(store.proposeCalls)
	c.crashNode(n)
	require.False(t, n.live)
	require.False(t, n.initLoaded)
	require.Nil(t, n.election)
	require.Same(t, store, n.nodeState)
	require.Len(t, n.nodeState.proposeCalls, proposeCallsBefore)
	require.NotNil(t, n.crashSnap)
	require.Equal(t, divergedLogTerm, n.crashSnap.logLeadershipTerm)

	// Store remains readable after crash (not closed).
	ct, err = n.nodeState.CandidateTerm()
	require.NoError(t, err)
	require.Equal(t, proposedID, ct.CandidateTermID())

	c.restartNode(t, n)
	require.True(t, n.live)
	require.True(t, n.initLoaded)
	require.NotNil(t, n.election)
	require.Same(t, store, n.nodeState)
	require.False(t, n.cfg.IsNodeStartup)
	require.NotNil(t, n.host)
	require.Equal(t, consensus.RoleFollower, n.host.Role())
	require.Equal(t, divergedLogTerm, election.LogLeadershipTermIDForTest(n.election))

	// Durable load on init: candidate term is at least proposed max.
	require.GreaterOrEqual(t, election.CandidateTermIDForTest(n.election), proposedID)
	require.Equal(t, election.CandidateTermIDForTest(n.election), n.maxCandidateSeen)

	ct, err = n.nodeState.CandidateTerm()
	require.NoError(t, err)
	require.Equal(t, n.nodeState.expectedCandidateTerm(), ct.CandidateTermID())
}

func TestRecordingNodeState_expectedCandidateTerm(t *testing.T) {
	// Empty recording: oracle is initial file NullValue (no writes yet).
	require.Equal(t, election.NullValue, (&recordingNodeState{}).expectedCandidateTerm())

	// Real store only so the mutators are callable; no multi-node cluster.
	store := openNodeStateInDir(t, t.TempDir(), true, recordinglog.FileSyncNone)
	ns := &recordingNodeState{store: store}
	require.Equal(t, election.NullValue, ns.expectedCandidateTerm())
	ct, err := ns.CandidateTerm()
	require.NoError(t, err)
	require.Equal(t, election.NullValue, ct.CandidateTermID())

	// ProposeMax is non-decreasing: lower id leaves durable max unchanged.
	// Sequence 10, 3, 15 -> max 15.
	_, err = ns.ProposeMaxCandidateTermID(10, 1, 1)
	require.NoError(t, err)
	_, err = ns.ProposeMaxCandidateTermID(3, 2, 2)
	require.NoError(t, err)
	_, err = ns.ProposeMaxCandidateTermID(15, 3, 3)
	require.NoError(t, err)
	require.Equal(t, int64(15), ns.expectedCandidateTerm())
	ct, err = ns.CandidateTerm()
	require.NoError(t, err)
	require.Equal(t, int64(15), ct.CandidateTermID())

	// Update sets unconditionally, lowering included; a later propose maxes
	// against the updated value.
	require.NoError(t, ns.UpdateCandidateTermID(7, 4, 4))
	require.Equal(t, int64(7), ns.expectedCandidateTerm())
	_, err = ns.ProposeMaxCandidateTermID(9, 5, 5)
	require.NoError(t, err)
	require.Equal(t, int64(9), ns.expectedCandidateTerm())
	ct, err = ns.CandidateTerm()
	require.NoError(t, err)
	require.Equal(t, int64(9), ct.CandidateTermID())
}

func TestElectionRapidCluster_deliverHoldUntilRestart(t *testing.T) {
	c := newElectionRapidCluster(t, rapidClusterSize)
	target := c.nodes[1]
	from := c.nodes[0]

	// Status interval canvass publishes peer-directed messages into the bag.
	c.advanceClock(from.cfg.ElectionStatusIntervalNs)
	from.election.DoWork(from.clock.nowNs)
	require.NotEmpty(t, c.bag)

	var held pendingMsg
	found := false
	for _, p := range c.bag {
		if p.to() == target.id && len(p.msg.Wire) > 0 {
			held = p
			found = true
			break
		}
	}
	require.True(t, found, "expected bag message to target after canvass publish")

	c.crashNode(target)
	require.False(t, c.tryDeliver(held), "dead target must hold message")

	// Shared deliverAllHolding path: dead-target msgs stay in bag.
	heldCount := c.deliverAllHolding()
	require.Positive(t, heldCount)
	require.NotEmpty(t, c.bag)

	c.restartNode(t, target)
	require.True(t, target.live)

	// All edges open and all nodes live again: nothing may stay held, so the
	// messages held for the crashed target must have delivered.
	require.Zero(t, c.deliverAllHolding(), "held messages deliver after restart")
}

func TestElectionRapidCluster_distinctMemberArrays(t *testing.T) {
	c := newElectionRapidCluster(t, rapidClusterSize)
	n := len(c.nodes)

	// Distinct Member backing arrays: peer slots must not alias across nodes.
	membersByNode := make([][]*election.Member, n)
	for i := range n {
		membersByNode[i] = election.MembersForTest(c.nodes[i].election)
		require.Len(t, membersByNode[i], n)
	}
	for i := range n {
		for j := i + 1; j < n; j++ {
			for k := range n {
				require.NotSame(t, membersByNode[i][k], membersByNode[j][k])
			}
		}
	}
}

func TestElectionRapidCluster_handleErrorReopensCandidateTermWindow(t *testing.T) {
	c := newElectionRapidCluster(t, rapidClusterSize)
	n := c.nodes[2]
	require.True(t, n.initLoaded)
	before := n.maxCandidateSeen
	require.GreaterOrEqual(t, election.CandidateTermIDForTest(n.election), before)

	// Inflate maxCandidateSeen without ProposeMax. HandleError -> Init reloads
	// durableCandidateTermID = max(store, leadershipTerm), which can sit below
	// maxCandidateSeen. Observer must clear initLoaded on Init.
	raised := before + 100
	n.maxCandidateSeen = raised
	forceErr := errors.New("force re-init")

	// Fixture seeds commit equal to logPosition. Assert log sync with a
	// distinct host commit.
	syncCommit := n.election.LogPosition() - 20
	n.host.commitPosition = syncCommit
	appendBefore := n.election.AppendPosition()

	n.election.HandleError(n.clock.nowNs, forceErr)
	require.Equal(t, election.ElectionStateInit, n.election.State())
	require.False(t, n.initLoaded, "re-entry to Init must reopen candidate-term window")
	require.Equal(t, forceErr, n.election.LastError())
	require.Equal(t, syncCommit, n.election.LogPosition())
	require.Equal(t, appendBefore, n.election.AppendPosition())

	n.election.DoWork(n.clock.nowNs)
	require.NotEqual(t, election.ElectionStateInit, n.election.State())
	require.True(t, n.initLoaded)

	// Re-baseline is durable-backed, not the inflated high water mark.
	require.Equal(t, election.CandidateTermIDForTest(n.election), n.maxCandidateSeen)
	require.Less(t, n.maxCandidateSeen, raised)
}

func TestElectionRapid_handleErrorSyncsLogPosition(t *testing.T) {
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		cfg, deps, _, _, nodeState := testElectionDeps(t)

		// Close the per-iteration store so open files do not accumulate across rapid
		// checks. The outer t.Cleanup close is then an idempotent no-op.
		defer func() { require.NoError(rt, nodeState.store.Close()) }()

		// Draw commit past append and the initial log position. HandleError assigns
		// Host.CommitPosition raw. The truncate path owns any clamp against append.
		commit := rapid.Int64Range(0, 2*cfg.AppendPosition).Draw(rt, "commit")
		host := &recordingElectionHost{commitPosition: commit}
		deps.Host = host
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)
		appendBefore := e.AppendPosition()

		var err error
		if rapid.Bool().Draw(rt, "nonNilErr") {
			err = errors.New("rapid force")
		}
		e.HandleError(5_000, err)

		require.Equal(rt, election.ElectionStateInit, e.State())
		require.Equal(rt, commit, e.LogPosition())
		require.Equal(rt, appendBefore, e.AppendPosition())
		require.Equal(rt, err, e.LastError())
	})
}

// ballotOnNodeZero ticks node 0 alone to CANDIDATE_BALLOT after one canvass
// exchange. Returns the candidate term.
func ballotOnNodeZero(t *testing.T, c *electionRapidCluster) int64 {
	t.Helper()
	candidate := c.nodes[0]
	c.tickAll()
	c.deliverAllHolding()
	for range 8 {
		if candidate.election.State() == election.ElectionStateCandidateBallot {
			break
		}
		c.tickOne(candidate)
	}
	require.Equal(t, election.ElectionStateCandidateBallot, candidate.election.State())
	return election.CandidateTermIDForTest(candidate.election)
}

// grantVote sends a TRUE vote from voter to candidateID at termID.
func grantVote(voter *rapidNode, candidateID, followerID int32, termID int64) {
	voter.publisher.PlaceVote(
		election.PublicationTarget{MemberID: candidateID},
		codecs.VoteFields{
			CandidateTermID:     termID,
			LogLeadershipTermID: election.LogLeadershipTermIDForTest(voter.election),
			LogPosition:         voter.election.AppendPosition(),
			CandidateMemberID:   candidateID,
			FollowerMemberID:    followerID,
			Vote:                clusterwire.BooleanType_TRUE,
		},
	)
}

// TestElectionRapidCluster_voteChecks plants one vote fault per case and
// expects the error text of the check that the fault trips.
func TestElectionRapidCluster_voteChecks(t *testing.T) {
	tests := []struct {
		name  string
		plant func(t *testing.T, c *electionRapidCluster, termID int64)
		want  string
	}{
		{
			name: "second grant",
			plant: func(_ *testing.T, c *electionRapidCluster, termID int64) {
				grantVote(c.nodes[0], 1, 0, termID)
			},
			want: "granted term",
		},
		{
			name: "wrong follower",
			plant: func(_ *testing.T, c *electionRapidCluster, termID int64) {
				grantVote(c.nodes[1], 0, 2, termID+10)
			},
			want: "names follower",
		},
		{
			name: "no request",
			plant: func(_ *testing.T, c *electionRapidCluster, termID int64) {
				grantVote(c.nodes[1], 0, 1, termID+10)
			},
			want: "answers no RequestVote",
		},
		{
			name: "ahead voter",
			plant: func(t *testing.T, c *electionRapidCluster, termID int64) {
				voter := c.nodes[1].election
				position := voter.AppendPosition()
				scratch := &election.SbePublisher{}
				require.True(t, scratch.RequestVote(
					election.PublicationTarget{MemberID: 1},
					codecs.RequestVoteFields{
						LogLeadershipTermID: election.LogLeadershipTermIDForTest(voter),
						LogPosition:         position,
						CandidateTermID:     termID + 10,
						CandidateMemberID:   0,
					},
				))
				c.inbound = &pendingMsg{from: 0, msg: scratch.Sent[0]}
				election.SetAppendPositionForTest(voter, position+1)
				grantVote(c.nodes[1], 0, 1, termID+10)
			},
			want: "ahead of candidate",
		},
		{
			name: "unbacked scan",
			plant: func(_ *testing.T, c *electionRapidCluster, termID int64) {
				member := election.MemberForTest(c.nodes[0].election, 1)
				member.Vote = new(true)
				member.CandidateTermID = termID
				c.checkVoteCounts()
			},
			want: "without a grant",
		},
		{
			// No checkVoteCounts call: the first failure sticks, so a scan
			// hit would mask a missing edge check.
			name: "unbacked edge",
			plant: func(t *testing.T, c *electionRapidCluster, termID int64) {
				candidate := c.nodes[0]
				member := election.MemberForTest(candidate.election, 1)
				member.Vote = new(true)
				member.CandidateTermID = termID
				c.advanceClock(candidate.cfg.ElectionTimeoutNs)
				c.tickOne(candidate)
				require.Equal(
					t,
					election.ElectionStateLeaderLogReplication,
					candidate.election.State(),
				)
			},
			want: "without a grant",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newElectionRapidCluster(t, rapidClusterSize)
			termID := ballotOnNodeZero(t, c)
			require.Empty(t, c.edgeFail)

			tt.plant(t, c, termID)
			require.Contains(t, c.edgeFail, tt.want)
		})
	}
}
