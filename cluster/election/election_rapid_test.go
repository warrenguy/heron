//go:build rapid

package election_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

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
// After every action: at most one self-leader win per leadership term, no live
// split-brain via election state, candidate term non-decreasing after init load,
// and durable candidate term matches the recorded write oracle. Self-win
// transitions also check leadership term equals candidate term and quorum.
// Crash and restart exercise durable store reuse across lives.
func TestElectionSafety_clusterStateMachine_rapid(t *testing.T) {
	runElectionSafetyPBT(t, false)
}

// TestElectionSafety_crashPreservesCandidateTerm_rapid biases crash/restart and
// checks candidate-term monotonicity plus durable propose-max.
func TestElectionSafety_crashPreservesCandidateTerm_rapid(t *testing.T) {
	runElectionSafetyPBT(t, true)
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
	n.election.HandleError(n.clock.nowNs, errors.New("force re-init"))
	require.Equal(t, election.ElectionStateInit, n.election.State())
	require.False(t, n.initLoaded, "re-entry to Init must reopen candidate-term window")

	n.election.DoWork(n.clock.nowNs)
	require.NotEqual(t, election.ElectionStateInit, n.election.State())
	require.True(t, n.initLoaded)

	// Re-baseline is durable-backed, not the inflated high water mark.
	require.Equal(t, election.CandidateTermIDForTest(n.election), n.maxCandidateSeen)
	require.Less(t, n.maxCandidateSeen, raised)
}
