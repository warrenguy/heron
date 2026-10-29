//go:build integration && cluster_mixed && cluster_java

package cluster_mixed_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
	"github.com/warrenguy/heron/test/integration/harness"
)

const commitTimeoutError = "election: timeout awaiting commit position"

// TestClusterMixedGoExLeaderRejoin stops a Go leader after one term. Two Java
// members then move the log forward. The Go member returns. The Go member
// copies the missed term from the Java leader. Runs only under
// HERON_CLUSTER_MIXED_EX_LEADER_REJOIN_COMPOSE.
//
// The test does not assert Java members' recordings.
//
// This test owns the compose-fixed MixedAeronDir paths, MixedClusterDirGo,
// and the PortBase 9000 block. Callers must not call t.Parallel.
func TestClusterMixedGoExLeaderRejoin(t *testing.T) {
	requireMixedCompose(
		t,
		clusterjava.MixedComposeExLeaderRejoinEnvKey,
		clusterjava.MixedComposeSkipHintExLeaderRejoin,
	)

	java := awaitJavaModuleState(t, nil, []int{0}, clusterjava.MixedStartupBudget)
	recorded := recordJavaCommitPositions(t, java)

	// Leave the readiness file unset. Java member 1 stays down until writeReady.
	cfg := buildMixedExLeaderRejoinConfig(t)
	f := openMixedGoMember(t, cfg)

	topology := f.module.Topology()
	require.NotNil(t, topology)
	require.Contains(t, topology.LogChannel(), clusterjava.MixedLogEndpoint)

	recordingBefore := awaitGoLeaderElection(t, f, java, clusterjava.MixedElectionBudget)
	goTerm := f.module.LeadershipTermID()

	streams := f.module.LogStreams()
	require.NotNil(t, streams)
	pub := streams.Publication()
	require.NotNil(t, pub)
	require.True(t, f.module.IsColdStart())
	recordingID, _ := requireGoLogRecording(t, f, pub.SessionID())

	awaitJavaCommitAdvance(t, f, java, recorded, clusterjava.MixedCommitBudget)
	floor := f.module.CommitPosition()
	require.Less(t, recordingBefore, floor)
	awaitGoRecordingReaches(
		t,
		f,
		java,
		recordingID,
		floor,
		clusterjava.MixedCommitBudget,
	)
	require.Equal(t, floor, pub.Position())

	require.NoError(t, f.module.Close())
	writeReady(t)

	member1 := awaitJavaModuleState(t, nil, []int{1}, clusterjava.MixedStartupBudget)
	java = append(java, member1...)

	javaTerm := goTerm + 1
	awaitJavaTermWithoutGo(t, java, javaTerm, floor, clusterjava.MixedJavaReelectBudget)
	writeMarkerFile(t, clusterjava.MixedStopPathMember1, "stop\n")
	awaitJavaLeaderEntersElection(t, java, clusterjava.MixedElectionBudget)

	// Warm restart on the same config. The recording id must survive the close.
	f = openMixedGoMember(t, cfg)
	require.False(t, f.module.IsColdStart())
	recordingLog := f.module.RecordingLog()
	require.NotNil(t, recordingLog)
	require.Equal(t, recordingID, recordingLog.TermRecordingID())

	entryStop := awaitGoRejoinByReplication(
		t,
		f,
		java,
		javaTerm,
		clusterjava.MixedRejoinBudget,
	)
	require.Equal(t, floor, entryStop)
	require.Equal(t, floor, termBaseLogPosition(t, f.module, javaTerm))
	require.Greater(t, termBaseLogPosition(t, f.module, f.module.LeadershipTermID()), floor)
	agent := f.module.Agent()
	require.NotNil(t, agent)
	require.Equal(t, recordingID, agent.LocalLogRecordingID())

	before := max(
		f.module.NotifiedCommitPosition(),
		harness.RecordingCounterPosition(f.module, recordingID),
	)
	induceSessionMessage(t, f)
	awaitNotifiedCommitAdvance(t, f, before, clusterjava.MixedCommitBudget)
	after := f.module.NotifiedCommitPosition()
	awaitGoRecordingReaches(
		t,
		f,
		java,
		recordingID,
		after,
		clusterjava.MixedCommitBudget,
	)
	agent = f.module.Agent()
	require.NotNil(t, agent)
	require.Equal(t, recordingID, agent.LocalLogRecordingID())

	require.NoError(t, f.module.Close())
	markMixedSmokeCompleted(t)
}

// javaElectionClosed reports whether one Java member's election state counter
// holds ElectionStateClosed.
func javaElectionClosed(c *harness.DriverCounters) bool {
	state, ok := c.Value(consensus.ClusterElectionStateCounterTypeID)
	return ok && state == int64(election.ElectionStateClosed.Code())
}

// awaitJavaTermWithoutGo waits until Java member 0 leads term and Java member
// 1 follows. Each member must close its election and commit above floor.
//
// The Go module is closed, so the dump passes nil.
func awaitJavaTermWithoutGo(
	t *testing.T,
	java []*harness.DriverCounters,
	term int64,
	floor int64,
	budget time.Duration,
) {
	t.Helper()

	start := time.Now()
	ok := harness.PollUntilBool(budget, mixedIdlePollInterval, func() bool {
		ready := true
		for i, c := range java {
			got, gotOK := c.Value(consensus.ClusterLeadershipTermIDCounterTypeID)
			if gotOK && got > term {
				t.Fatalf(
					"java term skip: javaMember=%d term=%d want=%d; "+
						"Election.publishNewLeadershipTerm reads "+
						"findTermEntry(logLeadershipTermId + 1) by exact id, "+
						"so a skipped term leaves no entry for the missed term:\n%s",
					i,
					got,
					term,
					dumpMixedState(nil, java),
				)
			}
			commit, commitOK := c.Value(consensus.ClusterCommitPositionCounterTypeID)
			if !gotOK || got != term || !javaElectionClosed(c) || !commitOK || commit <= floor {
				ready = false
			}
		}
		if !ready {
			return false
		}
		role0, role0OK := javaMemberRole(java[0])
		role1, role1OK := javaMemberRole(java[1])
		return role0OK && role0 == consensus.RoleLeader &&
			role1OK && role1 == consensus.RoleFollower
	})

	elapsed := time.Since(start)
	if !ok {
		t.Fatalf(
			"java term did not reach %d after %s (budget %s): floor=%d\n%s",
			term,
			elapsed,
			budget,
			floor,
			dumpMixedState(nil, java),
		)
	}
	t.Logf("java term without go elapsed=%s budget=%s term=%d", elapsed, budget, term)
}

// awaitJavaLeaderEntersElection waits until Java member 0 leaves
// election.ElectionStateClosed.
//
// The Go module is closed, so the dump passes nil.
func awaitJavaLeaderEntersElection(
	t *testing.T,
	java []*harness.DriverCounters,
	budget time.Duration,
) {
	t.Helper()

	closed := int64(election.ElectionStateClosed.Code())
	start := time.Now()
	var state int64
	var stateOK bool
	ok := harness.PollUntilBool(budget, mixedIdlePollInterval, func() bool {
		state, stateOK = java[0].Value(consensus.ClusterElectionStateCounterTypeID)
		return stateOK && state != closed
	})

	elapsed := time.Since(start)
	if !ok {
		t.Fatalf(
			"java member 0 did not enter an election after %s (budget %s): "+
				"stateOK=%v state=%d\n%s",
			elapsed,
			budget,
			stateOK,
			state,
			dumpMixedState(nil, java),
		)
	}
	t.Logf(
		"java member 0 entered election elapsed=%s budget=%s state=%d",
		elapsed,
		budget,
		state,
	)
}

// awaitGoRejoinByReplication ticks until the Go member finishes the election
// under the Java leader. The member must pass through FOLLOWER_LOG_REPLICATION
// without a commit timeout.
//
// Returns the recording stop position on entry to FOLLOWER_LOG_REPLICATION.
func awaitGoRejoinByReplication(
	t *testing.T,
	f *mixedMemberFixture,
	java []*harness.DriverCounters,
	javaTerm int64,
	budget time.Duration,
) int64 {
	t.Helper()

	m := f.module
	var replicated bool
	var entryStopPosition int64
	var timeoutErr error
	start := time.Now()
	ok := f.tickUntil(budget, func() bool {
		err := m.ElectionLastError()
		if timeoutErr == nil && err != nil && strings.Contains(err.Error(), commitTimeoutError) {
			timeoutErr = err
			t.Logf("go member reported %v:\n%s", err, dumpMixedState(m, java))
		}
		if !replicated && m.ElectionState() == election.ElectionStateFollowerLogReplication {
			replicated = true

			// The entry tick at most installs the replication helper. The helper
			// offers on a later tick. LogRecordingStopPosition still returns the
			// stop from before the offer.
			entryStopPosition = m.LogRecordingStopPosition()
			t.Logf(
				"go member entered FOLLOWER_LOG_REPLICATION:\n%s",
				dumpMixedState(m, java),
			)
		}
		if !m.ElectionComplete() || m.IsLeader() {
			return false
		}
		if m.LeaderMemberID() != 0 || m.LeadershipTermID() <= javaTerm {
			return false
		}
		role, roleOK := javaMemberRole(java[0])
		return roleOK && role == consensus.RoleLeader &&
			javaElectionClosed(java[0]) &&
			javaMembersAtTerm(java[:1], m.LeadershipTermID())
	})

	elapsed := time.Since(start)
	if !ok {
		t.Fatalf(
			"go member did not rejoin through replication after %s (budget %s): "+
				"replicated=%v timeoutErr=%v javaTerm=%d goTerm=%d leader=%d "+
				"complete=%v isLeader=%v\n%s",
			elapsed,
			budget,
			replicated,
			timeoutErr,
			javaTerm,
			m.LeadershipTermID(),
			m.LeaderMemberID(),
			m.ElectionComplete(),
			m.IsLeader(),
			dumpMixedState(m, java),
		)
	}
	require.True(
		t,
		replicated,
		"go member never entered FOLLOWER_LOG_REPLICATION:\n%s",
		dumpMixedState(m, java),
	)
	require.NoError(
		t,
		timeoutErr,
		"go member commit timeout:\n%s",
		dumpMixedState(m, java),
	)
	t.Logf(
		"go rejoin elapsed=%s budget=%s entryStop=%d goTerm=%d",
		elapsed,
		budget,
		entryStopPosition,
		m.LeadershipTermID(),
	)
	return entryStopPosition
}

// termBaseLogPosition reads leadershipTermID from RecordingLog. Fatals when no
// entry matches.
func termBaseLogPosition(
	t *testing.T,
	m *cluster.ConsensusModule,
	leadershipTermID int64,
) int64 {
	t.Helper()

	e := m.RecordingLog().FindTermEntry(leadershipTermID)
	require.NotNil(t, e, "no recording log entry for term %d", leadershipTermID)
	return e.TermBaseLogPosition
}
