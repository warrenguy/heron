//go:build integration && archive_live

package cluster_archive_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/harness"
)

// openDurableReElectionCluster starts three members on the durable-reelection
// port block.
func openDurableReElectionCluster(t *testing.T) *harness.ClusterMemberSet {
	t.Helper()
	return openDurableCluster(
		t,
		harness.DurableReElectionPortBase,
		harness.DurableReElectionLogEndpoint,
	)
}

// TestDurableCluster_reElectsAndRestartsRecording asserts a leader that loses
// follower quorum stops its recording and re-enters election. After one
// follower restarts, the survivor records again under the same recording id.
//
// ElectionCount and LastElectionTrigger detect the re-election. ElectionState
// and ElectionComplete report a transient a poll can miss. ElectionCount is
// monotonic. LastElectionTrigger is latched.
//
// No t.Parallel: the set owns fixed UDP ports.
func TestDurableCluster_reElectsAndRestartsRecording(t *testing.T) {
	harness.RequireArchiveRuntime(t)

	set := openDurableReElectionCluster(t)

	leader := requireSingleLeader(t, set, "")
	leaderIndex := int(leader.ThisMemberID())
	preTerm := leader.LeadershipTermID()
	agent := leader.Agent()
	preRecordingID := agent.LocalLogRecordingID()
	require.NotEqual(
		t,
		int64(-1),
		preRecordingID,
		"leader LocalLogRecordingID (term=%d):\n%s",
		preTerm,
		set.State(),
	)
	preElectionCount := agent.ElectionCount()
	require.Positive(
		t,
		preElectionCount,
		"leader ElectionCount before stop:\n%s",
		set.State(),
	)

	// Preconditions: recording active, rank path clean.
	require.True(
		t,
		agent.IsRecordingActive(),
		"IsRecordingActive before stop:\n%s",
		set.State(),
	)
	require.NoError(
		t,
		agent.AppendRankFailure(),
		"AppendRankFailure before stop:\n%s",
		set.State(),
	)

	// Type-100 counter snapshot before the quorum-loss transition.
	// Lower bound only. Residual ticks before inactive can advance past this.
	preCounter := harness.RecordingCounterPosition(leader, preRecordingID)
	require.NotEqual(
		t,
		int64(-1),
		preCounter,
		"type-100 counter missing for recording %d:\n%s",
		preRecordingID,
		set.State(),
	)

	// Role has not flipped yet after the second follower stop.
	stopFollowers(t, set, leaderIndex)
	leaders := set.Leaders()
	require.Len(t, leaders, 1, "leader role still held after follower stops:\n%s", set.State())
	require.Equal(
		t,
		int32(leaderIndex),
		leaders[0].ThisMemberID(),
		"original leader still reported after follower stops:\n%s",
		set.State(),
	)

	// Wait until recording stops and ElectionCount advances. Require the retained
	// id and nil rank failure on every pass. A clear or fail-closed mid-wait fails
	// at that pass. Failure context is built only on the failing branch: the
	// predicate runs per tick.
	requireAwaitTick(t, set, "recording stops after lost quorum", durableReElectBound, func() bool {
		m := set.Module(leaderIndex)
		if m == nil {
			return false
		}
		agent := m.Agent()
		if err := agent.AppendRankFailure(); err != nil {
			require.NoError(t, err, "AppendRankFailure during transition:\n%s", set.State())
		}
		if id := agent.LocalLogRecordingID(); id != preRecordingID {
			require.Equal(
				t,
				preRecordingID,
				id,
				"LocalLogRecordingID during transition:\n%s",
				set.State(),
			)
		}
		return !agent.IsRecordingActive() &&
			agent.ElectionCount() > preElectionCount
	})

	survivor := requireQuorumLossFollower(t, set, leaderIndex)

	// Post-stop type-100 freeze. Sample after inactive so the restart wait
	// asserts new writes after re-elect, not residual pre-inactive ticks. Missing
	// counter (deallocate) treats freeze as preCounter lower bound.
	inactiveBaseline := preCounter
	if pos := harness.RecordingCounterPosition(survivor, preRecordingID); pos != -1 {
		inactiveBaseline = pos
	}
	require.GreaterOrEqual(
		t,
		inactiveBaseline,
		preCounter,
		"type-100 freeze below pre-transition counter:\n%s",
		set.State(),
	)

	// Lone member of three cannot complete an election. Restart one follower
	// so a quorum can form and recording can start again.
	restartIndex := (leaderIndex + 1) % len(set.Modules())
	require.NoError(t, set.RestartMember(restartIndex))
	require.NotNil(
		t,
		set.Module(restartIndex),
		"RestartMember left slot empty:\n%s",
		set.State(),
	)

	requireAwaitTick(
		t, set, "recording restarts after quorum returns", durableWorkBound,
		func() bool {
			if len(set.Leaders()) != 1 {
				return false
			}
			m := set.Module(leaderIndex)
			if m == nil {
				return false
			}
			agent := m.Agent()
			if id := agent.LocalLogRecordingID(); id != preRecordingID {
				require.Equal(
					t,
					preRecordingID,
					id,
					"LocalLogRecordingID during restart wait:\n%s",
					set.State(),
				)
			}
			if !agent.IsRecordingActive() {
				return false
			}
			return harness.RecordingCounterPosition(m, preRecordingID) > inactiveBaseline
		},
	)

	// Recording id retained across the whole sequence.
	survivor = set.Module(leaderIndex)
	require.NotNil(t, survivor, "survivor after recording restart:\n%s", set.State())
	survivorAgent := survivor.Agent()
	require.Equal(
		t,
		preRecordingID,
		survivorAgent.LocalLogRecordingID(),
		"LocalLogRecordingID after recording restart:\n%s",
		set.State(),
	)
	require.True(
		t,
		survivorAgent.IsRecordingActive(),
		"IsRecordingActive after recording restart:\n%s",
		set.State(),
	)
}
