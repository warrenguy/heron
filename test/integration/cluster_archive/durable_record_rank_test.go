//go:build integration && archive_live

package cluster_archive_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/test/integration/harness"
)

// snapshotRecordingCounters returns each member's type-100 counter value.
// Fails when any counter is missing.
func snapshotRecordingCounters(t *testing.T, set *harness.ClusterMemberSet) []int64 {
	t.Helper()

	modules := set.Modules()
	out := make([]int64, len(modules))
	for i, m := range modules {
		recordingID := m.Agent().LocalLogRecordingID()
		pos := harness.RecordingCounterPosition(m, recordingID)
		require.NotEqual(
			t,
			int64(-1),
			pos,
			"member %d type-100 counter missing for recording %d:\n%s",
			m.ThisMemberID(),
			recordingID,
			set.State(),
		)
		out[i] = pos
	}
	return out
}

// TestDurableCluster_recordsOnEveryMember asserts every durable member records
// the leader log and advances its recording-position counter after an append.
func TestDurableCluster_recordsOnEveryMember(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set := openDurableCluster(
		t,
		harness.DurableRecordPortBase,
		harness.DurableRecordLogEndpoint,
	)

	leader := requireSingleLeader(t, set, "")

	requireMembersRecordLeaderLog(t, set, leader)

	for _, m := range set.Modules() {
		require.Positive(
			t,
			m.ArchiveClient().ControlSessionID(),
			"member %d archive control session:\n%s",
			m.ThisMemberID(),
			set.State(),
		)
	}

	snapshots := snapshotRecordingCounters(t, set)

	mustAppend(t, set, leader, "durable-record")

	requireAwaitTick(t, set, "recording counters advance", durableWorkBound, func() bool {
		for i, m := range set.Modules() {
			recordingID := m.Agent().LocalLogRecordingID()
			if harness.RecordingCounterPosition(m, recordingID) <= snapshots[i] {
				return false
			}
		}
		return true
	})
}

// TestDurableCluster_ranksOnTheRecordingPositionCounter asserts every durable
// member ranks append position from its type-100 recording-position counter.
func TestDurableCluster_ranksOnTheRecordingPositionCounter(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set := openDurableCluster(
		t,
		harness.DurableRankPortBase,
		harness.DurableRankLogEndpoint,
	)

	for _, m := range set.Modules() {
		_, ok := m.Agent().AppendSource().(*consensus.RecordingPosAppendPosition)
		require.True(
			t,
			ok,
			"member %d open must install recording-derived append high-water:\n%s",
			m.ThisMemberID(),
			set.State(),
		)
	}

	leader := requireSingleLeader(t, set, "")

	snapshots := snapshotRecordingCounters(t, set)

	appendPos := mustAppend(t, set, leader, "durable-rank")

	// Read AppendPosition and the type-100 counter in one predicate body with
	// no duty cycle between the two reads so both sample the same instant.
	requireAwaitTick(t, set, "append rank equals recording counter", durableWorkBound, func() bool {
		for i, m := range set.Modules() {
			if m.Agent().AppendRankFailure() != nil {
				return false
			}
			src := m.Agent().AppendSource()
			if src == nil {
				return false
			}
			position, ready := src.AppendPosition()
			recordingID := m.Agent().LocalLogRecordingID()
			counter := harness.RecordingCounterPosition(m, recordingID)
			if !ready || position != counter || position <= snapshots[i] {
				return false
			}
		}
		return true
	})

	for _, m := range set.Modules() {
		require.NoError(
			t,
			m.Agent().AppendRankFailure(),
			"member %d append rank failure:\n%s",
			m.ThisMemberID(),
			set.State(),
		)
	}

	requireAwaitTick(t, set, "leader commit reaches append", durableWorkBound, func() bool {
		return leader.CommitPosition() >= appendPos
	})
}
