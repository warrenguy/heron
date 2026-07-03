//go:build rapid

package recordinglog_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/recordinglog"
)

func isRecoveryAnchorError(err error) bool {
	return errors.Is(err, recordinglog.ErrSnapshotMissingForService) ||
		errors.Is(err, recordinglog.ErrSnapshotServiceIDOutOfRange)
}

func entryMatchesPlanSnapshot(
	e recordinglog.Entry,
	snap recordinglog.Snapshot,
	cm recordinglog.Snapshot,
) bool {
	inGroup := snap.ServiceID == recordinglog.ConsensusModuleServiceID &&
		recordinglog.IsValidConsensusModuleSnapshot(e)
	if snap.ServiceID != recordinglog.ConsensusModuleServiceID {
		inGroup = recordinglog.MatchesConsensusModuleSnapshotGroup(e, cm)
	}
	return inGroup &&
		e.RecordingID == snap.RecordingID &&
		e.ServiceID == snap.ServiceID &&
		e.LogPosition == snap.LogPosition
}

func requireSnapshotSourcesInEntries(
	rt *rapid.T,
	entries []recordinglog.Entry,
	snapshots []recordinglog.Snapshot,
) {
	rt.Helper()
	cm := snapshots[0]
	for _, snap := range snapshots {
		found := slices.ContainsFunc(entries, func(e recordinglog.Entry) bool {
			return entryMatchesPlanSnapshot(e, snap, cm)
		})
		require.True(rt, found)
	}
}

// TestPlanRecovery_invariants_rapid checks recovery-plan properties on random
// comparator-sorted entry caches.
func TestPlanRecovery_invariants_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 10).Draw(rt, "n")
		serviceCount := rapid.IntRange(0, 3).Draw(rt, "serviceCount")
		entries := recordinglog.DrawIndexedEntries(rt, n, "")
		slices.SortStableFunc(entries, recordinglog.CompareEntries)
		archive := fakeArchiveForEntries(entries)
		log, _ := openWithEntries(t, entries...)
		defer func() { require.NoError(rt, log.Close()) }()
		cfg := recoveryPlanConfig(serviceCount)

		plan, err := log.PlanRecovery(archive, cfg)
		if err != nil {
			require.True(rt, isRecoveryAnchorError(err))
			return
		}

		planAgain, err := log.PlanRecovery(archive, cfg)
		require.NoError(rt, err)
		require.Equal(rt, plan, planAgain)

		require.LessOrEqual(rt, len(plan.Snapshots), serviceCount+1)
		if len(plan.Snapshots) > 0 {
			require.Equal(rt, recordinglog.ConsensusModuleServiceID, plan.Snapshots[0].ServiceID)
			requireSnapshotSourcesInEntries(rt, entries, plan.Snapshots)
		}

		if plan.Log != nil {
			ext := plan.Log.Extent
			if ext.StopPosition != recordinglog.NullPosition {
				require.GreaterOrEqual(rt, ext.StopPosition, ext.StartPosition)
			}
			uncommitted := plan.Log.Term.LogPosition == recordinglog.NullPosition
			if uncommitted && len(plan.Snapshots) > 0 {
				require.Equal(rt, plan.Snapshots[0].LogPosition, plan.CommittedLogPosition)
				require.Equal(rt, plan.Log.Extent.StopPosition, plan.AppendedLogPosition)
			}
		}
	})
}
