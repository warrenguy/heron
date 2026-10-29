package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func TestRecoveryStateKey(t *testing.T) {
	t.Parallel()

	t.Run("snapshot plan lists recording ids by service id", func(t *testing.T) {
		t.Parallel()

		got := recoveryStateKey(recordinglog.RecoveryPlan{
			Snapshots: []recordinglog.Snapshot{
				{
					RecordingID:      10,
					LeadershipTermID: 4,
					LogPosition:      128,
					Timestamp:        900,
					ServiceID:        recordinglog.ConsensusModuleServiceID,
				},
				{RecordingID: 31, ServiceID: 0},
				{RecordingID: 22, ServiceID: 1},
			},
		})
		require.Equal(t, codecs.RecoveryStateKeyFields{
			LeadershipTermID:     4,
			LogPosition:          128,
			Timestamp:            900,
			ClusterID:            0,
			SnapshotRecordingIDs: []int64{31, 22},
		}, got)
	})

	noSnapshotKey := codecs.RecoveryStateKeyFields{LeadershipTermID: cnc.NullValue}
	for _, tc := range []struct {
		name string
		plan recordinglog.RecoveryPlan
	}{
		{
			name: "log only",
			plan: recordinglog.RecoveryPlan{Log: &recordinglog.RecoveryPlanLog{}},
		},
		{name: "cold", plan: recordinglog.RecoveryPlan{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, noSnapshotKey, recoveryStateKey(tc.plan))
		})
	}
}
