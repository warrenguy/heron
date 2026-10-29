package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func TestApplyRecoveryPlan(t *testing.T) {
	t.Parallel()

	seededCfg := func() election.ElectionConfig {
		cfg := election.ElectionTestConfig()
		cfg.LeadershipTermID = 9
		cfg.TermBaseLogPosition = 100
		cfg.LogPosition = 200
		cfg.AppendPosition = 300
		return cfg
	}

	for _, tc := range []struct {
		name string
		plan recordinglog.RecoveryPlan
	}{
		{name: "zero value plan seeds cold-start values"},
		{
			name: "true empty plan seeds cold-start values",
			plan: recordinglog.RecoveryPlan{LastLeadershipTermID: recordinglog.NullRecordingID},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := seededCfg()
			election.ApplyRecoveryPlan(&cfg, tc.plan)
			require.Equal(t, election.NullValue, cfg.LeadershipTermID)
			require.Equal(t, int64(0), cfg.TermBaseLogPosition)
			require.Equal(t, int64(0), cfg.LogPosition)
			require.Equal(t, int64(0), cfg.AppendPosition)
		})
	}

	t.Run("replicated empty seeds null term and append extent", func(t *testing.T) {
		t.Parallel()

		cfg := seededCfg()
		plan := recordinglog.RecoveryPlan{
			LastLeadershipTermID:    recordinglog.NullRecordingID,
			LastTermBaseLogPosition: 0,
			AppendedLogPosition:     50_000,
			CommittedLogPosition:    0,
			Log: &recordinglog.RecoveryPlanLog{
				Term: recordinglog.TermRef{
					RecordingID:         77,
					LeadershipTermID:    recordinglog.NullRecordingID,
					TermBaseLogPosition: 0,
					LogPosition:         0,
				},
				Extent: recordinglog.LogRecordingExtents{
					StartPosition: 0,
					StopPosition:  50_000,
				},
			},
		}
		election.ApplyRecoveryPlan(&cfg, plan)
		require.Equal(t, election.NullValue, cfg.LeadershipTermID)
		require.Equal(t, int64(0), cfg.TermBaseLogPosition)
		require.Equal(t, int64(0), cfg.LogPosition)
		require.Equal(t, int64(50_000), cfg.AppendPosition)
	})

	t.Run("term present seeds positions", func(t *testing.T) {
		t.Parallel()

		cfg := seededCfg()
		plan := recordinglog.RecoveryPlan{
			LastLeadershipTermID:    5,
			LastTermBaseLogPosition: 10,
			AppendedLogPosition:     400,
			CommittedLogPosition:    350,
			Log: &recordinglog.RecoveryPlanLog{
				Term: recordinglog.TermRef{
					RecordingID:         42,
					LeadershipTermID:    5,
					TermBaseLogPosition: 10,
					LogPosition:         350,
				},
			},
		}
		election.ApplyRecoveryPlan(&cfg, plan)
		require.Equal(t, int64(5), cfg.LeadershipTermID)
		require.Equal(t, int64(10), cfg.TermBaseLogPosition)
		require.Equal(t, int64(0), cfg.LogPosition)
		require.Equal(t, int64(400), cfg.AppendPosition)
	})

	t.Run("active recording uses committed as append floor", func(t *testing.T) {
		t.Parallel()

		cfg := seededCfg()
		plan := recordinglog.RecoveryPlan{
			LastLeadershipTermID:    3,
			LastTermBaseLogPosition: 0,
			AppendedLogPosition:     recordinglog.NullPosition,
			CommittedLogPosition:    250,
			Log: &recordinglog.RecoveryPlanLog{
				Term: recordinglog.TermRef{
					RecordingID:         11,
					LeadershipTermID:    3,
					TermBaseLogPosition: 0,
					LogPosition:         250,
				},
			},
		}
		election.ApplyRecoveryPlan(&cfg, plan)
		require.Equal(t, int64(3), cfg.LeadershipTermID)
		require.Equal(t, int64(0), cfg.LogPosition)
		require.Equal(t, int64(250), cfg.AppendPosition)
	})

	t.Run("snapshots without log still seed", func(t *testing.T) {
		t.Parallel()

		cfg := seededCfg()
		plan := recordinglog.RecoveryPlan{
			LastLeadershipTermID:    2,
			LastTermBaseLogPosition: 5,
			AppendedLogPosition:     80,
			CommittedLogPosition:    80,
			Snapshots: []recordinglog.Snapshot{{
				RecordingID:      99,
				LeadershipTermID: 2,
				LogPosition:      80,
				ServiceID:        -1,
			}},
		}
		election.ApplyRecoveryPlan(&cfg, plan)
		require.Equal(t, int64(2), cfg.LeadershipTermID)
		require.Equal(t, int64(5), cfg.TermBaseLogPosition)
		require.Equal(t, int64(80), cfg.LogPosition)
		require.Equal(t, int64(80), cfg.AppendPosition)
	})

	t.Run("snapshot log position leaves the append position", func(t *testing.T) {
		t.Parallel()

		cfg := seededCfg()
		plan := recordinglog.RecoveryPlan{
			LastLeadershipTermID:    4,
			LastTermBaseLogPosition: 0,
			AppendedLogPosition:     200,
			CommittedLogPosition:    128,
			Log: &recordinglog.RecoveryPlanLog{
				Term: recordinglog.TermRef{
					RecordingID:         42,
					LeadershipTermID:    4,
					TermBaseLogPosition: 0,
					LogPosition:         128,
				},
			},
			Snapshots: []recordinglog.Snapshot{{
				RecordingID:      55,
				LeadershipTermID: 4,
				LogPosition:      128,
				ServiceID:        -1,
			}},
		}
		election.ApplyRecoveryPlan(&cfg, plan)
		require.Equal(t, int64(4), cfg.LeadershipTermID)
		require.Equal(t, int64(128), cfg.LogPosition)
		require.Equal(t, int64(200), cfg.AppendPosition)
	})
}
