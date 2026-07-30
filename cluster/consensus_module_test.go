package cluster_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster"
)

func TestOpenConsensusModule_configErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(cfg *cluster.Config)
		wantErr string
	}{
		{
			name: "member id absent",
			mutate: func(cfg *cluster.Config) {
				cfg.MemberID = 7
				cfg.Election.MemberID = 7
			},
			wantErr: "cluster: MemberID 7 is not present in ClusterMembers",
		},
		{
			name:    "cluster dir empty",
			mutate:  func(cfg *cluster.Config) { cfg.ClusterDir = "" },
			wantErr: "cluster: ClusterDir is required",
		},
		{
			name:    "log channel not udp",
			mutate:  func(cfg *cluster.Config) { cfg.LogChannel = "aeron:ipc" },
			wantErr: "cluster: LogChannel must be a udp channel",
		},
		{
			name:    "consensus channel not udp",
			mutate:  func(cfg *cluster.Config) { cfg.ConsensusChannel = "aeron:ipc" },
			wantErr: "cluster: ConsensusChannel must be a udp channel",
		},
		{
			name:    "negative service count",
			mutate:  func(cfg *cluster.Config) { cfg.ServiceCount = -1 },
			wantErr: "cluster: ServiceCount must not be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := recoveryConfig(t, t.TempDir())
			tt.mutate(&cfg)

			module, err := cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
				Config: cfg,
			})
			require.Nil(t, module)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestOpenConsensusModule_recoveryRefusesNilLister asserts recovery refuses a
// non-empty recording log without a lister before any transport connect.
func TestOpenConsensusModule_recoveryRefusesNilLister(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTermLog(t, dir, 1)

	module, err := cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
		Config: recoveryConfig(t, dir),
	})
	require.Nil(t, module)
	require.ErrorContains(t, err, "needs an ArchiveLister")
}

func TestConsensusModule_CloseNilReceiver(t *testing.T) {
	t.Parallel()

	var module *cluster.ConsensusModule
	require.NoError(t, module.Close())
}
