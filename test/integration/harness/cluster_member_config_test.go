//go:build integration

package harness

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/consensus"
)

// TestClusterMemberSet_rejectsInvalidConfig asserts StartClusterMemberSet
// rejects zero MemberCount, zero PortBase, or empty LogMulticastEndpoint.
// Start fails before any media driver or Archive process starts.
func TestClusterMemberSet_rejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     ClusterMemberSetConfig
		wantMsg string
	}{
		{
			name: "MemberCount zero",
			cfg: ClusterMemberSetConfig{
				MemberCount:          0,
				PortBase:             ClusterModuleElectPortBase,
				LogMulticastEndpoint: "239.255.12.7:1",
			},
			wantMsg: "ClusterMemberSet: MemberCount must be >= 1",
		},
		{
			name: "PortBase zero",
			cfg: ClusterMemberSetConfig{
				MemberCount:          1,
				PortBase:             0,
				LogMulticastEndpoint: "239.255.12.7:1",
			},
			wantMsg: "ClusterMemberSet: PortBase must be positive",
		},
		{
			name: "empty LogMulticastEndpoint",
			cfg: ClusterMemberSetConfig{
				MemberCount:          1,
				PortBase:             ClusterModuleElectPortBase,
				LogMulticastEndpoint: "",
			},
			wantMsg: "ClusterMemberSet: LogMulticastEndpoint must be non-empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			capture := NewCaptureFailTB(t)
			set := StartClusterMemberSet(capture, tt.cfg)
			require.True(t, capture.Failed())
			require.Nil(t, set)
			require.Contains(t, capture.lastMsg, tt.wantMsg)
		})
	}
}

// TestIntegrationElectionConfig_shortHeartbeats asserts harness election
// heartbeats stay below production defaults and match the named constants.
func TestIntegrationElectionConfig_shortHeartbeats(t *testing.T) {
	t.Parallel()

	cfg := IntegrationElectionConfig()
	require.Equal(t, IntegrationLeaderHeartbeatTimeoutNs, cfg.LeaderHeartbeatTimeoutNs)
	require.Equal(t, IntegrationLeaderHeartbeatIntervalNs, cfg.LeaderHeartbeatIntervalNs)
	require.Less(t, cfg.LeaderHeartbeatTimeoutNs, consensus.DefaultLeaderHeartbeatTimeoutNs)
	require.Less(t, cfg.LeaderHeartbeatIntervalNs, consensus.DefaultLeaderHeartbeatIntervalNs)
	require.Less(t, cfg.LeaderHeartbeatIntervalNs, cfg.LeaderHeartbeatTimeoutNs)
}

// TestMemberClusterConfig_storesIntegrationElection asserts the config builder
// sets integration election heartbeats and copies its arguments.
func TestMemberClusterConfig_storesIntegrationElection(t *testing.T) {
	t.Parallel()

	cfg := MemberClusterConfig(1, "members", "/cluster", "/aeron")
	require.Equal(t, IntegrationElectionConfig(), cfg.Election)
	require.Equal(t, int32(1), cfg.MemberID)
	require.Equal(t, "members", cfg.ClusterMembers)
	require.Equal(t, "/cluster", cfg.ClusterDir)
	require.Equal(t, "/aeron", cfg.AeronDir)
	require.Equal(t, ClusterLogChannelTemplate, cfg.LogChannel)
}

// TestClusterPortBlocks_doNotOverlap asserts every reserved PortBase the live
// suite binds stays disjoint on the unicast range. Correct memberCount per
// block. Also asserts log multicast endpoints are pairwise unique.
func TestClusterPortBlocks_doNotOverlap(t *testing.T) {
	t.Parallel()

	type block struct {
		name        string
		portBase    int
		memberCount int
		logEndpoint string
	}
	blocks := []block{
		{
			name:        "consensus-module",
			portBase:    ConsensusModulePortBase,
			memberCount: 1,
			logEndpoint: ConsensusModuleLogEndpoint,
		},
		{
			name:        "runner",
			portBase:    RunnerPortBase,
			memberCount: 1,
			logEndpoint: RunnerLogEndpoint,
		},
		{
			name:        "module-elect",
			portBase:    ClusterModuleElectPortBase,
			memberCount: 3,
			logEndpoint: ClusterModuleElectLogEndpoint,
		},
		{
			name:        "member-smoke",
			portBase:    ClusterMemberSmokePortBase,
			memberCount: 2,
			logEndpoint: ClusterMemberSmokeLogEndpoint,
		},
		{
			name:        "member-restart",
			portBase:    ClusterMemberRestartPortBase,
			memberCount: 2,
			logEndpoint: ClusterMemberRestartLogEndpoint,
		},
		{
			name:        "replay",
			portBase:    ClusterReplayPortBase,
			memberCount: 1,
			logEndpoint: ClusterReplayLogEndpoint,
		},
		{
			name:        "durable-record",
			portBase:    DurableRecordPortBase,
			memberCount: 3,
			logEndpoint: DurableRecordLogEndpoint,
		},
		{
			name:        "durable-restart",
			portBase:    DurableRestartPortBase,
			memberCount: 3,
			logEndpoint: DurableRestartLogEndpoint,
		},
		{
			name:        "durable-reelection",
			portBase:    DurableReElectionPortBase,
			memberCount: 3,
			logEndpoint: DurableReElectionLogEndpoint,
		},
		{
			name:        "durable-catchup",
			portBase:    DurableCatchupPortBase,
			memberCount: 3,
			logEndpoint: DurableCatchupLogEndpoint,
		},
		{
			name:        "durable-divergence",
			portBase:    DurableDivergencePortBase,
			memberCount: 3,
			logEndpoint: DurableDivergenceLogEndpoint,
		},
		{
			name:        "durable-concurrent-catchup",
			portBase:    DurableConcurrentCatchupPortBase,
			memberCount: 5,
			logEndpoint: DurableConcurrentCatchupLogEndpoint,
		},
		{
			name:        "truncate",
			portBase:    ClusterTruncatePortBase,
			memberCount: 3,
			logEndpoint: ClusterTruncateLogEndpoint,
		},
		{
			name:        "module-commit",
			portBase:    ClusterModuleCommitPortBase,
			memberCount: 3,
			logEndpoint: ClusterModuleCommitLogEndpoint,
		},
		{
			name:        "module-rejoin",
			portBase:    ClusterModuleRejoinPortBase,
			memberCount: 3,
			logEndpoint: ClusterModuleRejoinLogEndpoint,
		},
		{
			name:        "module-reelect",
			portBase:    ClusterModuleReElectPortBase,
			memberCount: 3,
			logEndpoint: ClusterModuleReElectLogEndpoint,
		},
		{
			name:        "module-follower-stop",
			portBase:    ClusterModuleFollowerStopPortBase,
			memberCount: 3,
			logEndpoint: ClusterModuleFollowerStopLogEndpoint,
		},
		{
			name:        "durable-rank",
			portBase:    DurableRankPortBase,
			memberCount: 3,
			logEndpoint: DurableRankLogEndpoint,
		},
		{
			name:        "durable-restart-leader",
			portBase:    DurableRestartLeaderPortBase,
			memberCount: 3,
			logEndpoint: DurableRestartLeaderLogEndpoint,
		},
	}

	// Archive-node single control binds. Treat as one-port blocks for overlap.
	// The live URIs derive from the same port constants.
	type singlePort struct {
		name string
		port int
	}
	singles := []singlePort{
		{name: "archive-smoke-0", port: ArchiveNodeSmokeControlPort0},
		{name: "archive-pair-0", port: ArchiveNodePairControlPort0},
		{name: "archive-pair-1", port: ArchiveNodePairControlPort1},
	}

	type portRange struct {
		name        string
		first, last int
		logEndpoint string
	}
	ranges := make([]portRange, len(blocks))
	for i, b := range blocks {
		first := b.portBase + 1
		last := clusterPortBlockLast(b.portBase, b.memberCount)
		require.Greater(t, last, first, b.name)
		require.NotEmpty(t, b.logEndpoint, b.name)
		ranges[i] = portRange{
			name:        b.name,
			first:       first,
			last:        last,
			logEndpoint: b.logEndpoint,
		}
	}

	for i, a := range ranges {
		for _, b := range ranges[i+1:] {
			overlap := a.first <= b.last && b.first <= a.last
			require.False(
				t,
				overlap,
				"%s [%d,%d] overlaps %s [%d,%d]",
				a.name, a.first, a.last, b.name, b.first, b.last,
			)
			require.NotEqual(
				t,
				a.logEndpoint,
				b.logEndpoint,
				"%s and %s share log endpoint %q",
				a.name, b.name, a.logEndpoint,
			)
		}
		for _, s := range singles {
			inBlock := s.port >= a.first && s.port <= a.last
			require.False(
				t,
				inBlock,
				"%s port %d falls inside %s [%d,%d]",
				s.name, s.port, a.name, a.first, a.last,
			)
		}
	}

	for i, a := range singles {
		for _, b := range singles[i+1:] {
			require.NotEqual(
				t,
				a.port,
				b.port,
				"%s and %s share port",
				a.name, b.name,
			)
		}
	}
}

// clusterPortBlockLast returns the last unicast port a member set reserves:
// the transfer port of the highest member id. MemberCount must be >= 1.
func clusterPortBlockLast(portBase, memberCount int) int {
	return cluster.CalculatePort(int32(memberCount-1), portBase, cluster.TransferPortOffset)
}

// stubMediaDriver returns a MediaDriver Stop can complete without a child process.
// stopped is required and counts cancel invocations from Stop.
func stubMediaDriver(dir string, stopped *atomic.Int32) *MediaDriver {
	exited := make(chan struct{})
	close(exited)
	return &MediaDriver{
		Dir:    dir,
		cancel: func() { stopped.Add(1) },
		exited: exited,
	}
}

// injectMemberBootstrap returns a unit bootstrap spec with default identity and
// placeholder paths. Injectors never exec the paths.
//
// Caller sets startMediaDriver and startArchiveNode, and overrides identity
// fields by name.
func injectMemberBootstrap() memberBootstrap {
	return memberBootstrap{
		memberID:      0,
		portBase:      ClusterModuleElectPortBase,
		membersString: "members",
		clusterDir:    "/cluster",
		archiveDir:    "/archive",
		aeronmdPath:   "aeronmd",
		javaPath:      "java",
		jarPath:       "jar",
	}
}

// TestBootstrapMemberInfra_driverFail returns the driver error and no handles.
func TestBootstrapMemberInfra_driverFail(t *testing.T) {
	t.Parallel()

	inject := errors.New("inject driver fail")
	bootstrap := injectMemberBootstrap()
	bootstrap.startMediaDriver = func(string, DriverEnv) (*MediaDriver, error) {
		return nil, inject
	}
	bootstrap.startArchiveNode = func(string, string, string, string, string) (*ArchiveNode, error) {
		t.Fatal("archive start must not run after driver fail")
		return nil, nil
	}
	md, node, cfg, err := bootstrapMemberInfra(bootstrap)
	require.ErrorIs(t, err, inject)
	require.Nil(t, md)
	require.Nil(t, node)
	require.Equal(t, cluster.Config{}, cfg)
}

// TestBootstrapMemberInfra_archiveFailStopsDriver asserts Archive failure stops
// the driver and returns nil handles.
func TestBootstrapMemberInfra_archiveFailStopsDriver(t *testing.T) {
	t.Parallel()

	var stopped atomic.Int32
	inject := errors.New("inject archive fail")
	bootstrap := injectMemberBootstrap()
	bootstrap.startMediaDriver = func(string, DriverEnv) (*MediaDriver, error) {
		return stubMediaDriver("/aeron", &stopped), nil
	}
	bootstrap.startArchiveNode = func(string, string, string, string, string) (*ArchiveNode, error) {
		return nil, inject
	}
	md, node, cfg, err := bootstrapMemberInfra(bootstrap)
	require.ErrorIs(t, err, inject)
	require.Nil(t, md)
	require.Nil(t, node)
	require.Equal(t, cluster.Config{}, cfg)
	require.Equal(t, int32(1), stopped.Load(), "driver Stop must run on archive fail")
}

// TestBootstrapMemberInfra_successStoresElection asserts a full bootstrap path
// stores IntegrationElectionConfig on the member config.
func TestBootstrapMemberInfra_successStoresElection(t *testing.T) {
	t.Parallel()

	const aeronDir = "/aeron"
	bootstrap := injectMemberBootstrap()
	bootstrap.memberID = 2
	bootstrap.membersString = "members-string"
	bootstrap.clusterDir = "/cluster-dir"
	bootstrap.archiveDir = "/archive-dir"
	bootstrap.startMediaDriver = func(string, DriverEnv) (*MediaDriver, error) {
		return stubMediaDriver(aeronDir, new(atomic.Int32)), nil
	}
	bootstrap.startArchiveNode = func(string, string, string, string, string) (*ArchiveNode, error) {
		return &ArchiveNode{Dir: "/archive-dir", ArchiveID: "1"}, nil
	}
	md, node, cfg, err := bootstrapMemberInfra(bootstrap)
	require.NoError(t, err)
	require.NotNil(t, md)
	require.NotNil(t, node)
	require.Equal(t, IntegrationElectionConfig(), cfg.Election)
	require.Equal(t, int32(2), cfg.MemberID)
	require.Equal(t, "members-string", cfg.ClusterMembers)
	require.Equal(t, "/cluster-dir", cfg.ClusterDir)
	require.Equal(t, aeronDir, cfg.AeronDir)
}

// TestStartClusterMemberSet_bootstrapJoinErrors asserts the joined error names
// every failed member index when parallel bootstrap fails.
func TestStartClusterMemberSet_bootstrapJoinErrors(t *testing.T) {
	t.Parallel()

	var (
		calls       atomic.Int32
		archiveHits atomic.Int32
	)
	inject0 := errors.New("inject member 0")
	inject1 := errors.New("inject member 1")

	capture := NewCaptureFailTB(t)
	set := StartClusterMemberSet(capture, ClusterMemberSetConfig{
		MemberCount:          2,
		PortBase:             ClusterModuleElectPortBase,
		LogMulticastEndpoint: ClusterModuleElectLogEndpoint,
		StartMediaDriver: func(string, DriverEnv) (*MediaDriver, error) {
			n := calls.Add(1)
			switch n {
			case 1:
				return nil, inject0
			case 2:
				return nil, inject1
			default:
				return nil, errors.New("unexpected extra driver start")
			}
		},
		StartArchiveNode: func(string, string, string, string, string) (*ArchiveNode, error) {
			archiveHits.Add(1)
			return nil, errors.New("archive must not start when drivers fail")
		},
	})
	require.True(t, capture.Failed())
	require.Nil(t, set)
	require.Contains(t, capture.lastMsg, "member 0")
	require.Contains(t, capture.lastMsg, "member 1")
	require.Contains(t, capture.lastMsg, inject0.Error())
	require.Contains(t, capture.lastMsg, inject1.Error())
	require.Zero(t, archiveHits.Load(), "archive start must not run when drivers fail")
}

// TestStartClusterMemberSet_archiveFailOneMemberStopsThatDriver asserts Archive
// failure on one of two members stops that member's driver and leaves no
// leaked handle.
func TestStartClusterMemberSet_archiveFailOneMemberStopsThatDriver(t *testing.T) {
	t.Parallel()

	const (
		dir0 = "/aeron-0"
		dir1 = "/aeron-1"
	)
	var (
		driverN  atomic.Int32
		stopped0 atomic.Int32
		stopped1 atomic.Int32
	)
	inject := errors.New("inject archive fail member 1")

	capture := NewCaptureFailTB(t)
	set := StartClusterMemberSet(capture, ClusterMemberSetConfig{
		MemberCount:          2,
		PortBase:             ClusterModuleElectPortBase,
		LogMulticastEndpoint: ClusterModuleElectLogEndpoint,
		StartMediaDriver: func(string, DriverEnv) (*MediaDriver, error) {
			n := driverN.Add(1)
			switch n {
			case 1:
				return stubMediaDriver(dir0, &stopped0), nil
			case 2:
				return stubMediaDriver(dir1, &stopped1), nil
			default:
				return nil, errors.New("unexpected extra driver start")
			}
		},
		// Key Archive fail by aeronDir identity from the driver stub above.
		StartArchiveNode: func(aeronDir, _, _, _, _ string) (*ArchiveNode, error) {
			if aeronDir == dir1 {
				return nil, inject
			}
			return &ArchiveNode{Dir: "/archive-ok", ArchiveID: "ok"}, nil
		},
	})
	require.True(t, capture.Failed())
	require.Nil(t, set)
	require.Contains(t, capture.lastMsg, "member ")
	require.Contains(t, capture.lastMsg, inject.Error())
	// Failing member stops its driver inside bootstrapMemberInfra.
	require.Equal(t, int32(1), stopped1.Load(), "failed member driver must Stop")
	// Succeeding member's driver is registered on Cleanup; not stopped yet.
	require.Equal(t, int32(0), stopped0.Load(), "success member driver stops only at Cleanup")
}
