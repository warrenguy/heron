//go:build integration

package harness

import (
	"errors"
	"strconv"
	"sync"
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
// suite binds stays disjoint on the unicast range. Each block uses the correct
// memberCount. Log multicast endpoints must be pairwise unique. Unicast-only
// reservations carry no shared endpoint and skip that compare.
func TestClusterPortBlocks_doNotOverlap(t *testing.T) {
	t.Parallel()

	type portRange struct {
		name        string
		first, last int
		logEndpoint string
	}
	multicast := multicastPortLedger()
	unicast := unicastPortLedger()
	singles := singlePortLedger()
	for _, b := range multicast {
		require.NotEmpty(t, b.logEndpoint, b.name)
	}
	for _, b := range unicast {
		require.Empty(t, b.logEndpoint, b.name)
	}
	ranges := make([]portRange, 0, len(multicast)+len(unicast))
	for _, b := range append(multicast, unicast...) {
		first := b.portBase + 1
		last := portBlockLast(b.portBase, b.memberCount)
		require.Greater(t, last, first, b.name)
		require.Less(t, last, linuxEphemeralPortFloor, b.name)
		ranges = append(ranges, portRange{
			name:        b.name,
			first:       first,
			last:        last,
			logEndpoint: b.logEndpoint,
		})
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
			if a.logEndpoint != "" && b.logEndpoint != "" {
				require.NotEqual(
					t,
					a.logEndpoint,
					b.logEndpoint,
					"%s and %s share log endpoint %q",
					a.name, b.name, a.logEndpoint,
				)
			}
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
		require.Less(t, a.port, linuxEphemeralPortFloor, a.name)
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

func TestClientEgressPort_sitsInsideWideMemberBlocks(t *testing.T) {
	t.Parallel()

	for _, b := range append(multicastPortLedger(), unicastPortLedger()...) {
		if b.memberCount < 2 {
			continue
		}
		egress := ClientEgressPort(b.portBase)
		require.Equal(t, b.portBase+ClientEgressOffset, egress, b.name)
		require.True(t, portInBlock(egress, b.portBase, b.memberCount), b.name)
		require.True(t, isReservedUDPPort(egress), b.name)
	}
}

func TestIsReservedUDPPort_matchesLedger(t *testing.T) {
	t.Parallel()

	require.True(t, isReservedUDPPort(ClusterServiceTransferPortBase+cluster.ClientFacingPortOffset))
	require.True(t, isReservedUDPPort(ClientEgressPort(ClusterSessionTransferPortBase)))
	require.True(t, isReservedUDPPort(ArchiveNodeSmokeControlPort0))
	require.False(t, isReservedUDPPort(linuxEphemeralPortFloor))
	require.False(t, isReservedUDPPort(55080))
}

func TestFreeUDPPorts_skipReservedRanges(t *testing.T) {
	t.Parallel()

	const n = 8
	first := FreeUDPPorts(t, n)
	require.Len(t, first, n)
	// t still holds the first claim, so the second call returns other ports.
	ports := append(first, FreeUDPPorts(t, n)...)
	floor, ceil, err := liveEphemeralPortRange()
	require.NoError(t, err)
	seen := make(map[int]struct{}, len(ports))
	for _, port := range ports {
		require.False(t, isReservedUDPPort(port), port)
		require.True(t, port < floor || port > ceil, port)
		require.NotContains(t, seen, port)
		seen[port] = struct{}{}
	}
}

func TestFreeUDPPorts_claimExcludesOtherHolders(t *testing.T) {
	t.Parallel()

	// FreeUDPPort keeps other test binaries off port. The subtests claim port
	// again under a private dir that no other binary opens.
	port := FreeUDPPort(t)
	_, ok := lockFreeUDPPort(t, freeUDPClaimDir, port)
	require.False(t, ok)
	dir := t.TempDir()
	t.Run("held", func(t *testing.T) {
		require.True(t, claimFreeUDPPort(t, dir, port))
		claim, ok := lockFreeUDPPort(t, dir, port)
		require.False(t, ok)
		require.Nil(t, claim)
	})
	t.Run("released", func(t *testing.T) {
		claim, ok := lockFreeUDPPort(t, dir, port)
		require.True(t, ok)
		require.NoError(t, claim.Close())
	})
}

func TestFreeUDPPorts_parallelDisjoint(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	seen := make(map[int]struct{})
	for i := range 8 {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			ports := FreeUDPPorts(t, 3)
			mu.Lock()
			defer mu.Unlock()
			for _, port := range ports {
				require.NotContains(t, seen, port)
				seen[port] = struct{}{}
			}
		})
	}
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

// injectSimpleAuth returns a fixed SimpleAuthenticator for amend-hook tests.
func injectSimpleAuth() consensus.Authenticator {
	return consensus.NewSimpleAuthenticator([]consensus.SimplePrincipal{{
		EncodedPrincipal:   []byte("principal"),
		EncodedCredentials: []byte("credentials"),
	}})
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

// TestStartClusterMemberSet_appliesAmendMemberConfig asserts the set runs
// AmendMemberConfig once per member in member-id order over the config
// bootstrap built.
func TestStartClusterMemberSet_appliesAmendMemberConfig(t *testing.T) {
	t.Parallel()

	const memberCount = 2
	var amendedMemberIDs []int32
	auth := injectSimpleAuth()

	capture := NewCaptureFailTB(t)
	set := StartClusterMemberSet(capture, ClusterMemberSetConfig{
		MemberCount:          memberCount,
		PortBase:             ClusterModuleElectPortBase,
		LogMulticastEndpoint: ClusterModuleElectLogEndpoint,
		StartMediaDriver: func(string, DriverEnv) (*MediaDriver, error) {
			return stubMediaDriver("/aeron-amend", new(atomic.Int32)), nil
		},
		StartArchiveNode: func(string, string, string, string, string) (*ArchiveNode, error) {
			return &ArchiveNode{Dir: "/archive-amend", ArchiveID: "ok"}, nil
		},
		AmendMemberConfig: func(cfg cluster.Config) cluster.Config {
			amendedMemberIDs = append(amendedMemberIDs, cfg.MemberID)
			cfg.Authenticator = auth
			return cfg
		},
	})
	// Stub aeron directories cannot open live modules. AmendMemberConfig still
	// runs for each member. The set stores each amended per-slot config before
	// open fails.
	require.Equal(t, []int32{0, 1}, amendedMemberIDs)
	require.True(t, capture.Failed())
	require.NotNil(t, set)
	require.Equal(t, ClusterModuleElectPortBase, set.PortBase())
	for i := range memberCount {
		require.Same(t, auth, set.configs[i].Authenticator)
	}
}

// TestClusterMemberState_sessionCountsNilSafe asserts the dump renders session
// count fields. Nil module and nil agent stay panic-free and use sentinels.
func TestClusterMemberState_sessionCountsNilSafe(t *testing.T) {
	t.Parallel()

	require.Equal(t, "member=<nil>\n", ClusterMemberState(nil))

	dump := ClusterMemberState(&cluster.ConsensusModule{})
	require.Contains(t, dump, "openSessions=-1")
	require.Contains(t, dump, "pendingSessions=-1")
	require.Contains(t, dump, "closingSessions=-1")
	require.Contains(t, dump, "electionCount=-1")
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

// countingServiceInvoker is a ServiceInvoker stub. DoWork increments calls.
type countingServiceInvoker struct {
	calls int
}

func (inv *countingServiceInvoker) DoWork(int64) int {
	inv.calls++
	return 1
}

// TestClusterMemberSet_attachServiceRejects asserts AttachService fails the
// test when:
//   - index is outside [0, MemberCount)
//   - the invoker is nil
//   - the slot is empty
func TestClusterMemberSet_attachServiceRejects(t *testing.T) {
	t.Parallel()

	dummy := &countingServiceInvoker{}
	tests := []struct {
		name    string
		index   int
		svc     ServiceInvoker
		wantMsg string
	}{
		{name: "negative", index: -1, svc: dummy, wantMsg: "out of range"},
		{name: "equal to length", index: 2, svc: dummy, wantMsg: "out of range"},
		{name: "above length", index: 3, svc: dummy, wantMsg: "out of range"},
		{name: "nil invoker", index: 0, svc: nil, wantMsg: "must not be nil"},
		{name: "empty slot", index: 0, svc: dummy, wantMsg: "is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			set := &ClusterMemberSet{
				modules:         make([]*cluster.ConsensusModule, 2),
				serviceInvokers: make([][]ServiceInvoker, 2),
			}
			capture := NewCaptureFailTB(t)
			set.AttachService(capture, tt.index, tt.svc)
			require.True(t, capture.Failed())
			require.Contains(t, capture.lastMsg, tt.wantMsg)
			require.Equal(t, [][]ServiceInvoker{nil, nil}, set.serviceInvokers)
		})
	}
}

// TestClusterMemberSet_attachServiceKeepsListsByMemberIndex asserts attach
// order keyed by member id. Occupied StopMember clears invokers at that
// index.
func TestClusterMemberSet_attachServiceKeepsListsByMemberIndex(t *testing.T) {
	t.Parallel()

	inv0a := &countingServiceInvoker{}
	inv0b := &countingServiceInvoker{}
	inv1 := &countingServiceInvoker{}
	placeholder0 := &cluster.ConsensusModule{}
	placeholder1 := &cluster.ConsensusModule{}
	set := &ClusterMemberSet{
		modules:         []*cluster.ConsensusModule{placeholder0, placeholder1},
		serviceInvokers: make([][]ServiceInvoker, 2),
	}
	set.AttachService(t, 0, inv0a)
	set.AttachService(t, 0, inv0b)
	set.AttachService(t, 1, inv1)
	require.Equal(t, [][]ServiceInvoker{{inv0a, inv0b}, {inv1}}, set.serviceInvokers)

	require.NoError(t, set.StopMember(0))
	require.Nil(t, set.modules[0])
	require.Equal(t, [][]ServiceInvoker{nil, {inv1}}, set.serviceInvokers)
}
