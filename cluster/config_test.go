package cluster_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// Member-0 endpoints from goldenClusterMembersSharedLog; reused by Validate
// endpoint-mutation cases and the IPv6 happy path.
const (
	testIngressEndpoint   = "127.0.0.1:9002"
	testConsensusEndpoint = "127.0.0.1:9003"
	testLogEndpoint       = "239.255.12.1:9004"
	testCatchupEndpoint   = "127.0.0.1:9005"
	testArchiveEndpoint   = "127.0.0.1:9001"
)

// Endpoint slots of a members-string row, in field order.
const (
	ingressSlot = iota
	consensusSlot
	logSlot
	catchupSlot
	archiveSlot
	endpointSlotCount
)

// testMemberEndpoints is the member-0 endpoint row, indexed by slot.
var testMemberEndpoints = [endpointSlotCount]string{
	ingressSlot:   testIngressEndpoint,
	consensusSlot: testConsensusEndpoint,
	logSlot:       testLogEndpoint,
	catchupSlot:   testCatchupEndpoint,
	archiveSlot:   testArchiveEndpoint,
}

func TestNormalize_defaults(t *testing.T) {
	t.Parallel()

	cfg := cluster.Config{}.Normalize()

	require.Equal(t, cluster.DefaultConsensusChannel, cfg.ConsensusChannel)
	require.Equal(t, cluster.DefaultLogChannel, cfg.LogChannel)
	require.Equal(t, cluster.DefaultIngressChannel, cfg.IngressChannel)
	require.Equal(t, cluster.DefaultReplayChannel, cfg.ReplayChannel)
	require.Equal(t, cluster.DefaultServiceControlChannel, cfg.ServiceControlChannel)
	require.Equal(t, cluster.DefaultConsensusStreamID, cfg.ConsensusStreamID)
	require.Equal(t, cluster.DefaultLogStreamID, cfg.LogStreamID)
	require.Equal(t, cluster.DefaultReplayStreamID, cfg.ReplayStreamID)
	require.Equal(t, cluster.DefaultIngressStreamID, cfg.IngressStreamID)
	require.Equal(t, cluster.DefaultConsensusModuleStreamID, cfg.ConsensusModuleStreamID)
	require.Equal(t, cluster.DefaultServiceStreamID, cfg.ServiceStreamID)
	require.Equal(t, "aeron:ipc?alias=snapshot", cfg.SnapshotChannel)
	require.Equal(t, int32(107), cfg.SnapshotStreamID)
	require.NotNil(t, cfg.Idle)
	require.Equal(t, recordinglog.FileSyncData, cfg.RecordingLogFileSyncLevel)
	require.Equal(t, recordinglog.FileSyncData, cfg.NodeStateFileSyncLevel)
	// Archive channels stay empty: Normalize applies no default so Validate can refuse.
	require.Empty(t, cfg.ArchiveControlRequestChannel)
	require.Empty(t, cfg.ArchiveControlResponseChannel)
	// SessionTimeoutNs stays zero: Normalize applies no default; NewAgent maps
	// zero to DefaultSessionTimeoutNs. Negative fails Validate.
	require.Zero(t, cfg.SessionTimeoutNs)
}

func TestValidate_sessionTimeoutNsZeroOK(t *testing.T) {
	t.Parallel()

	// topologyConfig leaves SessionTimeoutNs at zero; positive also passes.
	cfg := topologyConfig(0)
	require.NoError(t, cfg.Validate())
	cfg.SessionTimeoutNs = 1
	require.NoError(t, cfg.Validate())
}

func TestNormalize_electionDefaultTripwire(t *testing.T) {
	t.Parallel()

	// Whole-struct equality catches a new non-zero default on ElectionConfig
	// that Normalize leaves unset, including leader sentinels (NullMemberID).
	require.Equal(
		t,
		election.DefaultElectionConfig(),
		cluster.Config{}.Normalize().Election,
	)
}

func TestNormalize_preservesExplicitValues(t *testing.T) {
	t.Parallel()

	const (
		channel               = "aeron:udp?term-length=128k"
		replayChannel         = "aeron:ipc?alias=replay"
		serviceControlChannel = "aeron:ipc?term-length=64k"
		snapshotChannel       = "aeron:ipc?term-length=64k|alias=snap"
	)
	explicitElection := election.ElectionConfig{
		ElectionTimeoutNs:         2_000_000_000,
		ElectionStatusIntervalNs:  200_000_000,
		LeaderHeartbeatTimeoutNs:  20_000_000_000,
		LeaderHeartbeatIntervalNs: 400_000_000,
		StartupCanvassTimeoutNs:   120_000_000_000,
	}
	cfg := cluster.Config{
		ConsensusChannel:          channel,
		LogChannel:                channel,
		IngressChannel:            channel,
		ReplayChannel:             replayChannel,
		ServiceControlChannel:     serviceControlChannel,
		SnapshotChannel:           snapshotChannel,
		ConsensusStreamID:         200,
		LogStreamID:               201,
		ReplayStreamID:            202,
		IngressStreamID:           203,
		ConsensusModuleStreamID:   204,
		ServiceStreamID:           205,
		SnapshotStreamID:          206,
		RecordingLogFileSyncLevel: recordinglog.FileSyncFull,
		NodeStateFileSyncLevel:    recordinglog.FileSyncFull,
		Election:                  explicitElection,
	}.Normalize()

	require.Equal(t, channel, cfg.ConsensusChannel)
	require.Equal(t, channel, cfg.LogChannel)
	require.Equal(t, channel, cfg.IngressChannel)
	require.Equal(t, replayChannel, cfg.ReplayChannel)
	require.Equal(t, serviceControlChannel, cfg.ServiceControlChannel)
	require.Equal(t, int32(200), cfg.ConsensusStreamID)
	require.Equal(t, int32(201), cfg.LogStreamID)
	require.Equal(t, int32(202), cfg.ReplayStreamID)
	require.Equal(t, int32(203), cfg.IngressStreamID)
	require.Equal(t, int32(204), cfg.ConsensusModuleStreamID)
	require.Equal(t, int32(205), cfg.ServiceStreamID)
	require.Equal(t, snapshotChannel, cfg.SnapshotChannel)
	require.Equal(t, int32(206), cfg.SnapshotStreamID)
	require.Equal(t, recordinglog.FileSyncFull, cfg.RecordingLogFileSyncLevel)
	require.Equal(t, recordinglog.FileSyncFull, cfg.NodeStateFileSyncLevel)

	// Normalize stamps MemberID and leader sentinels; timeouts stay.
	wantElection := explicitElection
	wantElection.MemberID = 0
	wantElection.AppointedLeaderID = election.NullMemberID
	wantElection.GracefulClosedLeaderID = election.NullMemberID
	require.Equal(t, wantElection, cfg.Election)
}

func TestNormalize_stampsMemberID(t *testing.T) {
	t.Parallel()

	cfg := cluster.Config{
		MemberID: 7,
		Election: election.ElectionConfig{
			MemberID: 99,
		},
	}.Normalize()

	require.Equal(t, int32(7), cfg.Election.MemberID)
}

func TestNormalize_materializesLeaderPointers(t *testing.T) {
	t.Parallel()

	cfg := cluster.Config{
		AppointedLeaderID:      new(int32(0)),
		GracefulClosedLeaderID: new(int32(0)),
	}.Normalize()

	require.Equal(t, int32(0), cfg.Election.AppointedLeaderID)
	require.Equal(t, int32(0), cfg.Election.GracefulClosedLeaderID)
}

func TestNormalize_overridesHandSetElectionSentinels(t *testing.T) {
	t.Parallel()

	cfg := cluster.Config{
		Election: election.ElectionConfig{
			AppointedLeaderID:      2,
			GracefulClosedLeaderID: 3,
		},
	}.Normalize()

	require.Equal(t, election.NullMemberID, cfg.Election.AppointedLeaderID)
	require.Equal(t, election.NullMemberID, cfg.Election.GracefulClosedLeaderID)
}

func TestNormalize_bridgesAeronDir(t *testing.T) {
	t.Parallel()

	customEpoch := func() int64 { return 42 }
	customNano := func() int64 { return 99 }

	cfg := cluster.Config{
		AeronDir: "/tmp/heron-aeron",
		AeronConfig: aeron.Config{
			EpochClock: customEpoch,
			NanoClock:  customNano,
		},
	}.Normalize()

	require.Equal(t, "/tmp/heron-aeron", cfg.AeronConfig.AeronDir)
	require.NotNil(t, cfg.AeronConfig.EpochClock)
	require.Equal(t, int64(42), cfg.AeronConfig.EpochClock())
	require.NotNil(t, cfg.AeronConfig.NanoClock)
	require.Equal(t, int64(99), cfg.AeronConfig.NanoClock())
}

func TestNormalize_preservesNestedAeronDir(t *testing.T) {
	t.Parallel()

	cfg := cluster.Config{
		AeronDir: "/tmp/top-level",
		AeronConfig: aeron.Config{
			AeronDir: "/tmp/nested",
		},
	}.Normalize()

	require.Equal(t, "/tmp/nested", cfg.AeronConfig.AeronDir)
}

func TestNormalize_idempotent(t *testing.T) {
	t.Parallel()

	initial := cluster.Config{
		MemberID:               1,
		ClusterMembers:         goldenClusterMembersSharedLog,
		ClusterDir:             "/tmp/cluster",
		AeronDir:               "/tmp/aeron",
		AppointedLeaderID:      new(int32(0)),
		GracefulClosedLeaderID: new(int32(1)),
	}
	once := initial.Normalize()
	twice := once.Normalize()

	// Idle is an interface; Same checks identity. Remaining fields use Equal.
	require.NotNil(t, once.Idle)
	require.Same(t, once.Idle, twice.Idle)
	once.Idle = nil
	twice.Idle = nil
	require.Equal(t, once, twice)
}

// memberSpec builds a single-member ClusterMembers string.
func memberSpec(id int32, endpoints [endpointSlotCount]string) string {
	return strconv.FormatInt(int64(id), 10) + "," + strings.Join(endpoints[:], ",") + "|"
}

// memberSpecWith builds the member-0 ClusterMembers string with one endpoint
// slot replaced.
func memberSpecWith(slot int, endpoint string) string {
	endpoints := testMemberEndpoints
	endpoints[slot] = endpoint
	return memberSpec(0, endpoints)
}

// withMemberEndpoint returns a Validate-error mutate that rewrites member 0
// ClusterMembers with one endpoint slot replaced.
func withMemberEndpoint(slot int, endpoint string) func(*cluster.Config) {
	return func(cfg *cluster.Config) {
		cfg.ClusterMembers = memberSpecWith(slot, endpoint)
	}
}

func TestValidate_happyPath(t *testing.T) {
	t.Parallel()

	require.NoError(t, topologyConfig(0).Validate())
}

func TestValidate_withoutNormalize(t *testing.T) {
	t.Parallel()

	// Unnormalized zero config fails the first required-field rule, not panic.
	err := cluster.Config{}.Validate()
	require.EqualError(t, err, "cluster: ClusterDir is required")
}

func TestValidate_injectedAeronClient(t *testing.T) {
	t.Parallel()

	cfg := topologyConfig(0)
	cfg.AeronDir = ""
	cfg.Aeron = &aeron.Client{}
	require.NoError(t, cfg.Validate())
}

func TestValidate_ipv6ConsensusEndpoint(t *testing.T) {
	t.Parallel()

	cfg := topologyConfig(0)
	cfg.ClusterMembers = memberSpecWith(consensusSlot, "[::1]:9003")
	require.NoError(t, cfg.Validate())
}

func TestValidate_errors(t *testing.T) {
	t.Parallel()

	// Same log multicast as member 0. Other endpoints use member-1 ports.
	dupEndpoints := testMemberEndpoints
	dupEndpoints[ingressSlot] = "127.0.0.1:9102"
	dupEndpoints[consensusSlot] = "127.0.0.1:9103"
	dupEndpoints[catchupSlot] = "127.0.0.1:9105"
	dupEndpoints[archiveSlot] = "127.0.0.1:9101"
	dupMembers := memberSpec(0, testMemberEndpoints) + memberSpec(0, dupEndpoints)

	parseErrInput := "bad"
	_, parseErr := election.ParseMembers(parseErrInput)
	require.Error(t, parseErr)
	wantParseErr := "cluster: invalid ClusterMembers: " + parseErr.Error()

	tests := []struct {
		name   string
		mutate func(*cluster.Config)
		want   string
	}{
		{
			name: "empty ClusterDir",
			mutate: func(cfg *cluster.Config) {
				cfg.ClusterDir = ""
			},
			want: "cluster: ClusterDir is required",
		},
		{
			name: "empty AeronDir without Aeron",
			mutate: func(cfg *cluster.Config) {
				cfg.AeronDir = ""
				cfg.Aeron = nil
			},
			want: "cluster: AeronDir is required when Aeron is not set",
		},
		{
			name: "empty ClusterMembers",
			mutate: func(cfg *cluster.Config) {
				cfg.ClusterMembers = ""
			},
			want: "cluster: ClusterMembers is required",
		},
		{
			name: "invalid ClusterMembers",
			mutate: func(cfg *cluster.Config) {
				cfg.ClusterMembers = parseErrInput
			},
			want: wantParseErr,
		},
		{
			name: "no members",
			mutate: func(cfg *cluster.Config) {
				cfg.ClusterMembers = "|"
			},
			want: "cluster: ClusterMembers must contain at least one member",
		},
		{
			name: "duplicate member id",
			mutate: func(cfg *cluster.Config) {
				cfg.ClusterMembers = dupMembers
			},
			want: "cluster: duplicate cluster member id 0",
		},
		{
			name: "MemberID absent",
			mutate: func(cfg *cluster.Config) {
				cfg.MemberID = 99
				cfg.Election.MemberID = 99
			},
			want: "cluster: MemberID 99 is not present in ClusterMembers",
		},
		{
			name:   "empty ingress endpoint",
			mutate: withMemberEndpoint(ingressSlot, ""),
			want:   "cluster: member 0 has an empty ingress endpoint",
		},
		{
			name:   "empty consensus endpoint",
			mutate: withMemberEndpoint(consensusSlot, ""),
			want:   "cluster: member 0 has an empty consensus endpoint",
		},
		{
			name:   "empty log endpoint",
			mutate: withMemberEndpoint(logSlot, ""),
			want:   "cluster: member 0 has an empty log endpoint",
		},
		{
			name:   "empty catchup endpoint",
			mutate: withMemberEndpoint(catchupSlot, ""),
			want:   "cluster: member 0 has an empty catchup endpoint",
		},
		{
			name: "empty archive endpoint",
			mutate: func(cfg *cluster.Config) {
				// Trailing empty archive is trimmed by ParseMembers; keep a 7th
				// optional field so the empty archive slot survives the parse.
				cfg.ClusterMembers = strings.TrimSuffix(memberSpecWith(archiveSlot, ""), "|") +
					",archive-response|"
			},
			want: "cluster: member 0 has an empty archive endpoint",
		},
		{
			name:   "endpoint contains question mark",
			mutate: withMemberEndpoint(consensusSlot, testConsensusEndpoint+"?x"),
			want:   "cluster: member 0 has an invalid consensus endpoint: " + testConsensusEndpoint + "?x",
		},
		{
			name:   "endpoint contains equals",
			mutate: withMemberEndpoint(ingressSlot, "host=port"),
			want:   "cluster: member 0 has an invalid ingress endpoint: host=port",
		},
		{
			name:   "consensus endpoint without port",
			mutate: withMemberEndpoint(consensusSlot, "127.0.0.1"),
			want:   "cluster: member 0 has a consensus endpoint without a port: 127.0.0.1",
		},
		{
			name:   "log endpoint without port",
			mutate: withMemberEndpoint(logSlot, "127.0.0.1"),
			want:   "cluster: member 0 has a log endpoint without a port: 127.0.0.1",
		},
		{
			name:   "ingress endpoint without port",
			mutate: withMemberEndpoint(ingressSlot, "127.0.0.1"),
			want:   "cluster: member 0 has a ingress endpoint without a port: 127.0.0.1",
		},
		{
			name:   "out-of-range consensus port",
			mutate: withMemberEndpoint(consensusSlot, "127.0.0.1:99999"),
			want:   "cluster: member 0 has an out-of-range consensus endpoint port: 127.0.0.1:99999",
		},
		{
			name:   "out-of-range ingress port",
			mutate: withMemberEndpoint(ingressSlot, "127.0.0.1:99999"),
			want:   "cluster: member 0 has an out-of-range ingress endpoint port: 127.0.0.1:99999",
		},
		{
			name:   "non-numeric consensus port",
			mutate: withMemberEndpoint(consensusSlot, "127.0.0.1:abc"),
			want:   "cluster: member 0 has an out-of-range consensus endpoint port: 127.0.0.1:abc",
		},
		{
			name:   "out-of-range log port",
			mutate: withMemberEndpoint(logSlot, "127.0.0.1:0"),
			want:   "cluster: member 0 has an out-of-range log endpoint port: 127.0.0.1:0",
		},
		{
			name: "ConsensusChannel not udp",
			mutate: func(cfg *cluster.Config) {
				cfg.ConsensusChannel = "aeron:ipc"
			},
			want: "cluster: ConsensusChannel must be a udp channel",
		},
		{
			name: "ConsensusChannel has endpoint",
			mutate: func(cfg *cluster.Config) {
				cfg.ConsensusChannel = "aeron:udp?endpoint=" + testConsensusEndpoint
			},
			want: "cluster: ConsensusChannel must not set endpoint=",
		},
		{
			name: "LogChannel not udp",
			mutate: func(cfg *cluster.Config) {
				cfg.LogChannel = "aeron:ipc"
			},
			want: "cluster: LogChannel must be a udp channel",
		},
		{
			name: "LogChannel has endpoint",
			mutate: func(cfg *cluster.Config) {
				cfg.LogChannel = "aeron:udp?term-length=64k|endpoint=" + testLogEndpoint
			},
			want: "cluster: LogChannel must not set endpoint=",
		},
		{
			name: "IngressChannel not udp",
			mutate: func(cfg *cluster.Config) {
				cfg.IngressChannel = "aeron:ipc"
			},
			want: "cluster: IngressChannel must be a udp channel",
		},
		{
			name: "IngressChannel has endpoint",
			mutate: func(cfg *cluster.Config) {
				cfg.IngressChannel = "aeron:udp?endpoint=" + testIngressEndpoint
			},
			want: "cluster: IngressChannel must not set endpoint=",
		},
		{
			name: "stream ids equal",
			mutate: func(cfg *cluster.Config) {
				cfg.LogStreamID = cfg.ConsensusStreamID
			},
			want: "cluster: ConsensusStreamID and LogStreamID must differ",
		},
		{
			name: "ReplayStreamID equals ConsensusStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.ReplayStreamID = cfg.ConsensusStreamID
			},
			want: "cluster: ReplayStreamID and ConsensusStreamID must differ",
		},
		{
			name: "ReplayStreamID equals LogStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.ReplayStreamID = cfg.LogStreamID
			},
			want: "cluster: ReplayStreamID and LogStreamID must differ",
		},
		{
			name: "IngressStreamID equals ConsensusStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.IngressStreamID = cfg.ConsensusStreamID
			},
			want: "cluster: IngressStreamID and ConsensusStreamID must differ",
		},
		{
			name: "IngressStreamID equals LogStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.IngressStreamID = cfg.LogStreamID
			},
			want: "cluster: IngressStreamID and LogStreamID must differ",
		},
		{
			name: "IngressStreamID equals ReplayStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.IngressStreamID = cfg.ReplayStreamID
			},
			want: "cluster: IngressStreamID and ReplayStreamID must differ",
		},
		{
			name: "ConsensusModuleStreamID equals ConsensusStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.ConsensusModuleStreamID = cfg.ConsensusStreamID
			},
			want: "cluster: ConsensusModuleStreamID and ConsensusStreamID must differ",
		},
		{
			name: "ConsensusModuleStreamID equals LogStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.ConsensusModuleStreamID = cfg.LogStreamID
			},
			want: "cluster: ConsensusModuleStreamID and LogStreamID must differ",
		},
		{
			name: "ConsensusModuleStreamID equals ReplayStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.ConsensusModuleStreamID = cfg.ReplayStreamID
			},
			want: "cluster: ConsensusModuleStreamID and ReplayStreamID must differ",
		},
		{
			name: "ConsensusModuleStreamID equals IngressStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.ConsensusModuleStreamID = cfg.IngressStreamID
			},
			want: "cluster: ConsensusModuleStreamID and IngressStreamID must differ",
		},
		{
			name: "ServiceStreamID equals ConsensusStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.ServiceStreamID = cfg.ConsensusStreamID
			},
			want: "cluster: ServiceStreamID and ConsensusStreamID must differ",
		},
		{
			name: "ServiceStreamID equals LogStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.ServiceStreamID = cfg.LogStreamID
			},
			want: "cluster: ServiceStreamID and LogStreamID must differ",
		},
		{
			name: "ServiceStreamID equals ReplayStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.ServiceStreamID = cfg.ReplayStreamID
			},
			want: "cluster: ServiceStreamID and ReplayStreamID must differ",
		},
		{
			name: "ServiceStreamID equals IngressStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.ServiceStreamID = cfg.IngressStreamID
			},
			want: "cluster: ServiceStreamID and IngressStreamID must differ",
		},
		{
			name: "ServiceStreamID equals ConsensusModuleStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.ServiceStreamID = cfg.ConsensusModuleStreamID
			},
			want: "cluster: ServiceStreamID and ConsensusModuleStreamID must differ",
		},
		{
			name: "ReplayChannel has session-id",
			mutate: func(cfg *cluster.Config) {
				cfg.ReplayChannel = "aeron:ipc?session-id=1"
			},
			want: "cluster: ReplayChannel must not set session-id=",
		},
		{
			name: "SnapshotChannel has session-id",
			mutate: func(cfg *cluster.Config) {
				cfg.SnapshotChannel = "aeron:ipc?alias=snapshot|session-id=7"
			},
			want: "cluster: SnapshotChannel must not set session-id=",
		},
		{
			name: "SnapshotStreamID equals LogStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.SnapshotStreamID = cfg.LogStreamID
			},
			want: "cluster: SnapshotStreamID and LogStreamID must differ",
		},
		{
			name: "SnapshotStreamID equals ConsensusStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.SnapshotStreamID = cfg.ConsensusStreamID
			},
			want: "cluster: SnapshotStreamID and ConsensusStreamID must differ",
		},
		{
			name: "SnapshotStreamID equals ReplayStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.SnapshotStreamID = cfg.ReplayStreamID
			},
			want: "cluster: SnapshotStreamID and ReplayStreamID must differ",
		},
		{
			name: "SnapshotStreamID equals IngressStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.SnapshotStreamID = cfg.IngressStreamID
			},
			want: "cluster: SnapshotStreamID and IngressStreamID must differ",
		},
		{
			name: "SnapshotStreamID equals ConsensusModuleStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.SnapshotStreamID = cfg.ConsensusModuleStreamID
			},
			want: "cluster: SnapshotStreamID and ConsensusModuleStreamID must differ",
		},
		{
			name: "SnapshotStreamID equals ServiceStreamID",
			mutate: func(cfg *cluster.Config) {
				cfg.SnapshotStreamID = cfg.ServiceStreamID
			},
			want: "cluster: SnapshotStreamID and ServiceStreamID must differ",
		},
		{
			name: "negative ServiceCount",
			mutate: func(cfg *cluster.Config) {
				cfg.ServiceCount = -1
			},
			want: "cluster: ServiceCount must not be negative",
		},
		{
			name: "negative SessionTimeoutNs",
			mutate: func(cfg *cluster.Config) {
				cfg.SessionTimeoutNs = -1
			},
			want: "cluster: SessionTimeoutNs must not be negative",
		},
		{
			name: "invalid Election config",
			mutate: func(cfg *cluster.Config) {
				cfg.Election.ElectionTimeoutNs = 0
			},
			want: "cluster: invalid Election config: election: ElectionTimeoutNs must be positive",
		},
		{
			name: "Election.MemberID mismatch",
			mutate: func(cfg *cluster.Config) {
				cfg.Election.MemberID = 99
			},
			want: "cluster: Election.MemberID must equal MemberID",
		},
		{
			name: "AppointedLeaderID absent",
			mutate: func(cfg *cluster.Config) {
				cfg.AppointedLeaderID = new(int32(99))
				cfg.Election.AppointedLeaderID = 99
			},
			want: "cluster: AppointedLeaderID 99 is not present in ClusterMembers",
		},
		{
			name: "GracefulClosedLeaderID absent",
			mutate: func(cfg *cluster.Config) {
				cfg.GracefulClosedLeaderID = new(int32(99))
				cfg.Election.GracefulClosedLeaderID = 99
			},
			want: "cluster: GracefulClosedLeaderID 99 is not present in ClusterMembers",
		},
		{
			// Normalize stamps NullMemberID; hand-setting zero after that leaves a
			// legal member id that only the consistency check rejects.
			name: "Election.AppointedLeaderID mismatch",
			mutate: func(cfg *cluster.Config) {
				cfg.Election.AppointedLeaderID = 0
			},
			want: "cluster: Election.AppointedLeaderID must match AppointedLeaderID",
		},
		{
			name: "Election.GracefulClosedLeaderID mismatch",
			mutate: func(cfg *cluster.Config) {
				cfg.Election.GracefulClosedLeaderID = 0
			},
			want: "cluster: Election.GracefulClosedLeaderID must match GracefulClosedLeaderID",
		},
		{
			name: "Idle nil",
			mutate: func(cfg *cluster.Config) {
				cfg.Idle = nil
			},
			want: "cluster: IdleStrategy is required",
		},
		{
			name: "archive request channel empty",
			mutate: func(cfg *cluster.Config) {
				cfg.ArchiveControlRequestChannel = ""
			},
			want: "cluster: ArchiveControlRequestChannel is required",
		},
		{
			name: "archive response channel empty",
			mutate: func(cfg *cluster.Config) {
				cfg.ArchiveControlResponseChannel = ""
			},
			want: "cluster: ArchiveControlResponseChannel is required",
		},
		{
			name: "RecordingLogFileSyncLevel invalid",
			mutate: func(cfg *cluster.Config) {
				cfg.RecordingLogFileSyncLevel = recordinglog.FileSyncNone
			},
			want: "cluster: RecordingLogFileSyncLevel must be FileSyncData or FileSyncFull",
		},
		{
			name: "NodeStateFileSyncLevel invalid",
			mutate: func(cfg *cluster.Config) {
				cfg.NodeStateFileSyncLevel = recordinglog.FileSyncNone
			},
			want: "cluster: NodeStateFileSyncLevel must be FileSyncData or FileSyncFull",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := topologyConfig(0)
			tc.mutate(&cfg)
			require.EqualError(t, cfg.Validate(), tc.want)
		})
	}
}

func TestValidate_appointedLeaderPresent(t *testing.T) {
	t.Parallel()

	cfg := topologyConfig(1)
	cfg.AppointedLeaderID = new(int32(0))
	cfg.GracefulClosedLeaderID = new(int32(2))
	cfg = cfg.Normalize()
	require.NoError(t, cfg.Validate())
}
