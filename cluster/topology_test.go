package cluster_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
)

// testArchiveControl channels satisfy Validate for fixtures that never connect
// an archive client. Same co-located IPC shape as OpenConsensusModule.
const (
	testArchiveControlRequestChannel  = archive.DefaultLocalControlChannel
	testArchiveControlResponseChannel = archive.DefaultLocalControlChannel
)

// IngressEndpoints output for the three members-string goldens.
const (
	wantIngressEndpointsSharedLog = "0=127.0.0.1:9002,1=127.0.0.1:9102,2=127.0.0.1:9202"
	wantIngressEndpointsFailover  = "0=cluster-0:9002,1=cluster-1:9102,2=cluster-2:9202"
	wantIngressEndpointsSingle    = "0=localhost:9002"
)

func topologyConfigMembers(memberID int32, members string) cluster.Config {
	return cluster.Config{
		MemberID:                      memberID,
		ClusterMembers:                members,
		ClusterDir:                    "/tmp/cluster",
		AeronDir:                      "/tmp/aeron",
		ArchiveControlRequestChannel:  testArchiveControlRequestChannel,
		ArchiveControlResponseChannel: testArchiveControlResponseChannel,
	}.Normalize()
}

func topologyConfig(memberID int32) cluster.Config {
	return topologyConfigMembers(memberID, goldenClusterMembersSharedLog)
}

func requireChannelEndpoint(t *testing.T, channel, wantEndpoint string) {
	t.Helper()
	idx := aeron.ChannelEndpointParamIndex(channel)
	require.GreaterOrEqual(t, idx, 0)
	require.Equal(t, wantEndpoint, aeron.ChannelParamValue(channel, idx, aeron.ChannelEndpointParam))
}

func TestBuildChannelWithEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		channel  string
		endpoint string
		want     string
		wantErr  string
	}{
		{
			name:     "append to template",
			channel:  cluster.DefaultConsensusChannel,
			endpoint: "127.0.0.1:9003",
			want:     "aeron:udp?term-length=64k|endpoint=127.0.0.1:9003",
		},
		{
			name:     "replace existing endpoint",
			channel:  "aeron:udp?endpoint=localhost:0",
			endpoint: "192.168.1.10:20110",
			want:     "aeron:udp?endpoint=192.168.1.10:20110",
		},
		{
			name:     "bare udp append",
			channel:  "aeron:udp",
			endpoint: "host:20110",
			want:     "aeron:udp?endpoint=host:20110",
		},
		{
			name:     "piped params replace",
			channel:  "aeron:udp?ttl=16|endpoint=localhost:0",
			endpoint: "host:20110",
			want:     "aeron:udp?ttl=16|endpoint=host:20110",
		},
		{
			name:     "not udp",
			channel:  "aeron:ipc",
			endpoint: "host:20110",
			wantErr:  "cluster: invalid channel: aeron:ipc",
		},
		{
			name:     "empty endpoint",
			channel:  "aeron:udp",
			endpoint: "",
			wantErr:  "cluster: endpoint is required",
		},
		{
			name:     "pipe in endpoint",
			channel:  "aeron:udp",
			endpoint: "host|20110",
			wantErr:  "cluster: invalid endpoint: host|20110",
		},
		{
			name:     "question in endpoint",
			channel:  "aeron:udp",
			endpoint: "host?20110",
			wantErr:  "cluster: invalid endpoint: host?20110",
		},
		{
			name:     "equals in endpoint",
			channel:  "aeron:udp",
			endpoint: "host=20110",
			wantErr:  "cluster: invalid endpoint: host=20110",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := cluster.BuildChannelWithEndpoint(tc.channel, tc.endpoint)
			if tc.wantErr != "" {
				require.Empty(t, got)
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestNewTopology_accessors(t *testing.T) {
	t.Parallel()

	cfg := topologyConfig(1)
	tp, err := cluster.NewTopology(cfg)
	require.NoError(t, err)

	// Members order and length.
	members := tp.Members()
	require.Len(t, members, 3)
	for i, m := range members {
		require.Equal(t, int32(i), m.ID)
	}

	// This member identity.
	this := tp.ThisMember()
	require.Same(t, members[1], this)
	require.Equal(t, int32(1), this.ID)

	// Peer channels exclude self.
	peers := tp.PeerConsensusChannels()
	require.Len(t, peers, 2)
	_, selfPresent := peers[1]
	require.False(t, selfPresent)

	// Peer channel content for members 0 and 2.
	const (
		wantPeer0 = "aeron:udp?term-length=64k|endpoint=127.0.0.1:9003"
		wantPeer2 = "aeron:udp?term-length=64k|endpoint=127.0.0.1:9203"
	)
	require.Equal(t, wantPeer0, peers[0])
	require.Equal(t, wantPeer2, peers[2])

	// ConsensusChannelFor self and unknown.
	_, ok := tp.ConsensusChannelFor(1)
	require.False(t, ok)
	_, ok = tp.ConsensusChannelFor(99)
	require.False(t, ok)
	ch, ok := tp.ConsensusChannelFor(0)
	require.True(t, ok)
	require.Equal(t, wantPeer0, ch)

	// Consensus bind channel uses this member's consensus endpoint.
	const wantSub = "aeron:udp?term-length=64k|endpoint=127.0.0.1:9103"
	subCh := tp.ConsensusSubscriptionChannel()
	require.Equal(t, wantSub, subCh)
	requireChannelEndpoint(t, subCh, this.ConsensusEndpoint)

	// Ingress bind channel uses this member's ingress endpoint (port offset 2).
	const wantIngress = "aeron:udp?term-length=64k|endpoint=127.0.0.1:9102"
	ingressCh := tp.IngressSubscriptionChannel()
	require.Equal(t, wantIngress, ingressCh)
	requireChannelEndpoint(t, ingressCh, this.IngressEndpoint)

	// Ingress endpoints string is fixed at construction in member-id order.
	require.Equal(t, wantIngressEndpointsSharedLog, tp.IngressEndpoints())

	// Stream getters match normalized config.
	require.Equal(t, cfg.ConsensusStreamID, tp.ConsensusStreamID())
	require.Equal(t, cfg.IngressStreamID, tp.IngressStreamID())
	require.Equal(t, cfg.LogStreamID, tp.LogStreamID())
	require.Equal(t, cfg.ReplayStreamID, tp.ReplayStreamID())
	require.Equal(t, cfg.ReplayChannel, tp.ReplayChannel())

	// Peer map is a copy.
	peers[99] = "x"
	delete(peers, 0)
	peersAgain := tp.PeerConsensusChannels()
	require.Len(t, peersAgain, 2)
	require.Equal(t, wantPeer0, peersAgain[0])
	_, mutated := peersAgain[99]
	require.False(t, mutated)
}

func TestNewTopology_logChannelIdentical(t *testing.T) {
	t.Parallel()

	const wantLog = "aeron:udp?term-length=64k|endpoint=239.255.12.1:9004"

	for memberID := range int32(3) {
		tp, err := cluster.NewTopology(topologyConfig(memberID))
		require.NoError(t, err)
		require.Equal(t, wantLog, tp.LogChannel())
	}
}

func TestNewTopology_ingressChannelPerMember(t *testing.T) {
	t.Parallel()

	wantIngress := [3]string{
		"aeron:udp?term-length=64k|endpoint=127.0.0.1:9002",
		"aeron:udp?term-length=64k|endpoint=127.0.0.1:9102",
		"aeron:udp?term-length=64k|endpoint=127.0.0.1:9202",
	}

	for memberID := range int32(3) {
		tp, err := cluster.NewTopology(topologyConfig(memberID))
		require.NoError(t, err)
		require.Equal(t, wantIngress[memberID], tp.IngressSubscriptionChannel())
		require.Equal(t, cluster.DefaultIngressStreamID, tp.IngressStreamID())
	}
}

func TestNewTopology_ingressEndpoints(t *testing.T) {
	t.Parallel()

	for memberID := range int32(3) {
		tp, err := cluster.NewTopology(topologyConfig(memberID))
		require.NoError(t, err)
		require.Equal(t, wantIngressEndpointsSharedLog, tp.IngressEndpoints())
	}

	// Failover hostnames still member-id order, same port layout.
	tp, err := cluster.NewTopology(
		topologyConfigMembers(0, goldenClusterMembersFailover),
	)
	require.NoError(t, err)
	require.Equal(t, wantIngressEndpointsFailover, tp.IngressEndpoints())

	// Single member.
	tp, err = cluster.NewTopology(
		topologyConfigMembers(0, goldenClusterMembersSingle),
	)
	require.NoError(t, err)
	require.Equal(t, wantIngressEndpointsSingle, tp.IngressEndpoints())

	// Members-string order non-ascending: output sorted by member id.
	// Reorder golden shared-log segments to 2|0|1.
	segments := strings.Split(strings.TrimSuffix(goldenClusterMembersSharedLog, "|"), "|")
	require.Len(t, segments, 3)
	reverseMembers := segments[2] + "|" + segments[0] + "|" + segments[1] + "|"
	tp, err = cluster.NewTopology(topologyConfigMembers(0, reverseMembers))
	require.NoError(t, err)
	require.Equal(t, wantIngressEndpointsSharedLog, tp.IngressEndpoints())

	// Client parse roundtrip on Topology multi-member output.
	tp, err = cluster.NewTopology(topologyConfig(0))
	require.NoError(t, err)
	parsed, err := client.ParseIngressEndpoints(tp.IngressEndpoints())
	require.NoError(t, err)
	require.Len(t, parsed, 3)
	for _, m := range tp.Members() {
		require.Contains(t, parsed, m.ID)
		require.Equal(t, m.IngressEndpoint, parsed[m.ID].Endpoint)
	}
}

func TestNewTopology_ingressTemplateParamsPreserved(t *testing.T) {
	t.Parallel()

	const template = "aeron:udp?term-length=128k|ttl=16"
	cfg := topologyConfig(0)
	cfg.IngressChannel = template
	cfg.IngressStreamID = 777

	tp, err := cluster.NewTopology(cfg)
	require.NoError(t, err)

	want := "aeron:udp?term-length=128k|ttl=16|endpoint=127.0.0.1:9002"
	require.Equal(t, want, tp.IngressSubscriptionChannel())
	require.Equal(t, int32(777), tp.IngressStreamID())
}

func TestNewTopology_replayAccessorsVerbatim(t *testing.T) {
	t.Parallel()

	const (
		replayChannel  = "aeron:ipc?alias=local-replay"
		replayStreamID = int32(777)
	)
	cfg := topologyConfig(0)
	cfg.ReplayChannel = replayChannel
	cfg.ReplayStreamID = replayStreamID

	tp, err := cluster.NewTopology(cfg)
	require.NoError(t, err)

	// No endpoint substitution. Stored and returned verbatim.
	require.Equal(t, replayChannel, tp.ReplayChannel())
	require.Equal(t, replayStreamID, tp.ReplayStreamID())
}

func TestNewTopology_rejectsUnnormalizedAndInvalid(t *testing.T) {
	t.Parallel()

	consensusBaked := topologyConfig(0)
	consensusBaked.ConsensusChannel = "aeron:udp?endpoint=127.0.0.1:1"
	logBaked := topologyConfig(0)
	logBaked.LogChannel = "aeron:udp?endpoint=127.0.0.1:1"

	tests := []struct {
		name    string
		cfg     cluster.Config
		wantErr string
	}{
		{
			name:    "empty config fails validate",
			cfg:     cluster.Config{},
			wantErr: "cluster: ClusterDir is required",
		},
		{
			name:    "template with baked endpoint rejected",
			cfg:     consensusBaked,
			wantErr: "cluster: ConsensusChannel must not set endpoint=",
		},
		{
			name:    "log template with baked endpoint rejected",
			cfg:     logBaked,
			wantErr: "cluster: LogChannel must not set endpoint=",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := cluster.NewTopology(tc.cfg)
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestNewTopology_singleMember(t *testing.T) {
	t.Parallel()

	tp, err := cluster.NewTopology(topologyConfigMembers(0, goldenClusterMembersSingle))
	require.NoError(t, err)
	members := tp.Members()
	require.Len(t, members, 1)
	require.Same(t, members[0], tp.ThisMember())
	require.Empty(t, tp.PeerConsensusChannels())
	_, ok := tp.ConsensusChannelFor(0)
	require.False(t, ok)
	require.Equal(
		t,
		"aeron:udp?term-length=64k|endpoint=localhost:9003",
		tp.ConsensusSubscriptionChannel(),
	)
	require.Equal(
		t,
		"aeron:udp?term-length=64k|endpoint=localhost:9004",
		tp.LogChannel(),
	)
	require.Equal(t, wantIngressEndpointsSingle, tp.IngressEndpoints())
}
