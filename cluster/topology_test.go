package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
)

func topologyConfigMembers(memberID int32, members string) cluster.Config {
	return cluster.Config{
		MemberID:       memberID,
		ClusterMembers: members,
		ClusterDir:     "/tmp/cluster",
		AeronDir:       "/tmp/aeron",
	}.Normalize()
}

func topologyConfig(memberID int32) cluster.Config {
	return topologyConfigMembers(memberID, goldenClusterMembersSharedLog)
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
	idx := aeron.ChannelEndpointParamIndex(subCh)
	require.GreaterOrEqual(t, idx, 0)
	require.Equal(
		t,
		this.ConsensusEndpoint,
		aeron.ChannelParamValue(subCh, idx, aeron.ChannelEndpointParam),
	)

	// Stream getters match normalized config.
	require.Equal(t, cfg.ConsensusStreamID, tp.ConsensusStreamID())
	require.Equal(t, cfg.LogStreamID, tp.LogStreamID())

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
	require.Len(t, tp.Members(), 1)
	require.Same(t, tp.Members()[0], tp.ThisMember())
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
}
