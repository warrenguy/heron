package client_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/client"
)

func TestMemberIngressMap_nilEntry(t *testing.T) {
	m := client.MemberIngressMap{0: nil}

	_, ok := m.Lookup(0)
	require.False(t, ok)

	_, err := m.Require(0)
	require.EqualError(t, err, "cluster client: member id 0 not in ingress map")

	clusterCtx := client.ClusterContext{IngressChannel: "aeron:udp?ttl=16"}
	_, err = m.ChannelFor(clusterCtx, 0)
	require.EqualError(t, err, "cluster client: member id 0 not in ingress map")
}

func TestMemberIngressMap_Lookup(t *testing.T) {
	m, err := client.ParseIngressEndpoints("0=localhost:20110")
	require.NoError(t, err)
	requireMemberIngressMap(t, m, map[int32]string{0: "localhost:20110"})

	_, ok := m.Lookup(1)
	require.False(t, ok)

	var nilMap client.MemberIngressMap
	_, ok = nilMap.Lookup(0)
	require.False(t, ok)
}

func TestMemberIngressMap_Require(t *testing.T) {
	m, err := client.ParseIngressEndpoints("1=host:20111")
	require.NoError(t, err)

	ingress, err := m.Require(1)
	require.NoError(t, err)
	require.Equal(t, "host:20111", ingress.Endpoint)
}

func TestMemberIngressMap_Require_notFound(t *testing.T) {
	m, err := client.ParseIngressEndpoints("0=host:20110")
	require.NoError(t, err)

	tests := []struct {
		name     string
		m        client.MemberIngressMap
		memberID int32
		wantErr  string
	}{
		{
			name:     "nil map",
			m:        nil,
			memberID: 0,
			wantErr:  "cluster client: member id 0 not in ingress map",
		},
		{
			name:     "empty map",
			m:        client.MemberIngressMap{},
			memberID: 0,
			wantErr:  "cluster client: member id 0 not in ingress map",
		},
		{
			name:     "missing member",
			m:        m,
			memberID: 1,
			wantErr:  "cluster client: member id 1 not in ingress map",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := testCase.m.Require(testCase.memberID)
			require.EqualError(t, err, testCase.wantErr)
		})
	}
}

func TestMemberIngressMap_ChannelFor_notFound(t *testing.T) {
	clusterCtx := client.ClusterContext{IngressChannel: "aeron:udp?ttl=16"}

	for _, m := range []client.MemberIngressMap{nil, {}} {
		_, err := m.ChannelFor(clusterCtx, 0)
		require.EqualError(t, err, "cluster client: member id 0 not in ingress map")
	}
}

func TestMemberIngressMap_ChannelFor(t *testing.T) {
	m, err := client.ParseIngressEndpoints("0=192.168.1.10:20110")
	require.NoError(t, err)

	clusterCtx := client.ClusterContext{IngressChannel: "aeron:udp?ttl=16"}
	got, err := m.ChannelFor(clusterCtx, 0)
	require.NoError(t, err)
	require.Equal(t, "aeron:udp?ttl=16|endpoint=192.168.1.10:20110", got)
}

func TestMemberIngressMap_ChannelFor_buildError(t *testing.T) {
	m, err := client.ParseIngressEndpoints("0=host:20110")
	require.NoError(t, err)

	clusterCtx := client.ClusterContext{IngressChannel: "aeron:unknown"}
	_, err = m.ChannelFor(clusterCtx, 0)
	require.EqualError(t, err, "cluster client: invalid ingress channel: aeron:unknown")
}
