package client_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/client"
)

const sampleIngressEndpoints = "0=localhost:20110,1=localhost:20111"

func TestParseIngressEndpoints_valid(t *testing.T) {
	got, err := client.ParseIngressEndpoints(sampleIngressEndpoints)
	require.NoError(t, err)
	requireMemberIngressMap(t, got, map[int32]string{
		0: "localhost:20110",
		1: "localhost:20111",
	})
}

func TestParseIngressEndpoints_single(t *testing.T) {
	got, err := client.ParseIngressEndpoints("2=host:9999")
	require.NoError(t, err)
	requireMemberIngressMap(t, got, map[int32]string{2: "host:9999"})
}

func TestParseIngressEndpoints_empty(t *testing.T) {
	got, err := client.ParseIngressEndpoints("")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Empty(t, got)
}

func TestParseIngressEndpointsBytes_empty(t *testing.T) {
	for _, endpoints := range [][]byte{nil, {}} {
		got, err := client.ParseIngressEndpointsBytes(endpoints)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	}
}

func TestParseIngressEndpointsBytes_valid(t *testing.T) {
	got, err := client.ParseIngressEndpointsBytes([]byte(sampleIngressEndpoints))
	require.NoError(t, err)
	requireMemberIngressMap(t, got, map[int32]string{
		0: "localhost:20110",
		1: "localhost:20111",
	})
}

func TestParseIngressEndpoints_errors(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name:    "missing equals",
			input:   "0=localhost:20110,1",
			wantErr: "cluster client: endpoint missing '=' separator: 0=localhost:20110,1",
		},
		{
			name:    "empty endpoint",
			input:   "0=",
			wantErr: "cluster client: ingress member endpoint is empty: 0=",
		},
		{
			name:    "bad input",
			input:   "bad",
			wantErr: "cluster client: endpoint missing '=' separator: bad",
		},
		{
			name:    "trailing comma",
			input:   "0=host:1,",
			wantErr: "cluster client: endpoint missing '=' separator: 0=host:1,",
		},
		{
			name:    "empty pair",
			input:   "0=host:1,,1=host:2",
			wantErr: "cluster client: endpoint missing '=' separator: 0=host:1,,1=host:2",
		},
		{
			name:    "empty member id",
			input:   "=host:20110",
			wantErr: "cluster client: invalid ingress member id: =host:20110",
		},
		{
			name:    "non digit member id",
			input:   "x=host:1",
			wantErr: "cluster client: invalid ingress member id: x=host:1",
		},
		{
			name:    "negative member id",
			input:   "-1=host:1",
			wantErr: "cluster client: invalid ingress member id: -1=host:1",
		},
		{
			name:    "overflow member id",
			input:   "99999999999=host:1",
			wantErr: "cluster client: invalid ingress member id: 99999999999=host:1",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := client.ParseIngressEndpoints(testCase.input)
			require.Nil(t, got)
			require.EqualError(t, err, testCase.wantErr)
		})
	}
}

func TestParseIngressEndpoints_duplicateMemberID(t *testing.T) {
	got, err := client.ParseIngressEndpoints("0=first:1,0=second:2")
	require.NoError(t, err)
	requireMemberIngressMap(t, got, map[int32]string{0: "second:2"})
}

func TestFormatIngressEndpoints_roundtrip(t *testing.T) {
	initial, err := client.ParseIngressEndpoints("1=host-b:2,0=host-a:1")
	require.NoError(t, err)

	formatted := client.FormatIngressEndpoints(initial)
	require.Equal(t, "0=host-a:1,1=host-b:2", formatted)

	roundtrip, err := client.ParseIngressEndpoints(formatted)
	require.NoError(t, err)
	requireMemberIngressMapsEqual(t, roundtrip, initial)
	require.Equal(t, formatted, client.FormatIngressEndpoints(roundtrip))
}

func TestFormatIngressEndpoints_nilAndEmpty(t *testing.T) {
	require.Empty(t, client.FormatIngressEndpoints(nil))
	require.Empty(t, client.FormatIngressEndpoints(client.MemberIngressMap{}))
}

func TestFormatIngressEndpoints_skipsNilEntry(t *testing.T) {
	m := client.MemberIngressMap{
		0: nil,
		1: &client.MemberIngress{MemberID: 1, Endpoint: "host:1"},
	}
	require.Equal(t, "1=host:1", client.FormatIngressEndpoints(m))
}

func TestNewMemberIngressMapFromContext(t *testing.T) {
	ctx := client.ClusterContext{IngressEndpoints: sampleIngressEndpoints}
	got, err := client.NewMemberIngressMapFromContext(ctx)
	require.NoError(t, err)
	requireMemberIngressMap(t, got, map[int32]string{
		0: "localhost:20110",
		1: "localhost:20111",
	})
}

func TestNewMemberIngressMapFromContext_invalid(t *testing.T) {
	ctx := client.ClusterContext{IngressEndpoints: "bad"}
	got, err := client.NewMemberIngressMapFromContext(ctx)
	require.Nil(t, got)
	require.EqualError(t, err, "cluster client: endpoint missing '=' separator: bad")
}
