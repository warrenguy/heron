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
	clusterCtx := client.ClusterContext{IngressEndpoints: sampleIngressEndpoints}
	got, err := client.NewMemberIngressMapFromContext(clusterCtx)
	require.NoError(t, err)
	requireMemberIngressMap(t, got, map[int32]string{
		0: "localhost:20110",
		1: "localhost:20111",
	})
}

func TestNewMemberIngressMapFromContext_invalid(t *testing.T) {
	clusterCtx := client.ClusterContext{IngressEndpoints: "bad"}
	got, err := client.NewMemberIngressMapFromContext(clusterCtx)
	require.Nil(t, got)
	require.EqualError(t, err, "cluster client: endpoint missing '=' separator: bad")
}

func TestIngressPortForMember(t *testing.T) {
	cases := []struct {
		name                       string
		memberID, portBase         int
		portsPerNode, offset, want int
	}{
		{
			name:         "member_0",
			memberID:     0,
			portBase:     client.DefaultIngressPortBase,
			portsPerNode: client.DefaultIngressPortsPerNode,
			offset:       client.DefaultClientFacingPortOffset,
			want:         9002,
		},
		{
			name:         "member_1",
			memberID:     1,
			portBase:     client.DefaultIngressPortBase,
			portsPerNode: client.DefaultIngressPortsPerNode,
			offset:       client.DefaultClientFacingPortOffset,
			want:         9102,
		},
		{
			name:         "member_2",
			memberID:     2,
			portBase:     client.DefaultIngressPortBase,
			portsPerNode: client.DefaultIngressPortsPerNode,
			offset:       client.DefaultClientFacingPortOffset,
			want:         9202,
		},
		{
			name:         "custom_member_1",
			memberID:     1,
			portBase:     8000,
			portsPerNode: 50,
			offset:       2,
			want:         8052,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := client.IngressPortForMember(
				tc.memberID,
				tc.portBase,
				tc.portsPerNode,
				tc.offset,
			)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestFormatIngressEndpointsFromHostnames(t *testing.T) {
	defaultPortBase := client.DefaultIngressPortBase
	defaultPortsPerNode := client.DefaultIngressPortsPerNode
	defaultOffset := client.DefaultClientFacingPortOffset

	cases := []struct {
		name          string
		hostnames     []string
		portBase      int
		portsPerNode  int
		offset        int
		wantFormatted string
	}{
		{
			name:          "default_hostnames",
			hostnames:     []string{"localhost", "localhost", "localhost"},
			wantFormatted: "0=localhost:9002,1=localhost:9102,2=localhost:9202",
		},
		{
			name:          "distinct_hostnames",
			hostnames:     []string{"host-a", "host-b", "host-c"},
			wantFormatted: "0=host-a:9002,1=host-b:9102,2=host-c:9202",
		},
		{
			name:          "single_hostname",
			hostnames:     []string{"localhost"},
			wantFormatted: "0=localhost:9002",
		},
		{
			name:          "empty_hostname",
			hostnames:     []string{""},
			wantFormatted: "0=:9002",
		},
		{name: "nil", hostnames: nil},
		{name: "empty_slice", hostnames: []string{}},
		{
			name:          "custom_params",
			hostnames:     []string{"h0", "h1"},
			portBase:      8000,
			portsPerNode:  50,
			offset:        2,
			wantFormatted: "0=h0:8002,1=h1:8052",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			portBase := defaultPortBase
			portsPerNode := defaultPortsPerNode
			offset := defaultOffset
			if tc.portBase != 0 {
				portBase = tc.portBase
				portsPerNode = tc.portsPerNode
				offset = tc.offset
			}

			formatted := client.FormatIngressEndpointsFromHostnames(
				tc.hostnames,
				portBase,
				portsPerNode,
				offset,
			)
			require.Equal(t, tc.wantFormatted, formatted)

			parsed, err := client.ParseIngressEndpoints(formatted)
			require.NoError(t, err)
			require.NotNil(t, parsed)
			if len(tc.hostnames) == 0 {
				require.Empty(t, parsed)
				return
			}

			requireMemberIngressMap(
				t,
				parsed,
				wantIngressEndpointsFromHostnames(tc.hostnames, portBase, portsPerNode, offset),
			)
		})
	}
}
