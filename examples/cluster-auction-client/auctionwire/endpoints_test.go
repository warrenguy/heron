package auctionwire_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/examples/cluster-auction-client/auctionwire"
)

func TestIngressEndpoints(t *testing.T) {
	cases := []struct {
		name          string
		hostnames     []string
		wantFormatted string
		wantEndpoints []string
	}{
		{
			name:          "default_hostnames",
			hostnames:     []string{"localhost", "localhost", "localhost"},
			wantFormatted: "0=localhost:9002,1=localhost:9102,2=localhost:9202",
			wantEndpoints: []string{"localhost:9002", "localhost:9102", "localhost:9202"},
		},
		{
			name:          "distinct_hostnames",
			hostnames:     []string{"host-a", "host-b", "host-c"},
			wantFormatted: "0=host-a:9002,1=host-b:9102,2=host-c:9202",
			wantEndpoints: []string{"host-a:9002", "host-b:9102", "host-c:9202"},
		},
		{name: "nil", hostnames: nil},
		{name: "empty_slice", hostnames: []string{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			formatted := auctionwire.IngressEndpoints(
				tc.hostnames,
				auctionwire.DefaultPortBase,
				auctionwire.DefaultPortsPerNode,
				auctionwire.DefaultClientFacingPortOffset,
			)
			require.Equal(t, tc.wantFormatted, formatted)
			if len(tc.wantEndpoints) == 0 {
				return
			}

			parsed, err := client.ParseIngressEndpoints(formatted)
			require.NoError(t, err)
			require.Len(t, parsed, len(tc.wantEndpoints))
			for i, wantEndpoint := range tc.wantEndpoints {
				ingress := parsed[int32(i)]
				require.NotNil(t, ingress)
				require.Equal(t, int32(i), ingress.MemberID)
				require.Equal(t, wantEndpoint, ingress.Endpoint)
			}
		})
	}
}
