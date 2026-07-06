package client_test

import (
	"strconv"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/client"
)

func wantIngressEndpointsFromHostnames(
	hostnames []string,
	portBase, portsPerNode, offset int,
) map[int32]string {
	want := make(map[int32]string, len(hostnames))
	for i, hostname := range hostnames {
		port := client.IngressPortForMember(i, portBase, portsPerNode, offset)
		want[int32(i)] = hostname + ":" + strconv.Itoa(port)
	}
	return want
}

func requireMemberIngressMap(
	t require.TestingT,
	got client.MemberIngressMap,
	want map[int32]string,
) {
	if helper, ok := t.(interface{ Helper() }); ok {
		helper.Helper()
	}
	wantMap := make(client.MemberIngressMap, len(want))
	for memberID, endpoint := range want {
		wantMap[memberID] = &client.MemberIngress{
			MemberID: memberID,
			Endpoint: endpoint,
		}
	}
	requireMemberIngressMapsEqual(t, got, wantMap)
}

func requireMemberIngressMapsEqual(
	t require.TestingT,
	got, want client.MemberIngressMap,
) {
	if helper, ok := t.(interface{ Helper() }); ok {
		helper.Helper()
	}
	require.Len(t, got, len(want))
	for memberID, wantIngress := range want {
		gotIngress, ok := got.Lookup(memberID)
		require.True(t, ok)
		require.Equal(t, wantIngress.MemberID, gotIngress.MemberID)
		require.Equal(t, wantIngress.Endpoint, gotIngress.Endpoint)
	}
}
