//go:build rapid

package cluster_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/election"
)

func drawHostname(t *rapid.T, label string) string {
	t.Helper()
	return rapid.StringMatching(`[a-z][a-z0-9]{0,7}`).Draw(t, label)
}

func drawHostnames(t *rapid.T, n int) []string {
	t.Helper()
	hosts := make([]string, n)
	for i := range hosts {
		hosts[i] = drawHostname(t, "hostname")
	}
	return hosts
}

// drawValidMembersPorts draws memberCount, startingMemberID, and portBase so
// every derived port for those members stays in MinValidPort..MaxValidPort.
func drawValidMembersPorts(t *rapid.T) (memberCount int, startID int32, portBase int) {
	t.Helper()
	memberCount = rapid.IntRange(1, 7).Draw(t, "memberCount")
	startID = rapid.Int32Range(0, 50).Draw(t, "startingMemberID")
	maxID := int(startID) + memberCount - 1
	maxPortBase := cluster.MaxValidPort - maxID*cluster.PortsPerNode - cluster.TransferPortOffset
	require.GreaterOrEqual(t, maxPortBase, 1024)
	portBase = rapid.IntRange(1024, maxPortBase).Draw(t, "portBase")
	return memberCount, startID, portBase
}

func expectedEndpoint(host string, memberID int32, portBase, offset int) string {
	return host + ":" + strconv.Itoa(cluster.CalculatePort(memberID, portBase, offset))
}

func TestBuildClusterMembersString_parseRoundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		memberCount, startID, portBase := drawValidMembersPorts(t)

		ingressHosts := drawHostnames(t, memberCount)
		clusterHosts := drawHostnames(t, memberCount)

		value, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
			StartingMemberID: startID,
			IngressHostnames: ingressHosts,
			ClusterHostnames: clusterHosts,
			PortBase:         portBase,
		})
		require.NoError(t, err)

		members, err := election.ParseMembers(value)
		require.NoError(t, err)
		require.Len(t, members, memberCount)

		for i, m := range members {
			id := startID + int32(i)
			require.Equal(t, id, m.ID)

			endpoints := [...]struct {
				got    string
				host   string
				offset int
			}{
				{m.IngressEndpoint, ingressHosts[i], cluster.ClientFacingPortOffset},
				{m.ConsensusEndpoint, clusterHosts[i], cluster.MemberFacingPortOffset},
				{m.LogEndpoint, clusterHosts[i], cluster.LogPortOffset},
				{m.CatchupEndpoint, clusterHosts[i], cluster.TransferPortOffset},
				{m.ArchiveEndpoint, clusterHosts[i], cluster.ArchiveControlPortOffset},
			}
			for _, ep := range endpoints {
				want := expectedEndpoint(ep.host, id, portBase, ep.offset)
				require.Equal(t, want, ep.got)
			}
		}
	})
}

func TestBuildClusterMembersString_sharedLogEndpoint_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		memberCount, startID, portBase := drawValidMembersPorts(t)

		hosts := drawHostnames(t, memberCount)
		sharedPort := rapid.IntRange(cluster.MinValidPort, cluster.MaxValidPort).Draw(t, "sharedPort")
		shared := drawHostname(t, "sharedHost") + ":" + strconv.Itoa(sharedPort)

		baseCfg := cluster.MembersStringConfig{
			StartingMemberID: startID,
			IngressHostnames: hosts,
			ClusterHostnames: hosts,
			PortBase:         portBase,
		}

		withoutShared, err := cluster.BuildClusterMembersString(baseCfg)
		require.NoError(t, err)

		withSharedCfg := baseCfg
		withSharedCfg.SharedLogEndpoint = shared
		withShared, err := cluster.BuildClusterMembersString(withSharedCfg)
		require.NoError(t, err)

		plainMembers, err := election.ParseMembers(withoutShared)
		require.NoError(t, err)
		sharedMembers, err := election.ParseMembers(withShared)
		require.NoError(t, err)
		require.Len(t, plainMembers, memberCount)
		require.Len(t, sharedMembers, memberCount)

		for i := range sharedMembers {
			require.Equal(t, shared, sharedMembers[i].LogEndpoint)

			// Endpoints embeds log; blank log-derived fields then compare the rest.
			want := plainMembers[i]
			got := sharedMembers[i]
			want.LogEndpoint, got.LogEndpoint = "", ""
			want.Endpoints, got.Endpoints = "", ""
			require.Equal(t, want, got)
		}
	})
}

func TestCalculatePort_monotonic_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		id := rapid.Int32Range(0, 1000).Draw(t, "memberID")
		base := rapid.IntRange(0, 50_000).Draw(t, "portBase")
		offset := rapid.IntRange(0, cluster.PortsPerNode-1).Draw(t, "offset")
		offset2 := rapid.IntRange(0, cluster.PortsPerNode-1).Draw(t, "offset2")

		require.Equal(
			t,
			cluster.PortsPerNode,
			cluster.CalculatePort(id+1, base, offset)-cluster.CalculatePort(id, base, offset),
		)
		require.Equal(
			t,
			offset2-offset,
			cluster.CalculatePort(id, base, offset2)-cluster.CalculatePort(id, base, offset),
		)
	})
}

func TestBuildClusterMembersString_portBounds_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		// PortBase at MaxValidPort or above overflows member 0's first derived port.
		portBase := rapid.IntRange(cluster.MaxValidPort, cluster.MaxValidPort+10_000).Draw(t, "portBase")
		host := drawHostname(t, "hostname")

		got, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
			IngressHostnames: []string{host},
			ClusterHostnames: []string{host},
			PortBase:         portBase,
		})
		require.Error(t, err)
		require.Empty(t, got)
	})
}
