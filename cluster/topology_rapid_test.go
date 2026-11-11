//go:build rapid

package cluster_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/client"
	"github.com/warrenguy/luas/internal/testutil"
)

func TestTopology_memberPointerIdentity_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		memberCount, startID, portBase := drawValidMembersPorts(t)

		hosts := drawHostnames(t, memberCount)
		value, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
			StartingMemberID: startID,
			IngressHostnames: hosts,
			ClusterHostnames: hosts,
			PortBase:         portBase,
		})
		require.NoError(t, err)

		thisIdx := rapid.IntRange(0, memberCount-1).Draw(t, "thisIdx")
		memberID := startID + int32(thisIdx)

		tp, err := cluster.NewTopology(topologyConfigMembers(memberID, value))
		require.NoError(t, err)

		members := tp.Members()
		require.Len(t, members, memberCount)
		this := tp.ThisMember()
		require.Same(t, members[thisIdx], this)
		require.Equal(t, memberID, this.ID)

		// All member pointers distinct.
		for i := range members {
			for j := i + 1; j < len(members); j++ {
				require.NotSame(t, members[i], members[j])
			}
		}

		peers := tp.PeerConsensusChannels()
		require.Len(t, peers, memberCount-1)
		_, selfPresent := peers[memberID]
		require.False(t, selfPresent)

		for i, m := range members {
			if i == thisIdx {
				_, ok := tp.ConsensusChannelFor(m.ID)
				require.False(t, ok)
				continue
			}
			ch, ok := tp.ConsensusChannelFor(m.ID)
			require.True(t, ok)
			require.Equal(t, peers[m.ID], ch)
			idx := aeron.ChannelEndpointParamIndex(ch)
			require.GreaterOrEqual(t, idx, 0)
			require.Equal(
				t,
				m.ConsensusEndpoint,
				aeron.ChannelParamValue(ch, idx, aeron.ChannelEndpointParam),
			)
		}

		// IngressEndpoints matches client wire format, sorted by member id.
		wantIngress := make(client.MemberIngressMap, len(members))
		for _, m := range members {
			wantIngress[m.ID] = &client.MemberIngress{Endpoint: m.IngressEndpoint}
		}
		require.Equal(t, client.FormatIngressEndpoints(wantIngress), tp.IngressEndpoints())
	})
}

func TestBuildChannelWithEndpoint_idempotent_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		// Draw 0-3 non-endpoint params for a udp template.
		paramCount := rapid.IntRange(0, 3).Draw(t, "paramCount")
		params := make([]string, 0, paramCount)
		used := make(map[string]struct{}, paramCount)
		for i := range paramCount {
			key := rapid.StringMatching(`[a-z]{1,8}`).Filter(func(s string) bool {
				if s == "endpoint" {
					return false
				}
				_, exists := used[s]
				return !exists
			}).Draw(t, fmt.Sprintf("paramKey%d", i))
			used[key] = struct{}{}
			val := rapid.StringMatching(`[a-z0-9]{1,8}`).Draw(t, fmt.Sprintf("paramVal%d", i))
			params = append(params, key+"="+val)
		}

		channel := "aeron:udp"
		if len(params) > 0 {
			channel = "aeron:udp?" + strings.Join(params, "|")
		}

		host := drawHostname(t, "host")
		port := rapid.IntRange(cluster.MinValidPort, cluster.MaxValidPort).Draw(t, "port")
		endpoint := host + ":" + strconv.Itoa(port)

		once, err := cluster.BuildChannelWithEndpoint(channel, endpoint)
		require.NoError(t, err)
		twice, err := cluster.BuildChannelWithEndpoint(once, endpoint)
		require.NoError(t, err)
		require.Equal(t, once, twice)

		idx := aeron.ChannelEndpointParamIndex(once)
		require.GreaterOrEqual(t, idx, 0)
		require.Equal(t, endpoint, aeron.ChannelParamValue(once, idx, aeron.ChannelEndpointParam))

		for _, p := range params {
			require.Contains(t, once, p)
		}
	})
}

// TestNewTopology_logMode_rapid asserts the log channels for shared and
// distinct log endpoints, and the error for partly shared ones.
func TestNewTopology_logMode_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		memberCount, startID, portBase := drawValidMembersPorts(rt)
		thisIdx := rapid.IntRange(0, memberCount-1).Draw(rt, "thisIdx")
		hosts := drawHostnames(rt, memberCount)

		modeRoll := rapid.IntRange(0, 99).Draw(rt, "modeRoll")
		mode := "mdc"
		switch {
		case memberCount < 2 || modeRoll < 35:
			mode = "shared"
		case memberCount >= 3 && modeRoll < 50:
			mode = "partial"
		}

		logs := make([]string, memberCount)
		for i := range logs {
			logs[i] = expectedEndpoint(hosts[i], startID+int32(i), portBase, cluster.LogPortOffset)
		}
		switch mode {
		case "shared":
			for i := range logs {
				logs[i] = logs[0]
			}
		case "partial":
			logs[1] = logs[0]
		}

		var members string
		for i, host := range hosts {
			id := startID + int32(i)
			members += memberSpec(id, [endpointSlotCount]string{
				expectedEndpoint(host, id, portBase, cluster.ClientFacingPortOffset),
				expectedEndpoint(host, id, portBase, cluster.MemberFacingPortOffset),
				logs[i],
				expectedEndpoint(host, id, portBase, cluster.TransferPortOffset),
				expectedEndpoint(host, id, portBase, cluster.ArchiveControlPortOffset),
			})
		}

		tp, err := cluster.NewTopology(topologyConfigMembers(startID+int32(thisIdx), members))
		if mode == "partial" {
			require.EqualError(
				rt,
				err,
				"cluster: ClusterMembers log endpoints must be all equal or all distinct",
			)
			return
		}
		require.NoError(rt, err)
		require.Equal(rt, mode == "mdc", tp.IsLogMDC())

		if mode == "shared" {
			require.Equal(rt, tp.LogChannel(), tp.LogPublicationChannel())
			require.Nil(rt, tp.LogPublicationDestinations())
			return
		}

		endpoint := tp.ThisMember().LogEndpoint
		control := wantControlEndpoint(testutil.IterationTB(t, rt), endpoint)
		require.Equal(
			rt,
			"aeron:udp?term-length=64k|control-mode=manual|control="+control+"|ssc=false",
			tp.LogPublicationChannel(),
		)

		want := make([]string, 0, memberCount-1)
		for _, member := range tp.Members() {
			if member == tp.ThisMember() {
				continue
			}
			want = append(want, "aeron:udp?endpoint="+member.LogEndpoint)
			require.NotEqual(rt, endpoint, member.LogEndpoint)
		}
		require.Len(rt, want, memberCount-1)
		got := tp.LogPublicationDestinations()
		require.Equal(rt, want, got)
		got[0] = "mutated"
		require.Equal(rt, want, tp.LogPublicationDestinations())
	})
}

// wantControlEndpoint returns endpoint with port 0. A manual
// multi-destination-cast log publication uses it as its control endpoint.
func wantControlEndpoint(tb testing.TB, endpoint string) string {
	tb.Helper()
	colon := strings.LastIndex(endpoint, ":")
	require.GreaterOrEqual(tb, colon, 0)
	return endpoint[:colon+1] + "0"
}
