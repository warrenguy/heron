//go:build rapid

package cluster_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
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
