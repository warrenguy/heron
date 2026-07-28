//go:build integration && cluster_java

// Golden tables hand-sync with ClusterConstantsMixedTest.
// Java ClusterConfig helpers are authoritative for topology strings.
package clusterjava_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
)

// javaDefaultAppVersion matches Java ConsensusModule.Context default appVersion.
var javaDefaultAppVersion = cnc.ComposeSemanticVersion(0, 0, 1)

func TestMixedHarnessConstants(t *testing.T) {
	tests := []struct {
		name string
		got  any
		want any
	}{
		{
			name: "MixedComposeGateEnvKey",
			got:  clusterjava.MixedComposeGateEnvKey,
			want: "HERON_CLUSTER_MIXED_COMPOSE",
		},
		{
			name: "MixedComposeSkipHint",
			got:  clusterjava.MixedComposeSkipHint,
			want: "cluster mixed requires docker compose; run make test-cluster-mixed",
		},
		{name: "PortBase", got: clusterjava.PortBase, want: 9000},
		{name: "MixedMemberCount", got: clusterjava.MixedMemberCount, want: 3},
		{name: "MixedGoMemberID", got: clusterjava.MixedGoMemberID, want: int32(2)},
		{
			name: "MixedAppointedLeaderIDJavaLeader",
			got:  clusterjava.MixedAppointedLeaderIDJavaLeader,
			want: int32(0),
		},
		{
			name: "MixedReadyPath",
			got:  clusterjava.MixedReadyPath,
			want: "/dev/shm/heron-mixed/member-ready",
		},
		{
			name: "MixedIngressEndpoints",
			got:  clusterjava.MixedIngressEndpoints,
			want: "0=172.30.0.10:9002,1=172.30.0.11:9102",
		},
		{
			name: "MixedIngressChannel",
			got:  clusterjava.MixedIngressChannel,
			want: "aeron:udp?term-length=64k",
		},
		{
			name: "MixedEgressChannel",
			got:  clusterjava.MixedEgressChannel,
			want: "aeron:udp?endpoint=172.30.0.12:0|term-length=64k",
		},
		{name: "MixedMember0Address", got: clusterjava.MixedMember0Address, want: "172.30.0.10"},
		{name: "MixedMember1Address", got: clusterjava.MixedMember1Address, want: "172.30.0.11"},
		{name: "MixedMember2Address", got: clusterjava.MixedMember2Address, want: "172.30.0.12"},
		{name: "MixedAeronMDAddress", got: clusterjava.MixedAeronMDAddress, want: "172.30.0.12"},
		{name: "MixedAeronDirMember0", got: clusterjava.MixedAeronDirMember0, want: "/dev/shm/aeron-j0"},
		{name: "MixedAeronDirMember1", got: clusterjava.MixedAeronDirMember1, want: "/dev/shm/aeron-j1"},
		{name: "MixedAeronDirGo", got: clusterjava.MixedAeronDirGo, want: "/dev/shm/aeron-go"},
		{
			name: "MixedBaseDirPrefix",
			got:  clusterjava.MixedBaseDirPrefix,
			want: "/tmp/heron-cluster-",
		},
		{
			name: "MixedClusterDirMember0",
			got:  clusterjava.MixedClusterDirMember0,
			want: "/tmp/heron-cluster-0",
		},
		{
			name: "MixedClusterDirMember1",
			got:  clusterjava.MixedClusterDirMember1,
			want: "/tmp/heron-cluster-1",
		},
		{
			name: "MixedClusterDirGo",
			got:  clusterjava.MixedClusterDirGo,
			want: "/tmp/heron-cluster-2",
		},
		{
			name: "MixedLogChannelUnicast",
			got:  clusterjava.MixedLogChannelUnicast,
			want: "aeron:udp?term-length=64k",
		},
		{
			name: "MixedClusterMembersJavaLeader",
			got:  clusterjava.MixedClusterMembersJavaLeader,
			want: "0,172.30.0.10:9002,172.30.0.10:9003,172.30.0.10:9004," +
				"172.30.0.10:9005,172.30.0.10:9001|1,172.30.0.11:9102,172.30.0.11:9103," +
				"172.30.0.11:9104,172.30.0.11:9105,172.30.0.11:9101|2,172.30.0.12:9202," +
				"172.30.0.12:9203,172.30.0.12:9204,172.30.0.12:9205,172.30.0.12:9201|",
		},
		{name: "MixedStartupBudget", got: clusterjava.MixedStartupBudget, want: 30 * time.Second},
		{name: "MixedElectionBudget", got: clusterjava.MixedElectionBudget, want: 20 * time.Second},
		{name: "MixedConnectBudget", got: clusterjava.MixedConnectBudget, want: 20 * time.Second},
		{name: "MixedCommitBudget", got: clusterjava.MixedCommitBudget, want: 20 * time.Second},
		{
			name: "MixedTestTimeoutJavaLeader",
			got:  clusterjava.MixedTestTimeoutJavaLeader,
			want: 120 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.got)
		})
	}
}

func TestMixedTopologyInvariants(t *testing.T) {
	require.Len(t, clusterjava.MixedMemberHostnames(), clusterjava.MixedMemberCount)

	require.GreaterOrEqual(t, clusterjava.MixedAppointedLeaderIDJavaLeader, int32(0))
	require.Less(t, clusterjava.MixedAppointedLeaderIDJavaLeader, int32(clusterjava.MixedMemberCount))

	require.GreaterOrEqual(t, clusterjava.MixedGoMemberID, int32(0))
	require.Less(t, clusterjava.MixedGoMemberID, int32(clusterjava.MixedMemberCount))

	// Appointed leader must be a Java member. Must not equal the Go member id.
	require.NotEqual(t, clusterjava.MixedGoMemberID, clusterjava.MixedAppointedLeaderIDJavaLeader)
}

func TestMixedIngressEndpoints_omitsGoMember(t *testing.T) {
	port0 := cluster.CalculatePort(
		0,
		clusterjava.PortBase,
		cluster.ClientFacingPortOffset,
	)
	port1 := cluster.CalculatePort(
		1,
		clusterjava.PortBase,
		cluster.ClientFacingPortOffset,
	)
	want := fmt.Sprintf(
		"0=%s:%d,1=%s:%d",
		clusterjava.MixedMember0Address,
		port0,
		clusterjava.MixedMember1Address,
		port1,
	)

	require.Equal(t, want, clusterjava.MixedIngressEndpoints)
	require.NotContains(t, clusterjava.MixedIngressEndpoints, "2=")
	require.NotContains(t, clusterjava.MixedIngressEndpoints, clusterjava.MixedMember2Address)
}

func TestMixedEgressChannel_usesSharedDriverAddress(t *testing.T) {
	wantEndpoint := "endpoint=" + clusterjava.MixedAeronMDAddress + ":0"
	require.Contains(t, clusterjava.MixedEgressChannel, wantEndpoint)
	require.NotContains(t, clusterjava.MixedEgressChannel, "127.0.0.1")
	require.NotContains(t, clusterjava.MixedEgressChannel, "localhost")
	require.NotContains(t, clusterjava.MixedEgressChannel, "endpoint=0.0.0.0")
}

func TestMixedIngressChannel_noEndpointQuery(t *testing.T) {
	require.NotContains(t, clusterjava.MixedIngressChannel, "?endpoint=")
	require.NotContains(t, clusterjava.MixedIngressChannel, "|endpoint=")
}

func TestMixedLogChannelUnicast_noMulticastParams(t *testing.T) {
	require.NotContains(t, clusterjava.MixedLogChannelUnicast, "interface=")
	require.NotContains(t, clusterjava.MixedLogChannelUnicast, "ttl=")
	require.NotContains(t, clusterjava.MixedLogChannelUnicast, "endpoint=")
}

func TestMixedReadyPath_outsideAeronDirs(t *testing.T) {
	require.False(t, strings.HasPrefix(clusterjava.MixedReadyPath, clusterjava.MixedAeronDirMember0))
	require.False(t, strings.HasPrefix(clusterjava.MixedReadyPath, clusterjava.MixedAeronDirMember1))
	require.False(t, strings.HasPrefix(clusterjava.MixedReadyPath, clusterjava.MixedAeronDirGo))
	require.True(t, strings.HasPrefix(clusterjava.MixedReadyPath, "/dev/shm/heron-mixed/"))
}

func TestMixedMemberHostnames_returnsFreshSlice(t *testing.T) {
	a := clusterjava.MixedMemberHostnames()
	b := clusterjava.MixedMemberHostnames()
	want := []string{
		clusterjava.MixedMember0Address,
		clusterjava.MixedMember1Address,
		clusterjava.MixedMember2Address,
	}
	require.Equal(t, want, a)
	require.Equal(t, a, b)

	a[0] = "mutated"
	require.Equal(t, want, b)
	require.Equal(t, want, clusterjava.MixedMemberHostnames())
}

func TestMixedStackedBudgets_fitInsideTestTimeout(t *testing.T) {
	stacked := clusterjava.MixedStartupBudget +
		clusterjava.MixedElectionBudget +
		clusterjava.MixedConnectBudget +
		clusterjava.MixedCommitBudget
	require.Less(t, stacked, clusterjava.MixedTestTimeoutJavaLeader)
}

// TestMixedAppVersion_matchesJavaMajor asserts Go DefaultElectionConfig AppVersion
// major matches the Java default major. Full version equality is not required.
func TestMixedAppVersion_matchesJavaMajor(t *testing.T) {
	require.Equal(
		t,
		cnc.ComposeSemanticVersion(0, 0, 1),
		javaDefaultAppVersion,
		"javaDefaultAppVersion must equal SemanticVersion.compose(0, 0, 1)",
	)

	def := election.DefaultElectionConfig()
	require.Equal(
		t,
		cnc.MajorVersion(javaDefaultAppVersion),
		cnc.MajorVersion(def.AppVersion),
		"DefaultElectionConfig.AppVersion major must match Java default major "+
			"(javaAppVersion=%d goAppVersion=%d; full equality not required)",
		javaDefaultAppVersion,
		def.AppVersion,
	)
}
