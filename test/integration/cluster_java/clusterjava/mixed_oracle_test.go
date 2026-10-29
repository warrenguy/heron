//go:build integration && cluster_java

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
	"github.com/warrenguy/heron/test/integration/harness"
)

// javaDefaultAppVersion matches Java ConsensusModule.Context default appVersion.
var javaDefaultAppVersion = cnc.ComposeSemanticVersion(0, 0, 1)

// TestMixedHarnessConstants asserts the mixed golden table. The table
// hand-syncs with Java ClusterConstantsMixedTest. Java ClusterConfig helpers
// are authoritative for topology strings.
func TestMixedHarnessConstants(t *testing.T) {
	tests := []struct {
		name string
		got  any
		want any
	}{
		{
			name: "MixedComposeJavaLeaderEnvKey",
			got:  clusterjava.MixedComposeJavaLeaderEnvKey,
			want: "HERON_CLUSTER_MIXED_JAVA_LEADER_COMPOSE",
		},
		{
			name: "MixedComposeGoLeaderEnvKey",
			got:  clusterjava.MixedComposeGoLeaderEnvKey,
			want: "HERON_CLUSTER_MIXED_GO_LEADER_COMPOSE",
		},
		{
			name: "MixedSmokeNameJavaLeader",
			got:  clusterjava.MixedSmokeNameJavaLeader,
			want: "TestClusterMixedJavaLeader",
		},
		{
			name: "MixedSmokeNameGoLeader",
			got:  clusterjava.MixedSmokeNameGoLeader,
			want: "TestClusterMixedGoLeader",
		},
		{
			name: "MixedHostnamesEnvKey",
			got:  clusterjava.MixedHostnamesEnvKey,
			want: "HERON_CLUSTER_HOSTNAMES",
		},
		{
			name: "MixedComposeSkipHintJavaLeader",
			got:  clusterjava.MixedComposeSkipHintJavaLeader,
			want: "cluster mixed requires docker compose; run make smoke-cluster-mixed-java-leader",
		},
		{
			name: "MixedComposeSkipHintGoLeader",
			got:  clusterjava.MixedComposeSkipHintGoLeader,
			want: "cluster mixed requires docker compose; run make smoke-cluster-mixed-go-leader",
		},
		{
			name: "MixedComposeExLeaderRejoinEnvKey",
			got:  clusterjava.MixedComposeExLeaderRejoinEnvKey,
			want: "HERON_CLUSTER_MIXED_EX_LEADER_REJOIN_COMPOSE",
		},
		{
			name: "MixedSmokeNameExLeaderRejoin",
			got:  clusterjava.MixedSmokeNameExLeaderRejoin,
			want: "TestClusterMixedGoExLeaderRejoin",
		},
		{
			name: "MixedComposeSkipHintExLeaderRejoin",
			got:  clusterjava.MixedComposeSkipHintExLeaderRejoin,
			want: "cluster mixed requires docker compose; run make smoke-cluster-mixed-ex-leader-rejoin",
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
			name: "MixedAppointedLeaderIDGoLeader",
			got:  clusterjava.MixedAppointedLeaderIDGoLeader,
			want: int32(2),
		},
		{
			name: "MixedAppointedLeaderIDNone",
			got:  clusterjava.MixedAppointedLeaderIDNone,
			want: int32(-1),
		},
		{
			name: "MixedReadyPath",
			got:  clusterjava.MixedReadyPath,
			want: "/dev/shm/heron-mixed/member-ready",
		},
		{
			name: "MixedStopPathMember1",
			got:  clusterjava.MixedStopPathMember1,
			want: "/dev/shm/heron-mixed/stop-member-1",
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
			name: "ServiceProbeFilename",
			got:  clusterjava.ServiceProbeFilename,
			want: "service-probe",
		},
		{
			name: "ServiceProbeKeyTerm",
			got:  clusterjava.ServiceProbeKeyTerm,
			want: "term",
		},
		{
			name: "ServiceProbeKeyLogPosition",
			got:  clusterjava.ServiceProbeKeyLogPosition,
			want: "logPosition",
		},
		{
			name: "ServiceProbeKeyRole",
			got:  clusterjava.ServiceProbeKeyRole,
			want: "role",
		},
		{
			name: "ServiceProbeKeyMemberID",
			got:  clusterjava.ServiceProbeKeyMemberID,
			want: "memberId",
		},

		{
			name: "MixedLogChannelUnicast",
			got:  clusterjava.MixedLogChannelUnicast,
			want: "aeron:udp?term-length=64k",
		},
		{
			name: "MixedLogEndpoint",
			got:  clusterjava.MixedLogEndpoint,
			want: "239.255.12.15:9004",
		},
		{
			name: "MixedLogChannelMulticast",
			got:  clusterjava.MixedLogChannelMulticast,
			// Template only: no endpoint=. Topology merges MixedLogEndpoint.
			want: "aeron:udp?term-length=64k|interface=172.30.0.0/16|ttl=1",
		},
		{
			name: "MixedClusterMembersJavaLeader",
			got:  clusterjava.MixedClusterMembersJavaLeader,
			want: "0,172.30.0.10:9002,172.30.0.10:9003,172.30.0.10:9004," +
				"172.30.0.10:9005,172.30.0.10:9001|1,172.30.0.11:9102,172.30.0.11:9103," +
				"172.30.0.11:9104,172.30.0.11:9105,172.30.0.11:9101|2,172.30.0.12:9202," +
				"172.30.0.12:9203,172.30.0.12:9204,172.30.0.12:9205,172.30.0.12:9201|",
		},
		{
			name: "MixedClusterMembersGoLeader",
			got:  clusterjava.MixedClusterMembersGoLeader,
			want: "0,172.30.0.10:9002,172.30.0.10:9003,239.255.12.15:9004," +
				"172.30.0.10:9005,172.30.0.10:9001|1,172.30.0.11:9102,172.30.0.11:9103," +
				"239.255.12.15:9004,172.30.0.11:9105,172.30.0.11:9101|2,172.30.0.12:9202," +
				"172.30.0.12:9203,239.255.12.15:9004,172.30.0.12:9205,172.30.0.12:9201|",
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
		{
			name: "MixedTestTimeoutGoLeader",
			got:  clusterjava.MixedTestTimeoutGoLeader,
			want: 120 * time.Second,
		},
		{
			name: "MixedRejoinStartupCanvassTimeout",
			got:  clusterjava.MixedRejoinStartupCanvassTimeout,
			want: 2 * time.Second,
		},
		{
			name: "MixedJavaReelectBudget",
			got:  clusterjava.MixedJavaReelectBudget,
			want: 30 * time.Second,
		},
		{
			name: "MixedRejoinBudget",
			got:  clusterjava.MixedRejoinBudget,
			want: 30 * time.Second,
		},
		{
			name: "MixedTestTimeoutExLeaderRejoin",
			got:  clusterjava.MixedTestTimeoutExLeaderRejoin,
			want: 300 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.got)
		})
	}
}

// TestClusterJavaPorts_matchHarnessReservation asserts the harness port matrix
// reserves the block this package binds.
//
// Harness restates the port base, member count, and mixed log endpoint. The
// matrix must build under the integration tag alone, where this package does
// not compile. harness.TestClusterPortBlocks_doNotOverlap keeps that block
// disjoint from every other live reservation.
//
// Endpoint placement inside the block needs no separate check because
// TestMixedComposeClusterMembers_matchesDefaultGoldens asserts both goldens
// against BuildClusterMembersString over the same base, so every port is
// derived.
func TestClusterJavaPorts_matchHarnessReservation(t *testing.T) {
	require.Equal(t, clusterjava.PortBase, harness.ClusterJavaPortBase)
	require.Equal(t, clusterjava.MixedLogEndpoint, harness.ClusterJavaMixedLogEndpoint)
	require.Equal(t, clusterjava.MixedMemberCount, harness.ClusterJavaMemberCount)
	require.Equal(t, clusterjava.FailoverMemberCount, harness.ClusterJavaMemberCount)
}

func TestMixedRequiredSmoke(t *testing.T) {
	// required is always name != "". Table checks only the name.
	tests := []struct {
		name     string
		env      map[string]string
		wantName string
	}{
		{
			name:     "neither",
			env:      nil,
			wantName: "",
		},
		{
			name: "javaGateOne",
			env: map[string]string{
				clusterjava.MixedComposeJavaLeaderEnvKey: "1",
			},
			wantName: clusterjava.MixedSmokeNameJavaLeader,
		},
		{
			name: "goGateOne",
			env: map[string]string{
				clusterjava.MixedComposeGoLeaderEnvKey: "1",
			},
			wantName: clusterjava.MixedSmokeNameGoLeader,
		},
		{
			name: "javaGateZero",
			env: map[string]string{
				clusterjava.MixedComposeJavaLeaderEnvKey: "0",
			},
			wantName: "",
		},
		{
			name: "goGateZero",
			env: map[string]string{
				clusterjava.MixedComposeGoLeaderEnvKey: "0",
			},
			wantName: "",
		},
		{
			name: "bothGatesOne",
			env: map[string]string{
				clusterjava.MixedComposeJavaLeaderEnvKey: "1",
				clusterjava.MixedComposeGoLeaderEnvKey:   "1",
			},
			wantName: clusterjava.MixedSmokeNameJavaLeader,
		},
		{
			name: "exLeaderRejoinEnvOne",
			env: map[string]string{
				clusterjava.MixedComposeExLeaderRejoinEnvKey: "1",
			},
			wantName: clusterjava.MixedSmokeNameExLeaderRejoin,
		},
		{
			name: "exLeaderRejoinEnvZero",
			env: map[string]string{
				clusterjava.MixedComposeExLeaderRejoinEnvKey: "0",
			},
			wantName: "",
		},
		{
			name: "javaEnvBeforeRejoin",
			env: map[string]string{
				clusterjava.MixedComposeJavaLeaderEnvKey:     "1",
				clusterjava.MixedComposeExLeaderRejoinEnvKey: "1",
			},
			wantName: clusterjava.MixedSmokeNameJavaLeader,
		},
		{
			name: "goEnvBeforeRejoin",
			env: map[string]string{
				clusterjava.MixedComposeGoLeaderEnvKey:       "1",
				clusterjava.MixedComposeExLeaderRejoinEnvKey: "1",
			},
			wantName: clusterjava.MixedSmokeNameGoLeader,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getEnv := func(key string) string {
				return tt.env[key]
			}
			gotName, gotRequired := clusterjava.MixedRequiredSmoke(getEnv)
			require.Equal(t, tt.wantName, gotName)
			require.Equal(t, tt.wantName != "", gotRequired)
		})
	}
}

func TestMixedTopologyInvariants(t *testing.T) {
	require.Len(t, clusterjava.MixedMemberHostnames(), clusterjava.MixedMemberCount)

	require.GreaterOrEqual(t, clusterjava.MixedAppointedLeaderIDJavaLeader, int32(0))
	require.Less(t, clusterjava.MixedAppointedLeaderIDJavaLeader, int32(clusterjava.MixedMemberCount))

	require.GreaterOrEqual(t, clusterjava.MixedAppointedLeaderIDGoLeader, int32(0))
	require.Less(t, clusterjava.MixedAppointedLeaderIDGoLeader, int32(clusterjava.MixedMemberCount))

	require.GreaterOrEqual(t, clusterjava.MixedGoMemberID, int32(0))
	require.Less(t, clusterjava.MixedGoMemberID, int32(clusterjava.MixedMemberCount))

	// java-leader appoints a Java member. Must not equal the Go member id.
	require.NotEqual(t, clusterjava.MixedGoMemberID, clusterjava.MixedAppointedLeaderIDJavaLeader)

	// go-leader appoints the Go member.
	require.Equal(t, clusterjava.MixedGoMemberID, clusterjava.MixedAppointedLeaderIDGoLeader)
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

func TestMixedLogChannelMulticast_templateHasInterfaceAndTtlNoEndpoint(t *testing.T) {
	ch := clusterjava.MixedLogChannelMulticast
	require.True(t, strings.HasPrefix(ch, "aeron:udp?term-length=64k"))
	require.Contains(t, ch, "interface=172.30.0.0/16")
	require.Contains(t, ch, "ttl=1")

	// endpoint= is forbidden on cluster.Config.LogChannel; members string carries it.
	require.NotContains(t, ch, "endpoint=")
}

func TestMixedClusterMembersGoLeader_sharesLogEndpoint(t *testing.T) {
	got, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		IngressHostnames:  clusterjava.MixedMemberHostnames(),
		ClusterHostnames:  clusterjava.MixedMemberHostnames(),
		PortBase:          clusterjava.PortBase,
		SharedLogEndpoint: clusterjava.MixedLogEndpoint,
	})
	require.NoError(t, err)
	require.Equal(t, clusterjava.MixedClusterMembersGoLeader, got)
}

func TestMixedReadyPath_outsideAeronDirs(t *testing.T) {
	for _, path := range []string{
		clusterjava.MixedReadyPath,
		clusterjava.MixedStopPathMember1,
	} {
		require.False(t, strings.HasPrefix(path, clusterjava.MixedAeronDirMember0))
		require.False(t, strings.HasPrefix(path, clusterjava.MixedAeronDirMember1))
		require.False(t, strings.HasPrefix(path, clusterjava.MixedAeronDirGo))
		require.True(t, strings.HasPrefix(path, "/dev/shm/heron-mixed/"))
	}
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

func TestMixedComposeHostnames_defaultsWithoutEnv(t *testing.T) {
	t.Setenv(clusterjava.MixedHostnamesEnvKey, "")
	got, err := clusterjava.MixedComposeHostnames()
	require.NoError(t, err)
	require.Equal(t, clusterjava.MixedMemberHostnames(), got)
}

func TestMixedComposeHostnames_readsEnv(t *testing.T) {
	t.Setenv(clusterjava.MixedHostnamesEnvKey, "172.30.42.10,172.30.42.11,172.30.42.12")
	got, err := clusterjava.MixedComposeHostnames()
	require.NoError(t, err)
	require.Equal(t, []string{
		"172.30.42.10",
		"172.30.42.11",
		"172.30.42.12",
	}, got)
}

func TestMixedComposeHostnames_rejectsBadEnv(t *testing.T) {
	t.Setenv(clusterjava.MixedHostnamesEnvKey, "only-one-host")
	_, err := clusterjava.MixedComposeHostnames()
	require.Error(t, err)
	require.Contains(t, err.Error(), "exactly 3 hostnames")

	t.Setenv(clusterjava.MixedHostnamesEnvKey, "a,,c")
	_, err = clusterjava.MixedComposeHostnames()
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty hostname")

	t.Setenv(clusterjava.MixedHostnamesEnvKey, "a,b,c,d")
	_, err = clusterjava.MixedComposeHostnames()
	require.Error(t, err)
	require.Contains(t, err.Error(), "exactly 3 hostnames")
}

func TestMixedComposeClusterMembers_matchesDefaultGoldens(t *testing.T) {
	t.Setenv(clusterjava.MixedHostnamesEnvKey, "")

	javaLeader, err := clusterjava.MixedComposeClusterMembers("")
	require.NoError(t, err)
	require.Equal(t, clusterjava.MixedClusterMembersJavaLeader, javaLeader)

	goLeader, err := clusterjava.MixedComposeClusterMembers(clusterjava.MixedLogEndpoint)
	require.NoError(t, err)
	require.Equal(t, clusterjava.MixedClusterMembersGoLeader, goLeader)
}

func TestMixedComposeClusterMembers_usesEnvHosts(t *testing.T) {
	t.Setenv(clusterjava.MixedHostnamesEnvKey, "172.30.7.10,172.30.7.11,172.30.7.12")

	got, err := clusterjava.MixedComposeClusterMembers("")
	require.NoError(t, err)
	want, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		IngressHostnames: []string{"172.30.7.10", "172.30.7.11", "172.30.7.12"},
		ClusterHostnames: []string{"172.30.7.10", "172.30.7.11", "172.30.7.12"},
	})
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Contains(t, got, "172.30.7.10")
	require.NotContains(t, got, "172.30.0.10")
}

func TestMixedComposeIngressAndEgress_useEnvHosts(t *testing.T) {
	t.Setenv(clusterjava.MixedHostnamesEnvKey, "172.30.9.10,172.30.9.11,172.30.9.12")

	ingress, err := clusterjava.MixedComposeIngressEndpoints()
	require.NoError(t, err)
	require.Equal(t, "0=172.30.9.10:9002,1=172.30.9.11:9102", ingress)

	egress, err := clusterjava.MixedComposeEgressChannel()
	require.NoError(t, err)
	require.Equal(t, "aeron:udp?endpoint=172.30.9.12:0|term-length=64k", egress)
}

func TestMixedComposeIngressAndEgress_rejectBadEnv(t *testing.T) {
	t.Setenv(clusterjava.MixedHostnamesEnvKey, "only-one-host")

	_, err := clusterjava.MixedComposeIngressEndpoints()
	require.Error(t, err)
	_, err = clusterjava.MixedComposeEgressChannel()
	require.Error(t, err)
	_, err = clusterjava.MixedComposeClusterMembers("")
	require.Error(t, err)
}

func TestMixedComposeReplicationChannel_usesEnvHosts(t *testing.T) {
	t.Setenv(clusterjava.MixedHostnamesEnvKey, "")
	got, err := clusterjava.MixedComposeReplicationChannel()
	require.NoError(t, err)
	require.Equal(t, "aeron:udp?endpoint=172.30.0.12:0", got)

	t.Setenv(clusterjava.MixedHostnamesEnvKey, "172.30.9.10,172.30.9.11,172.30.9.12")
	got, err = clusterjava.MixedComposeReplicationChannel()
	require.NoError(t, err)
	require.Equal(t, "aeron:udp?endpoint=172.30.9.12:0", got)

	t.Setenv(clusterjava.MixedHostnamesEnvKey, "only-one-host")
	_, err = clusterjava.MixedComposeReplicationChannel()
	require.Error(t, err)
	require.Contains(t, err.Error(), "exactly 3 hostnames")
}

func TestMixedStackedBudgets_fitInsideTestTimeout(t *testing.T) {
	// 2x MixedCommitBudget: commit advance, then recorded-position reach.
	stackedJavaLeader := clusterjava.MixedStartupBudget +
		clusterjava.MixedElectionBudget +
		clusterjava.MixedConnectBudget +
		2*clusterjava.MixedCommitBudget
	require.Less(t, stackedJavaLeader, clusterjava.MixedTestTimeoutJavaLeader)

	// go-leader has no client connect wait. Service probe reuses MixedElectionBudget.
	stackedGoLeader := clusterjava.MixedStartupBudget +
		clusterjava.MixedElectionBudget +
		clusterjava.MixedElectionBudget +
		2*clusterjava.MixedCommitBudget
	require.Less(t, stackedGoLeader, clusterjava.MixedTestTimeoutGoLeader)

	// Ex-leader rejoin stack: startup twice, election twice, the Java term, the
	// rejoin, the client connect, the offer bound, then commit four times.
	stackedExLeaderRejoin := 2*clusterjava.MixedStartupBudget +
		2*clusterjava.MixedElectionBudget +
		clusterjava.MixedJavaReelectBudget +
		clusterjava.MixedRejoinBudget +
		clusterjava.MixedConnectBudget +
		harness.ConnectedPollTimeout +
		4*clusterjava.MixedCommitBudget
	require.Equal(t, 270*time.Second, stackedExLeaderRejoin)
	require.Less(t, stackedExLeaderRejoin, clusterjava.MixedTestTimeoutExLeaderRejoin)
}

func TestServiceProbePath_underMemberBaseDir(t *testing.T) {
	require.Equal(t, "/tmp/heron-cluster-0/service-probe", clusterjava.ServiceProbePath(0))
	require.Equal(t, "/tmp/heron-cluster-1/service-probe", clusterjava.ServiceProbePath(1))
}

func TestParseServiceProbe_valid(t *testing.T) {
	tests := []struct {
		name string
		data string
		want clusterjava.ServiceProbe
	}{
		{
			name: "followerFields",
			data: "term=3\nlogPosition=64\nrole=FOLLOWER\nmemberId=1\n",
			want: clusterjava.ServiceProbe{
				Term: 3, LogPosition: 64, Role: "FOLLOWER", MemberID: 1,
			},
		},
		{
			name: "zeroLogPosition",
			data: "term=0\nlogPosition=0\nrole=FOLLOWER\nmemberId=0\n",
			want: clusterjava.ServiceProbe{
				Term: 0, LogPosition: 0, Role: "FOLLOWER", MemberID: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe, err := clusterjava.ParseServiceProbe([]byte(tt.data))
			require.NoError(t, err)
			require.Equal(t, tt.want, probe)
		})
	}
}

func TestParseServiceProbe_rejectMatrix(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		wantSub string
	}{
		{
			name:    "missingMemberId",
			data:    "term=1\nlogPosition=0\nrole=FOLLOWER\n",
			wantSub: "memberId",
		},
		{
			name:    "unknownKey",
			data:    "term=1\nlogPosition=0\nrole=FOLLOWER\nmemberId=0\nextra=1\n",
			wantSub: "unknown key",
		},
		{
			name:    "emptyRole",
			data:    "term=1\nlogPosition=0\nrole=\nmemberId=0\n",
			wantSub: "role",
		},
		{
			name:    "malformedLine",
			data:    "not-a-pair\nterm=1\nlogPosition=0\nrole=FOLLOWER\nmemberId=0\n",
			wantSub: "key=value",
		},
		{
			name:    "badTerm",
			data:    "term=x\nlogPosition=0\nrole=FOLLOWER\nmemberId=0\n",
			wantSub: "term",
		},
		{
			name:    "badLogPosition",
			data:    "term=1\nlogPosition=nope\nrole=FOLLOWER\nmemberId=0\n",
			wantSub: "logPosition",
		},
		{
			name:    "badMemberId",
			data:    "term=1\nlogPosition=0\nrole=FOLLOWER\nmemberId=z\n",
			wantSub: "memberId",
		},
		{
			name:    "emptyBody",
			data:    "",
			wantSub: "missing key",
		},
		{
			name:    "emptyKey",
			data:    "=1\nterm=1\nlogPosition=0\nrole=FOLLOWER\nmemberId=0\n",
			wantSub: "key=value",
		},
		{
			name:    "duplicateTerm",
			data:    "term=1\nterm=2\nlogPosition=0\nrole=FOLLOWER\nmemberId=0\n",
			wantSub: "duplicate key",
		},
		{
			name:    "duplicateRole",
			data:    "term=1\nlogPosition=0\nrole=FOLLOWER\nrole=LEADER\nmemberId=0\n",
			wantSub: "duplicate key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := clusterjava.ParseServiceProbe([]byte(tt.data))
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantSub)
		})
	}
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
