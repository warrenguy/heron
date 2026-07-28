//go:build integration && cluster_java

package clusterjava

import (
	"time"

	"github.com/warrenguy/heron/cluster"
)

// Mixed Go/Java multi-node goldens. Hand-sync with ClusterConstantsMixedTest.
// Java ClusterConfig helpers stay authoritative for topology strings.
// Appointed leader is member 0 (Java). Go member is id 2.

const (
	MixedComposeGateEnvKey = "HERON_CLUSTER_MIXED_COMPOSE"
	MixedComposeSkipHint   = "cluster mixed " + composeSkipRunMake + "test-cluster-mixed"

	MixedMemberCount = 3
	MixedGoMemberID  = int32(2)

	// MixedAppointedLeaderIDJavaLeader is the appointed leader id for java-leader.
	MixedAppointedLeaderIDJavaLeader = int32(0)

	// MixedReadyPath is the readiness file path. The Go service writes MixedReadyPath after
	// the consensus module opens. MixedReadyPath sits outside every Aeron directory so
	// no driver maps it. The Go healthcheck tests MixedReadyPath. The java-leader
	// compose overlay starts Java peers only after that healthcheck passes.
	MixedReadyPath = "/dev/shm/heron-mixed/member-ready"
)

// Bridge network address literals. Compose sets each as ipv4_address.
// MixedMember2Address is also the aeronmd address. The Go test binary joins that
// driver with network_mode service:aeronmd so consensus and log endpoints
// listen on the address peers use in the members string.
const (
	MixedMember0Address = "172.30.0.10"
	MixedMember1Address = "172.30.0.11"
	MixedMember2Address = "172.30.0.12"
	MixedAeronMDAddress = MixedMember2Address
)

const (
	// MixedIngressEndpoints is the induction client endpoint map for Java members
	// 0 and 1 only. Go member 2 has no client ingress listener, so the map omits
	// that member. The cluster members string still carries member 2's ingress
	// port slot for topology.
	MixedIngressEndpoints = "0=" + MixedMember0Address + ":9002,1=" + MixedMember1Address + ":9102"

	// MixedIngressChannel is the induction client ingress URI. The URI carries no
	// endpoint query. Endpoints come from MixedIngressEndpoints.
	MixedIngressChannel = udpChannel64k

	// MixedEgressChannel advertises the shared driver address (member 2 /
	// aeronmd). MixedEgressChannel must not use loopback. Cluster nodes publish
	// egress from separate containers.
	MixedEgressChannel = UDPChannel + "?endpoint=" + MixedAeronMDAddress + ":0|" + termLength64kParam
)

// Per-member Aeron media directories under the shared /dev/shm volume.
const (
	MixedAeronDirMember0 = "/dev/shm/aeron-j0"
	MixedAeronDirMember1 = "/dev/shm/aeron-j1"
	MixedAeronDirGo      = "/dev/shm/aeron-go"
)

// Per-member cluster directories. The Go member uses member id 2.
const (
	// MixedBaseDirPrefix matches ClusterConstants.MIXED_BASE_DIR_PREFIX.
	MixedBaseDirPrefix = "/tmp/heron-cluster-"

	MixedClusterDirMember0 = MixedBaseDirPrefix + "0"
	MixedClusterDirMember1 = MixedBaseDirPrefix + "1"
	MixedClusterDirGo      = MixedBaseDirPrefix + "2"
)

const (
	// MixedLogChannelUnicast is the Go log channel template when a Java member is the
	// appointed leader. Topology appends the per-member log endpoint. Unicast
	// only. No interface or ttl. Same URI as cluster.DefaultLogChannel.
	MixedLogChannelUnicast = cluster.DefaultLogChannel
)

// MixedClusterMembersJavaLeader is the three-node members golden with per-member
// unicast log endpoints. PortBase is 9000. Hosts are the MixedMember*Address constants.
//
// Ports: PortBase + memberId*100 + offset (archive=1, client-facing=2,
// member=3, log=4, transfer=5).
const MixedClusterMembersJavaLeader = "0,172.30.0.10:9002,172.30.0.10:9003,172.30.0.10:9004," +
	"172.30.0.10:9005,172.30.0.10:9001|1,172.30.0.11:9102,172.30.0.11:9103," +
	"172.30.0.11:9104,172.30.0.11:9105,172.30.0.11:9101|2,172.30.0.12:9202," +
	"172.30.0.12:9203,172.30.0.12:9204,172.30.0.12:9205,172.30.0.12:9201|"

// Budgets for mixed cluster smokes. Each value is a deadline ceiling.
const (
	MixedStartupBudget  = 30 * time.Second
	MixedElectionBudget = 20 * time.Second
	MixedConnectBudget  = 20 * time.Second
	MixedCommitBudget   = 20 * time.Second

	// MixedTestTimeoutJavaLeader bounds the java-leader smoke wall clock.
	MixedTestTimeoutJavaLeader = 120 * time.Second
)

// MixedMemberHostnames returns the three member address literals in id order.
//
// Ownership: the caller owns the returned slice.
func MixedMemberHostnames() []string {
	return []string{MixedMember0Address, MixedMember1Address, MixedMember2Address}
}
