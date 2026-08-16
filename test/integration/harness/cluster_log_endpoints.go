//go:build integration

package harness

import (
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster"
)

// Multicast endpoints for live cluster-log tests.
//
// Each Test* that opens a member set must own a distinct PortBase and log
// multicast endpoint. Two tests sharing a group address and port see each
// other's images: the group is joined per interface and the port is what
// separates them. Add a constant rather than reusing one.
//
// Port formula: PortBase + memberID*100 + offset (offsets 1..5). Three members
// reserve base+1 .. base+205. Adjacent three-member bases sit 300 apart so
// ranges never overlap. Unique bases make t.Parallel safe inside the package.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing it and rejects an even address at
// publication add.
const (
	ClusterLogMulticastGroup   = "239.255.12.1"
	ClusterLogNLTMulticastPort = 24404
)

// Unicast PortBase values for test/integration below the cluster_archive floor.
// Each live Test* that binds unicast owns one block. Two tests must not share a
// PortBase under t.Parallel.
//
// Member counts:
//   - ConsensusPublications: three members (base+1 .. base+205)
//   - ClusterStreams*: two members (base+1 .. base+105)
//
// Single-endpoint UDP pubs and subs (channel status, multi-destination) call
// FreeUDPPort instead of fixed ports so parallel hosts do not collide.
const (
	ConsensusPublicationsPortBase   = 24000
	ClusterStreamsMulticastPortBase = 23700
	ClusterStreamsCommitPortBase    = 24500
)

// ClusterStreamsMulticastLogEndpoint is derived from the cluster-log multicast
// constants so the port ledger and the live bind cannot drift.
var ClusterStreamsMulticastLogEndpoint = ClusterLogMulticastGroup + ":" +
	strconv.Itoa(ClusterLogNLTMulticastPort)

// ClusterJavaPortBase is the default port base every cluster_java fixture
// binds (single node, failover, mixed). Restated in harness so the port matrix
// builds under the integration tag alone, where the clusterjava package does
// not compile. The clusterjava package asserts these against its own topology
// constants.
const (
	ClusterJavaPortBase = cluster.DefaultPortBase

	// ClusterJavaMemberCount is the widest cluster_java fixture. Failover and
	// mixed both run three members. The single-node fixture runs one.
	ClusterJavaMemberCount = 3
)

// ClusterJavaMixedLogEndpoint is derived from ClusterJavaPortBase so the
// ledger and the live bind cannot drift. Only the group address is
// cluster_java's own. Only the mixed go-leader fixture binds this endpoint.
var ClusterJavaMixedLogEndpoint = "239.255.12.15:" + strconv.Itoa(
	cluster.CalculatePort(0, ClusterJavaPortBase, cluster.LogPortOffset),
)

// Three-node module-test endpoints. Each Test* owns one block so the suite may
// run those tests in parallel. Fixtures that never bind (config reject, hook
// stubs) reuse the elect block.
//
// The final octet of the group address must be odd.
const (
	ClusterModuleElectPortBase           = 25000
	ClusterModuleElectLogEndpoint        = "239.255.12.7:25004"
	ClusterModuleCommitPortBase          = 28200
	ClusterModuleCommitLogEndpoint       = "239.255.12.31:28204"
	ClusterModuleRejoinPortBase          = 28500
	ClusterModuleRejoinLogEndpoint       = "239.255.12.33:28504"
	ClusterModuleReElectPortBase         = 28800
	ClusterModuleReElectLogEndpoint      = "239.255.12.35:28804"
	ClusterModuleFollowerStopPortBase    = 29100
	ClusterModuleFollowerStopLogEndpoint = "239.255.12.37:29104"
)

// One-member cluster-log replay restart endpoints. Reserves the consensus port
// block ClusterReplayPortBase+1 .. ClusterReplayPortBase+(memberCount-1)*100+5.
// At one member that range is 25701-25705. The shared cluster log uses the
// multicast endpoint below.
//
// Member-restart affordance tests use 25500-25605 at two members and
// 239.255.12.11:25504. Replay and restart-affordance blocks are disjoint on
// every axis.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterReplayPortBase    = 25700
	ClusterReplayLogEndpoint = "239.255.12.17:25704"
)

// Durable live-Archive scenario endpoints. Each pair owns one consensus port
// block base+1 .. base+(memberCount-1)*100+5 and one shared cluster-log
// multicast endpoint. Default durable sets use three members
// (base+1 .. base+205). DurableConcurrentCatchup uses five
// (base+1 .. base+405).
//
// DurableRecord and DurableRank are separate so the two record/rank tests may
// run in parallel. DurableRestart and DurableRestartLeader are separate for the
// two restart tests. DurableCatchup and DurableConcurrentCatchup are separate
// for the single-follower and concurrent-follower catchup tests.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	DurableRecordPortBase               = 26000
	DurableRecordLogEndpoint            = "239.255.12.19:26004"
	DurableRankPortBase                 = 29400
	DurableRankLogEndpoint              = "239.255.12.39:29404"
	DurableRestartPortBase              = 26300
	DurableRestartLogEndpoint           = "239.255.12.21:26304"
	DurableRestartLeaderPortBase        = 29700
	DurableRestartLeaderLogEndpoint     = "239.255.12.41:29704"
	DurableReElectionPortBase           = 26600
	DurableReElectionLogEndpoint        = "239.255.12.23:26604"
	DurableCatchupPortBase              = 26900
	DurableCatchupLogEndpoint           = "239.255.12.25:26904"
	DurableDivergencePortBase           = 27200
	DurableDivergenceLogEndpoint        = "239.255.12.27:27204"
	DurableConcurrentCatchupPortBase    = 30400
	DurableConcurrentCatchupLogEndpoint = "239.255.12.43:30404"
)

// Session-client endpoints. Each pair owns one three-member consensus port
// block base+1 .. base+205 and one shared cluster-log multicast endpoint.
// Smoke and reject are separate so the two session tests may run in
// parallel. Bases sit above DurableConcurrentCatchup (30400 + 405).
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterSessionSmokePortBase     = 30900
	ClusterSessionSmokeLogEndpoint  = "239.255.12.45:30904"
	ClusterSessionRejectPortBase    = 31200
	ClusterSessionRejectLogEndpoint = "239.255.12.47:31204"
)

// Stop/restart amplifier endpoints. Two members reserve base+1 .. base+105 and
// one shared cluster-log multicast endpoint. Bases sit above
// ClusterSessionRejectPortBase (31200 + 205).
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterMemberAmplifierPortBase    = 31500
	ClusterMemberAmplifierLogEndpoint = "239.255.12.49:31504"
)

// Live service-ack endpoints. Three members reserve base+1 .. base+205 and
// one shared cluster-log multicast endpoint. Bases sit above
// ClusterMemberAmplifierPortBase (31500 + 105).
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterServiceAckPortBase    = 31800
	ClusterServiceAckLogEndpoint = "239.255.12.51:31804"
)

// Session-extend live-scenario endpoints. Each pair reserves one three-member
// consensus port block (base+1 .. base+205) and one shared cluster-log
// multicast endpoint. Restart and re-elect are separate so the two scenarios
// may run in parallel. Bases sit above ClusterServiceAckPortBase
// (31800 + 205).
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterSessionExtendRestartPortBase    = 32100
	ClusterSessionExtendRestartLogEndpoint = "239.255.12.53:32104"
	ClusterSessionExtendReElectPortBase    = 32400
	ClusterSessionExtendReElectLogEndpoint = "239.255.12.55:32404"
)

// Multi-service live-scenario endpoints. Three members reserve base+1 ..
// base+205 and one shared cluster-log multicast endpoint. Bases sit above
// ClusterSessionExtendReElectPortBase (32400 + 205).
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterMultiServicePortBase    = 32700
	ClusterMultiServiceLogEndpoint = "239.255.12.57:32704"
)

// Session-transfer live-scenario endpoints. Each pair reserves one three-member
// consensus port block (base+1 .. base+205) and one shared cluster-log
// multicast endpoint. Session-transfer and service-transfer are separate so
// the two scenarios may run in parallel. Bases sit above
// ClusterMultiServicePortBase (32700 + 205).
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterSessionTransferPortBase    = 33000
	ClusterSessionTransferLogEndpoint = "239.255.12.59:33004"
	ClusterServiceTransferPortBase    = 33300
	ClusterServiceTransferLogEndpoint = "239.255.12.61:33304"
)

// cluster_archive one-off and smoke endpoints. Harness holds one matrix for
// every PortBase and log multicast the cluster_archive package binds under
// t.Parallel.
//
// Sibling packages bind below the matrix floor of 24700 and may run in
// parallel go test processes:
//   - cluster_java: ClusterJavaPortBase (9000) with
//     ClusterJavaMixedLogEndpoint (9004)
//   - test/integration: ConsensusPublicationsPortBase (24000),
//     ClusterStreamsMulticastPortBase (23700), ClusterStreamsCommitPortBase
//     (24500), and ClusterStreamsMulticastLogEndpoint (24404). Channel-status
//     and multi-destination endpoints use FreeUDPPort.
//
// TestClusterPortBlocks_doNotOverlap registers every reservation above. The
// floor therefore applies only to new cluster_archive bases. Keep those at
// 24700 or higher.
//
// Member counts for reserved unicast ranges:
//   - consensus and runner: one member (base+1..+5)
//   - member-set smoke and restart affordance: two members (base+1..+105)
//   - truncate: three members (base+1..+205)
//
// Archive-node control URIs are single UDP binds, not consensus port blocks.
const (
	ConsensusModulePortBase    = 24700
	ConsensusModuleLogEndpoint = "239.255.12.3:24704"

	RunnerPortBase    = 24800
	RunnerLogEndpoint = "239.255.12.13:24804"

	ClusterMemberSmokePortBase    = 25300
	ClusterMemberSmokeLogEndpoint = "239.255.12.9:25304"

	ClusterMemberRestartPortBase    = 25500
	ClusterMemberRestartLogEndpoint = "239.255.12.11:25504"

	ClusterTruncatePortBase    = 27500
	ClusterTruncateLogEndpoint = "239.255.12.29:27504"

	// Archive-node launcher smoke. Single control binds; not PortBase blocks.
	ArchiveNodeSmokeControlPort0 = 28001
	ArchiveNodePairControlPort0  = 30001
	ArchiveNodePairControlPort1  = 30101
)

// Archive-node launcher smoke control URIs, derived from the port constants
// above so the non-overlap matrix and live binds cannot drift.
var (
	ArchiveNodeSmokeControl0 = ArchiveControlChannel(ArchiveNodeSmokeControlPort0)
	ArchiveNodePairControl0  = ArchiveControlChannel(ArchiveNodePairControlPort0)
	ArchiveNodePairControl1  = ArchiveControlChannel(ArchiveNodePairControlPort1)
)

// ArchiveControlChannel returns the loopback UDP Archive control URI for port.
func ArchiveControlChannel(port int) string {
	return "aeron:udp?endpoint=" + LoopbackUDPEndpoint(port)
}

// LoopbackUDPEndpoint returns host:port for a loopback UDP endpoint.
func LoopbackUDPEndpoint(port int) string {
	return "127.0.0.1:" + strconv.Itoa(port)
}

// FreeUDPPort returns one unused loopback UDP port.
//
// Closes the probe socket before return so the caller can bind. Prefer this
// over a fixed port when the test runs under SharedParallel or parallel hosts.
// Does not retry on later bind failure.
func FreeUDPPort(t testing.TB) int {
	t.Helper()
	return FreeUDPPorts(t, 1)[0]
}

// FreeUDPPorts returns n distinct unused loopback UDP ports.
//
// Opens all probe sockets first so the kernel cannot hand back the same port
// twice, then closes them so the caller can bind.
func FreeUDPPorts(t testing.TB, n int) []int {
	t.Helper()
	require.Positive(t, n)

	conns := make([]net.PacketConn, 0, n)
	ports := make([]int, 0, n)

	// Close every probe socket before return so each port is free to bind.
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	for range n {
		c, err := net.ListenPacket("udp", "127.0.0.1:0")
		require.NoError(t, err)
		conns = append(conns, c)
		addr, ok := c.LocalAddr().(*net.UDPAddr)
		require.True(t, ok, "ListenPacket LocalAddr must be *net.UDPAddr")
		require.Positive(t, addr.Port)
		ports = append(ports, addr.Port)
	}

	return ports
}

// ClusterLogChannelTemplate confines cluster-log multicast to loopback.
//
// Without interface=, the driver joins the group on the default route interface
// and the frames leave the host. ttl=0 confines traffic to a single host. A
// multi-host cluster needs the cluster interface and a positive ttl.
const ClusterLogChannelTemplate = "aeron:udp?term-length=64k|interface=127.0.0.1|ttl=0"
