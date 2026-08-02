//go:build integration

package harness

// Multicast endpoints for live cluster-log tests.
//
// Each test must own a distinct port. Two tests sharing a group address and port
// see each other's images: the group is joined per interface and the port is
// what separates them. Add a constant rather than reusing one.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing it and rejects an even address at
// publication add.
const (
	ClusterLogMulticastGroup   = "239.255.12.1"
	ClusterLogNLTMulticastPort = 24404
)

// Three-node module-test endpoints. Reserves the consensus port block
// ClusterModulePortBase+1 .. ClusterModulePortBase+(memberCount-1)*100+5.
// At three members that range is 25001-25205. The shared cluster log rides
// the multicast endpoint below.
//
// Cluster-log tests use 24501-24605 at two members and 239.255.12.1:24404.
// Module and cluster-log blocks are disjoint on every axis.
//
// The final octet of the group address must be odd. The driver derives the
// multicast control address by incrementing that octet.
const (
	ClusterModulePortBase    = 25000
	ClusterModuleLogEndpoint = "239.255.12.7:25004"
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

// ClusterLogChannelTemplate confines cluster-log multicast to loopback.
//
// Without interface=, the driver joins the group on the default route interface
// and the frames leave the host. ttl=0 confines traffic to a single host. A
// multi-host cluster needs the cluster interface and a positive ttl.
const ClusterLogChannelTemplate = "aeron:udp?term-length=64k|interface=127.0.0.1|ttl=0"
