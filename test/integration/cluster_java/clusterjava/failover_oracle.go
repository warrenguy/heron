//go:build integration && cluster_java

package clusterjava

import (
	"time"

	"github.com/warrenguy/heron/cluster/client"
)

const (
	FailoverMemberCount         = 3
	FailoverIngressEndpointHost = "cluster-0"
	// FailoverNewLeaderTimeout hand-syncs with failoverRecoveryTimeout and Java
	// FAILOVER_SESSION_TIMEOUT_NS.
	FailoverNewLeaderTimeout = 120 * time.Second
)

const (
	// FailoverBaseDirPrefix matches ClusterConstants.FAILOVER_BASE_DIR_PREFIX.
	FailoverBaseDirPrefix = "/dev/shm/heron-cluster-"
)

const (
	// UDPChannel matches ClusterConstants.UDP_CHANNEL.
	UDPChannel = "aeron:udp"

	// FailoverIngressChannel and FailoverEgressChannel pin 64KiB term length for Java
	// cluster UDP parity.
	//
	// FailoverEgressChannel binds endpoint=aeronmd:0 so the resolved ephemeral port
	// advertises the shared aeronmd driver container address. Cluster nodes publish
	// egress to that responseChannel from separate containers; a loopback or wildcard
	// bind would not be routable across the compose network.
	FailoverIngressChannel   = UDPChannel + "?" + termLength64kParam
	FailoverEgressChannel    = UDPChannel + "?endpoint=aeronmd:0|" + termLength64kParam
	FailoverIngressEndpoints = "0=cluster-0:9002,1=cluster-1:9102,2=cluster-2:9202"
)

// FailoverClusterMembersString is the 3-node topology golden for PortBase=9000 and
// cluster-0, cluster-1, cluster-2 hostnames. Ports: port = PortBase + (memberId * 100) +
// offset (archive=1, client-facing=2, member=3, log=4, transfer=5).
const FailoverClusterMembersString = "0,cluster-0:9002,cluster-0:9003,cluster-0:9004," +
	"cluster-0:9005,cluster-0:9001|1,cluster-1:9102,cluster-1:9103,cluster-1:9104," +
	"cluster-1:9105,cluster-1:9101|2,cluster-2:9202,cluster-2:9203,cluster-2:9204," +
	"cluster-2:9205,cluster-2:9201|"

// FailoverHarnessConfig returns cluster client defaults for UDP 3-node failover
// connect tests. Stream IDs are left at zero so callers or NewAsyncConnect Normalize
// applies defaults. AeronDir is unset; callers assign it from compose env before connect.
func FailoverHarnessConfig() client.Config {
	cfg := harnessConfig(
		FailoverIngressChannel,
		FailoverEgressChannel,
		FailoverIngressEndpoints,
	)
	cfg.NewLeaderTimeout = FailoverNewLeaderTimeout
	cfg.MessageTimeout = FailoverNewLeaderTimeout
	return cfg
}
