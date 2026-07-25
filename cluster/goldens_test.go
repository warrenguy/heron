package cluster_test

// Members-string goldens hand-synced to io.aeron.samples.cluster.ClusterConfig.
// Sibling copies of the single-node and failover strings live in
// cluster/election/member_parse_test.go and the cluster_java oracle packages.
//
// When ClusterConfig port layout changes, re-read PORTS_PER_NODE and the five
// port offsets that shape these members strings.

// goldenClusterMembersSingle is upstream parity for one node on localhost with
// portBase 9000. Byte-identical to the election parser single-member fixture
// and the cluster_java single-node oracle.
const goldenClusterMembersSingle = "0,localhost:9002,localhost:9003,localhost:9004," +
	"localhost:9005,localhost:9001|"

// goldenClusterMembersFailover is upstream parity for three nodes with
// cluster-N hosts and portBase 9000. Byte-identical to the election parser
// failover fixture and the cluster_java failover oracle.
const goldenClusterMembersFailover = "0,cluster-0:9002,cluster-0:9003,cluster-0:9004," +
	"cluster-0:9005,cluster-0:9001|1,cluster-1:9102,cluster-1:9103,cluster-1:9104," +
	"cluster-1:9105,cluster-1:9101|2,cluster-2:9202,cluster-2:9203,cluster-2:9204," +
	"cluster-2:9205,cluster-2:9201|"

// goldenClusterMembersSharedLog is three nodes on loopback with a shared
// multicast log endpoint 239.255.12.1:9004 and portBase 9000.
//
// Aeron requires the multicast data address final octet to be odd; the driver
// derives the control address by incrementing that octet. 239.255.12.1 is
// legal (control 239.255.12.2). An even group address is rejected when the
// publication is added. 239.255.0.0/16 is IPv4 Local Scope (RFC 2365).
// Port 9004 is member 0's log port under the standard block, reused as the
// group port by every member.
const goldenClusterMembersSharedLog = "0,127.0.0.1:9002,127.0.0.1:9003,239.255.12.1:9004," +
	"127.0.0.1:9005,127.0.0.1:9001|1,127.0.0.1:9102,127.0.0.1:9103,239.255.12.1:9004," +
	"127.0.0.1:9105,127.0.0.1:9101|2,127.0.0.1:9202,127.0.0.1:9203,239.255.12.1:9004," +
	"127.0.0.1:9205,127.0.0.1:9201|"
