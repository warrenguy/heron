package heron.clusterjava;

import java.util.concurrent.TimeUnit;

public final class ClusterConstants {

    // Single-node IPC smoke defaults.

    // clusterjava.PortBase
    public static final int PORT_BASE = 9000;
    public static final int MEMBER_ID = 0;
    public static final int MEMBER_COUNT = 1;
    public static final String INGRESS_ENDPOINT_HOST = "localhost";
    public static final int INGRESS_ENDPOINT_PORT = ClusterConfig.calculatePort(
        MEMBER_ID, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET);

    // client.DefaultIngressStreamID
    public static final int INGRESS_STREAM_ID = 101;

    // client.DefaultEgressStreamID
    public static final int EGRESS_STREAM_ID = 102;

    // harness.IPCChannel
    public static final String IPC_CHANNEL = "aeron:ipc";

    private static final String TERM_LENGTH_64K_PARAM = "term-length=64k";
    private static final String IPC_CHANNEL_64K = IPC_CHANNEL + "?" + TERM_LENGTH_64K_PARAM;

    public static final String INGRESS_CHANNEL = IPC_CHANNEL_64K;
    public static final String EGRESS_CHANNEL = IPC_CHANNEL_64K;
    public static final String INGRESS_ENDPOINTS = "";

    public static final String COMPOSE_AERON_DIR = "/dev/shm/aeron";
    public static final String BASE_DIR = "/tmp/heron-cluster-0";

    // logbuffer.TermMinLength
    public static final int TERM_MIN_LENGTH = 65536;

    public static final String CLUSTER_MEMBERS_STRING =
        "0,localhost:9002,localhost:9003,localhost:9004,localhost:9005,localhost:9001|";

    public static final String INGRESS_ENDPOINTS_STRING =
        MEMBER_ID + "=" + INGRESS_ENDPOINT_HOST + ":" + INGRESS_ENDPOINT_PORT;


    // Multi-node UDP failover defaults (clusterjava/failover_oracle.go).

    public static final int FAILOVER_MEMBER_COUNT = 3;

    public static final String FAILOVER_BASE_DIR_PREFIX = "/dev/shm/heron-cluster-";

    public static final String FAILOVER_INGRESS_ENDPOINT_HOST = "cluster-0";

    public static final String UDP_CHANNEL = "aeron:udp";

    private static final String UDP_CHANNEL_64K = UDP_CHANNEL + "?" + TERM_LENGTH_64K_PARAM;

    public static final String FAILOVER_INGRESS_CHANNEL = UDP_CHANNEL_64K;

    // Client egress subscription channel. endpoint=aeronmd:0 binds the shared aeronmd
    // driver container so the resolved ephemeral endpoint is routable from cluster nodes.
    public static final String FAILOVER_EGRESS_CHANNEL =
        UDP_CHANNEL + "?endpoint=aeronmd:0|" + TERM_LENGTH_64K_PARAM;

    // Consensus module egress template. Must omit endpoint: the module merges these
    // params onto every client responseChannel, so an endpoint here would overwrite the
    // client's resolved ephemeral endpoint and fail publication with port=0.
    public static final String FAILOVER_CONSENSUS_EGRESS_CHANNEL = UDP_CHANNEL_64K;

    // Session must outlive the leader-election gap when the client cannot send ingress
    // keepalives. The 10s Aeron default expires the session mid-failover.
    public static final long FAILOVER_SESSION_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(120);

    public static final String FAILOVER_INGRESS_ENDPOINTS =
        "0=" + FAILOVER_INGRESS_ENDPOINT_HOST + ":" + INGRESS_ENDPOINT_PORT
        + ",1=cluster-1:" + ClusterConfig.calculatePort(1, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET)
        + ",2=cluster-2:" + ClusterConfig.calculatePort(2, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET);

    public static final String FAILOVER_CLUSTER_MEMBERS_STRING =
        "0,cluster-0:9002,cluster-0:9003,cluster-0:9004,cluster-0:9005,cluster-0:9001|"
        + "1,cluster-1:9102,cluster-1:9103,cluster-1:9104,cluster-1:9105,cluster-1:9101|"
        + "2,cluster-2:9202,cluster-2:9203,cluster-2:9204,cluster-2:9205,cluster-2:9201|";


    // Mixed Go/Java multi-node defaults (clusterjava/mixed_oracle.go).
    // Appointed leader is member 0 (Java). Go member is id 2.

    public static final int MIXED_MEMBER_COUNT = 3;

    // Appointed leader member id (Java member 0).
    public static final int MIXED_APPOINTED_LEADER_ID_JAVA = 0;

    // Cluster data parent for mixed compose volumes: /tmp/heron-cluster-{memberId}.
    public static final String MIXED_BASE_DIR_PREFIX = "/tmp/heron-cluster-";

    // Log channel when a Java member is appointed. Unicast only. No interface, ttl,
    // or endpoint. Topology derives per-member log endpoints. Same as Go
    // MixedLogChannelUnicast / DefaultLogChannel.
    public static final String MIXED_LOG_CHANNEL_UNICAST = UDP_CHANNEL_64K;

    // Address literals for members 0-2. Comma-separated for ENV_HOSTNAMES parse.
    public static final String MIXED_HOSTNAMES = "172.30.0.10,172.30.0.11,172.30.0.12";

    // Three-node members golden with per-member unicast log endpoints.
    // Hand-sync with clusterjava.MixedClusterMembersJavaLeader.
    public static final String MIXED_CLUSTER_MEMBERS_JAVA_LEADER =
        "0,172.30.0.10:9002,172.30.0.10:9003,172.30.0.10:9004,"
            + "172.30.0.10:9005,172.30.0.10:9001|1,172.30.0.11:9102,172.30.0.11:9103,"
            + "172.30.0.11:9104,172.30.0.11:9105,172.30.0.11:9101|2,172.30.0.12:9202,"
            + "172.30.0.12:9203,172.30.0.12:9204,172.30.0.12:9205,172.30.0.12:9201|";

    private ClusterConstants() {
    }
}
