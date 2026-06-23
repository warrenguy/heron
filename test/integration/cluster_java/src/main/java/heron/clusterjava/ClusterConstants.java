package heron.clusterjava;

public final class ClusterConstants {
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
    public static final String INGRESS_CHANNEL = IPC_CHANNEL + "?term-length=64k";
    public static final String EGRESS_CHANNEL = IPC_CHANNEL + "?term-length=64k";
    public static final String INGRESS_ENDPOINTS = "";
    public static final String COMPOSE_AERON_DIR = "/dev/shm/aeron";
    public static final String BASE_DIR = "/tmp/heron-cluster-0";
    // logbuffer.TermMinLength
    public static final int TERM_MIN_LENGTH = 65536;
    public static final String CLUSTER_MEMBERS_STRING =
        "0,localhost:9002,localhost:9003,localhost:9004,localhost:9005,localhost:9001|";
    public static final String INGRESS_ENDPOINTS_STRING =
        MEMBER_ID + "=" + INGRESS_ENDPOINT_HOST + ":" + INGRESS_ENDPOINT_PORT;

    private ClusterConstants() {
    }
}
