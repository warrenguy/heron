package heron.clusterjava;

import io.aeron.cluster.ConsensusModule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterConstantsTest {
    @Test
    void configureConsensusIPC_usesClusterConstants() {
        final ClusterConfig config = defaultClusterConfig();
        ClusterNode.configureConsensusIPC(config);
        final ConsensusModule.Context consensus = config.consensusModuleContext();
        assertEquals(ClusterConstants.INGRESS_CHANNEL, consensus.ingressChannel());
        assertEquals(ClusterConstants.EGRESS_CHANNEL, consensus.egressChannel());
        assertEquals(ClusterConstants.INGRESS_STREAM_ID, consensus.ingressStreamId());
    }

    @Test
    void clusterConfigHelpers_matchGoldenStrings() {
        final List<String> hosts = defaultHosts();
        final String members = ClusterConfig.clusterMembers(hosts, hosts, ClusterConstants.PORT_BASE);
        final String ingress = ClusterConfig.ingressEndpoints(
            hosts,
            ClusterConstants.PORT_BASE,
            ClusterConfig.CLIENT_FACING_PORT_OFFSET);
        assertEquals(ClusterConstants.CLUSTER_MEMBERS_STRING, members);
        assertEquals(ClusterConstants.INGRESS_ENDPOINTS_STRING, ingress);
        assertEquals(
            ClusterConstants.INGRESS_ENDPOINT_PORT,
            ClusterConfig.calculatePort(
                ClusterConstants.MEMBER_ID,
                ClusterConstants.PORT_BASE,
                ClusterConfig.CLIENT_FACING_PORT_OFFSET));
    }

    @Test
    void clusterMembers_rejectsMismatchedHostnameListSizes() {
        assertThrows(
            IllegalArgumentException.class,
            () -> ClusterConfig.clusterMembers(
                List.of("a"), List.of("a", "b"), ClusterConstants.PORT_BASE));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("harnessConstantGoldens")
    void constant_matchesGoOracle(final String name, final Object expected, final Object actual) {
        assertEquals(expected, actual, name);
    }

    private static List<String> defaultHosts() {
        return List.of(ClusterConstants.INGRESS_ENDPOINT_HOST);
    }

    private static ClusterConfig defaultClusterConfig() {
        return ClusterConfig.create(
            ClusterConstants.MEMBER_ID,
            defaultHosts(),
            ClusterConstants.PORT_BASE,
            new EchoService());
    }

    private static Stream<Arguments> harnessConstantGoldens() {
        return Stream.of(
            Arguments.of("PORT_BASE", 9000, ClusterConstants.PORT_BASE),
            Arguments.of("MEMBER_ID", 0, ClusterConstants.MEMBER_ID),
            Arguments.of("MEMBER_COUNT", 1, ClusterConstants.MEMBER_COUNT),
            Arguments.of("INGRESS_ENDPOINT_HOST", "localhost", ClusterConstants.INGRESS_ENDPOINT_HOST),
            Arguments.of("INGRESS_ENDPOINT_PORT", 9002, ClusterConstants.INGRESS_ENDPOINT_PORT),
            Arguments.of("INGRESS_STREAM_ID", 101, ClusterConstants.INGRESS_STREAM_ID),
            Arguments.of("EGRESS_STREAM_ID", 102, ClusterConstants.EGRESS_STREAM_ID),
            Arguments.of("INGRESS_CHANNEL", "aeron:ipc?term-length=64k", ClusterConstants.INGRESS_CHANNEL),
            Arguments.of("EGRESS_CHANNEL", "aeron:ipc?term-length=64k", ClusterConstants.EGRESS_CHANNEL),
            Arguments.of("INGRESS_ENDPOINTS", "", ClusterConstants.INGRESS_ENDPOINTS),
            Arguments.of("COMPOSE_AERON_DIR", "/dev/shm/aeron", ClusterConstants.COMPOSE_AERON_DIR),
            Arguments.of("BASE_DIR", "/tmp/heron-cluster-0", ClusterConstants.BASE_DIR),
            Arguments.of("TERM_MIN_LENGTH", 65536, ClusterConstants.TERM_MIN_LENGTH),
            Arguments.of("CLUSTER_MEMBERS_STRING", "0,localhost:9002,localhost:9003,localhost:9004,localhost:9005,localhost:9001|", ClusterConstants.CLUSTER_MEMBERS_STRING),
            Arguments.of("INGRESS_ENDPOINTS_STRING", "0=localhost:9002", ClusterConstants.INGRESS_ENDPOINTS_STRING)
        );
    }
}
