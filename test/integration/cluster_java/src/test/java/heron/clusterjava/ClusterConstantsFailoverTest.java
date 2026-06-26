package heron.clusterjava;

import io.aeron.cluster.ConsensusModule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ClusterConstantsFailoverTest {
    @Test
    void configureConsensusUDP_usesFailoverConstants() {
        final ClusterConfig config = ClusterNodeTest.createClusterConfig(1, ClusterNodeTest.failoverHosts());
        final ConsensusModule.Context consensus = config.consensusModuleContext();
        assertFalse(consensus.isIpcIngressAllowed());
        ClusterNode.configureConsensusUDP(config);
        ClusterNodeTest.assertConsensusUsesUdpChannels(consensus);
    }

    @Test
    void consensusEgressChannel_omitsEndpoint() {
        assertEquals("aeron:udp?term-length=64k", ClusterConstants.FAILOVER_CONSENSUS_EGRESS_CHANNEL);
        assertFalse(ClusterConstants.FAILOVER_CONSENSUS_EGRESS_CHANNEL.contains("endpoint="));
    }

    @Test
    void clusterConfigHelpers_matchFailoverGoldenStrings() {
        final List<String> hosts = ClusterNodeTest.failoverHosts();
        final String members = ClusterConfig.clusterMembers(hosts, hosts, ClusterConstants.PORT_BASE);
        final String ingress = ClusterConfig.ingressEndpoints(
            hosts,
            ClusterConstants.PORT_BASE,
            ClusterConfig.CLIENT_FACING_PORT_OFFSET);
        assertEquals(ClusterConstants.FAILOVER_CLUSTER_MEMBERS_STRING, members);
        assertEquals(ClusterConstants.FAILOVER_INGRESS_ENDPOINTS, ingress);
        assertEquals(
            9102,
            ClusterConfig.calculatePort(
                1,
                ClusterConstants.PORT_BASE,
                ClusterConfig.CLIENT_FACING_PORT_OFFSET));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("failoverConstantGoldens")
    void constant_matchesGoOracle(final String name, final Object expected, final Object actual) {
        assertEquals(expected, actual, name);
    }

    // Scalar goldens hand-synced with clusterjava/failover_oracle_test.go; topology strings
    // are asserted in clusterConfigHelpers_matchFailoverGoldenStrings.
    private static Stream<Arguments> failoverConstantGoldens() {
        return Stream.of(
            Arguments.of("FAILOVER_MEMBER_COUNT", 3, ClusterConstants.FAILOVER_MEMBER_COUNT),
            Arguments.of("FAILOVER_INGRESS_ENDPOINT_HOST", "cluster-0", ClusterConstants.FAILOVER_INGRESS_ENDPOINT_HOST),
            Arguments.of("FAILOVER_BASE_DIR_PREFIX", "/dev/shm/heron-cluster-", ClusterConstants.FAILOVER_BASE_DIR_PREFIX),
            Arguments.of("FAILOVER_INGRESS_CHANNEL", "aeron:udp?term-length=64k", ClusterConstants.FAILOVER_INGRESS_CHANNEL),
            Arguments.of("FAILOVER_EGRESS_CHANNEL", "aeron:udp?endpoint=aeronmd:0|term-length=64k", ClusterConstants.FAILOVER_EGRESS_CHANNEL)
        );
    }
}
