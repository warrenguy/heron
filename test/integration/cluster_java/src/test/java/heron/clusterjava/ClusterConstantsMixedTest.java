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
import static org.junit.jupiter.api.Assertions.assertNull;

// Channel, appointed leader, and members string: asserted in
// ClusterNodeTest.mixedClusterConfig_javaLeader_appliesUnicastOnTopOfUdp.
// Topology string: asserted in clusterConfigHelpers_matchMixedGoldenStrings.
class ClusterConstantsMixedTest {
    @Test
    void configureConsensusMixed_javaLeader_leavesClusterClockUnset() {
        final ConsensusModule.Context consensus = ClusterNodeTest.configureMixedConsensus();

        // Mixed overlay does not set clusterClock. Context.conclude resolves the default later.
        assertNull(consensus.clusterClock());
    }

    @Test
    void clusterConfigHelpers_matchMixedGoldenStrings() {
        final List<String> hosts = ClusterNodeTest.MIXED_HOSTS;
        final String members = ClusterConfig.clusterMembers(hosts, hosts, ClusterConstants.PORT_BASE);
        assertEquals(ClusterConstants.MIXED_CLUSTER_MEMBERS_JAVA_LEADER, members);
    }

    @Test
    void unicastChannel_omitsMulticastParams() {
        assertFalse(ClusterConstants.MIXED_LOG_CHANNEL_UNICAST.contains("interface="));
        assertFalse(ClusterConstants.MIXED_LOG_CHANNEL_UNICAST.contains("ttl="));
        assertFalse(ClusterConstants.MIXED_LOG_CHANNEL_UNICAST.contains("endpoint="));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("mixedConstantGoldens")
    void constant_matchesGoOracle(final String name, final Object expected, final Object actual) {
        assertEquals(expected, actual, name);
    }

    // Scalar goldens hand-sync with clusterjava/mixed_oracle.go.
    // Topology strings are asserted in clusterConfigHelpers_matchMixedGoldenStrings.
    private static Stream<Arguments> mixedConstantGoldens() {
        return Stream.of(
            Arguments.of("MIXED_MEMBER_COUNT", 3, ClusterConstants.MIXED_MEMBER_COUNT),
            Arguments.of(
                "MIXED_APPOINTED_LEADER_ID_JAVA",
                0,
                ClusterConstants.MIXED_APPOINTED_LEADER_ID_JAVA),
            Arguments.of(
                "MIXED_LOG_CHANNEL_UNICAST",
                "aeron:udp?term-length=64k",
                ClusterConstants.MIXED_LOG_CHANNEL_UNICAST),
            Arguments.of(
                "MIXED_HOSTNAMES",
                "172.30.0.10,172.30.0.11,172.30.0.12",
                ClusterConstants.MIXED_HOSTNAMES),
            Arguments.of(
                "MIXED_BASE_DIR_PREFIX",
                "/tmp/heron-cluster-",
                ClusterConstants.MIXED_BASE_DIR_PREFIX)
        );
    }
}
