package heron.clusterjava;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Channel, appointed leader, members string, and clusterClock: asserted in
// ClusterNodeTest mixed cases.
//
// Topology string: asserted in clusterConfigHelpers_matchMixedGoldenStrings.
class ClusterConstantsMixedTest {
    @Test
    void clusterConfigHelpers_matchMixedGoldenStrings() {
        final List<String> hosts = ClusterNodeTest.MIXED_HOSTS;
        final String members = ClusterConfig.clusterMembers(hosts, hosts, ClusterConstants.PORT_BASE);
        assertEquals(ClusterConstants.MIXED_CLUSTER_MEMBERS_JAVA_LEADER, members);
        assertEquals(
            ClusterConstants.MIXED_CLUSTER_MEMBERS_GO_LEADER,
            ClusterConfig.sharedLogEndpoint(members, ClusterConstants.MIXED_LOG_ENDPOINT));
    }

    @Test
    void sharedLogEndpoint_rewritesOnlyLogSlots() {
        final String unicast = ClusterConstants.MIXED_CLUSTER_MEMBERS_JAVA_LEADER;
        final String shared = ClusterConfig.sharedLogEndpoint(
            unicast, ClusterConstants.MIXED_LOG_ENDPOINT);
        final String[] unicastRows = unicast.split("\\|", -1);
        final String[] sharedRows = shared.split("\\|", -1);
        assertEquals(unicastRows.length, sharedRows.length);
        for (int i = 0; i < unicastRows.length; i++)
        {
            if (unicastRows[i].isEmpty())
            {
                assertTrue(sharedRows[i].isEmpty());
                continue;
            }
            final String[] unicastFields = unicastRows[i].split(",", -1);
            final String[] sharedFields = sharedRows[i].split(",", -1);
            assertEquals(6, unicastFields.length, "unicast row " + i);
            assertEquals(6, sharedFields.length, "shared row " + i);
            assertEquals(
                ClusterConstants.MIXED_LOG_ENDPOINT,
                sharedFields[3],
                "log slot row " + i);
            for (int f = 0; f < 6; f++)
            {
                if (f == 3)
                {
                    continue;
                }
                assertEquals(
                    unicastFields[f],
                    sharedFields[f],
                    "row " + i + " field " + f);
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("sharedLogEndpointRejects")
    void sharedLogEndpoint_rejects(
        final String name,
        final String clusterMembers,
        final String endpoint,
        final String wantMessagePrefix) {
        final IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ClusterConfig.sharedLogEndpoint(clusterMembers, endpoint));
        assertTrue(ex.getMessage().startsWith(wantMessagePrefix), name + ": " + ex.getMessage());
    }

    private static Stream<Arguments> sharedLogEndpointRejects() {
        final String members = ClusterConstants.MIXED_CLUSTER_MEMBERS_JAVA_LEADER;
        return Stream.of(
            Arguments.of("blankEndpoint", members, "  ", "endpoint must not be blank"),
            Arguments.of("nullEndpoint", members, null, "endpoint must not be blank"),
            Arguments.of(
                "nullClusterMembers",
                null,
                ClusterConstants.MIXED_LOG_ENDPOINT,
                "clusterMembers must not be empty"),
            Arguments.of(
                "emptyClusterMembers",
                "",
                ClusterConstants.MIXED_LOG_ENDPOINT,
                "clusterMembers must not be empty"),
            Arguments.of(
                "malformedMemberRow",
                "0,only-two-fields|",
                ClusterConstants.MIXED_LOG_ENDPOINT,
                "member row must have 6 fields")
        );
    }

    @Test
    void unicastChannel_omitsMulticastParams() {
        assertFalse(ClusterConstants.MIXED_LOG_CHANNEL_UNICAST.contains("interface="));
        assertFalse(ClusterConstants.MIXED_LOG_CHANNEL_UNICAST.contains("ttl="));
        assertFalse(ClusterConstants.MIXED_LOG_CHANNEL_UNICAST.contains("endpoint="));
    }

    @Test
    void multicastChannel_isFinalUriWithEndpointInterfaceAndTtl() {
        // Java final URI carries endpoint= to clear isLogMdc. Go template omits it.
        final String channel = ClusterConstants.MIXED_LOG_CHANNEL_MULTICAST;
        assertTrue(channel.startsWith("aeron:udp?term-length=64k"));
        assertTrue(channel.contains("endpoint=" + ClusterConstants.MIXED_LOG_ENDPOINT));
        assertTrue(channel.contains("interface=172.30.0.0/16"));
        assertTrue(channel.contains("ttl=1"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("mixedConstantGoldens")
    void constant_matchesGoOracle(final String name, final Object expected, final Object actual) {
        assertEquals(expected, actual, name);
    }

    // Scalar goldens hand-sync with clusterjava/mixed_oracle.go where values match.
    // MIXED_LOG_CHANNEL_MULTICAST is the Java final URI (endpoint= present); Go
    // MixedLogChannelMulticast is a template without endpoint=. Not byte-identical.
    // Topology strings are asserted in clusterConfigHelpers_matchMixedGoldenStrings.
    private static Stream<Arguments> mixedConstantGoldens() {
        return Stream.of(
            Arguments.of("MIXED_MEMBER_COUNT", 3, ClusterConstants.MIXED_MEMBER_COUNT),
            Arguments.of(
                "MIXED_APPOINTED_LEADER_ID_JAVA",
                0,
                ClusterConstants.MIXED_APPOINTED_LEADER_ID_JAVA),
            Arguments.of(
                "MIXED_APPOINTED_LEADER_ID_GO",
                2,
                ClusterConstants.MIXED_APPOINTED_LEADER_ID_GO),
            Arguments.of(
                "MIXED_APPOINTED_LEADER_ID_NONE",
                -1,
                ClusterConstants.MIXED_APPOINTED_LEADER_ID_NONE),
            Arguments.of(
                "MIXED_LOG_ENDPOINT",
                "239.255.12.15:9004",
                ClusterConstants.MIXED_LOG_ENDPOINT),
            Arguments.of(
                "MIXED_LOG_CHANNEL_UNICAST",
                "aeron:udp?term-length=64k",
                ClusterConstants.MIXED_LOG_CHANNEL_UNICAST),
            Arguments.of(
                "MIXED_LOG_CHANNEL_MULTICAST",
                "aeron:udp?term-length=64k|endpoint=239.255.12.15:9004"
                    + "|interface=172.30.0.0/16|ttl=1",
                ClusterConstants.MIXED_LOG_CHANNEL_MULTICAST),
            Arguments.of(
                "MIXED_HOSTNAMES",
                "172.30.0.10,172.30.0.11,172.30.0.12",
                ClusterConstants.MIXED_HOSTNAMES),
            Arguments.of(
                "MIXED_BASE_DIR_PREFIX",
                "/tmp/heron-cluster-",
                ClusterConstants.MIXED_BASE_DIR_PREFIX),
            Arguments.of(
                "SERVICE_PROBE_FILENAME",
                "service-probe",
                ClusterConstants.SERVICE_PROBE_FILENAME),
            Arguments.of(
                "SERVICE_PROBE_KEY_TERM",
                "term",
                ClusterConstants.SERVICE_PROBE_KEY_TERM),
            Arguments.of(
                "SERVICE_PROBE_KEY_LOG_POSITION",
                "logPosition",
                ClusterConstants.SERVICE_PROBE_KEY_LOG_POSITION),
            Arguments.of(
                "SERVICE_PROBE_KEY_ROLE",
                "role",
                ClusterConstants.SERVICE_PROBE_KEY_ROLE),
            Arguments.of(
                "SERVICE_PROBE_KEY_MEMBER_ID",
                "memberId",
                ClusterConstants.SERVICE_PROBE_KEY_MEMBER_ID),
            Arguments.of(
                "SNAPSHOT_PROBE_FILENAME",
                "snapshot-probe",
                ClusterConstants.SNAPSHOT_PROBE_FILENAME)
        );
    }
}
