package heron.clusterjava;

import io.aeron.Aeron;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.NanosecondClusterClock;
import io.aeron.driver.exceptions.InvalidChannelException;
import io.aeron.exceptions.AeronException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterNodeTest {
    private static final String STARTUP_FAILURE_PREFIX = "heron: cluster node failed: ";
    private static final String HOSTNAMES_REQUIRED_MESSAGE =
        "HERON_CLUSTER_HOSTNAMES required for memberCount=3; "
            + "set comma-separated hostnames (e.g. cluster-0,cluster-1,cluster-2)";
    private static final String EMPTY_HOSTNAME_MESSAGE =
        "HERON_CLUSTER_HOSTNAMES contains empty hostname; "
            + "use comma-separated hostnames without blanks";

    private final Map<String, String> savedProperties = new HashMap<>();

    @AfterEach
    void restoreSystemProperties() {
        savedProperties.forEach(ClusterNodeTest::applySystemProperty);
        savedProperties.clear();
    }

    @Test
    void runCluster_throwsWhenAeronDirUnset() {
        final NullPointerException ex = assertThrows(
            NullPointerException.class,
            () -> ClusterNode.runCluster(null)
        );
        assertEquals("AERON_DIR required", ex.getMessage());
    }

    @Test
    void startupExitCode_returnsOneWhenAeronDirUnset() {
        final String stderr = captureStderr(
            () -> assertEquals(1, ClusterNode.startupExitCode(null))
        );

        assertEquals(STARTUP_FAILURE_PREFIX + "AERON_DIR required" + System.lineSeparator(), stderr);
        assertFalse(stderr.contains("\tat "));
    }

    @Test
    void parseIntEnv_treatsBlankValuesAsDefault() {
        assertEquals(ClusterConstants.MEMBER_ID, ClusterNode.parseIntEnv(
            ClusterNode.ENV_MEMBER_ID, null, ClusterConstants.MEMBER_ID));
        assertEquals(ClusterConstants.MEMBER_ID, ClusterNode.parseIntEnv(
            ClusterNode.ENV_MEMBER_ID, "", ClusterConstants.MEMBER_ID));
        assertEquals(ClusterConstants.MEMBER_ID, ClusterNode.parseIntEnv(
            ClusterNode.ENV_MEMBER_ID, "   ", ClusterConstants.MEMBER_ID));
        assertEquals(ClusterConstants.MEMBER_COUNT, ClusterNode.parseIntEnv(
            ClusterNode.ENV_MEMBER_COUNT, "  \t  ", ClusterConstants.MEMBER_COUNT));
    }

    @Test
    void startupExitCode_returnsOneWhenMemberIdEnvInvalid() {
        assertStartupFailureStderr(
            "HERON_CLUSTER_MEMBER_ID must be an integer (got \"foo\")",
            () -> assertEquals(1, ClusterNode.startupExitCode("/dev/shm/aeron", "foo", null, null)));
    }

    @Test
    void startupExitCode_returnsOneWhenMemberCountEnvInvalid() {
        assertStartupFailureStderr(
            "HERON_CLUSTER_MEMBER_COUNT must be an integer (got \"1.5\")",
            () -> assertEquals(1, ClusterNode.startupExitCode("/dev/shm/aeron", null, "1.5", null)));
    }

    @Test
    void isFatalClusterError_survivesWarnAndUnresolvedHost() {
        assertFalse(ClusterNode.isFatalClusterError(
            new AeronException("could not re-resolve: endpoint=cluster-0:9003", AeronException.Category.WARN)));
        assertFalse(ClusterNode.isFatalClusterError(
            new InvalidChannelException(new UnknownHostException(
                "unresolved - endpoint=cluster-1:9103, "
                    + "name-resolver=TimeTracking(io.aeron.driver.DefaultNameResolver)"))));
        assertFalse(ClusterNode.isFatalClusterError(
            new UnknownHostException("unresolved - endpoint=cluster-1:9103")));
        assertTrue(ClusterNode.isFatalClusterError(new InvalidChannelException("malformed channel")));
        assertTrue(ClusterNode.isFatalClusterError(
            new AeronException("boom", AeronException.Category.ERROR)));
        assertTrue(ClusterNode.isFatalClusterError(
            new AeronException("boom", AeronException.Category.FATAL)));
        assertTrue(ClusterNode.isFatalClusterError(new RuntimeException("boom")));
    }

    @Test
    void reportStartupFailure_printsMessageWithoutStackTraceByDefault() {
        withProperty("heron.debug", "false");

        final String stderr = captureStderr(
            () -> ClusterNode.reportStartupFailure(new NullPointerException("AERON_DIR required"))
        );

        assertEquals(STARTUP_FAILURE_PREFIX + "AERON_DIR required" + System.lineSeparator(), stderr);
        assertFalse(stderr.contains("\tat "));
    }

    @Test
    void parseHostnames_usesFallbackWhenEnvMissingOrBlankAndMemberCountOne() {
        assertEquals(List.of("localhost"), ClusterNode.parseHostnames(null, "localhost", 1));
        assertEquals(List.of("localhost"), ClusterNode.parseHostnames("", "localhost", 1));
        assertEquals(List.of("localhost"), ClusterNode.parseHostnames("   ", "localhost", 1));
    }

    @Test
    void parseHostnames_acceptsExplicitSingleHostnameWhenMemberCountOne() {
        assertEquals(List.of("custom-host"), ClusterNode.parseHostnames("custom-host", "localhost", 1));
    }

    @Test
    void parseHostnames_requiresEnvWhenMemberCountGreaterThanOne() {
        assertIllegalArgument(() -> ClusterNode.parseHostnames(null, "localhost", 3), HOSTNAMES_REQUIRED_MESSAGE);
        assertIllegalArgument(() -> ClusterNode.parseHostnames("", "localhost", 3), HOSTNAMES_REQUIRED_MESSAGE);
        assertIllegalArgument(() -> ClusterNode.parseHostnames("   ", "localhost", 3), HOSTNAMES_REQUIRED_MESSAGE);
    }

    @Test
    void parseHostnames_rejectsMemberCountLessThanOne() {
        assertIllegalArgument(
            () -> ClusterNode.parseHostnames(null, "localhost", 0),
            "memberCount must be >= 1 (got 0)");
    }

    @Test
    void parseHostnames_requiresExactHostnameCount() {
        assertIllegalArgument(
            () -> ClusterNode.parseHostnames("cluster-0,cluster-1", "localhost", 3),
            exactlyHostnamesMessage(3, 2));
        assertIllegalArgument(
            () -> ClusterNode.parseHostnames("cluster-0,cluster-1,cluster-2,cluster-3", "localhost", 3),
            exactlyHostnamesMessage(3, 4));
    }

    @Test
    void parseHostnames_rejectsEmptyHostnameTokens() {
        assertIllegalArgument(
            () -> ClusterNode.parseHostnames("cluster-0,,cluster-2", "localhost", 3),
            EMPTY_HOSTNAME_MESSAGE);
        assertIllegalArgument(
            () -> ClusterNode.parseHostnames("cluster-0, ,cluster-2", "localhost", 3),
            EMPTY_HOSTNAME_MESSAGE);
        assertIllegalArgument(
            () -> ClusterNode.parseHostnames("cluster-0,cluster-1,cluster-2,", "localhost", 3),
            EMPTY_HOSTNAME_MESSAGE);
    }

    @Test
    void parseHostnames_trimsWhitespaceAndAcceptsThreeHosts() {
        assertEquals(
            List.of("cluster-0", "cluster-1", "cluster-2"),
            ClusterNode.parseHostnames("cluster-0, cluster-1 , cluster-2", "localhost", 3));
    }

    @Test
    void parseHostnames_failoverHostnamesMatchGoldenStrings() {
        final List<String> hostnames = ClusterNode.parseHostnames(
            failoverHostnamesEnv(),
            "localhost",
            ClusterConstants.FAILOVER_MEMBER_COUNT);
        assertEquals(
            ClusterConstants.FAILOVER_CLUSTER_MEMBERS_STRING,
            ClusterConfig.clusterMembers(hostnames, hostnames, ClusterConstants.PORT_BASE));
        assertEquals(
            ClusterConstants.FAILOVER_INGRESS_ENDPOINTS,
            ClusterConfig.ingressEndpoints(
                hostnames,
                ClusterConstants.PORT_BASE,
                ClusterConfig.CLIENT_FACING_PORT_OFFSET));
    }

    @Test
    void baseDirForMember_usesClusterConstantsBaseDirForSingleNode() {
        assertEquals(new File(ClusterConstants.BASE_DIR), ClusterNode.baseDirForMember(0, 1));
    }

    @Test
    void baseDirForMember_usesShmPathForMultiNode() {
        assertEquals(
            new File(ClusterConstants.FAILOVER_BASE_DIR_PREFIX + "0"),
            ClusterNode.baseDirForMember(0, 3));
        assertEquals(
            new File(ClusterConstants.FAILOVER_BASE_DIR_PREFIX + "2"),
            ClusterNode.baseDirForMember(2, 3));
    }

    @Test
    void launcherSettingsFromEnv_defaultsMatchSingleNodeConstants() {
        final ClusterNode.LauncherSettings settings = ClusterNode.launcherSettingsFromEnv(null, null, null);

        assertEquals(ClusterConstants.MEMBER_ID, settings.memberId);
        assertEquals(ClusterConstants.MEMBER_COUNT, settings.memberCount);
        assertEquals(List.of(ClusterConstants.INGRESS_ENDPOINT_HOST), settings.hostnames);
        assertFalse(settings.mixed);
        assertEquals(ClusterConstants.MIXED_APPOINTED_LEADER_ID_JAVA, settings.appointedLeaderId);
        assertEquals(new File(ClusterConstants.BASE_DIR), ClusterNode.baseDirForMember(
            settings.memberId,
            settings.memberCount,
            settings.mixed));
    }

    @Test
    void launcherSettingsFromEnv_treatsBlankMemberEnvAsDefaults() {
        final ClusterNode.LauncherSettings settings = ClusterNode.launcherSettingsFromEnv("   ", "  ", "  ");

        assertEquals(ClusterConstants.MEMBER_ID, settings.memberId);
        assertEquals(ClusterConstants.MEMBER_COUNT, settings.memberCount);
        assertEquals(List.of(ClusterConstants.INGRESS_ENDPOINT_HOST), settings.hostnames);
        assertFalse(settings.mixed);
        assertEquals(ClusterConstants.MIXED_APPOINTED_LEADER_ID_JAVA, settings.appointedLeaderId);
    }

    @Test
    void validateMemberCount_rejectsNonPositiveValues() {
        assertIllegalArgument(() -> ClusterNode.validateMemberCount(0), memberCountNonPositiveMessage(0));
        assertIllegalArgument(() -> ClusterNode.validateMemberCount(-1), memberCountNonPositiveMessage(-1));
    }

    @Test
    void launcherSettingsFromEnv_rejectsNonPositiveMemberCount() {
        assertIllegalArgument(
            () -> ClusterNode.launcherSettingsFromEnv(null, "0", null),
            memberCountNonPositiveMessage(0));
        assertIllegalArgument(
            () -> ClusterNode.launcherSettingsFromEnv(null, "-1", null),
            memberCountNonPositiveMessage(-1));
    }

    @Test
    void validateMemberId_rejectsNegativeMemberId() {
        assertIllegalArgument(
            () -> ClusterNode.validateMemberId(-1, 3),
            memberIdOutOfRangeMessage(-1, 3));
    }

    @Test
    void validateMemberId_rejectsMemberIdAtOrAboveMemberCount() {
        assertIllegalArgument(
            () -> ClusterNode.validateMemberId(3, 3),
            memberIdOutOfRangeMessage(3, 3));
        assertIllegalArgument(
            () -> ClusterNode.validateMemberId(2, 2),
            memberIdOutOfRangeMessage(2, 2));
    }

    @Test
    void validateMemberId_rejectsSingleNodeMemberIdMismatch() {
        assertIllegalArgument(
            () -> ClusterNode.validateMemberId(1, 1),
            memberIdOutOfRangeMessage(1, 1));
    }

    @Test
    void startupExitCode_returnsOneWhenMemberIdOutOfRange() {
        assertStartupFailureStderr(
            memberIdOutOfRangeMessage(3, 3),
            () -> assertEquals(1, ClusterNode.startupExitCode("/dev/shm/aeron", "3", "3", null)));
    }

    @Test
    void startupExitCode_returnsOneWhenMemberCountNonPositive() {
        assertStartupFailureStderr(
            memberCountNonPositiveMessage(0),
            () -> assertEquals(1, ClusterNode.startupExitCode("/dev/shm/aeron", null, "0", null)));
    }

    @Test
    void multiNodeClusterConfig_usesFailoverUdpChannels() {
        final ClusterNode.LauncherSettings settings = ClusterNode.launcherSettingsFromEnv(
            "1",
            String.valueOf(ClusterConstants.FAILOVER_MEMBER_COUNT),
            failoverHostnamesEnv(),
            null);
        final ClusterConfig multiNodeConfig = createClusterConfig(settings.memberId, settings.hostnames);
        multiNodeConfig.baseDir(ClusterNode.baseDirForMember(
            settings.memberId,
            settings.memberCount,
            settings.mixed));
        ClusterNode.configureConsensus(multiNodeConfig, settings.memberCount);

        final ClusterConfig singleNodeConfig = createClusterConfig(
            ClusterConstants.MEMBER_ID,
            List.of(ClusterConstants.INGRESS_ENDPOINT_HOST));
        ClusterNode.configureConsensus(singleNodeConfig, 1);

        assertConsensusUsesUdpChannels(multiNodeConfig.consensusModuleContext());
        assertConsensusLeavesMixedFieldsAtAeronDefaults(multiNodeConfig.consensusModuleContext());
        assertConsensusUsesIpcChannels(singleNodeConfig.consensusModuleContext());
        assertFalse(settings.mixed);
    }

    @Test
    void launcherSettingsFromEnv_mixedUsesEnvTopology() {
        final ClusterNode.LauncherSettings settings = ClusterNode.launcherSettingsFromEnv(
            "0",
            String.valueOf(ClusterConstants.MIXED_MEMBER_COUNT),
            ClusterConstants.MIXED_HOSTNAMES,
            "1");

        assertTrue(settings.mixed);
        assertEquals(0, settings.memberId);
        assertEquals(ClusterConstants.MIXED_MEMBER_COUNT, settings.memberCount);
        assertEquals(MIXED_HOSTS, settings.hostnames);
        assertEquals(ClusterConstants.MIXED_APPOINTED_LEADER_ID_JAVA, settings.appointedLeaderId);
        assertEquals(
            new File(ClusterConstants.MIXED_BASE_DIR_PREFIX + "0"),
            ClusterNode.baseDirForMember(settings.memberId, settings.memberCount, settings.mixed));
    }

    @Test
    void launcherSettingsFromEnv_mixedParsesAppointedLeaderId() {
        final ClusterNode.LauncherSettings goLeader = ClusterNode.launcherSettingsFromEnv(
            "0",
            String.valueOf(ClusterConstants.MIXED_MEMBER_COUNT),
            ClusterConstants.MIXED_HOSTNAMES,
            "1",
            String.valueOf(ClusterConstants.MIXED_APPOINTED_LEADER_ID_GO));
        assertTrue(goLeader.mixed);
        assertEquals(ClusterConstants.MIXED_APPOINTED_LEADER_ID_GO, goLeader.appointedLeaderId);

        final ClusterNode.LauncherSettings javaLeader = ClusterNode.launcherSettingsFromEnv(
            "0",
            String.valueOf(ClusterConstants.MIXED_MEMBER_COUNT),
            ClusterConstants.MIXED_HOSTNAMES,
            "1",
            String.valueOf(ClusterConstants.MIXED_APPOINTED_LEADER_ID_JAVA));
        assertEquals(ClusterConstants.MIXED_APPOINTED_LEADER_ID_JAVA, javaLeader.appointedLeaderId);

        final ClusterNode.LauncherSettings blankAppointed = ClusterNode.launcherSettingsFromEnv(
            "0",
            String.valueOf(ClusterConstants.MIXED_MEMBER_COUNT),
            ClusterConstants.MIXED_HOSTNAMES,
            "1",
            "  ");
        assertEquals(
            ClusterConstants.MIXED_APPOINTED_LEADER_ID_JAVA,
            blankAppointed.appointedLeaderId);
    }

    @Test
    void launcherSettingsFromEnv_rejectsNonIntegerAppointedLeaderId() {
        assertIllegalArgument(
            () -> ClusterNode.launcherSettingsFromEnv(
                "0",
                String.valueOf(ClusterConstants.MIXED_MEMBER_COUNT),
                ClusterConstants.MIXED_HOSTNAMES,
                "1",
                "not-an-int"),
            "HERON_CLUSTER_APPOINTED_LEADER_ID must be an integer (got \"not-an-int\")");
    }

    @Test
    void mixedClusterConfig_javaLeader_appliesUnicastOnTopOfUdp() {
        final ConsensusModule.Context consensus = configureMixedConsensus();

        assertConsensusUsesFailoverUdpTransport(consensus);
        assertEquals(ClusterConstants.MIXED_APPOINTED_LEADER_ID_JAVA, consensus.appointedLeaderId());
        assertEquals(ClusterConstants.MIXED_LOG_CHANNEL_UNICAST, consensus.logChannel());
        assertEquals(ClusterConstants.MIXED_CLUSTER_MEMBERS_JAVA_LEADER, consensus.clusterMembers());
        assertNull(consensus.clusterClock());
    }

    @Test
    void mixedClusterConfig_goLeader_appliesMulticastClockAndSharedLog() {
        final ConsensusModule.Context consensus = configureMixedConsensusGoLeader();

        assertConsensusUsesFailoverUdpTransport(consensus);
        assertEquals(ClusterConstants.MIXED_APPOINTED_LEADER_ID_GO, consensus.appointedLeaderId());
        assertEquals(ClusterConstants.MIXED_LOG_CHANNEL_MULTICAST, consensus.logChannel());
        assertEquals(ClusterConstants.MIXED_CLUSTER_MEMBERS_GO_LEADER, consensus.clusterMembers());
        assertInstanceOf(NanosecondClusterClock.class, consensus.clusterClock());
    }

    @Test
    void configureConsensusMixed_rejectsUnsupportedAppointedLeaderId() {
        final ClusterConfig config = createClusterConfig(0, MIXED_HOSTS);
        ClusterNode.configureConsensus(config, ClusterConstants.MIXED_MEMBER_COUNT);

        final IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ClusterNode.configureConsensusMixed(config, 1));
        assertEquals(
            "HERON_CLUSTER_APPOINTED_LEADER_ID in mixed mode must be 0 (java leader) or "
                + "2 (go leader); got 1",
            ex.getMessage());
    }

    @Test
    void baseDirForMember_usesMixedPrefixWhenMixed() {
        final int memberCount = ClusterConstants.MIXED_MEMBER_COUNT;
        assertEquals(
            new File(ClusterConstants.MIXED_BASE_DIR_PREFIX + "1"),
            ClusterNode.baseDirForMember(1, memberCount, true));
        // Failover path unchanged when mixed is false.
        assertEquals(
            new File(ClusterConstants.FAILOVER_BASE_DIR_PREFIX + "1"),
            ClusterNode.baseDirForMember(1, memberCount, false));
        // Two-arg form is the non-mixed path only.
        assertEquals(
            new File(ClusterConstants.FAILOVER_BASE_DIR_PREFIX + "1"),
            ClusterNode.baseDirForMember(1, memberCount));
    }

    @Test
    void reportStartupFailure_printsStackTraceWhenDebugEnabled() {
        withProperty("heron.debug", "true");

        final String stderr = captureStderr(
            () -> ClusterNode.reportStartupFailure(new NullPointerException("AERON_DIR required"))
        );

        assertTrue(stderr.startsWith(STARTUP_FAILURE_PREFIX + "AERON_DIR required"));
        assertTrue(stderr.contains("java.lang.NullPointerException: AERON_DIR required"));
        assertTrue(stderr.contains("\tat "));
    }

    static final List<String> FAILOVER_HOSTS = IntStream.range(0, ClusterConstants.FAILOVER_MEMBER_COUNT)
        .mapToObj(i -> "cluster-" + i)
        .toList();

    static final List<String> MIXED_HOSTS = List.of(ClusterConstants.MIXED_HOSTNAMES.split(","));

    static List<String> failoverHosts() {
        return FAILOVER_HOSTS;
    }

    static String failoverHostnamesEnv() {
        return String.join(",", FAILOVER_HOSTS);
    }

    static ClusterConfig createClusterConfig(final int memberId, final List<String> hostnames) {
        return ClusterConfig.create(memberId, hostnames, ClusterConstants.PORT_BASE, new EchoService());
    }

    // Builds consensus after multi-node UDP then mixed overlay for member 0 (java leader).
    static ConsensusModule.Context configureMixedConsensus() {
        return configureMixedConsensusWithAppointed(
            ClusterConstants.MIXED_APPOINTED_LEADER_ID_JAVA);
    }

    // Builds consensus after multi-node UDP then mixed overlay for go leader (appointed 2).
    static ConsensusModule.Context configureMixedConsensusGoLeader() {
        return configureMixedConsensusWithAppointed(
            ClusterConstants.MIXED_APPOINTED_LEADER_ID_GO);
    }

    static ConsensusModule.Context configureMixedConsensusWithAppointed(final int appointedLeaderId) {
        final ClusterNode.LauncherSettings settings = ClusterNode.launcherSettingsFromEnv(
            "0",
            String.valueOf(ClusterConstants.MIXED_MEMBER_COUNT),
            ClusterConstants.MIXED_HOSTNAMES,
            "1",
            String.valueOf(appointedLeaderId));
        final ClusterConfig config = createClusterConfig(settings.memberId, settings.hostnames);
        ClusterNode.configureConsensus(config, settings);
        return config.consensusModuleContext();
    }

    // Failover and mixed UDP transport fields shared by multi-node consensus setup.
    static void assertConsensusUsesFailoverUdpTransport(final ConsensusModule.Context consensus) {
        assertFalse(consensus.isIpcIngressAllowed());
        assertEquals(ClusterConstants.FAILOVER_INGRESS_CHANNEL, consensus.ingressChannel());
        assertEquals(ClusterConstants.FAILOVER_CONSENSUS_EGRESS_CHANNEL, consensus.egressChannel());
        assertEquals(ClusterConstants.FAILOVER_SESSION_TIMEOUT_NS, consensus.sessionTimeoutNs());
        assertEquals(ClusterConstants.INGRESS_STREAM_ID, consensus.ingressStreamId());
    }

    static void assertConsensusUsesUdpChannels(final ConsensusModule.Context consensus) {
        assertConsensusUsesFailoverUdpTransport(consensus);
        assertEquals(ClusterConstants.FAILOVER_CLUSTER_MEMBERS_STRING, consensus.clusterMembers());
    }

    // Non-mixed configureConsensus must not touch appointed leader, log channel, or clock.
    // Defaults match Aeron ConsensusModule.Context before conclude().
    static void assertConsensusLeavesMixedFieldsAtAeronDefaults(final ConsensusModule.Context consensus) {
        assertEquals(Aeron.NULL_VALUE, consensus.appointedLeaderId());
        assertEquals("aeron:udp?term-length=64m", consensus.logChannel());
        assertNull(consensus.clusterClock());
    }

    static void assertConsensusUsesIpcChannels(final ConsensusModule.Context consensus) {
        assertTrue(consensus.isIpcIngressAllowed());
        assertEquals(ClusterConstants.INGRESS_CHANNEL, consensus.ingressChannel());
        assertEquals(ClusterConstants.EGRESS_CHANNEL, consensus.egressChannel());
        assertEquals(ClusterConstants.INGRESS_STREAM_ID, consensus.ingressStreamId());
    }

    private static String memberCountNonPositiveMessage(final int got) {
        return "HERON_CLUSTER_MEMBER_COUNT must be >= 1 (got " + got + ")";
    }

    private static String memberIdOutOfRangeMessage(final int memberId, final int memberCount) {
        return "HERON_CLUSTER_MEMBER_ID must satisfy 0 <= memberId < memberCount "
            + "(got memberId=" + memberId + ", memberCount=" + memberCount + ")";
    }

    private static String exactlyHostnamesMessage(final int memberCount, final int got) {
        return "HERON_CLUSTER_HOSTNAMES must list exactly " + memberCount + " hostnames (got " + got + ")";
    }

    private static void assertIllegalArgument(final Executable action, final String expectedMessage) {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, action);
        assertEquals(expectedMessage, ex.getMessage());
    }

    private static void assertStartupFailureStderr(final String failureDetail, final Executable startup) {
        assertEquals(
            STARTUP_FAILURE_PREFIX + failureDetail + System.lineSeparator(),
            captureStderr(startup));
    }

    static String captureStderr(final Executable action) {
        final PrintStream originalErr = System.err;
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        System.setErr(new PrintStream(buffer, true, StandardCharsets.UTF_8));
        try {
            action.execute();
        } catch (final Throwable t) {
            if (t instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (t instanceof Error error) {
                throw error;
            }
            throw new AssertionError(t);
        } finally {
            System.setErr(originalErr);
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    private void withProperty(final String key, final String value) {
        if (!savedProperties.containsKey(key)) {
            savedProperties.put(key, System.getProperty(key));
        }
        applySystemProperty(key, value);
    }

    private static void applySystemProperty(final String key, final String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
