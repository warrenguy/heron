package heron.clusterjava;

import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.exceptions.AeronException;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ClusterNode
{
    static final String ENV_MEMBER_ID = "HERON_CLUSTER_MEMBER_ID";
    static final String ENV_MEMBER_COUNT = "HERON_CLUSTER_MEMBER_COUNT";
    static final String ENV_HOSTNAMES = "HERON_CLUSTER_HOSTNAMES";

    static final class LauncherSettings
    {
        final int memberId;
        final int memberCount;
        final List<String> hostnames;

        LauncherSettings(final int memberId, final int memberCount, final List<String> hostnames)
        {
            this.memberId = memberId;
            this.memberCount = memberCount;
            this.hostnames = hostnames;
        }
    }

    private ClusterNode()
    {
    }

    // Aeron 1.51.0 ConsensusModule.Context has no egressStreamId(); egress stream 102
    // parity is checked via client AeronCluster default oracle in tests.
    static void configureConsensusIPC(final ClusterConfig clusterConfig)
    {
        configureConsensusChannels(
            clusterConfig,
            ClusterConstants.INGRESS_CHANNEL,
            ClusterConstants.EGRESS_CHANNEL,
            true);
    }

    static void configureConsensusUDP(final ClusterConfig clusterConfig)
    {
        configureConsensusChannels(
            clusterConfig,
            ClusterConstants.FAILOVER_INGRESS_CHANNEL,
            ClusterConstants.FAILOVER_CONSENSUS_EGRESS_CHANNEL,
            false);
        clusterConfig.consensusModuleContext()
            .sessionTimeoutNs(ClusterConstants.FAILOVER_SESSION_TIMEOUT_NS);
    }

    private static void configureConsensusChannels(
        final ClusterConfig clusterConfig,
        final String ingressChannel,
        final String egressChannel,
        final boolean ipcIngressAllowed)
    {
        final ConsensusModule.Context consensus = clusterConfig.consensusModuleContext()
            .ingressChannel(ingressChannel)
            .egressChannel(egressChannel)
            .ingressStreamId(ClusterConstants.INGRESS_STREAM_ID);
        if (ipcIngressAllowed)
        {
            // IPC channel-only connect requires leader ipc ingress subscription (Aeron default false).
            consensus.isIpcIngressAllowed(true);
        }
    }

    static void configureConsensus(final ClusterConfig clusterConfig, final int memberCount)
    {
        if (memberCount == 1)
        {
            configureConsensusIPC(clusterConfig);
        }
        else
        {
            configureConsensusUDP(clusterConfig);
        }
    }

    /**
     * Heron cluster launcher for compose harness (single-node IPC or multi-node UDP).
     */
    public static void main(final String[] args)
    {
        final int exitCode = startupExitCode(System.getenv("AERON_DIR"));
        if (exitCode != 0)
        {
            System.exit(exitCode);
        }
        // Success path relies on implicit JVM exit 0 after barrier shutdown (no System.exit(0)).
    }

    static int startupExitCode(final String aeronDir)
    {
        return startupExitCode(
            aeronDir,
            System.getenv(ENV_MEMBER_ID),
            System.getenv(ENV_MEMBER_COUNT),
            System.getenv(ENV_HOSTNAMES));
    }

    static int startupExitCode(
        final String aeronDir,
        final String memberIdEnv,
        final String memberCountEnv,
        final String hostnamesEnv)
    {
        try
        {
            runCluster(
                aeronDir,
                launcherSettingsFromEnv(memberIdEnv, memberCountEnv, hostnamesEnv));
            return 0;
        }
        catch (final Exception e)
        {
            reportStartupFailure(e);
            return 1;
        }
    }

    static void reportStartupFailure(final Exception e)
    {
        System.err.println("heron: cluster node failed: " + e.getMessage());
        if (Boolean.getBoolean("heron.debug"))
        {
            e.printStackTrace(System.err);
        }
    }

    // WARN-category AeronExceptions are recoverable; everything else exits the node.
    static boolean isFatalClusterError(final Throwable throwable)
    {
        if (throwable instanceof AeronException aeronException)
        {
            return aeronException.category() != AeronException.Category.WARN;
        }
        return true;
    }

    static int parseIntEnv(final String envName, final String envValue, final int defaultValue)
    {
        if (envValue == null || envValue.isBlank())
        {
            return defaultValue;
        }

        try
        {
            return Integer.parseInt(envValue.trim());
        }
        catch (final NumberFormatException e)
        {
            throw new IllegalArgumentException(
                envName + " must be an integer (got \"" + envValue + "\")");
        }
    }

    static void validateMemberCount(final int memberCount)
    {
        if (memberCount < 1)
        {
            throw new IllegalArgumentException(
                ENV_MEMBER_COUNT + " must be >= 1 (got " + memberCount + ")");
        }
    }

    static void validateMemberId(final int memberId, final int memberCount)
    {
        if (memberId < 0 || memberId >= memberCount)
        {
            throw new IllegalArgumentException(
                ENV_MEMBER_ID + " must satisfy 0 <= memberId < memberCount "
                    + "(got memberId=" + memberId + ", memberCount=" + memberCount + ")");
        }
    }

    static LauncherSettings launcherSettingsFromEnv(
        final String memberIdEnv,
        final String memberCountEnv,
        final String hostnamesEnv)
    {
        final int memberCount = parseIntEnv(ENV_MEMBER_COUNT, memberCountEnv, ClusterConstants.MEMBER_COUNT);
        validateMemberCount(memberCount);
        final int memberId = parseIntEnv(ENV_MEMBER_ID, memberIdEnv, ClusterConstants.MEMBER_ID);
        validateMemberId(memberId, memberCount);
        final List<String> hostnames = parseHostnames(
            hostnamesEnv,
            ClusterConstants.INGRESS_ENDPOINT_HOST,
            memberCount);
        return new LauncherSettings(memberId, memberCount, hostnames);
    }

    /**
     * Parse comma-separated hostnames from {@code env}.
     *
     * @param env         raw {@link #ENV_HOSTNAMES} value; null, empty, or whitespace-only uses
     *                    {@code fallback} when {@code memberCount == 1}
     * @param fallback    single-host default when env is unset and {@code memberCount == 1}
     * @param memberCount expected hostname count (must be &gt;= 1)
     * @return immutable hostname list
     * @throws IllegalArgumentException when {@code memberCount &lt; 1}, env is required but unset
     *         for multi-node, token count mismatches, or a comma-delimited token is blank
     */
    static List<String> parseHostnames(final String env, final String fallback, final int memberCount)
    {
        if (memberCount < 1)
        {
            throw new IllegalArgumentException("memberCount must be >= 1 (got " + memberCount + ")");
        }

        if (env == null || env.isBlank())
        {
            if (memberCount == 1)
            {
                return List.of(fallback);
            }
            throw new IllegalArgumentException(
                ENV_HOSTNAMES + " required for memberCount=" + memberCount
                    + "; set comma-separated hostnames (e.g. cluster-0,cluster-1,cluster-2)");
        }

        final List<String> hostnames = new ArrayList<>();
        for (final String part : env.split(",", -1))
        {
            final String trimmed = part.trim();
            // Reject blank tokens (e.g. trailing comma or "cluster-0, ,cluster-2") before count check.
            if (trimmed.isEmpty())
            {
                throw new IllegalArgumentException(
                    ENV_HOSTNAMES + " contains empty hostname; "
                        + "use comma-separated hostnames without blanks");
            }
            hostnames.add(trimmed);
        }

        if (hostnames.size() != memberCount)
        {
            throw new IllegalArgumentException(
                ENV_HOSTNAMES + " must list exactly " + memberCount
                    + " hostnames (got " + hostnames.size() + ")");
        }

        return List.copyOf(hostnames);
    }

    /**
     * Resolve cluster persistence base directory for this member.
     *
     * @param memberId    cluster member id (ignored for path when {@code memberCount == 1})
     * @param memberCount total members; single-node uses {@link ClusterConstants#BASE_DIR},
     *                    multi-node uses {@link ClusterConstants#FAILOVER_BASE_DIR_PREFIX}{@code memberId}
     */
    static File baseDirForMember(final int memberId, final int memberCount)
    {
        if (memberCount == 1)
        {
            return new File(ClusterConstants.BASE_DIR);
        }
        return new File(ClusterConstants.FAILOVER_BASE_DIR_PREFIX + memberId);
    }

    @SuppressWarnings("try")
    static void runCluster(final String aeronDir)
    {
        runCluster(
            aeronDir,
            launcherSettingsFromEnv(
                System.getenv(ENV_MEMBER_ID),
                System.getenv(ENV_MEMBER_COUNT),
                System.getenv(ENV_HOSTNAMES)));
    }

    @SuppressWarnings("try")
    static void runCluster(
        final String aeronDir,
        final String memberIdEnv,
        final String memberCountEnv,
        final String hostnamesEnv)
    {
        runCluster(aeronDir, launcherSettingsFromEnv(memberIdEnv, memberCountEnv, hostnamesEnv));
    }

    @SuppressWarnings("try")
    static void runCluster(final String aeronDir, final LauncherSettings settings)
    {
        Objects.requireNonNull(aeronDir, "AERON_DIR required");
        final ClusterConfig clusterConfig = ClusterConfig.create(
            settings.memberId,
            settings.hostnames,
            ClusterConstants.PORT_BASE,
            new EchoService());

        clusterConfig.aeronDirectoryName(aeronDir);
        clusterConfig.baseDir(baseDirForMember(settings.memberId, settings.memberCount));
        configureConsensus(clusterConfig, settings.memberCount);
        // WARN-category agent errors are transient during failover (e.g. re-resolve of a
        // downed member endpoint); log and survive so quorum holds. Non-WARN errors exit;
        // SIGTERM still uses barrier teardown below.
        final AtomicBoolean fatalExited = new AtomicBoolean(false);
        clusterConfig.errorHandler((Throwable throwable) -> {
            final String detail =
                Objects.toString(throwable.getMessage(), throwable.getClass().getSimpleName());
            if (!isFatalClusterError(throwable))
            {
                System.err.println("heron: cluster warn: " + detail);
                return;
            }
            if (fatalExited.getAndSet(true))
            {
                return;
            }
            System.err.println("heron: cluster fatal: " + detail);
            throwable.printStackTrace(System.err);
            System.exit(1);
        });

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
            ClusteredMediaDriver _ = ClusteredMediaDriver.launch(
                clusterConfig.mediaDriverContext().terminationHook(barrier::signalAll),
                clusterConfig.archiveContext(),
                clusterConfig.consensusModuleContext().terminationHook(barrier::signalAll));
            ClusteredServiceContainer _ = ClusteredServiceContainer.launch(
                clusterConfig.clusteredServiceContext().terminationHook(barrier::signalAll)))
        {
            System.out.println("heron: cluster node ready memberId=" + settings.memberId);
            barrier.await();
        }
    }
}
