package heron.clusterjava;

import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;
import java.util.List;
import java.util.Objects;

public final class ClusterNode
{
    private ClusterNode()
    {
    }

    // Aeron 1.51.0 ConsensusModule.Context has no egressStreamId(); egress stream 102
    // parity is checked via client AeronCluster default oracle in tests.
    static void configureConsensusIPC(final ClusterConfig clusterConfig)
    {
        clusterConfig.consensusModuleContext()
            .ingressChannel(ClusterConstants.INGRESS_CHANNEL)
            .egressChannel(ClusterConstants.EGRESS_CHANNEL)
            .ingressStreamId(ClusterConstants.INGRESS_STREAM_ID)
            // IPC channel-only connect requires leader ipc ingress subscription (Aeron default false).
            .isIpcIngressAllowed(true);
    }

    /**
     * Heron single-node cluster launcher for compose harness.
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
        try
        {
            runCluster(aeronDir);
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

    @SuppressWarnings("try")
    static void runCluster(final String aeronDir)
    {
        Objects.requireNonNull(aeronDir, "AERON_DIR required");
        final List<String> hostnames = List.of(ClusterConstants.INGRESS_ENDPOINT_HOST);
        final ClusterConfig clusterConfig = ClusterConfig.create(
            ClusterConstants.MEMBER_ID, hostnames, ClusterConstants.PORT_BASE, new EchoService());

        clusterConfig.aeronDirectoryName(aeronDir);
        clusterConfig.baseDir(new File(ClusterConstants.BASE_DIR));
        configureConsensusIPC(clusterConfig);
        clusterConfig.errorHandler(
            (Throwable throwable) ->
            {
                System.err.println("Cluster Node");
                throwable.printStackTrace(System.err);
            });

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
            ClusteredMediaDriver _ = ClusteredMediaDriver.launch(
                clusterConfig.mediaDriverContext().terminationHook(barrier::signalAll),
                clusterConfig.archiveContext(),
                clusterConfig.consensusModuleContext().terminationHook(barrier::signalAll));
            ClusteredServiceContainer _ = ClusteredServiceContainer.launch(
                clusterConfig.clusteredServiceContext().terminationHook(barrier::signalAll)))
        {
            System.out.println(
                "heron: cluster node ready memberId=" + ClusterConstants.MEMBER_ID);
            barrier.await();
        }
    }
}
