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

    // Consensus IPC channels from ClusterConstants.
    static void configureConsensusIPC(final ClusterConfig clusterConfig)
    {
        clusterConfig.consensusModuleContext()
            .ingressChannel(ClusterConstants.INGRESS_CHANNEL)
            .egressChannel(ClusterConstants.EGRESS_CHANNEL)
            .ingressStreamId(ClusterConstants.INGRESS_STREAM_ID);
    }

    /**
     * Heron single-node cluster launcher for compose harness.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        final String aeronDir = Objects.requireNonNull(
            System.getenv("AERON_DIR"),
            "AERON_DIR required");
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
