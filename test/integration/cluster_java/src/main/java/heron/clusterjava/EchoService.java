/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Modified from io.aeron.samples.cluster.EchoService.
// Package renamed to heron.clusterjava.
// Echoes inbound session messages via session.offer with idle-strategy retries.
//
// On term start, writes a service-probe file under the member baseDir.
// Mixed go-leader smokes assert that file for Java service term-event decode.
package heron.clusterjava;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeUnit;

class EchoService implements ClusteredService
{
    static final int SEND_ATTEMPTS = 3;
    static final String OFFER_EXHAUSTED_MESSAGE =
        "heron: EchoService offer exhausted after " + SEND_ATTEMPTS + " attempts";
    static final String SERVICE_PROBE_WRITE_FAILED_PREFIX =
        probeWriteFailedPrefix(ClusterConstants.SERVICE_PROBE_FILENAME);

    private IdleStrategy idleStrategy;
    private Cluster cluster;

    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;
        this.idleStrategy = cluster.idleStrategy();
        if (snapshotImage != null)
        {
            writeSnapshotProbe(cluster.logPosition());
        }
    }

    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
    }

    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
    }

    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        idleStrategy.reset();
        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = session.offer(buffer, offset, length);
            if (result > 0)
            {
                return;
            }
            idleStrategy.idle();
        }
        while (--attempts > 0);
        System.err.println(OFFER_EXHAUSTED_MESSAGE);
    }

    public void onTimerEvent(final long correlationId, final long timestamp)
    {
    }

    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
    }

    public void onRoleChange(final Cluster.Role newRole)
    {
    }

    public void onTerminate(final Cluster cluster)
    {
    }

    /**
     * Writes term-start state under the member baseDir for mixed-smoke asserts.
     *
     * <p>File is {@link ClusterConstants#SERVICE_PROBE_FILENAME} in the parent of
     * {@code cluster.context().clusterDir()}. Write order: temp file, then rename.
     *
     * <p>Failure logs to stderr. Does not throw.
     */
    public void onNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final int leaderMemberId,
        final int logSessionId,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        writeServiceProbe(leadershipTermId, logPosition);
    }

    // package-private for unit tests
    void writeServiceProbe(final long term, final long logPosition)
    {
        final File baseDir = probeBaseDir();
        if (baseDir == null)
        {
            return;
        }

        final String roleName = cluster.role() == null ? "null" : cluster.role().name();
        final String content = formatServiceProbe(term, logPosition, roleName, cluster.memberId());
        writeProbeFile(baseDir, ClusterConstants.SERVICE_PROBE_FILENAME, content);
    }

    void writeSnapshotProbe(final long logPosition)
    {
        final File baseDir = probeBaseDir();
        if (baseDir == null)
        {
            return;
        }

        writeProbeFile(baseDir, ClusterConstants.SNAPSHOT_PROBE_FILENAME, formatSnapshotProbe(logPosition));
    }

    private static String probeWriteFailedPrefix(final String fileName)
    {
        return "heron: EchoService " + fileName + " write failed: ";
    }

    private static void writeProbeFile(final File baseDir, final String fileName, final String content)
    {
        final File dest = new File(baseDir, fileName);
        final File tmp = new File(baseDir, fileName + ".tmp");

        try
        {
            if (!baseDir.exists() && !baseDir.mkdirs())
            {
                System.err.println(probeWriteFailedPrefix(fileName) + "mkdirs " + baseDir);
                return;
            }
            Files.writeString(tmp.toPath(), content, StandardCharsets.US_ASCII);

            // tmp and dest share baseDir. ATOMIC_MOVE renames on one filesystem.
            Files.move(
                tmp.toPath(),
                dest.toPath(),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
        }
        catch (final IOException ex)
        {
            try
            {
                Files.deleteIfExists(tmp.toPath());
            }
            catch (final IOException ignored)
            {
            }
            System.err.println(probeWriteFailedPrefix(fileName) + ex.getMessage());
        }
    }

    /**
     * Member baseDir that holds the service-probe and snapshot-probe files.
     *
     * <p>Returns null when cluster is unset, context is null, or clusterDir is null.
     * clusterDir is null before {@link #onStart} when the context omits it.
     */
    private File probeBaseDir()
    {
        if (cluster == null)
        {
            return null;
        }

        final ClusteredServiceContainer.Context context = cluster.context();
        if (context == null)
        {
            return null;
        }

        final File clusterDir = context.clusterDir();
        return clusterDir == null ? null : clusterDir.getParentFile();
    }

    // package-private for unit tests
    static String formatServiceProbe(
        final long term,
        final long logPosition,
        final String role,
        final int memberId)
    {
        return ClusterConstants.SERVICE_PROBE_KEY_TERM + "=" + term + "\n"
            + ClusterConstants.SERVICE_PROBE_KEY_LOG_POSITION + "=" + logPosition + "\n"
            + ClusterConstants.SERVICE_PROBE_KEY_ROLE + "=" + role + "\n"
            + ClusterConstants.SERVICE_PROBE_KEY_MEMBER_ID + "=" + memberId + "\n";
    }

    static String formatSnapshotProbe(final long logPosition)
    {
        return ClusterConstants.SERVICE_PROBE_KEY_LOG_POSITION + "=" + logPosition + "\n";
    }
}
