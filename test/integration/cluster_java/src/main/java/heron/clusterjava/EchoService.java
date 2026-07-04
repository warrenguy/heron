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
// Modified from io.aeron.samples.cluster.EchoService:
// package renamed to heron.clusterjava; echoes inbound session messages via session.offer
// with cluster idle-strategy retries (integration smoke).
package heron.clusterjava;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;

class EchoService implements ClusteredService
{
    static final int SEND_ATTEMPTS = 3;
    static final String OFFER_EXHAUSTED_MESSAGE =
        "heron: EchoService offer exhausted after " + SEND_ATTEMPTS + " attempts";
    private IdleStrategy idleStrategy;

    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.idleStrategy = cluster.idleStrategy();
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
}
