package heron.clusterjava;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.AdminRequestType;
import io.aeron.cluster.codecs.AdminResponseCode;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import io.aeron.security.CredentialsSupplier;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Stdin commands drive one cluster session.
 *
 * <p>Stdout lines start with {@code heron-client}.
 *
 * <p>The process exits 0 after CLOSE. The process exits 1 on failure.
 */
public final class ClusterClient
{
    static final String ENV_AERON_DIR = "AERON_DIR";
    static final String ENV_INGRESS_CHANNEL = "HERON_CLUSTER_CLIENT_INGRESS_CHANNEL";
    static final String ENV_INGRESS_ENDPOINTS = "HERON_CLUSTER_CLIENT_INGRESS_ENDPOINTS";
    static final String ENV_EGRESS_CHANNEL = "HERON_CLUSTER_CLIENT_EGRESS_CHANNEL";
    static final String ENV_MESSAGE_TIMEOUT_MS = "HERON_CLUSTER_CLIENT_MESSAGE_TIMEOUT_MS";
    static final String ENV_NEW_LEADER_TIMEOUT_MS = "HERON_CLUSTER_CLIENT_NEW_LEADER_TIMEOUT_MS";
    static final String ENV_CREDENTIALS = "HERON_CLUSTER_CLIENT_CREDENTIALS";
    static final String ENV_CHALLENGE = "HERON_CLUSTER_CLIENT_CHALLENGE";
    static final String ENV_CHALLENGE_RESPONSE = "HERON_CLUSTER_CLIENT_CHALLENGE_RESPONSE";
    static final String ENV_PAYLOAD_PREFIX = "HERON_CLUSTER_CLIENT_PAYLOAD_PREFIX";

    static final String EVENT_PREFIX = "heron-client";

    private static final long KEEPALIVE_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);
    private static final long IDLE_NS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long MAX_TIMEOUT_MS = Long.MAX_VALUE / TimeUnit.MILLISECONDS.toNanos(1);
    private static final byte[] EMPTY = new byte[0];
    private static final Object STDOUT = new Object();

    record Settings(
        String aeronDir,
        String ingressChannel,
        String ingressEndpoints,
        String egressChannel,
        long messageTimeoutNs,
        long newLeaderTimeoutNs,
        byte[] credentials,
        byte[] challenge,
        byte[] challengeResponse,
        String payloadPrefix)
    {
    }

    record Command(boolean close, int count)
    {
    }

    private static final class RunState
    {
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        volatile boolean closing;
        long lastKeepAliveNs = System.nanoTime();
        int nextSeq = 1;

        void fail(final Throwable cause)
        {
            if (failure.compareAndSet(null, cause))
            {
                emit(formatError(cause));
            }
        }

        boolean failed()
        {
            return failure.get() != null;
        }
    }

    private ClusterClient()
    {
    }

    public static void main(final String[] args)
    {
        final Settings settings;
        try
        {
            settings = parseSettings(System.getenv());
        }
        catch (final RuntimeException ex)
        {
            emit(formatError(ex));
            System.exit(1);
            return;
        }

        final RunState state = new RunState();
        final AeronCluster.Context context = new AeronCluster.Context()
            .aeronDirectoryName(settings.aeronDir())
            .ingressChannel(settings.ingressChannel())
            .ingressEndpoints(settings.ingressEndpoints())
            .egressChannel(settings.egressChannel())
            .messageTimeoutNs(settings.messageTimeoutNs())
            .newLeaderTimeoutNs(settings.newLeaderTimeoutNs())
            .credentialsSupplier(new ClientCredentials(settings))
            .egressListener(new ClientEgress())
            .errorHandler(cause ->
            {
                if (!state.closing)
                {
                    state.fail(cause);
                }
            });

        final AeronCluster cluster;
        try
        {
            cluster = AeronCluster.connect(context);
        }
        catch (final Throwable ex)
        {
            failAndExit(state, null, ex);
            return;
        }

        if (state.failed())
        {
            exitFailed(state, cluster);
        }

        emit(connectedLine(cluster.clusterSessionId(), cluster.leadershipTermId(), cluster.leaderMemberId()));
        run(cluster, settings, state);
    }

    static Settings parseSettings(final Map<String, String> env)
    {
        return new Settings(
            asciiText(ENV_AERON_DIR, env.get(ENV_AERON_DIR), false),
            asciiText(ENV_INGRESS_CHANNEL, env.get(ENV_INGRESS_CHANNEL), false),
            asciiText(ENV_INGRESS_ENDPOINTS, env.get(ENV_INGRESS_ENDPOINTS), false),
            asciiText(ENV_EGRESS_CHANNEL, env.get(ENV_EGRESS_CHANNEL), false),
            timeoutNs(ENV_MESSAGE_TIMEOUT_MS, env.get(ENV_MESSAGE_TIMEOUT_MS)),
            timeoutNs(ENV_NEW_LEADER_TIMEOUT_MS, env.get(ENV_NEW_LEADER_TIMEOUT_MS)),
            ascii(ENV_CREDENTIALS, env.get(ENV_CREDENTIALS)),
            ascii(ENV_CHALLENGE, env.get(ENV_CHALLENGE)),
            ascii(ENV_CHALLENGE_RESPONSE, env.get(ENV_CHALLENGE_RESPONSE)),
            asciiText(ENV_PAYLOAD_PREFIX, env.get(ENV_PAYLOAD_PREFIX), false));
    }

    static Command parseCommand(final String line)
    {
        if ("CLOSE".equals(line))
        {
            return new Command(true, 0);
        }
        final String send = "SEND ";
        if (line != null && line.startsWith(send))
        {
            try
            {
                final int count = Integer.parseInt(line.substring(send.length()));
                if (count > 0)
                {
                    return new Command(false, count);
                }
            }
            catch (final NumberFormatException ignored)
            {
            }
        }
        throw new IllegalArgumentException("invalid command");
    }

    static byte[] payloadBytes(final String prefix, final int seq)
    {
        return (prefix + "-" + seq).getBytes(StandardCharsets.US_ASCII);
    }

    static byte[] challengeReply(final byte[] response, final boolean match)
    {
        return match ? response : EMPTY;
    }

    static String challengeLine(final boolean match)
    {
        return EVENT_PREFIX + " CHALLENGE match=" + match;
    }

    static String connectedLine(final long session, final long term, final int leader)
    {
        return EVENT_PREFIX + " CONNECTED session=" + session + " term=" + term + " leader=" + leader;
    }

    static String sentLine(final int first, final int last, final long position)
    {
        return EVENT_PREFIX + " SENT first=" + first + " last=" + last + " position=" + position;
    }

    static String newLeaderLine(final long session, final long term, final int leader, final String endpoints)
    {
        return EVENT_PREFIX + " NEW_LEADER session=" + session +
            " term=" + term +
            " leader=" + leader +
            " endpoints=" + endpoints;
    }

    static String sessionEventLine(
        final long correlation,
        final long session,
        final long term,
        final int leader,
        final String code,
        final String detail)
    {
        return EVENT_PREFIX + " SESSION_EVENT correlation=" + correlation +
            " session=" + session +
            " term=" + term +
            " leader=" + leader +
            " code=" + code +
            " detail=" + detail;
    }

    static String messageLine(final long session, final int length)
    {
        return EVENT_PREFIX + " MESSAGE session=" + session + " length=" + length;
    }

    static String errorLine(final String className, final String detail)
    {
        return EVENT_PREFIX + " ERROR class=" + className + " detail=" + detail;
    }

    static String closedLine()
    {
        return EVENT_PREFIX + " CLOSED";
    }

    static String formatError(final Throwable failure)
    {
        final String detail = failure.getMessage();
        return errorLine(failure.getClass().getSimpleName(), detail == null ? "" : detail);
    }

    private static void run(final AeronCluster cluster, final Settings settings, final RunState state)
    {
        final ConcurrentLinkedQueue<Object> stdin = new ConcurrentLinkedQueue<>();
        final Thread reader = new Thread(() -> readStdin(stdin), "heron-cluster-client-stdin");
        reader.setDaemon(true);
        reader.start();

        while (true)
        {
            try
            {
                tick(cluster, state);
                final Object item = stdin.poll();
                if (item instanceof Throwable failure)
                {
                    failAndExit(state, cluster, failure);
                }
                if (item instanceof String line && !line.isEmpty())
                {
                    final Command command = parseCommand(line);
                    if (command.close())
                    {
                        closeAndExit(cluster, state);
                    }
                    sendCount(cluster, settings, state, command.count());
                }
            }
            catch (final Throwable ex)
            {
                failAndExit(state, cluster, ex);
            }
            LockSupport.parkNanos(IDLE_NS);
        }
    }

    private static void sendCount(
        final AeronCluster cluster,
        final Settings settings,
        final RunState state,
        final int count)
    {
        final int first = state.nextSeq;
        long position = 0;
        for (int i = 0; i < count; i++)
        {
            final byte[] payload = payloadBytes(settings.payloadPrefix(), state.nextSeq);
            position = offerUntil(cluster, settings, state, payload);
            state.nextSeq++;
        }
        emit(sentLine(first, state.nextSeq - 1, position));
    }

    private static long offerUntil(
        final AeronCluster cluster,
        final Settings settings,
        final RunState state,
        final byte[] payload)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(payload);
        final long deadlineNs = System.nanoTime() + settings.messageTimeoutNs();
        while (true)
        {
            final long position = cluster.offer(buffer, 0, payload.length);
            if (position > 0)
            {
                return position;
            }
            tick(cluster, state);
            if (System.nanoTime() - deadlineNs >= 0)
            {
                failAndExit(state, cluster, new IllegalStateException("offer timed out"));
            }
            LockSupport.parkNanos(IDLE_NS);
        }
    }

    private static void tick(final AeronCluster cluster, final RunState state)
    {
        cluster.pollEgress();
        if (state.failed())
        {
            exitFailed(state, cluster);
        }
        keepAlive(cluster, state);
        if (cluster.isClosed())
        {
            failAndExit(state, cluster, new IllegalStateException("cluster client closed"));
        }
    }

    private static void keepAlive(final AeronCluster cluster, final RunState state)
    {
        final long nowNs = System.nanoTime();
        if (nowNs - state.lastKeepAliveNs >= KEEPALIVE_INTERVAL_NS)
        {
            cluster.sendKeepAlive();
            state.lastKeepAliveNs = nowNs;
        }
    }

    private static void closeAndExit(final AeronCluster cluster, final RunState state)
    {
        state.closing = true;
        try
        {
            cluster.close();
        }
        catch (final Throwable ex)
        {
            state.closing = false;
            failAndExit(state, null, ex);
        }
        emit(closedLine());
        System.exit(0);
    }

    private static void failAndExit(final RunState state, final AeronCluster cluster, final Throwable cause)
    {
        state.fail(cause);
        exitFailed(state, cluster);
    }

    private static void exitFailed(final RunState state, final AeronCluster cluster)
    {
        if (cluster != null)
        {
            state.closing = true;
            try
            {
                cluster.close();
            }
            catch (final Throwable ignored)
            {
                // fail emits the failure line before close. A close error must not
                // replace the failure line.
            }
        }
        System.exit(1);
        throw new IllegalStateException("unreachable");
    }

    private static void readStdin(final Queue<Object> stdin)
    {
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(System.in, StandardCharsets.US_ASCII)))
        {
            readLines(reader, stdin);
        }
        catch (final Throwable ex)
        {
            stdin.add(ex);
        }
    }

    // Queues each line, then one Throwable that ends the stream. Queue order
    // runs every command before the end.
    static void readLines(final BufferedReader reader, final Queue<Object> out) throws IOException
    {
        String line;
        while ((line = reader.readLine()) != null)
        {
            out.add(line);
        }
        out.add(new IllegalStateException("stdin closed"));
    }

    private static void emit(final String line)
    {
        synchronized (STDOUT)
        {
            System.out.println(line);
            System.out.flush();
        }
    }

    private static long timeoutNs(final String key, final String raw)
    {
        final long millis;
        try
        {
            millis = Long.parseLong(raw);
        }
        catch (final NumberFormatException ex)
        {
            throw new IllegalArgumentException("invalid " + key);
        }
        if (millis <= 0 || millis > MAX_TIMEOUT_MS)
        {
            throw new IllegalArgumentException("invalid " + key);
        }
        return TimeUnit.MILLISECONDS.toNanos(millis);
    }

    private static byte[] ascii(final String key, final String value)
    {
        return asciiText(key, value, true).getBytes(StandardCharsets.US_ASCII);
    }

    private static String asciiText(final String key, final String value, final boolean allowEmpty)
    {
        if (value == null || (!allowEmpty && value.isEmpty()))
        {
            throw new IllegalArgumentException("missing " + key);
        }
        for (int i = 0; i < value.length(); i++)
        {
            if (value.charAt(i) > 127)
            {
                throw new IllegalArgumentException("invalid " + key);
            }
        }
        return value;
    }

    private static final class ClientCredentials implements CredentialsSupplier
    {
        private final Settings settings;

        private ClientCredentials(final Settings settings)
        {
            this.settings = settings;
        }

        public byte[] encodedCredentials()
        {
            return settings.credentials();
        }

        public byte[] onChallenge(final byte[] encodedChallenge)
        {
            final boolean match = Arrays.equals(settings.challenge(), encodedChallenge);
            emit(challengeLine(match));
            return challengeReply(settings.challengeResponse(), match);
        }
    }

    private static final class ClientEgress implements EgressListener
    {
        public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            emit(messageLine(clusterSessionId, length));
        }

        public void onSessionEvent(
            final long correlationId,
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final EventCode code,
            final String detail)
        {
            emit(sessionEventLine(
                correlationId,
                clusterSessionId,
                leadershipTermId,
                leaderMemberId,
                code == null ? "" : code.name(),
                detail == null ? "" : detail));
        }

        public void onNewLeader(
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final String ingressEndpoints)
        {
            emit(newLeaderLine(
                clusterSessionId,
                leadershipTermId,
                leaderMemberId,
                ingressEndpoints == null ? "" : ingressEndpoints));
        }

        public void onAdminResponse(
            final long clusterSessionId,
            final long correlationId,
            final AdminRequestType requestType,
            final AdminResponseCode responseCode,
            final String message,
            final DirectBuffer payload,
            final int payloadOffset,
            final int payloadLength)
        {
        }
    }
}
