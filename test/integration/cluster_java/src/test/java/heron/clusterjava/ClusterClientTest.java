package heron.clusterjava;

import io.aeron.security.AuthenticationException;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterClientTest {
    @Test
    void parseSettings_rejectsEachMissingKey() {
        for (final String key : validEnv().keySet()) {
            final Map<String, String> env = validEnv();
            env.remove(key);
            final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> ClusterClient.parseSettings(env));
            assertTrue(ex.getMessage().contains(key), ex.getMessage());
        }
    }

    @Test
    void parseSettings_acceptsEmptyCredentialsAndChallenge() {
        final Map<String, String> env = validEnv();
        env.put(ClusterClient.ENV_CREDENTIALS, "");
        env.put(ClusterClient.ENV_CHALLENGE, "");
        env.put(ClusterClient.ENV_CHALLENGE_RESPONSE, "");

        final ClusterClient.Settings settings = ClusterClient.parseSettings(env);

        assertArrayEquals(new byte[0], settings.credentials());
        assertArrayEquals(new byte[0], settings.challenge());
        assertArrayEquals(new byte[0], settings.challengeResponse());
        assertEquals("jc", settings.payloadPrefix());
        assertEquals(20_000_000_000L, settings.messageTimeoutNs());
        assertEquals(60_000_000_000L, settings.newLeaderTimeoutNs());
    }

    @Test
    void parseSettings_rejectsInvalidTimeoutAndNonAscii() {
        assertParseSettingsRejects(ClusterClient.ENV_MESSAGE_TIMEOUT_MS, "0");
        assertParseSettingsRejects(ClusterClient.ENV_NEW_LEADER_TIMEOUT_MS, "60s");
        assertParseSettingsRejects(
            ClusterClient.ENV_MESSAGE_TIMEOUT_MS,
            Long.toString(Long.MAX_VALUE / 1_000_000L + 1));
        assertParseSettingsRejects(ClusterClient.ENV_CREDENTIALS, "bad\u00e9");
        assertParseSettingsRejects(ClusterClient.ENV_PAYLOAD_PREFIX, "p\u00e9");
    }

    @Test
    void readLines_queuesEveryCommandBeforeEnd() throws IOException {
        final Queue<Object> out = new ArrayDeque<>();
        ClusterClient.readLines(new BufferedReader(new StringReader("SEND 1\nCLOSE\n")), out);

        assertEquals(3, out.size());
        assertEquals("SEND 1", out.poll());
        assertEquals("CLOSE", out.poll());
        assertEquals("stdin closed", assertInstanceOf(IllegalStateException.class, out.poll()).getMessage());
    }

    @Test
    void parseCommand_acceptsSendAndClose() {
        final ClusterClient.Command send = ClusterClient.parseCommand("SEND 3");
        assertFalse(send.close());
        assertEquals(3, send.count());

        final ClusterClient.Command close = ClusterClient.parseCommand("CLOSE");
        assertTrue(close.close());
        assertEquals(0, close.count());

        assertThrows(IllegalArgumentException.class, () -> ClusterClient.parseCommand("SEND"));
        assertThrows(IllegalArgumentException.class, () -> ClusterClient.parseCommand("SEND 0"));
        assertThrows(IllegalArgumentException.class, () -> ClusterClient.parseCommand("SEND -1"));
        assertThrows(IllegalArgumentException.class, () -> ClusterClient.parseCommand("NOPE"));
        assertThrows(IllegalArgumentException.class, () -> ClusterClient.parseCommand(null));
    }

    @Test
    void eventLines_coverEveryKind() {
        assertEquals("heron-client CHALLENGE match=true", ClusterClient.challengeLine(true));
        assertEquals("heron-client CHALLENGE match=false", ClusterClient.challengeLine(false));
        assertEquals(
            "heron-client CONNECTED session=7 term=2 leader=1",
            ClusterClient.connectedLine(7, 2, 1));
        assertEquals(
            "heron-client SENT first=1 last=3 position=40",
            ClusterClient.sentLine(1, 3, 40));
        assertEquals(
            "heron-client NEW_LEADER session=7 term=3 leader=0 endpoints=0=127.0.0.1:1",
            ClusterClient.newLeaderLine(7, 3, 0, "0=127.0.0.1:1"));
        assertEquals(
            "heron-client SESSION_EVENT correlation=9 session=7 term=2 leader=1 code=ERROR detail=boom now",
            ClusterClient.sessionEventLine(9, 7, 2, 1, "ERROR", "boom now"));
        assertEquals("heron-client MESSAGE session=7 length=4", ClusterClient.messageLine(7, 4));
        assertEquals("heron-client CLOSED", ClusterClient.closedLine());
        assertEquals(
            "heron-client ERROR class=IllegalStateException detail=offer timed out",
            ClusterClient.errorLine("IllegalStateException", "offer timed out"));
    }

    @Test
    void formatError_keepsAuthenticationExceptionCategoryPrefix() {
        final AuthenticationException ex = new AuthenticationException("session failed authentication");
        assertEquals(
            "heron-client ERROR class=AuthenticationException detail=ERROR - session failed authentication",
            ClusterClient.formatError(ex));
    }

    @Test
    void payloadBytes_usesPrefixAndSequence() {
        assertArrayEquals(
            "jc-1".getBytes(StandardCharsets.US_ASCII),
            ClusterClient.payloadBytes("jc", 1));
        assertArrayEquals(
            "jc-6".getBytes(StandardCharsets.US_ASCII),
            ClusterClient.payloadBytes("jc", 6));
    }

    @Test
    void challengeReply_returnsResponseOnlyOnMatch() {
        final byte[] response = "heron-java-client-response".getBytes(StandardCharsets.US_ASCII);

        assertArrayEquals(response, ClusterClient.challengeReply(response, true));
        assertArrayEquals(new byte[0], ClusterClient.challengeReply(response, false));
    }

    private static void assertParseSettingsRejects(final String key, final String value) {
        final Map<String, String> env = validEnv();
        env.put(key, value);
        assertThrows(
            IllegalArgumentException.class,
            () -> ClusterClient.parseSettings(env),
            key + "=" + value);
    }

    private static Map<String, String> validEnv() {
        final Map<String, String> env = new HashMap<>();
        env.put(ClusterClient.ENV_AERON_DIR, "/tmp/aeron");
        env.put(ClusterClient.ENV_INGRESS_CHANNEL, "aeron:udp?term-length=64k");
        env.put(ClusterClient.ENV_INGRESS_ENDPOINTS, "0=127.0.0.1:1");
        env.put(ClusterClient.ENV_EGRESS_CHANNEL, "aeron:udp?endpoint=127.0.0.1:2");
        env.put(ClusterClient.ENV_MESSAGE_TIMEOUT_MS, "20000");
        env.put(ClusterClient.ENV_NEW_LEADER_TIMEOUT_MS, "60000");
        env.put(ClusterClient.ENV_CREDENTIALS, "heron-java-client-credentials");
        env.put(ClusterClient.ENV_CHALLENGE, "heron-java-client-challenge");
        env.put(ClusterClient.ENV_CHALLENGE_RESPONSE, "heron-java-client-response");
        env.put(ClusterClient.ENV_PAYLOAD_PREFIX, "jc");
        return env;
    }
}
