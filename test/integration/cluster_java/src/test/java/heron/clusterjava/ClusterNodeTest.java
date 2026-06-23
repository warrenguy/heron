package heron.clusterjava;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterNodeTest {
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

        assertEquals("heron: cluster node failed: AERON_DIR required" + System.lineSeparator(), stderr);
        assertFalse(stderr.contains("\tat "));
    }

    @Test
    void reportStartupFailure_printsMessageWithoutStackTraceByDefault() {
        withProperty("heron.debug", "false");

        final String stderr = captureStderr(
            () -> ClusterNode.reportStartupFailure(new NullPointerException("AERON_DIR required"))
        );

        assertEquals("heron: cluster node failed: AERON_DIR required" + System.lineSeparator(), stderr);
        assertFalse(stderr.contains("\tat "));
    }

    @Test
    void reportStartupFailure_printsStackTraceWhenDebugEnabled() {
        withProperty("heron.debug", "true");

        final String stderr = captureStderr(
            () -> ClusterNode.reportStartupFailure(new NullPointerException("AERON_DIR required"))
        );

        assertTrue(stderr.startsWith("heron: cluster node failed: AERON_DIR required"));
        assertTrue(stderr.contains("java.lang.NullPointerException: AERON_DIR required"));
        assertTrue(stderr.contains("\tat "));
    }

    private String captureStderr(final Runnable action) {
        final PrintStream originalErr = System.err;
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        System.setErr(new PrintStream(buffer, true, StandardCharsets.UTF_8));
        try {
            action.run();
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
