//go:build integration

package harness

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	envClusterClientIngressChannel     = "HERON_CLUSTER_CLIENT_INGRESS_CHANNEL"
	envClusterClientIngressEndpoints   = "HERON_CLUSTER_CLIENT_INGRESS_ENDPOINTS"
	envClusterClientEgressChannel      = "HERON_CLUSTER_CLIENT_EGRESS_CHANNEL"
	envClusterClientMessageTimeoutMs   = "HERON_CLUSTER_CLIENT_MESSAGE_TIMEOUT_MS"
	envClusterClientNewLeaderTimeoutMs = "HERON_CLUSTER_CLIENT_NEW_LEADER_TIMEOUT_MS"
	envClusterClientCredentials        = "HERON_CLUSTER_CLIENT_CREDENTIALS"
	envClusterClientChallenge          = "HERON_CLUSTER_CLIENT_CHALLENGE"
	envClusterClientChallengeResponse  = "HERON_CLUSTER_CLIENT_CHALLENGE_RESPONSE"
	envClusterClientPayloadPrefix      = "HERON_CLUSTER_CLIENT_PAYLOAD_PREFIX"

	javaClusterClientMainClass = "heron.clusterjava.ClusterClient"
	javaClusterClientJarName   = "heron-cluster-java.jar"
	javaClusterClientEnvJar    = "HERON_CLUSTER_JAVA_JAR"

	javaClusterClientEventPrefix = "heron-client"
	javaClusterClientStderrLimit = 64 << 10
	javaClusterClientStdoutKeep  = 64
)

// errClusterJavaRuntimeMissing reports an absent cluster client jar or Java
// runtime.
var errClusterJavaRuntimeMissing = errors.New(
	"cluster java jar not found; run make test-cluster-archive",
)

var defaultClusterJavaJar = sync.OnceValues(func() (string, error) {
	if path := repoBinFile(javaClusterClientJarName); path != "" {
		return path, nil
	}
	return "", errClusterJavaRuntimeMissing
})

type JavaClusterClientConfig struct {
	AeronDir          string
	IngressChannel    string
	IngressEndpoints  string
	EgressChannel     string
	Credentials       string
	Challenge         string
	ChallengeResponse string
	MessageTimeout    time.Duration
	NewLeaderTimeout  time.Duration
	PayloadPrefix     string
}

type JavaClusterClientEvent struct {
	Kind   string
	Fields map[string]string
	Line   string
}

// StartJavaClusterClient registers Stop on t.Cleanup.
//
// Concurrency: invoker-only, except:
//   - The stdout reader writes the event queue, stdout lines, and the
//     parse error.
//   - The stderr reader writes the stderr buffer.
//   - The wait goroutine writes the exit code.
//
// Ownership: the client owns the child process.
type JavaClusterClient struct {
	tb     testing.TB
	cancel context.CancelFunc
	stdin  io.WriteCloser
	done   chan struct{}

	mu         sync.Mutex
	pending    []JavaClusterClientEvent
	exitCode   int
	hasExit    bool
	stderr     *tailBuffer
	stdout     []string
	parseError error

	stopOnce sync.Once
}

// Returns after the child process starts. Does not wait for a stdout
// event.
//
// The call fails the test when the jar or the java binary is missing.
//
// Concurrency: invoker-only.
//
// Ownership: the returned client owns the child process. t.Cleanup
// stops the child.
func StartJavaClusterClient(t testing.TB, cfg JavaClusterClientConfig) *JavaClusterClient {
	t.Helper()
	require.NotEmpty(t, cfg.AeronDir, "JavaClusterClientConfig.AeronDir")
	require.NotEmpty(t, cfg.IngressChannel, "JavaClusterClientConfig.IngressChannel")
	require.NotEmpty(t, cfg.IngressEndpoints, "JavaClusterClientConfig.IngressEndpoints")
	require.NotEmpty(t, cfg.EgressChannel, "JavaClusterClientConfig.EgressChannel")
	require.Positive(t, cfg.MessageTimeout, "JavaClusterClientConfig.MessageTimeout")
	require.Positive(t, cfg.NewLeaderTimeout, "JavaClusterClientConfig.NewLeaderTimeout")
	require.NotEmpty(t, cfg.PayloadPrefix, "JavaClusterClientConfig.PayloadPrefix")

	javaPath, jarPath := resolveClusterJavaRuntime(t)
	client, err := startJavaClusterClient(t, cfg, javaPath, jarPath)
	if err != nil {
		t.Fatalf("harness: %v", err)
	}
	t.Cleanup(func() { client.Stop() })
	return client
}

// Writes one SEND command. A write error fails the test.
func (c *JavaClusterClient) Send(count int) {
	c.tb.Helper()
	c.writeCommand(fmt.Sprintf("SEND %d", count))
}

// Writes one CLOSE command. A write error fails the test.
func (c *JavaClusterClient) RequestClose() {
	c.tb.Helper()
	c.writeCommand("CLOSE")
}

// Returns one queued stdout event. A false result means the queue
// is empty. The call never blocks.
//
// A stdout line that does not parse fails the test.
func (c *JavaClusterClient) PollEvent() (JavaClusterClientEvent, bool) {
	if c == nil {
		return JavaClusterClientEvent{}, false
	}
	c.tb.Helper()
	if err := c.stdoutParseError(); err != nil {
		c.tb.Fatalf("harness: cluster client stdout: %v\n%s", err, c.Dump())
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) == 0 {
		return JavaClusterClientEvent{}, false
	}
	ev := c.pending[0]
	c.pending[0] = JavaClusterClientEvent{}
	c.pending = c.pending[1:]
	return ev, true
}

// Reports the child exit code. The bool stays false until Cmd.Wait
// returns and PollEvent drains every queued event.
func (c *JavaClusterClient) ExitCode() (int, bool) {
	if c == nil {
		return 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exitCode, c.hasExit && len(c.pending) == 0
}

// Cancels the child process. Waits for the child to exit.
//
// Behaviour: idempotent and nil-safe. A later call does not wait again.
func (c *JavaClusterClient) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() {
		_ = c.stdin.Close()
		c.cancel()
		<-c.done
	})
}

// Returns the exit state, any parse error, the last 64 stdout lines,
// and at most 64 KiB of stderr.
func (c *JavaClusterClient) Dump() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var b strings.Builder
	if c.hasExit {
		fmt.Fprintf(&b, "exit=%d\n", c.exitCode)
	} else {
		b.WriteString("exit=running\n")
	}
	if c.parseError != nil {
		fmt.Fprintf(&b, "parse=%v\n", c.parseError)
	}
	b.WriteString("--- stdout ---\n")
	for _, line := range c.stdout {
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteString("--- stderr ---\n")
	b.Write(c.stderr.bytes())
	return b.String()
}

// Parses one stdout line. An event line is "heron-client" or begins
// with "heron-client ".
//
// Returns:
//   - false when the line is not an event line.
//   - an error for a missing, unknown, or malformed kind.
//   - an error for a malformed field or a duplicate key.
//
// A detail value keeps the rest of the line, including spaces.
func ParseJavaClusterClientLine(line string) (JavaClusterClientEvent, bool, error) {
	if line == javaClusterClientEventPrefix {
		return JavaClusterClientEvent{}, true, errors.New("missing kind")
	}
	rest, ok := strings.CutPrefix(line, javaClusterClientEventPrefix+" ")
	if !ok {
		return JavaClusterClientEvent{}, false, nil
	}
	kind, fieldText, _ := strings.Cut(rest, " ")
	if kind == "" || strings.Contains(kind, "=") {
		return JavaClusterClientEvent{}, true, errors.New("malformed kind")
	}
	if !isJavaClusterClientKind(kind) {
		return JavaClusterClientEvent{}, true, fmt.Errorf("unknown kind %q", kind)
	}
	parsed, err := parseJavaClusterClientFields(fieldText)
	if err != nil {
		return JavaClusterClientEvent{}, true, err
	}
	return JavaClusterClientEvent{Kind: kind, Fields: parsed, Line: line}, true, nil
}

// Returns the US-ASCII payload <prefix>-<seq>.
func JavaClusterClientPayload(prefix string, seq int) []byte {
	return []byte(prefix + "-" + strconv.Itoa(seq))
}

func isJavaClusterClientKind(kind string) bool {
	switch kind {
	case "CHALLENGE", "CONNECTED", "SENT", "NEW_LEADER", "SESSION_EVENT", "MESSAGE", "ERROR", "CLOSED":
		return true
	default:
		return false
	}
}

func parseJavaClusterClientFields(rest string) (map[string]string, error) {
	fields := map[string]string{}
	for rest != "" {
		if value, ok := strings.CutPrefix(rest, "detail="); ok {
			fields["detail"] = value
			return fields, nil
		}
		token, next, more := strings.Cut(rest, " ")
		key, value, ok := strings.Cut(token, "=")
		if token == "" || !ok || key == "" {
			return nil, fmt.Errorf("malformed field %q", token)
		}
		if _, dup := fields[key]; dup {
			return nil, fmt.Errorf("duplicate field %q", key)
		}
		fields[key] = value
		if !more {
			return fields, nil
		}
		rest = next
	}
	return fields, nil
}

func resolveClusterJavaRuntime(t testing.TB) (javaPath, jarPath string) {
	t.Helper()
	jarPath, err := resolveJavaJar(javaClusterClientEnvJar, defaultClusterJavaJar)
	if err != nil {
		t.Fatalf("harness: resolve cluster java jar: %v", err)
	}
	javaPath, err = exec.LookPath("java")
	if err != nil {
		t.Fatalf("harness: %v: %v", errClusterJavaRuntimeMissing, err)
	}
	return javaPath, jarPath
}

func startJavaClusterClient(
	tb testing.TB,
	cfg JavaClusterClientConfig,
	javaPath, jarPath string,
) (*JavaClusterClient, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := javaMainCommand(ctx, javaPath, jarPath, javaClusterClientMainClass)
	cmd.Env = mergeCmdEnv(javaClusterClientEnv(cfg))
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("cluster client stdin: %w", err)
	}
	fail := func(err error) (*JavaClusterClient, error) {
		cancel()
		_ = stdin.Close()
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fail(fmt.Errorf("cluster client stdout: %w", err))
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fail(fmt.Errorf("cluster client stderr: %w", err))
	}
	if err := cmd.Start(); err != nil {
		return fail(fmt.Errorf("start cluster client: %w", err))
	}

	client := &JavaClusterClient{
		tb:     tb,
		cancel: cancel,
		stdin:  stdin,
		done:   make(chan struct{}),
		stderr: newTailBuffer(javaClusterClientStderrLimit),
	}
	readers := new(sync.WaitGroup)
	readers.Go(func() { client.readStdout(stdout) })
	readers.Go(func() { _, _ = io.Copy(client.stderr, stderr) })
	go client.wait(cmd, readers)
	return client, nil
}

func javaClusterClientEnv(cfg JavaClusterClientConfig) map[string]string {
	return map[string]string{
		envDir:                             cfg.AeronDir,
		envClusterClientIngressChannel:     cfg.IngressChannel,
		envClusterClientIngressEndpoints:   cfg.IngressEndpoints,
		envClusterClientEgressChannel:      cfg.EgressChannel,
		envClusterClientMessageTimeoutMs:   strconv.FormatInt(cfg.MessageTimeout.Milliseconds(), 10),
		envClusterClientNewLeaderTimeoutMs: strconv.FormatInt(cfg.NewLeaderTimeout.Milliseconds(), 10),
		envClusterClientCredentials:        cfg.Credentials,
		envClusterClientChallenge:          cfg.Challenge,
		envClusterClientChallengeResponse:  cfg.ChallengeResponse,
		envClusterClientPayloadPrefix:      cfg.PayloadPrefix,
	}
}

func (c *JavaClusterClient) writeCommand(line string) {
	c.tb.Helper()
	_, err := io.WriteString(c.stdin, line+"\n")
	require.NoError(c.tb, err)
}

func (c *JavaClusterClient) readStdout(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		c.noteStdout(line)
		ev, ok, err := ParseJavaClusterClientLine(line)
		if err != nil {
			c.noteParseError(err)
			continue
		}
		if !ok {
			continue
		}
		c.enqueue(ev)
	}
	if err := scanner.Err(); err != nil {
		c.noteParseError(err)
	}
}

// wait drains stdout and stderr before Cmd.Wait. Cmd.Wait closes those
// read ends. enqueue does not block. The drain cannot stall on the
// event queue.
func (c *JavaClusterClient) wait(cmd *exec.Cmd, readers *sync.WaitGroup) {
	defer close(c.done)
	readers.Wait()
	err := cmd.Wait()
	code := 0
	if err != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			code = exitErr.ExitCode()
		} else {
			code = -1
		}
	}
	c.mu.Lock()
	c.exitCode = code
	c.hasExit = true
	c.mu.Unlock()
}

func (c *JavaClusterClient) enqueue(ev JavaClusterClientEvent) {
	c.mu.Lock()
	c.pending = append(c.pending, ev)
	c.mu.Unlock()
}

func (c *JavaClusterClient) noteStdout(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stdout = append(c.stdout, line)
	if excess := len(c.stdout) - javaClusterClientStdoutKeep; excess > 0 {
		c.stdout = c.stdout[:copy(c.stdout, c.stdout[excess:])]
	}
}

func (c *JavaClusterClient) noteParseError(err error) {
	c.mu.Lock()
	c.parseError = err
	c.mu.Unlock()
}

func (c *JavaClusterClient) stdoutParseError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.parseError
}

type tailBuffer struct {
	mu    sync.Mutex
	buf   []byte
	limit int
}

func newTailBuffer(limit int) *tailBuffer {
	return &tailBuffer{limit: limit}
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if excess := len(b.buf) - b.limit; excess > 0 {
		b.buf = b.buf[:copy(b.buf, b.buf[excess:])]
	}
	return len(p), nil
}

func (b *tailBuffer) bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf...)
}
