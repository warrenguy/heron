//go:build integration && cluster_java

package cluster_java_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/idlestrategy"
	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
	"github.com/warrenguy/heron/test/integration/harness"
)

const (
	echoSmokeTimeout        = 30 * time.Second
	adminSmokeTimeout       = 60 * time.Second // snapshot archive IO; slower than echo poll
	connectSmokeTimeout     = 60 * time.Second
	failoverConnectTimeout  = 90 * time.Second // 3 JVM cold start
	failoverRecoveryTimeout = 120 * time.Second

	// Post-failover offer/echo budgets: ingress repoint + election can exceed IPC smoke defaults.
	failoverPostRecoveryRoundTripTimeout = 60 * time.Second

	// dockerKillPollTimeout bounds compose kill while the IPC client must stay serviced.
	dockerKillPollTimeout = 30 * time.Second
)

type clusterComposeGate struct {
	envKey         string
	skipHint       string
	requireDocker  bool
	requireIPCTerm bool
}

func (g clusterComposeGate) enabled() bool {
	return os.Getenv(g.envKey) == "1"
}

var (
	clusterJavaComposeGate = clusterComposeGate{
		envKey:         clusterjava.ComposeGateEnvKey,
		skipHint:       clusterjava.ComposeSkipHint,
		requireIPCTerm: true,
	}
	clusterJavaFailoverComposeGate = clusterComposeGate{
		envKey:        clusterjava.FailoverComposeGateEnvKey,
		skipHint:      clusterjava.FailoverComposeSkipHint,
		requireDocker: true,
	}
)

var (
	echoPingPayload         = []byte("heron-cluster-echo-ping")
	echoPostFailoverPayload = []byte("heron-cluster-echo-post-failover")
)

type adminResponseCapture struct {
	armed             bool
	wantCorrelationID int64
	requestType       client.AdminRequestType
	responseCode      client.AdminResponseCode
	message           []byte
	payload           []byte
	seen              bool
}

func (c *adminResponseCapture) reset() {
	*c = adminResponseCapture{}
}

func newEchoCaptureHandlers() (*client.EgressHandlers, *[]byte) {
	handlers, echoed, _ := newEchoAndAdminCaptureHandlers()
	return handlers, echoed
}

func newEchoAndAdminCaptureHandlers() (*client.EgressHandlers, *[]byte, *adminResponseCapture) {
	var echoed []byte
	adminCap := &adminResponseCapture{}
	return &client.EgressHandlers{
		MessageFunc: func(
			_, _ int64,
			buf atomicbuffer.AtomicBuffer,
			offset, length int,
			_ *aeron.Header,
		) {
			echoed = append([]byte(nil), buf.Slice(offset, length).Bytes()...)
		},
		AdminResponseFunc: func(
			_ int64,
			correlationID int64,
			requestType client.AdminRequestType,
			responseCode client.AdminResponseCode,
			message, payload []byte,
		) {
			if adminCap.seen {
				return
			}
			if !adminCap.armed || correlationID != adminCap.wantCorrelationID {
				return
			}
			adminCap.requestType = requestType
			adminCap.responseCode = responseCode
			adminCap.message = message
			adminCap.payload = payload
			adminCap.seen = true
		},
	}, &echoed, adminCap
}

// ingressOfferUntilPositive wraps session.IngressOfferUntilPositive; hadClientError
// is checked inside offerFn after DoWork.
func ingressOfferUntilPositive(
	t *testing.T,
	session *client.AeronCluster,
	hadClientError *atomic.Bool,
	offerTimeout time.Duration,
	offer func() int64,
) int64 {
	t.Helper()
	offerCtx := t.Context()
	if offerTimeout > 0 {
		var cancel context.CancelFunc
		offerCtx, cancel = context.WithTimeout(offerCtx, offerTimeout)
		defer cancel()
	}
	pos, err := session.IngressOfferUntilPositive(
		offerCtx,
		client.IngressOfferConfig{
			IdleStrategy: idlestrategy.NewSleeping(aeron.DefaultIdleSleepDuration),
		}, func() int64 {
			require.False(t, hadClientError.Load(), "transport error during offer")
			return offer()
		},
	)
	if err != nil {
		t.Fatalf(
			"cluster offer failed: %v sessionState=%v hadClientError=%v",
			err,
			session.SessionState(),
			hadClientError.Load(),
		)
	}
	return pos
}

func pollEgressUntil(
	t *testing.T,
	aeronClient *aeron.Client,
	session *client.AeronCluster,
	hadClientError *atomic.Bool,
	timeout time.Duration,
	transportErrMsg string,
	done func() bool,
	timeoutMsg func() string,
) {
	t.Helper()

	if done() {
		return
	}
	harness.PollClientUntilTransportClean(
		t,
		aeronClient,
		hadClientError,
		timeout,
		transportErrMsg,
		func() bool {
			session.PollEgress()
			return done()
		},
		timeoutMsg,
	)
}

func pollEchoUntilMessage(
	t *testing.T,
	aeronClient *aeron.Client,
	session *client.AeronCluster,
	hadClientError *atomic.Bool,
	echoed *[]byte,
	echoTimeout time.Duration,
) {
	t.Helper()

	pollEgressUntil(
		t, aeronClient, session, hadClientError, echoTimeout,
		"transport error during echo poll",
		func() bool { return len(*echoed) > 0 },
		func() string {
			return fmt.Sprintf(
				"cluster echo poll timed out: echoedLen=%d sessionState=%v hadClientError=%v",
				len(*echoed), session.SessionState(), hadClientError.Load(),
			)
		},
	)
}

func roundTripEcho(
	t *testing.T,
	aeronClient *aeron.Client,
	session *client.AeronCluster,
	hadClientError *atomic.Bool,
	echoed *[]byte,
	payload []byte,
	offerTimeout, echoTimeout time.Duration,
) int64 {
	t.Helper()

	*echoed = nil
	pos := ingressOfferUntilPositive(
		t, session, hadClientError, offerTimeout,
		func() int64 { return session.Offer(payload) },
	)
	pollEchoUntilMessage(t, aeronClient, session, hadClientError, echoed, echoTimeout)
	require.Equal(t, payload, *echoed)
	return pos
}

func pollAdminUntilResponse(
	t *testing.T,
	aeronClient *aeron.Client,
	session *client.AeronCluster,
	hadClientError *atomic.Bool,
	capture *adminResponseCapture,
	adminTimeout time.Duration,
) {
	t.Helper()

	pollEgressUntil(
		t, aeronClient, session, hadClientError, adminTimeout,
		"transport error during admin poll",
		func() bool { return capture.seen },
		func() string {
			return fmt.Sprintf(
				"cluster admin poll timed out: seen=%v armed=%v want=%d "+
					"sessionState=%v hadClientError=%v",
				capture.seen, capture.armed, capture.wantCorrelationID,
				session.SessionState(), hadClientError.Load(),
			)
		},
	)
}

func roundTripAdminSnapshot(
	t *testing.T,
	aeronClient *aeron.Client,
	session *client.AeronCluster,
	hadClientError *atomic.Bool,
	capture *adminResponseCapture,
	correlationID int64,
) int64 {
	t.Helper()

	capture.reset()
	capture.armed = true
	capture.wantCorrelationID = correlationID
	pos := ingressOfferUntilPositive(
		t, session, hadClientError, harness.ConnectedPollTimeout,
		func() int64 { return session.SendSnapshotRequest(correlationID) },
	)
	pollAdminUntilResponse(
		t, aeronClient, session, hadClientError, capture, adminSmokeTimeout,
	)
	require.Equal(t, client.AdminRequestTypeSnapshot, capture.requestType)
	require.Equal(t, client.AdminResponseCodeOK, capture.responseCode)
	return pos
}

func requireAeronDir(t *testing.T) string {
	t.Helper()

	dir := os.Getenv("AERON_DIR")
	require.NotEmpty(t, dir)
	return dir
}

func joinComposeIPCClient(
	t *testing.T,
	dir string,
) (*atomic.Bool, *aeron.Client, *harness.MediaDriver) {
	t.Helper()

	md := harness.JoinExternal(t, dir)

	aeronClient, hadClientError := harness.ConnectIPCClient(t, md)
	require.False(t, hadClientError.Load(), "transport errors before connect")
	return hadClientError, aeronClient, md
}

func requireClusterComposeEnv(
	t *testing.T,
	gate clusterComposeGate,
) (string, *atomic.Bool, *aeron.Client, *harness.MediaDriver, string) {
	t.Helper()

	if !gate.enabled() {
		t.Skip(gate.skipHint)
	}
	if gate.requireDocker {
		requireDockerSocketAccess(t)
	}

	dir := requireAeronDir(t)
	if gate.requireIPCTerm {
		require.Equal(
			t,
			strconv.Itoa(clusterjava.TermMinLength),
			os.Getenv("AERON_IPC_TERM_BUFFER_LENGTH"),
		)
	}

	hadClientError, aeronClient, md := joinComposeIPCClient(t, dir)
	composeDir := ""
	if gate.requireDocker {
		composeDir = clusterComposeDir()
	}
	return dir, hadClientError, aeronClient, md, composeDir
}

func smokeConfig(
	base func() client.Config,
	dir string,
	aeronClient *aeron.Client,
	egressHandler client.EgressHandler,
	messageTimeout time.Duration,
) client.Config {
	cfg := base()
	cfg.AeronDir = dir
	cfg.Aeron = aeronClient
	if cfg.MessageTimeout == 0 {
		cfg.MessageTimeout = messageTimeout
	}
	cfg.EgressHandler = egressHandler
	return cfg
}

func connectClusterAsync(
	t *testing.T,
	cfg client.Config,
	aeronClient *aeron.Client,
	hadClientError *atomic.Bool,
	connectTimeout time.Duration,
) *client.ConnectResult {
	t.Helper()

	ac, err := client.NewAsyncConnect(t.Context(), cfg)
	require.NoError(t, err)
	harness.CleanupClose(t, "close async connect", ac.Close)
	return pollAsyncConnectUntilResult(t, aeronClient, ac, hadClientError, connectTimeout)
}

func requireConnectResultHandlesTransferred(t *testing.T, result *client.ConnectResult) {
	t.Helper()

	require.Nil(t, result.IngressPublication)
	require.Nil(t, result.EgressSubscription)
	require.Nil(t, result.EgressImage)
}

func newConnectedClusterSession(
	t *testing.T,
	result *client.ConnectResult,
) *client.AeronCluster {
	t.Helper()

	session, err := client.NewAeronCluster(result)
	require.NoError(t, err)
	harness.CleanupClose(t, "close aeron cluster", session.Close)
	require.Equal(t, client.SessionStateConnected, session.SessionState())
	return session
}

func connectClusterUntilResult(
	t *testing.T,
	gate clusterComposeGate,
	contextBase func() client.Config,
	connectTimeout time.Duration,
	egressHandler client.EgressHandler,
) (*atomic.Bool, *aeron.Client, *harness.MediaDriver, *client.ConnectResult, string) {
	t.Helper()

	dir, hadClientError, aeronClient, md, composeDir := requireClusterComposeEnv(t, gate)
	cfg := smokeConfig(contextBase, dir, aeronClient, egressHandler, connectTimeout)
	result := connectClusterAsync(t, cfg, aeronClient, hadClientError, connectTimeout)
	return hadClientError, aeronClient, md, result, composeDir
}

func asyncConnectUntilResult(
	t *testing.T,
	egressHandler client.EgressHandler,
) (*aeron.Client, *atomic.Bool, *client.ConnectResult) {
	t.Helper()

	hadClientError, aeronClient, md, result, _ := connectClusterUntilResult(
		t,
		clusterJavaComposeGate,
		clusterjava.HarnessConfig,
		connectSmokeTimeout,
		egressHandler,
	)
	md.AssertAlive(t)
	return aeronClient, hadClientError, result
}

var dockerSocketAccess = struct {
	once   sync.Once
	reason string
}{}

func requireDockerSocketAccess(t *testing.T) {
	t.Helper()

	dockerSocketAccess.once.Do(func() {
		if _, err := os.Stat("/var/run/docker.sock"); err != nil {
			dockerSocketAccess.reason = "docker socket not mounted; failover kill unavailable"
			return
		}
		const dockerHint = "host: add user to docker group or set DOCKER_GID for group_add"
		for _, check := range []struct {
			args  []string
			label string
		}{
			{[]string{"compose", "version"}, "docker compose unavailable"},
			{[]string{"info"}, "docker daemon unavailable"},
		} {
			cmd := exec.Command("docker", check.args...)
			if out, err := cmd.CombinedOutput(); err != nil {
				dockerSocketAccess.reason = fmt.Sprintf(
					"%s (%v): %s\n%s", check.label, err, out, dockerHint,
				)
				return
			}
		}
	})
	if dockerSocketAccess.reason != "" {
		t.Fatal(dockerSocketAccess.reason)
	}
}

func clusterComposeDir() string {
	if dir := os.Getenv(clusterjava.ComposeDirEnvKey); dir != "" {
		return dir
	}
	return clusterjava.ComposeDir
}

func clusterFailoverComposeArgs(composeDir string) []string {
	return []string{
		"compose",
		"-f", filepath.Join(composeDir, "compose.yaml"),
		"-f", filepath.Join(composeDir, "compose.failover.yaml"),
		"--profile", clusterjava.FailoverComposeProfileName,
	}
}

type PollConfig struct {
	KilledLeader int32
	SessionID    int64
	MinTerm      int64
	Timeout      time.Duration
}

func pollUntilFailoverRecovered(
	t *testing.T,
	aeronClient *aeron.Client,
	session *client.AeronCluster,
	hadClientError *atomic.Bool,
	cfg PollConfig,
) {
	t.Helper()

	start := time.Now()
	pollEgressUntil(
		t,
		aeronClient,
		session,
		hadClientError,
		cfg.Timeout,
		"transport error during recovery",
		func() bool {
			if session.SessionState() != client.SessionStateConnected ||
				session.ClusterSessionID() != cfg.SessionID ||
				session.LeadershipTermID() < cfg.MinTerm ||
				session.LeaderMemberID() == cfg.KilledLeader {
				return false
			}
			ingress := session.IngressPublication()
			if ingress == nil || !ingress.IsConnected() {
				return false
			}
			t.Logf(
				"failover recovered: leader=%d term=%d state=%v elapsed=%s",
				session.LeaderMemberID(),
				session.LeadershipTermID(),
				session.SessionState(),
				time.Since(start),
			)
			return true
		},
		func() string {
			ingress := session.IngressPublication()
			ingressConnected := ingress != nil && ingress.IsConnected()
			return fmt.Sprintf(
				"failover recovery timeout after %s: state=%v leader=%d term=%d "+
					"(want term>=%d) session=%d killed=%d ingressConnected=%v hadClientError=%v",
				cfg.Timeout,
				session.SessionState(),
				session.LeaderMemberID(),
				session.LeadershipTermID(),
				cfg.MinTerm,
				session.ClusterSessionID(),
				cfg.KilledLeader,
				ingressConnected,
				hadClientError.Load(),
			)
		},
	)
}

func killClusterMemberContainer(
	t *testing.T,
	aeronClient *aeron.Client,
	hadClientError *atomic.Bool,
	memberID int32,
	composeDir string,
) {
	t.Helper()

	require.NotEmpty(t, composeDir, "failover compose dir required for container kill")
	service := fmt.Sprintf("cluster-%d", memberID)
	project := os.Getenv("COMPOSE_PROJECT_NAME")
	t.Logf("killing leader container: service=%s project=%s", service, project)
	args := append(clusterFailoverComposeArgs(composeDir), "kill", "-s", "SIGTERM", service)

	harness.PollClientDuring(
		t,
		aeronClient,
		hadClientError,
		dockerKillPollTimeout,
		"transport error during leader kill",
		func() string {
			return fmt.Sprintf(
				"docker kill timeout after %s: service=%s project=%s",
				dockerKillPollTimeout,
				service,
				project,
			)
		},
		func() error {
			out, err := exec.Command("docker", args...).CombinedOutput()
			if err != nil {
				return fmt.Errorf("kill %s: %w: %s", service, err, out)
			}
			return nil
		},
	)
}

func pollAsyncConnectUntilResult(
	t *testing.T,
	aeronClient *aeron.Client,
	ac *client.AsyncConnect,
	hadClientError *atomic.Bool,
	pollTimeout time.Duration,
) *client.ConnectResult {
	t.Helper()

	var got *client.ConnectResult
	harness.PollClientUntilTransportClean(
		t,
		aeronClient,
		hadClientError,
		pollTimeout,
		"transport error during connect poll",
		func() bool {
			result, err := ac.Poll()
			if err != nil {
				t.Fatalf(
					"cluster connect failed: %v (state=%v step=%v hadClientError=%v)",
					err, ac.State(), ac.Step(), hadClientError.Load(),
				)
			}
			if result != nil {
				got = result
				return true
			}
			return false
		},
		func() string {
			return fmt.Sprintf(
				"cluster connect poll timed out after %s: state=%v step=%v hadClientError=%v",
				pollTimeout, ac.State(), ac.Step(), hadClientError.Load(),
			)
		},
	)
	return got
}

func cleanupConnectResultHandles(t *testing.T, result *client.ConnectResult) {
	t.Helper()

	harness.CleanupClose(t, "close ingress publication", result.IngressPublication.Close)
	harness.CleanupClose(t, "close egress subscription", result.EgressSubscription.Close)
}

func requireConnectSmokeResult(
	t *testing.T,
	aeronClient *aeron.Client,
	result *client.ConnectResult,
) {
	t.Helper()

	require.Positive(t, result.ClusterSessionID)
	require.Equal(t, int32(clusterjava.MemberID), result.LeaderMemberID)
	require.Equal(t, int64(0), result.LeadershipTermID)
	require.False(t, result.OwnsAeronClient)
	require.Same(t, aeronClient, result.Aeron)
	require.True(t, result.IngressPublication.IsConnected())
	require.True(t, result.EgressSubscription.IsConnected())
	require.NotNil(t, result.EgressImage)
	require.Equal(t, clusterjava.IngressChannel, result.Config.IngressChannel)
	require.Equal(t, clusterjava.EgressChannel, result.Config.EgressChannel)
	require.Equal(t, clusterjava.IngressStreamID, result.Config.IngressStreamID)
	require.Equal(t, clusterjava.EgressStreamID, result.Config.ResponseStreamID)
	require.Equal(t, clusterjava.IngressEndpoints, result.Config.IngressEndpoints)
}
