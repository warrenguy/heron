//go:build integration && cluster_java

package cluster_java_test

import (
	"bytes"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/service"
	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
	"github.com/warrenguy/heron/test/integration/harness"
)

var mixedServicePayload = []byte("heron-mixed-service-payload")

const mixedServicePollInterval = time.Millisecond

// javaCommitPosition reads the Java consensus-module commit-pos counter
// (type 203). Returns 0 while the counter is unallocated: the agent then
// polls at bound 0 and consumes nothing.
func javaCommitPosition(counters *harness.DriverCounters) func() int64 {
	return func() int64 {
		value, ok := counters.Value(consensus.ClusterCommitPositionCounterTypeID)
		if !ok {
			return 0
		}
		return value
	}
}

// recordingService records ClusteredService callbacks. Invoker-only. Copies
// payload bytes. Callback slices are transient.
//
// SessionMessage offers each payload back to its session and records the
// Offer result in echoResults.
type recordingService struct {
	startCount  int
	opens       []int64
	messages    []recordedMessage
	closes      []int64
	order       []recordedEvent
	echoResults []int64
}

// nilSessionID marks a recordedMessage whose SessionMessage session was nil.
const nilSessionID int64 = -1

type recordedMessage struct {
	sessionID int64
	payload   []byte
}

type recordedEvent struct {
	kind      string
	sessionID int64
}

const (
	eventOpen    = "open"
	eventMessage = "message"
	eventClose   = "close"
)

func (s *recordingService) Start(service.Cluster) error {
	s.startCount++
	return nil
}

func (s *recordingService) SessionOpen(session service.ClientSession, _ int64) {
	sessionID := session.ID()
	s.opens = append(s.opens, sessionID)
	s.order = append(s.order, recordedEvent{kind: eventOpen, sessionID: sessionID})
}

func (s *recordingService) SessionClose(
	session service.ClientSession,
	_ int64,
	_ clusterwire.CloseReason,
) {
	sessionID := session.ID()
	s.closes = append(s.closes, sessionID)
	s.order = append(s.order, recordedEvent{kind: eventClose, sessionID: sessionID})
}

// SessionMessage records the message and its echo result. The result is 0
// for a nil session.
func (s *recordingService) SessionMessage(
	session service.ClientSession,
	_ int64,
	payload []byte,
) {
	sessionID := nilSessionID
	if session != nil {
		sessionID = session.ID()
	}
	s.messages = append(s.messages, recordedMessage{
		sessionID: sessionID,
		payload:   bytes.Clone(payload),
	})
	s.order = append(s.order, recordedEvent{kind: eventMessage, sessionID: sessionID})
	var result int64
	if session != nil {
		result = session.Offer(payload)
	}
	s.echoResults = append(s.echoResults, result)
}

func (s *recordingService) TimerEvent(_, _ int64) {}

func (s *recordingService) holdsOpen(sessionID int64) bool {
	return slices.Contains(s.opens, sessionID)
}

func (s *recordingService) holdsClose(sessionID int64) bool {
	return slices.Contains(s.closes, sessionID)
}

func (s *recordingService) openCount(sessionID int64) int {
	n := 0
	for _, id := range s.opens {
		if id == sessionID {
			n++
		}
	}
	return n
}

func (s *recordingService) messageCount(sessionID int64) int {
	n := 0
	for _, msg := range s.messages {
		if msg.sessionID == sessionID {
			n++
		}
	}
	return n
}

func (s *recordingService) eventIndex(kind string, sessionID int64) int {
	return slices.IndexFunc(s.order, func(ev recordedEvent) bool {
		return ev.kind == kind && ev.sessionID == sessionID
	})
}

// holdsPayloadAfterOpen reports at least one message for sessionID with
// payload, recorded after that session's open. Every message for sessionID
// must match payload.
//
// The Java leader adds the configured ingress channel and a second IPC
// ingress subscription when isIpcIngressAllowed is true. One offer can
// therefore appear more than once on the log. Copies of the same payload
// still assert committed delivery.
func (s *recordingService) holdsPayloadAfterOpen(
	sessionID int64,
	payload []byte,
) bool {
	openIdx := s.eventIndex(eventOpen, sessionID)
	msgIdx := s.eventIndex(eventMessage, sessionID)
	if openIdx < 0 || msgIdx <= openIdx {
		return false
	}
	matched := 0
	for _, msg := range s.messages {
		if msg.sessionID != sessionID {
			continue
		}
		if !bytes.Equal(msg.payload, payload) {
			return false
		}
		matched++
	}
	return matched >= 1
}

// boundCheckedTick ticks one service agent and records the first commit-bound
// violation. Reads the Java commit-pos counter after the agent tick.
type boundCheckedTick struct {
	agent     *service.Agent
	commitPos func() int64
	violation error
}

func (b *boundCheckedTick) DoWork(nowNs int64) int {
	n := b.agent.DoWork(nowNs)
	if b.violation != nil {
		return n
	}
	logPos := b.agent.LogPosition()
	commit := b.commitPos()
	if logPos > commit {
		b.violation = fmt.Errorf(
			"service log position %d exceeds Java commit position %d",
			logPos,
			commit,
		)
	}
	return n
}

type mixedServiceAwait struct {
	aeronClient    *aeron.Client
	hadClientError *atomic.Bool
	checked        *boundCheckedTick
	counters       *harness.DriverCounters
	recorder       *recordingService
	session        *client.AeronCluster
}

func (p mixedServiceAwait) dump() string {
	var (
		state, role, commit       int64
		stateOK, roleOK, commitOK bool
	)
	if p.counters != nil {
		state, stateOK = p.counters.Value(consensus.ClusterConsensusModuleStateCounterTypeID)
		role, roleOK = p.counters.Value(consensus.ClusterNodeRoleCounterTypeID)
		commit, commitOK = p.counters.Value(consensus.ClusterCommitPositionCounterTypeID)
	}
	line := fmt.Sprintf(
		"java stateOK=%v state=%d roleOK=%v role=%d commitOK=%v commit=%d",
		stateOK, state, roleOK, role, commitOK, commit,
	)
	if p.checked == nil || p.checked.agent == nil {
		return line
	}
	agent := p.checked.agent
	start, opens, msgs, closes := 0, []int64(nil), 0, []int64(nil)
	var echoResults []int64
	if p.recorder != nil {
		start = p.recorder.startCount
		opens = p.recorder.opens
		msgs = len(p.recorder.messages)
		closes = p.recorder.closes
		echoResults = p.recorder.echoResults
	}
	return line + fmt.Sprintf(
		" bound=%v logPos=%d nextAck=%d pendingJoin=%v"+
			" role=%v serviceFail=%v violation=%v start=%d opens=%v msgs=%d closes=%v"+
			" responseAdds=%d echoResults=%v",
		agent.IsLogBound(),
		agent.LogPosition(),
		agent.NextAckID(),
		agent.HasPendingJoin(),
		agent.Role(),
		agent.ServiceFailure(),
		p.checked.violation,
		start,
		opens,
		msgs,
		closes,
		agent.PendingResponsePublicationAdds(),
		echoResults,
	)
}

func (p mixedServiceAwait) requireHealthy(t *testing.T, label string) {
	t.Helper()

	if p.hadClientError.Load() {
		t.Fatalf("transport error during %s:\n%s", label, p.dump())
	}
	if p.checked != nil && p.checked.violation != nil {
		t.Fatalf("commit bound during %s:\n%s", label, p.dump())
	}
	if p.checked != nil && p.checked.agent != nil {
		if err := p.checked.agent.ServiceFailure(); err != nil {
			t.Fatalf("service failure during %s:\n%s", label, p.dump())
		}
	}
}

// pump drives the transport client, the service agent, then session egress.
//
// Order matches the in-process service duty cycle. The transport client is
// the duty-cycle runner. This process has no consensus module. This process
// has no archive poll.
func (p mixedServiceAwait) pump(lastKeepalive *time.Time) {
	p.aeronClient.DoWork()
	if p.checked != nil {
		p.checked.DoWork(time.Now().UnixNano())
	}
	if p.session == nil {
		return
	}
	p.session.PollEgress()
	now := time.Now()
	if now.Sub(*lastKeepalive) >= time.Second {
		_ = p.session.SendKeepAlive()
		*lastKeepalive = now
	}
}

func awaitMixedService(
	t *testing.T,
	p mixedServiceAwait,
	timeout time.Duration,
	done func() bool,
	label string,
) {
	t.Helper()

	p.requireHealthy(t, label)
	if done() {
		return
	}
	deadline := time.Now().Add(timeout)
	lastKeepalive := time.Now()
	for time.Now().Before(deadline) {
		p.requireHealthy(t, label)
		p.pump(&lastKeepalive)
		p.requireHealthy(t, label)
		if done() {
			return
		}
		time.Sleep(mixedServicePollInterval)
	}
	require.FailNow(
		t,
		label+" timed out after "+timeout.String(),
		"%s",
		p.dump(),
	)
}

func offerMixedServiceUntilPositive(
	t *testing.T,
	p mixedServiceAwait,
	timeout time.Duration,
	offer func() int64,
) int64 {
	t.Helper()

	lastKeepalive := time.Now()
	deadline := time.Now().Add(timeout)
	for {
		p.requireHealthy(t, "session offer")
		p.pump(&lastKeepalive)
		p.requireHealthy(t, "session offer")
		pos := offer()
		if pos > 0 {
			return pos
		}
		if !aeron.IsIngressRetriable(pos) {
			t.Fatalf(
				"cluster offer failed: status=%d sessionState=%v hadClientError=%v\n%s",
				pos,
				p.session.SessionState(),
				p.hadClientError.Load(),
				p.dump(),
			)
		}
		if !time.Now().Before(deadline) {
			require.FailNow(
				t,
				"cluster offer timed out after "+timeout.String(),
				"%s",
				p.dump(),
			)
		}
		time.Sleep(mixedServicePollInterval)
	}
}

func connectMixedServiceSession(
	t *testing.T,
	p mixedServiceAwait,
	dir string,
	egress client.EgressHandler,
) *client.AeronCluster {
	t.Helper()

	cfg := smokeConfig(
		clusterjava.HarnessConfig,
		dir,
		p.aeronClient,
		egress,
		clusterjava.MixedServiceConnectBudget,
	)
	ac, err := client.NewAsyncConnect(t.Context(), cfg)
	require.NoError(t, err, "%s", p.dump())
	harness.CleanupClose(t, "close async connect", ac.Close)

	var got *client.ConnectResult
	awaitMixedService(
		t, p, clusterjava.MixedServiceConnectBudget,
		func() bool {
			result, pollErr := ac.Poll()
			if pollErr != nil {
				t.Fatalf(
					"cluster connect failed: %v (state=%v step=%v hadClientError=%v)\n%s",
					pollErr,
					ac.State(),
					ac.Step(),
					p.hadClientError.Load(),
					p.dump(),
				)
			}
			if result != nil {
				got = result
				return true
			}
			return false
		},
		"session connect",
	)
	return newConnectedClusterSession(t, got)
}

// TestClusterMixedServiceSmoke asserts a Go service container joins a live
// Java consensus module and observes at least one committed session open,
// payload, and close for the induction session. The service echoes each
// payload to the Go client. Makes no claim about Java recordings.
func TestClusterMixedServiceSmoke(t *testing.T) {
	dir, hadClientError, aeronClient, md, _ := requireClusterComposeEnv(t, clusterComposeGate{
		envKey:         clusterjava.MixedServiceComposeGateEnvKey,
		skipHint:       clusterjava.MixedServiceComposeSkipHint,
		requireIPCTerm: true,
	})
	md.AssertAlive(t)

	counters, err := harness.OpenDriverCounters(dir)
	require.NoError(t, err)
	harness.CleanupClose(t, "close driver counters", counters.Close)

	pump := mixedServiceAwait{
		aeronClient:    aeronClient,
		hadClientError: hadClientError,
		counters:       counters,
	}

	awaitMixedService(
		t, pump, clusterjava.MixedServiceCountersBudget,
		func() bool {
			_, ok := counters.Value(consensus.ClusterCommitPositionCounterTypeID)
			return ok
		},
		"commit-pos counter allocated",
	)
	commit, ok := counters.Value(consensus.ClusterCommitPositionCounterTypeID)
	require.True(t, ok, "commit-pos counter allocated:\n%s", pump.dump())
	require.Equal(t, int64(0), commit, "cold-start commit-pos:\n%s", pump.dump())

	commitFn := javaCommitPosition(counters)
	recorder := &recordingService{}
	container, err := service.Open(service.Config{
		Aeron:             aeronClient,
		Service:           recorder,
		CommittedPosition: commitFn,
		StartAck: &service.StartAckConfig{
			LogPosition: 0,
			RelevantID:  aeronClient.ClientId(),
		},
	})
	require.NoError(t, err)
	harness.CleanupClose(t, "close service container", container.Close)

	agent := container.Agent()
	checked := &boundCheckedTick{agent: agent, commitPos: commitFn}
	pump.checked = checked
	pump.recorder = recorder

	awaitMixedService(
		t, pump, clusterjava.MixedServiceJoinBudget,
		func() bool {
			return agent.NextAckID() == 2 &&
				agent.IsLogBound() &&
				!agent.HasPendingJoin() &&
				recorder.startCount == 1 &&
				agent.Role() == codecs.RoleLeader &&
				agent.ServiceFailure() == nil
		},
		"start ack and join settle",
	)

	var echoes []recordedMessage
	egress := &client.EgressHandlers{
		MessageBytesFunc: func(clusterSessionID, _ int64, payload []byte, _ *aeron.Header) {
			echoes = append(echoes, recordedMessage{
				sessionID: clusterSessionID,
				payload:   bytes.Clone(payload),
			})
		},
	}
	session := connectMixedServiceSession(t, pump, dir, egress)
	pump.session = session
	sessionID := session.ClusterSessionID()

	awaitMixedService(
		t, pump, clusterjava.MixedServiceDeliverBudget,
		func() bool {
			return recorder.holdsOpen(sessionID) && agent.PendingResponsePublicationAdds() == 0
		},
		"session open delivered",
	)
	require.Equal(
		t,
		1,
		recorder.openCount(sessionID),
		"induction SessionOpen expected once:\n%s",
		pump.dump(),
	)

	offerMixedServiceUntilPositive(
		t, pump, clusterjava.MixedServiceDeliverBudget,
		func() int64 { return session.Offer(mixedServicePayload) },
	)
	awaitMixedService(
		t, pump, clusterjava.MixedServiceDeliverBudget,
		func() bool {
			return recorder.holdsPayloadAfterOpen(sessionID, mixedServicePayload)
		},
		"session payload delivered",
	)
	require.LessOrEqual(
		t,
		recorder.messageCount(sessionID),
		2,
		"payload count:\n%s",
		pump.dump(),
	)

	// The service echoes once per delivery. One offer can reach the log twice.
	awaitMixedService(
		t, pump, clusterjava.MixedServiceDeliverBudget,
		func() bool { return len(echoes) > 0 },
		"echo received",
	)
	requireMixedServiceEchoes(t, pump, recorder, sessionID, echoes)
	require.GreaterOrEqual(
		t,
		commitFn(),
		agent.LogPosition(),
		"Java commit-pos must be at or past service log position:\n%s",
		pump.dump(),
	)

	require.Equal(
		t,
		int64(2),
		agent.NextAckID(),
		"session traffic must not move ack id:\n%s",
		pump.dump(),
	)

	require.NoError(t, session.Close())
	awaitMixedService(
		t, pump, clusterjava.MixedServiceDeliverBudget,
		func() bool { return recorder.holdsClose(sessionID) },
		"session close delivered",
	)
	requireMixedServiceEchoes(t, pump, recorder, sessionID, echoes)

	require.NoError(
		t,
		checked.violation,
		"commit bound:\n%s",
		pump.dump(),
	)
	require.NoError(
		t,
		agent.ServiceFailure(),
		"ServiceFailure:\n%s",
		pump.dump(),
	)
	require.NoError(t, container.Close())
	require.NoError(t, aeronClient.Close())

	t.Logf(
		"cluster mixed service smoke passed: session=%d logPos=%d commit=%d nextAck=%d",
		sessionID,
		agent.LogPosition(),
		commitFn(),
		agent.NextAckID(),
	)
}

// requireMixedServiceEchoes asserts every service echo offer succeeded. The
// client holds no more echoes than deliveries. Each echo carries sessionID
// and the smoke payload.
func requireMixedServiceEchoes(
	t *testing.T,
	p mixedServiceAwait,
	recorder *recordingService,
	sessionID int64,
	echoes []recordedMessage,
) {
	t.Helper()

	for _, result := range recorder.echoResults {
		require.Positive(t, result, "echo offer result:\n%s", p.dump())
	}
	require.LessOrEqual(t, len(echoes), recorder.messageCount(sessionID), "echo count:\n%s", p.dump())
	for _, echo := range echoes {
		require.Equal(t, recordedMessage{sessionID: sessionID, payload: mixedServicePayload}, echo,
			"echo:\n%s", p.dump())
	}
}
