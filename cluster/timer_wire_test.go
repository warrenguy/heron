//go:build testhooks

package cluster

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/service"
)

const (
	timerWireCorrelationID    int64 = 42
	timerWireDeadline         int64 = 1000
	timerWireLeadershipTermID int64 = 3
	timerWireSessionID        int64 = 1

	timerWireCmdSchedule = "schedule"
	timerWireCmdCancel   = "cancel"
)

// timerScriptService calls Cluster timer methods from SessionMessage.
type timerScriptService struct {
	cluster service.Cluster
	timers  []timerWireRec
}

type timerWireRec struct {
	correlationID int64
	timestamp     int64
}

func (s *timerScriptService) Start(cluster service.Cluster) error {
	s.cluster = cluster
	return nil
}

func (s *timerScriptService) SessionOpen(_, _ int64) {}

func (s *timerScriptService) SessionClose(_, _ int64, _ clusterwire.CloseReason) {}

func (s *timerScriptService) SessionMessage(_, _ int64, payload []byte) {
	switch string(payload) {
	case timerWireCmdSchedule:
		_ = s.cluster.ScheduleTimer(timerWireCorrelationID, timerWireDeadline)
	case timerWireCmdCancel:
		_ = s.cluster.CancelTimer(timerWireCorrelationID)
	}
}

func (s *timerScriptService) TimerEvent(correlationID, timestamp int64) {
	s.timers = append(s.timers, timerWireRec{
		correlationID: correlationID,
		timestamp:     timestamp,
	})
}

// timerWireHarness is an in-process consensus-module-plus-service timer path.
// Bridges stream 105 and the cluster log through test publications on one
// goroutine.
type timerWireHarness struct {
	wireServiceSide

	cm      *consensus.Agent
	logTerm *aeron.ExclusivePublication
	offLog  int

	script *timerScriptService
}

func newTimerWireHarness(t testing.TB) *timerWireHarness {
	t.Helper()

	logPub, logTerm := boundTimerLogPublisher(t)
	commit := &atomic.Int64{}
	cm := consensus.NewAgent(consensus.AgentConfig{
		Role:             consensus.RoleLeader,
		LogPublisher:     logPub,
		LeadershipTermID: timerWireLeadershipTermID,
		ServiceCount:     1,
		Commit:           commit,
	})

	script := &timerScriptService{}

	return &timerWireHarness{
		wireServiceSide: newWireServiceSide(t, cm, script),
		cm:              cm,
		logTerm:         logTerm,
		script:          script,
	}
}

func boundTimerLogPublisher(
	t testing.TB,
) (*consensus.LogPublisher, *aeron.ExclusivePublication) {
	t.Helper()
	pub := aeron.NewTestExclusivePublicationForHooks(t)
	p := consensus.NewLogPublisher()
	p.SetPublication(pub)
	return p, pub
}

// tick runs consensus module DoWork, service DoWork, then both bridges.
// Asserts the log poll bound after.
func (h *timerWireHarness) tick(t testing.TB, nowNs int64) {
	t.Helper()
	h.cm.DoWork(nowNs)
	h.svc.DoWork(nowNs)
	h.bridge105(t)
	h.bridgeLog(t)
	require.LessOrEqual(t, h.svc.LogPosition(), h.cm.CommitPosition())
}

// requireTickUntil runs one duty-cycle pair until done returns true. After
// eight calls without done, requireTickUntil fatals with harness state.
func (h *timerWireHarness) requireTickUntil(
	t testing.TB,
	nowNs int64,
	done func() bool,
	msg string,
) {
	t.Helper()
	for range 8 {
		h.tick(t, nowNs)
		if done() {
			return
		}
	}
	t.Fatalf(
		"%s: bound=%v count=%d timers=%v ackFailure=%v timerFailure=%v svcFailure=%v",
		msg,
		h.svc.IsLogBound(),
		h.cm.ScheduledTimerCount(),
		h.script.timers,
		h.cm.ServiceAckFailure(),
		h.cm.TimerFailure(),
		h.svc.ServiceFailure(),
	)
}

// bridgeLog copies new DATA frames from the consensus module log publication
// onto the service image and advances the committed bound to the image end.
func (h *timerWireHarness) bridgeLog(t testing.TB) {
	t.Helper()
	bridgeTerm(t, h.logTerm.TermBuffer(0), &h.offLog, func(msg []byte) {
		h.writeFrame(t, msg)
	})
	h.cm.ProposeMaxCommit(int64(h.frameOffset))
}

func (h *timerWireHarness) bindJoin(t testing.TB) {
	t.Helper()
	h.deliverJoinLog(t, 0)
	h.requireTickUntil(
		t,
		0,
		func() bool {
			return h.svc.IsLogBound()
		},
		"join did not bind",
	)
	require.NoError(t, h.svc.ServiceFailure())
}

func (h *timerWireHarness) writeSessionMessage(t testing.TB, payload string) {
	t.Helper()
	h.writeFrame(
		t,
		encodeWireSessionMessage(
			t,
			codecs.SessionMessageHeaderFields{
				LeadershipTermID: timerWireLeadershipTermID,
				ClusterSessionID: timerWireSessionID,
				Timestamp:        1,
			},
			[]byte(payload),
		),
	)
	h.cm.ProposeMaxCommit(int64(h.frameOffset))
}

func (h *timerWireHarness) scheduleOne(t testing.TB) {
	t.Helper()
	h.writeSessionMessage(t, timerWireCmdSchedule)
	h.requireTickUntil(
		t,
		0,
		func() bool {
			return h.cm.ScheduledTimerCount() == 1
		},
		"schedule count stayed 0",
	)
}

func TestTimerWire_ScheduleRoundTrip(t *testing.T) {
	h := newTimerWireHarness(t)
	h.bindJoin(t)

	h.scheduleOne(t)
	require.Equal(t, int64(1), h.cm.ScheduledTimerCount())
	require.NoError(t, h.cm.TimerFailure())
	require.NoError(t, h.svc.ServiceFailure())
	require.Empty(t, h.script.timers)
}

func TestTimerWire_ExpiryRoundTrip(t *testing.T) {
	h := newTimerWireHarness(t)
	h.bindJoin(t)

	h.scheduleOne(t)
	require.Empty(t, h.script.timers)

	h.requireTickUntil(
		t,
		timerWireDeadline,
		func() bool {
			return h.cm.ScheduledTimerCount() == 0 && len(h.script.timers) == 1
		},
		"expiry did not deliver",
	)
	require.Equal(
		t,
		[]timerWireRec{{
			correlationID: timerWireCorrelationID,
			timestamp:     timerWireDeadline,
		}},
		h.script.timers,
	)
	require.Equal(t, int64(0), h.cm.ScheduledTimerCount())
	require.NoError(t, h.svc.ServiceFailure())

	termBuf := h.logTerm.TermBuffer(0)
	frameLength := int(logbuffer.FrameLengthVolatile(termBuf, 0))
	require.Positive(t, frameLength)
	wire := termBuf.Bytes()[logbuffer.OffsetFramePayload:frameLength]
	got, decOK := codecs.TryDecodeTimerEventAt(wire, 0)
	require.True(t, decOK)
	require.Equal(
		t,
		codecs.TimerEventFields{
			LeadershipTermID: timerWireLeadershipTermID,
			CorrelationID:    timerWireCorrelationID,
			Timestamp:        timerWireDeadline,
		},
		got,
	)
}

func TestTimerWire_CancelRoundTrip(t *testing.T) {
	h := newTimerWireHarness(t)
	h.bindJoin(t)

	h.scheduleOne(t)

	h.writeSessionMessage(t, timerWireCmdCancel)
	h.requireTickUntil(
		t,
		0,
		func() bool {
			return h.cm.ScheduledTimerCount() == 0
		},
		"cancel left a scheduled timer",
	)

	h.tick(t, timerWireDeadline)
	h.tick(t, timerWireDeadline)
	require.Equal(t, int64(0), h.cm.ScheduledTimerCount())
	require.Empty(t, h.script.timers)
	require.NoError(t, h.svc.ServiceFailure())
	require.Zero(t, int(logbuffer.FrameLengthVolatile(h.logTerm.TermBuffer(0), 0)))
}
