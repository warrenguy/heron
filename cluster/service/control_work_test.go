//go:build testhooks

package service

import (
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func encodeJoinLogControl(tb assertTB, fields codecs.JoinLogFields) []byte {
	tb.Helper()
	// Generous buffer for channel var field.
	buf := make([]byte, 256+len(fields.LogChannel))
	wireEnd, ok := codecs.EncodeJoinLog(
		buf,
		0,
		uint16(clusterwire.JoinLogSbeSchemaVersion),
		fields,
	)
	require.True(tb, ok)
	return buf[:wireEnd]
}

func encodeRequestServiceAckControl(tb assertTB, logPosition int64) []byte {
	tb.Helper()
	buf := make([]byte, codecs.RequestServiceAckEncodedLen())
	wireEnd, ok := codecs.EncodeRequestServiceAck(
		buf,
		0,
		uint16(clusterwire.RequestServiceAckSbeSchemaVersion),
		codecs.RequestServiceAckFields{LogPosition: logPosition},
	)
	require.True(tb, ok)
	return buf[:wireEnd]
}

func encodeServiceTerminationPositionControl(tb assertTB, logPosition int64) []byte {
	tb.Helper()
	bufLen := codecs.MessageHeaderEncodedLen() +
		int(clusterwire.ServiceTerminationPositionSbeBlockLength)
	buf := make([]byte, bufLen)
	var msg clusterwire.ServiceTerminationPosition
	msg.WrapAndApplyHeader(buf, 0, uint64(len(buf))).SetLogPosition(logPosition)
	return buf
}

func defaultJoinLogFields(logPosition int64) codecs.JoinLogFields {
	e := defaultJoinEvent(logPosition)
	isStartup := clusterwire.BooleanType_FALSE
	if e.isStartup {
		isStartup = clusterwire.BooleanType_TRUE
	}
	return codecs.JoinLogFields{
		LogPosition:    e.logPosition,
		MaxLogPosition: e.maxLogPosition,
		MemberID:       e.memberID,
		LogSessionID:   e.logSessionID,
		LogStreamID:    e.logStreamID,
		IsStartup:      isStartup,
		Role:           e.role.Code(),
		IsStandby:      clusterwire.BooleanType_FALSE,
		LogChannel:     []byte(e.logChannel),
	}
}

// assertTB is the assertion surface shared by *testing.T and *rapid.T.
// Helpers used inside rapid.Check take this so failures stay on the property path.
type assertTB interface {
	Helper()
	Errorf(format string, args ...any)
	FailNow()
}

// decodeServiceAcksFromPub walks term 0 and decodes every DATA ServiceAck.
func decodeServiceAcksFromPub(tb assertTB, pub *aeron.Publication) []codecs.ServiceAckFields {
	tb.Helper()
	return decodeFramesFromPub(tb, pub, codecs.TryDecodeServiceAckAt)
}

// decodeFramesFromPub walks term 0 and decodes every DATA frame that
// tryDecodeAt accepts, in stream order.
func decodeFramesFromPub[F any](
	tb assertTB,
	pub *aeron.Publication,
	tryDecodeAt func([]byte, int) (F, bool),
) []F {
	tb.Helper()
	termBuf := pub.TermBuffer(0)
	var frames []F
	off := 0
	for {
		frameLength := int(logbuffer.FrameLengthVolatile(termBuf, off))
		if frameLength <= 0 {
			break
		}
		wire := termBuf.Bytes()[off+logbuffer.OffsetFramePayload : off+frameLength]
		if fields, ok := tryDecodeAt(wire, 0); ok {
			frames = append(frames, fields)
		}
		off += logbuffer.AlignFrameLength(frameLength)
		if off >= termBuf.Capacity() {
			break
		}
	}
	return frames
}

func serviceAckFields(logPosition, ackID, relevantID int64) codecs.ServiceAckFields {
	return codecs.ServiceAckFields{
		LogPosition: logPosition,
		Timestamp:   0,
		AckID:       ackID,
		RelevantID:  relevantID,
		ServiceID:   1,
	}
}

// controlAgentHarness holds an agent with control queue, log image, and module proxy.
type controlAgentHarness struct {
	agent     *Agent
	image     *aeron.Image
	committed *int64
	control   *QueueControlIngress
	pub       *aeron.Publication
	svc       *recordingService
}

func newControlAgentHarness(tb testing.TB, joinPosition int64) *controlAgentHarness {
	tb.Helper()
	return newControlAgentHarnessWithStartAck(tb, joinPosition, nil)
}

// newControlAgentHarnessWithStartAck builds the control harness. A nil
// startAck does not queue a start ack.
func newControlAgentHarnessWithStartAck(
	tb testing.TB,
	joinPosition int64,
	startAck *StartAckConfig,
) *controlAgentHarness {
	tb.Helper()
	svc := &recordingService{}
	sub, image := newLogImage(tb, testLogSessionID, testLogStreamID, joinPosition)
	committed := joinPosition
	control := &QueueControlIngress{}
	proxy, pub := boundModuleProxy(tb)

	agent := NewAgent(AgentConfig{
		ServiceID:         1,
		Service:           svc,
		CommittedPosition: func() int64 { return committed },
		Subscriptions:     &stubAdder{sub: sub},
		Control:           control,
		ModuleProxy:       proxy,
		StartAck:          startAck,
	})
	return &controlAgentHarness{
		agent:     agent,
		image:     image,
		committed: &committed,
		control:   control,
		pub:       pub,
		svc:       svc,
	}
}

// bindJoin delivers a JoinLog via the control queue and runs until bound.
func (h *controlAgentHarness) bindJoin(tb testing.TB, joinPosition int64) {
	tb.Helper()
	h.control.Enqueue(encodeJoinLogControl(tb, defaultJoinLogFields(joinPosition)))
	// Start, poll JoinLog, and bind run on the same tick. Drain runs before join
	// work, so the join ack stays ready and unsent (NextAckID still 0).
	work := h.agent.DoWork(0)
	require.NoError(tb, h.agent.ServiceFailure())
	require.True(tb, h.agent.IsLogBound())
	require.False(tb, h.agent.HasPendingJoin())
	require.GreaterOrEqual(tb, work, 3)
	require.Equal(tb, 1, h.agent.PendingCoordinationAcks())
	require.Equal(tb, 1, h.agent.ReadyCoordinationAcks())
	require.Equal(tb, int64(0), h.agent.NextAckID())
}

// drainJoin publishes the ready join ack left by bindJoin.
func (h *controlAgentHarness) drainJoin(tb testing.TB) {
	tb.Helper()
	require.Equal(tb, 1, h.agent.DoWork(0))
	require.Equal(tb, int64(1), h.agent.NextAckID())
	require.Equal(tb, 0, h.agent.PendingCoordinationAcks())
}

// writeTimer encodes a timer event frame at frameOffset. LeadershipTermID is 1.
func (h *controlAgentHarness) writeTimer(
	tb testing.TB,
	frameOffset int,
	correlationID, timestamp int64,
) int {
	tb.Helper()
	payload := encodeTimerEvent(tb, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    correlationID,
		Timestamp:        timestamp,
	})
	return writeAgentFrame(tb, h.image, frameOffset, payload)
}

func TestControlWork_JoinLogViaQueue(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	// Distinct channel bytes: codec copies the var field out of the buffer.
	channel := []byte("aeron:ipc?alias=join-copy")
	fields := defaultJoinLogFields(0)
	fields.LogChannel = append([]byte(nil), channel...)
	wire := encodeJoinLogControl(t, fields)
	// Corrupt the source after encode; wire already holds the channel bytes.
	channel[0] = 'X'
	fields.LogChannel[0] = 'Y'

	h.control.Enqueue(wire)
	h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.Equal(t, 1, h.agent.PendingCoordinationAcks())
	// Image present for session: join binds same tick after control poll.
	require.True(t, h.agent.IsLogBound())
	require.False(t, h.agent.HasPendingJoin())
	require.Equal(t, testMemberID, h.agent.MemberID())
	// Wire still decodes the original channel (encode-time copy into wire).
	decoded, ok := codecs.TryDecodeJoinLogAt(wire, 0)
	require.True(t, ok)
	require.Equal(t, []byte("aeron:ipc?alias=join-copy"), decoded.LogChannel)
}

func TestControlWork_RequestAppendUnready(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.bindJoin(t, 0)
	// Drain join first so the request is alone as the first queued entry after
	// append.
	h.drainJoin(t)

	h.control.Enqueue(encodeRequestServiceAckControl(t, 128))
	work := h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.GreaterOrEqual(t, work, 1) // control fragment
	require.Equal(t, 1, h.agent.PendingCoordinationAcks())
	require.Equal(t, 0, h.agent.ReadyCoordinationAcks())
	// Only prior join ack on the wire.
	require.Len(t, decodeServiceAcksFromPub(t, h.pub), 1)
}

// Request at the already-reached log position becomes ready and drains on the
// delivery tick (poll then sweep then drain; first entry free of unready
// prefix).
func TestControlWork_AlreadyReachedRequestSameTick(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.bindJoin(t, 0)
	h.drainJoin(t)
	require.Equal(t, int64(0), h.agent.LogPosition())

	h.control.Enqueue(encodeRequestServiceAckControl(t, 0))
	work := h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.GreaterOrEqual(t, work, 2) // control fragment + drain
	require.Equal(t, 0, h.agent.PendingCoordinationAcks())
	require.Equal(t, int64(2), h.agent.NextAckID())

	acks := decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 2)
	require.Equal(t, int64(0), acks[0].AckID)
	require.Equal(t, int64(1), acks[1].AckID)
	require.Equal(t, int64(0), acks[1].LogPosition)
	require.Equal(t, int64(-1), acks[1].RelevantID)
	require.Equal(t, int32(1), acks[1].ServiceID)
}

func TestControlWork_ReachedExactlyDrain(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.bindJoin(t, 0)
	h.drainJoin(t)

	// One timer frame; request at its end position.
	reqPos := int64(h.writeTimer(t, 0, 1, 50))
	*h.committed = reqPos

	h.control.Enqueue(encodeRequestServiceAckControl(t, reqPos))
	// Deliver request (unready), poll capped at reqPos. Sweep ran before poll
	// so readiness waits until next tick.
	h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.Equal(t, reqPos, h.agent.LogPosition())
	require.Equal(t, 1, h.agent.PendingCoordinationAcks())
	require.Equal(t, 0, h.agent.ReadyCoordinationAcks())

	// Sweep marks ready; drain sends ack id 1 at reqPos.
	work := h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.GreaterOrEqual(t, work, 1)
	require.Equal(t, 0, h.agent.PendingCoordinationAcks())
	require.Equal(t, int64(2), h.agent.NextAckID())

	acks := decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 2)
	require.Equal(t, int64(0), acks[0].AckID)
	require.Equal(t, int64(1), acks[1].AckID)
	require.Equal(t, reqPos, acks[1].LogPosition)
	require.Equal(t, int64(-1), acks[1].RelevantID)
	require.Equal(t, int32(1), acks[1].ServiceID)
}

func TestControlWork_BatchBoundaryCap(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.bindJoin(t, 0)
	h.drainJoin(t)

	span1 := h.writeTimer(t, 0, 1, 10)
	span2 := h.writeTimer(t, span1, 2, 20)
	reqPos := int64(span1)
	*h.committed = int64(span1 + span2)

	h.control.Enqueue(encodeRequestServiceAckControl(t, reqPos))
	// First tick after request: poll capped at reqPos; only first frame.
	h.agent.DoWork(0)
	require.Equal(t, reqPos, h.agent.LogPosition())
	require.Len(t, h.svc.timers, 1)

	// Second tick: ready + drain; poll may consume remainder.
	h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	acks := decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 2)
	require.Equal(t, reqPos, acks[1].LogPosition)
	// Remainder may land same tick as drain (poll after control) or next.
	for h.agent.LogPosition() < int64(span1+span2) && h.agent.ServiceFailure() == nil {
		h.agent.DoWork(0)
	}
	require.Equal(t, int64(span1+span2), h.agent.LogPosition())
	require.Len(t, h.svc.timers, 2)
}

func TestControlWork_PassedAtDelivery(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.bindJoin(t, 0)
	h.drainJoin(t)

	span := h.writeTimer(t, 0, 1, 1)
	*h.committed = int64(span)
	h.agent.DoWork(0)
	require.Equal(t, int64(span), h.agent.LogPosition())

	// Request below current position.
	h.control.Enqueue(encodeRequestServiceAckControl(t, 0))
	work := h.agent.DoWork(0)
	require.ErrorIs(t, h.agent.ServiceFailure(), errAckRequestPassed)
	require.GreaterOrEqual(t, work, 1)
	require.Equal(t, 1, h.agent.PendingCoordinationAcks())
	// Only join ack on the wire.
	require.Len(t, decodeServiceAcksFromPub(t, h.pub), 1)
	require.Equal(t, 0, h.agent.DoWork(0))
}

func TestControlWork_MidFrameRequest(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.bindJoin(t, 0)
	h.drainJoin(t)

	span := h.writeTimer(t, 0, 1, 1)
	// Request inside the frame; containing frame delivers whole past it.
	mid := int64(span) / 2
	if mid == 0 {
		mid = 1
	}
	require.Less(t, mid, int64(span))
	*h.committed = int64(span)

	h.control.Enqueue(encodeRequestServiceAckControl(t, mid))
	// Request appended; poll bound at mid; frame starts at 0 < mid so it
	// delivers; log rests at span past mid.
	h.agent.DoWork(0)
	require.Equal(t, int64(span), h.agent.LogPosition())
	// Sweep already ran before poll this tick. Next tick marks failure.
	h.agent.DoWork(0)
	require.ErrorIs(t, h.agent.ServiceFailure(), errAckRequestPassed)
	require.Len(t, decodeServiceAcksFromPub(t, h.pub), 1)
}

func TestControlWork_OutOfOrderRequests(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.bindJoin(t, 0)
	h.drainJoin(t)

	span1 := h.writeTimer(t, 0, 1, 10)
	span2 := h.writeTimer(t, span1, 2, 20)
	pos1 := int64(span1)
	pos2 := int64(span1 + span2)
	*h.committed = pos2

	// Deliver P2 then P1 (P1 < P2).
	h.control.Enqueue(encodeRequestServiceAckControl(t, pos2))
	h.control.Enqueue(encodeRequestServiceAckControl(t, pos1))
	// Cap rests at min unready = pos1; only first frame delivers.
	h.agent.DoWork(0)
	require.Equal(t, pos1, h.agent.LogPosition())
	require.Equal(t, 2, h.agent.PendingCoordinationAcks())

	// Sweep marks P1 ready (behind unready first entry P2). Cap is pos2 so the
	// second frame also delivers this tick. Prefix gate holds: no send.
	h.agent.DoWork(0)
	require.Equal(t, pos2, h.agent.LogPosition())
	require.Equal(t, 0, h.agent.ReadyCoordinationAcks()) // first entry P2, unready until next sweep
	require.False(t, h.agent.coordinationAcks[0].ready)
	require.True(t, h.agent.coordinationAcks[1].ready)
	require.Len(t, decodeServiceAcksFromPub(t, h.pub), 1) // join only

	// Sweep marks P2 ready; drain P2 (delivery order first).
	h.agent.DoWork(0)
	acks := decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 2) // join + P2
	require.Equal(t, pos2, acks[1].LogPosition)
	require.Equal(t, int64(1), acks[1].AckID)

	// Drain P1 next.
	h.agent.DoWork(0)
	acks = decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 3)
	require.Equal(t, pos1, acks[2].LogPosition)
	require.Equal(t, int64(2), acks[2].AckID)
	require.NoError(t, h.agent.ServiceFailure())
	require.Equal(t, int64(3), h.agent.NextAckID())
}

func TestControlWork_SupersessionDeliveryOrder(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	*h.committed = 1 << 20

	// Two joins before bind: first superseded ready, second pending.
	first := defaultJoinLogFields(0)
	first.MemberID = 1
	second := defaultJoinLogFields(0)
	second.MemberID = 9
	h.control.Enqueue(encodeJoinLogControl(t, first))
	h.control.Enqueue(encodeJoinLogControl(t, second))

	// Poll supersedes first (ready) then drains it same tick (drain after poll).
	// Join work then binds the second entry and marks it ready.
	h.agent.DoWork(0)
	require.True(t, h.agent.IsLogBound())
	require.Equal(t, int32(9), h.agent.MemberID())
	require.Equal(t, 1, h.agent.PendingCoordinationAcks())
	require.Equal(t, 1, h.agent.ReadyCoordinationAcks())
	acks := decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 1)
	require.Equal(t, int64(0), acks[0].AckID)
	require.Equal(t, int64(0), acks[0].LogPosition)
	require.Equal(t, int64(1), h.agent.NextAckID())

	// Next tick drains the bound join (id 1).
	h.agent.DoWork(0)
	acks = decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 2)
	require.Equal(t, int64(1), acks[1].AckID)
	require.Equal(t, int64(2), h.agent.NextAckID())
}

func TestControlWork_PrefixGate(t *testing.T) {
	// Pending join (unready) then request at current position 0 (ready).
	// Empty subscription: join stays pending until image is added.
	svc := &recordingService{}
	sub := aeron.NewSubscriptionForHooks(9003, testLogStreamID, testLogChannel)
	control := &QueueControlIngress{}
	proxy, pub := boundModuleProxy(t)
	agent := NewAgent(AgentConfig{
		ServiceID:         1,
		Service:           svc,
		CommittedPosition: func() int64 { return 0 },
		Subscriptions:     &stubAdder{sub: sub},
		Control:           control,
		ModuleProxy:       proxy,
	})

	control.Enqueue(encodeJoinLogControl(t, defaultJoinLogFields(0)))
	control.Enqueue(encodeRequestServiceAckControl(t, 0))
	agent.DoWork(0)
	require.True(t, agent.HasPendingJoin())
	require.Equal(t, 2, agent.PendingCoordinationAcks())
	// Request at logPosition 0 is ready; unready join first entry yields ready
	// prefix 0.
	require.Equal(t, 0, agent.ReadyCoordinationAcks())
	require.True(t, agent.coordinationAcks[1].ready)
	require.Empty(t, decodeServiceAcksFromPub(t, pub))

	// Bind join: replace subscription with one that has the image.
	sub2, _ := newLogImage(t, testLogSessionID, testLogStreamID, 0)
	agent.subscriptions = &stubAdder{sub: sub2}
	// Channel/stream match so joinWork reuses logSubscription without re-add.
	// Force re-add by closing the empty sub path: set logSubscription nil.
	if agent.logSubscription != nil {
		_ = agent.logSubscription.Close()
		agent.logSubscription = nil
	}
	agent.DoWork(0)
	require.True(t, agent.IsLogBound())
	require.Equal(t, 2, agent.ReadyCoordinationAcks())

	// Two ticks drain join then request.
	agent.DoWork(0)
	require.Len(t, decodeServiceAcksFromPub(t, pub), 1)
	agent.DoWork(0)
	acks := decodeServiceAcksFromPub(t, pub)
	require.Len(t, acks, 2)
	require.Equal(t, int64(0), acks[0].LogPosition)
	require.Equal(t, int64(0), acks[1].LogPosition)
	require.Equal(t, int64(0), acks[0].AckID)
	require.Equal(t, int64(1), acks[1].AckID)
}

func TestControlWork_OnePerTick(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.bindJoin(t, 0)
	// Two ready entries: join (ready) + request at 0 (ready after sweep).
	h.control.Enqueue(encodeRequestServiceAckControl(t, 0))
	// First queued entry is join (ready). Drain join only this tick.
	work := h.agent.DoWork(0)
	require.GreaterOrEqual(t, work, 1)
	require.Equal(t, int64(1), h.agent.NextAckID())
	require.Equal(t, 1, h.agent.PendingCoordinationAcks())
	require.Len(t, decodeServiceAcksFromPub(t, h.pub), 1)

	// Next tick drains request.
	h.agent.DoWork(0)
	require.Equal(t, int64(2), h.agent.NextAckID())
	require.Equal(t, 0, h.agent.PendingCoordinationAcks())
	require.Len(t, decodeServiceAcksFromPub(t, h.pub), 2)
}

func TestControlWork_RetriableNotConnected(t *testing.T) {
	svc := &recordingService{}
	sub, _ := newLogImage(t, testLogSessionID, testLogStreamID, 0)
	control := &QueueControlIngress{}
	// Limit 0 + disconnected: Offer returns NotConnected after the retry budget.
	proxy := boundModuleProxyWithLimit(t, 0, false)
	agent := NewAgent(AgentConfig{
		ServiceID:         1,
		Service:           svc,
		CommittedPosition: func() int64 { return 0 },
		Subscriptions:     &stubAdder{sub: sub},
		Control:           control,
		ModuleProxy:       proxy,
	})
	control.Enqueue(encodeJoinLogControl(t, defaultJoinLogFields(0)))
	agent.DoWork(0)
	require.True(t, agent.IsLogBound())
	require.Equal(t, 1, agent.ReadyCoordinationAcks())

	// Drain attempts NotConnected; entry and ackID stay.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, int64(0), agent.NextAckID())
	require.Equal(t, 1, agent.PendingCoordinationAcks())

	// Replace with connected publication.
	connected := aeron.NewTestPublicationForHooksWithConnected(t, true)
	proxy.SetPublication(connected)
	agent.DoWork(0)
	require.Equal(t, int64(1), agent.NextAckID())
	require.Equal(t, 0, agent.PendingCoordinationAcks())
	require.Len(t, decodeServiceAcksFromPub(t, connected), 1)
}

func TestControlWork_TerminalClosed(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.bindJoin(t, 0)
	require.NoError(t, h.pub.Close())

	h.agent.DoWork(0)
	require.ErrorIs(t, h.agent.ServiceFailure(), errAckPublish)
	require.Equal(t, 1, h.agent.PendingCoordinationAcks())
	require.Equal(t, int64(0), h.agent.NextAckID())
	require.Equal(t, 0, h.agent.DoWork(0))
}

func TestControlWork_NilProxy(t *testing.T) {
	svc := &recordingService{}
	sub, _ := newLogImage(t, testLogSessionID, testLogStreamID, 0)
	control := &QueueControlIngress{}
	agent := NewAgent(AgentConfig{
		Service:           svc,
		CommittedPosition: func() int64 { return 0 },
		Subscriptions:     &stubAdder{sub: sub},
		Control:           control,
		// ModuleProxy nil
	})
	control.Enqueue(encodeJoinLogControl(t, defaultJoinLogFields(0)))
	agent.DoWork(0)
	require.True(t, agent.IsLogBound())
	require.Equal(t, 1, agent.ReadyCoordinationAcks())
	agent.DoWork(0)
	require.Equal(t, 1, agent.PendingCoordinationAcks())
	require.Equal(t, int64(0), agent.NextAckID())
	require.NoError(t, agent.ServiceFailure())
}

func TestControlWork_UnboundProxyThenSetPublication(t *testing.T) {
	svc := &recordingService{}
	sub, _ := newLogImage(t, testLogSessionID, testLogStreamID, 0)
	control := &QueueControlIngress{}
	proxy := NewConsensusModuleProxy()
	agent := NewAgent(AgentConfig{
		ServiceID:         1,
		Service:           svc,
		CommittedPosition: func() int64 { return 0 },
		Subscriptions:     &stubAdder{sub: sub},
		Control:           control,
		ModuleProxy:       proxy,
	})
	control.Enqueue(encodeJoinLogControl(t, defaultJoinLogFields(0)))
	agent.DoWork(0)
	require.True(t, agent.IsLogBound())
	require.Equal(t, 1, agent.ReadyCoordinationAcks())
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, int64(0), agent.NextAckID())
	require.Equal(t, 1, agent.PendingCoordinationAcks())

	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	proxy.SetPublication(pub)
	agent.DoWork(0)
	require.Equal(t, int64(1), agent.NextAckID())
	require.Equal(t, 0, agent.PendingCoordinationAcks())
	require.Len(t, decodeServiceAcksFromPub(t, pub), 1)
}

func TestControlWork_NilControl(t *testing.T) {
	agent := NewAgent(AgentConfig{})
	require.Equal(t, 1, agent.DoWork(0)) // start only
	require.Equal(t, 0, agent.DoWork(0))
	require.Equal(t, 0, agent.PendingCoordinationAcks())
	require.Equal(t, int64(0), agent.NextAckID())
}

func TestControlWork_UnknownTemplate42(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.control.Enqueue(encodeServiceTerminationPositionControl(t, 99))
	h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.Equal(t, 0, h.agent.PendingCoordinationAcks())
	require.False(t, h.agent.HasPendingJoin())
}

func TestControlWork_WrongSchemaAndUndecodable(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	// Wrong schema id.
	h.control.Enqueue(encodeMessageHeader(t, 8, clusterwire.JoinLogSbeTemplateID, 999, 16))
	// Truncated RequestServiceAck (known template, undecodable).
	trunc := encodeRequestServiceAckControl(t, 1)
	h.control.Enqueue(trunc[:len(trunc)-4])
	h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.Equal(t, 0, h.agent.PendingCoordinationAcks())
}

func TestControlWork_SessionMessageNoAck(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.bindJoin(t, 0)
	// Drain join so NextAckID becomes 1; session messages must not advance it.
	h.drainJoin(t)
	before := len(decodeServiceAcksFromPub(t, h.pub))

	payload := encodeSessionMessage(t, codecs.SessionMessageHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: 1,
		Timestamp:        9,
	}, []byte("body"))
	span := writeAgentFrame(t, h.image, 0, payload)
	*h.committed = int64(span)
	h.agent.DoWork(0)
	require.Equal(t, int64(span), h.agent.LogPosition())
	require.Equal(t, int64(1), h.agent.NextAckID())
	require.Len(t, decodeServiceAcksFromPub(t, h.pub), before)
}

func TestControlWork_TimestampAndRelevantID(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.bindJoin(t, 0)

	// Advance clusterTime via a log event, then request at that position.
	span := h.writeTimer(t, 0, 1, 777)
	*h.committed = int64(span)
	h.control.Enqueue(encodeRequestServiceAckControl(t, int64(span)))

	// Join still first and ready: drain join; poll adopts span. Request unready
	// (sweep ran before poll at logPosition 0).
	h.agent.DoWork(0)
	require.Equal(t, int64(span), h.agent.LogPosition())
	require.Equal(t, int64(777), h.agent.ClusterTime())
	require.Equal(t, int64(1), h.agent.NextAckID())

	// Sweep marks request ready and drains.
	h.agent.DoWork(0)
	acks := decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 2)
	reqAck := acks[1]
	require.Equal(t, int64(span), reqAck.LogPosition)
	require.Equal(t, int64(777), reqAck.Timestamp)
	require.Equal(t, int64(-1), reqAck.RelevantID)
}

func TestControlWork_JoinThenSnapshotDrainOrder(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.bindJoin(t, 0)
	fake := InstallSnapshotTakeFakeForTest(t, h.agent)

	const timestamp int64 = 321
	end := writeTermEventAndSnapshot(t, h.image, h.committed, timestamp)

	// Tick 1: the join ack sends. The snapshot entry stays unready.
	h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.Equal(t, end, h.agent.LogPosition())
	require.Equal(t, timestamp, h.agent.ClusterTime())
	require.Equal(t, 1, h.agent.PendingCoordinationAcks())
	require.Equal(t, ackKindSnapshot, h.agent.coordinationAcks[0].kind)
	require.False(t, h.agent.coordinationAcks[0].ready)

	acks := decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 1)
	require.Equal(t, codecs.ServiceAckFields{
		LogPosition: 0,
		Timestamp:   0,
		AckID:       0,
		RelevantID:  nullValue,
		ServiceID:   1,
	}, acks[0])

	// Tick 2: the take records. No ack before STOP.
	h.agent.DoWork(0)
	require.Len(t, decodeServiceAcksFromPub(t, h.pub), 1)

	// Tick 3: the snapshot ack carries the recording id.
	h.agent.DoWork(0)
	acks = decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 2)
	require.Equal(t, codecs.ServiceAckFields{
		LogPosition: end,
		Timestamp:   timestamp,
		AckID:       1,
		RelevantID:  fake.RecordingID(),
		ServiceID:   1,
	}, acks[1])
	require.Equal(t, 0, h.agent.PendingCoordinationAcks())
}

func TestControlWork_RelevantIDRemainsNull(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.bindJoin(t, 0)
	h.drainJoin(t)
	acks := decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 1)
	require.Equal(t, codecs.ServiceAckFields{
		LogPosition: 0,
		Timestamp:   0,
		AckID:       0,
		RelevantID:  nullValue,
		ServiceID:   1,
	}, acks[0])

	h.control.Enqueue(encodeRequestServiceAckControl(t, 0))
	h.agent.DoWork(0)
	acks = decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 2)
	require.Equal(t, codecs.ServiceAckFields{
		LogPosition: 0,
		Timestamp:   0,
		AckID:       1,
		RelevantID:  nullValue,
		ServiceID:   1,
	}, acks[1])
}

func TestControlWork_DrainReadsRelevantID(t *testing.T) {
	proxy, pub := boundModuleProxy(t)
	agent := NewAgent(AgentConfig{
		ServiceID:   1,
		ModuleProxy: proxy,
	})
	const (
		logPosition int64 = 100
		timestamp   int64 = 777
		relevantID  int64 = 42
	)
	agent.clusterTime = timestamp
	agent.coordinationAcks = []coordinationAck{{
		logPosition: logPosition,
		kind:        ackKindSnapshot,
		ready:       true,
		relevantID:  relevantID,
	}}

	require.Equal(t, 1, agent.drainReadyCoordinationAck())
	require.Equal(t, 0, agent.PendingCoordinationAcks())
	acks := decodeServiceAcksFromPub(t, pub)
	require.Len(t, acks, 1)
	require.Equal(t, codecs.ServiceAckFields{
		LogPosition: logPosition,
		Timestamp:   timestamp,
		AckID:       0,
		RelevantID:  relevantID,
		ServiceID:   1,
	}, acks[0])
}

func TestControlWork_NoSpontaneousStartAck(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	// Start only: no control frames, no acks.
	h.agent.DoWork(0)
	require.Equal(t, int64(0), h.agent.NextAckID())
	require.Empty(t, decodeServiceAcksFromPub(t, h.pub))
	require.Equal(t, 0, h.agent.PendingCoordinationAcks())
}

func TestControlWork_StartAckSendsAfterStart(t *testing.T) {
	const (
		startPos   int64 = 0
		relevantID int64 = 42
	)
	h := newControlAgentHarnessWithStartAck(t, 0, &StartAckConfig{
		LogPosition: startPos,
		RelevantID:  relevantID,
	})

	require.Equal(t, 0, h.svc.startCount)
	require.Equal(t, int64(0), h.agent.NextAckID())
	require.Equal(t, 1, h.agent.PendingCoordinationAcks())
	require.Equal(t, 0, h.agent.ReadyCoordinationAcks())
	require.Empty(t, decodeServiceAcksFromPub(t, h.pub))

	work := h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.Equal(t, 1, h.svc.startCount)
	require.GreaterOrEqual(t, work, 2, "Start + drain")
	require.Equal(t, int64(1), h.agent.NextAckID())
	require.Equal(t, 0, h.agent.PendingCoordinationAcks())
	require.Equal(t, []codecs.ServiceAckFields{
		serviceAckFields(startPos, 0, relevantID),
	}, decodeServiceAcksFromPub(t, h.pub))
}

func TestControlWork_StartAckBeforeJoin(t *testing.T) {
	const (
		startPos   int64 = 0
		relevantID int64 = 99
	)
	h := newControlAgentHarnessWithStartAck(t, 0, &StartAckConfig{
		LogPosition: startPos,
		RelevantID:  relevantID,
	})
	h.control.Enqueue(encodeJoinLogControl(t, defaultJoinLogFields(0)))

	work := h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.True(t, h.agent.IsLogBound())
	require.False(t, h.agent.HasPendingJoin())
	require.GreaterOrEqual(t, work, 4, "Start + JoinLog + start drain + bind")
	require.Equal(t, int64(1), h.agent.NextAckID())
	require.Equal(t, 1, h.agent.PendingCoordinationAcks())
	require.Equal(t, 1, h.agent.ReadyCoordinationAcks())
	require.Equal(t, []codecs.ServiceAckFields{
		serviceAckFields(startPos, 0, relevantID),
	}, decodeServiceAcksFromPub(t, h.pub))

	require.Equal(t, 1, h.agent.DoWork(0))
	require.Equal(t, int64(2), h.agent.NextAckID())
	require.Equal(t, 0, h.agent.PendingCoordinationAcks())
	require.Equal(t, []codecs.ServiceAckFields{
		serviceAckFields(startPos, 0, relevantID),
		serviceAckFields(0, 1, nullValue),
	}, decodeServiceAcksFromPub(t, h.pub))
}

func TestControlWork_StartAckRelevantIDThenJoinNull(t *testing.T) {
	const relevantID int64 = 7
	h := newControlAgentHarnessWithStartAck(t, 0, &StartAckConfig{
		LogPosition: 0,
		RelevantID:  relevantID,
	})
	h.control.Enqueue(encodeJoinLogControl(t, defaultJoinLogFields(0)))

	h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	acks := decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 1)
	require.Equal(t, serviceAckFields(0, 0, relevantID), acks[0])

	h.agent.DoWork(0)
	require.Equal(t, []codecs.ServiceAckFields{
		serviceAckFields(0, 0, relevantID),
		serviceAckFields(0, 1, nullValue),
	}, decodeServiceAcksFromPub(t, h.pub))
}

func TestControlWork_StartAckRetriableNotConnected(t *testing.T) {
	const (
		startPos   int64 = 0
		relevantID int64 = 42
	)
	svc := &recordingService{}
	proxy := boundModuleProxyWithLimit(t, 0, false)
	agent := NewAgent(AgentConfig{
		ServiceID:   1,
		Service:     svc,
		ModuleProxy: proxy,
		StartAck: &StartAckConfig{
			LogPosition: startPos,
			RelevantID:  relevantID,
		},
	})

	require.Equal(t, 1, agent.PendingCoordinationAcks())
	require.Equal(t, 0, agent.ReadyCoordinationAcks())

	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, 1, svc.startCount)
	require.Equal(t, int64(0), agent.NextAckID())
	require.Equal(t, 1, agent.PendingCoordinationAcks())
	require.Equal(t, 1, agent.ReadyCoordinationAcks())

	connected := aeron.NewTestPublicationForHooksWithConnected(t, true)
	proxy.SetPublication(connected)
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, int64(1), agent.NextAckID())
	require.Equal(t, 0, agent.PendingCoordinationAcks())
	require.Equal(t, []codecs.ServiceAckFields{
		serviceAckFields(startPos, 0, relevantID),
	}, decodeServiceAcksFromPub(t, connected))
}

func TestControlWork_StartAckStartErrorRetriableMarksFailure(t *testing.T) {
	const startPos int64 = 64
	startErr := errors.New("start boom")
	svc := &recordingService{startErr: startErr}
	proxy := boundModuleProxyWithLimit(t, 0, false)
	control := &QueueControlIngress{}
	agent := NewAgent(AgentConfig{
		ServiceID:   1,
		Service:     svc,
		Control:     control,
		ModuleProxy: proxy,
		StartAck: &StartAckConfig{
			LogPosition: startPos,
			RelevantID:  42,
		},
	})
	failedEntry := []coordinationAck{{
		logPosition: startPos,
		kind:        ackKindStart,
		ready:       true,
		relevantID:  nullValue,
		failure:     startErr,
	}}

	// Phase 1: the module stream is not connected. The ack stays queued.
	// The agent does not mark the Start error.
	require.Equal(t, 1, agent.DoWork(0))
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, 1, svc.startCount)
	require.Equal(t, int64(0), agent.NextAckID())
	require.Equal(t, failedEntry, agent.coordinationAcks)

	// Phase 2: later ticks drain only the start entry. The agent does not
	// poll control.
	control.Enqueue(encodeRequestServiceAckControl(t, 0))
	require.Equal(t, 0, agent.DoWork(1))
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, 1, svc.startCount)
	require.Equal(t, failedEntry, agent.coordinationAcks)

	// Phase 3: the stream connects. The tick sends one NULL_VALUE ack.
	// The tick then marks the Start error.
	connected := aeron.NewTestPublicationForHooksWithConnected(t, true)
	proxy.SetPublication(connected)
	require.Equal(t, 1, agent.DoWork(2))
	require.ErrorIs(t, agent.ServiceFailure(), startErr)
	require.Equal(t, int64(1), agent.NextAckID())
	require.Empty(t, agent.coordinationAcks)
	require.Equal(t, []codecs.ServiceAckFields{
		serviceAckFields(startPos, 0, nullValue),
	}, decodeServiceAcksFromPub(t, connected))

	// Phase 4: the marked failure stops the agent.
	require.Equal(t, 0, agent.DoWork(3))
	require.Len(t, decodeServiceAcksFromPub(t, connected), 1)
}

func TestControlWork_StartAckStartErrorTimeoutMarksFailure(t *testing.T) {
	// The Start tick reads clock 0. Start then runs past every timeout, so
	// the first later tick reads startNs.
	const startNs = 2 * int64(DefaultRecoveryTimeout)
	tests := []struct {
		name       string
		timeout    time.Duration
		wantWaitNs int64
	}{
		{name: "set timeout", timeout: 10 * time.Nanosecond, wantWaitNs: 10},
		{name: "zero timeout", wantWaitNs: DefaultRecoveryTimeout.Nanoseconds()},
		{name: "negative timeout", timeout: -1, wantWaitNs: DefaultRecoveryTimeout.Nanoseconds()},
		{name: "saturated deadline", timeout: math.MaxInt64, wantWaitNs: math.MaxInt64 - startNs},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startErr := errors.New("start boom")
			proxy := boundModuleProxyWithLimit(t, 0, false)
			agent := NewAgent(AgentConfig{
				ServiceID:   1,
				Service:     &recordingService{startErr: startErr},
				ModuleProxy: proxy,
				StartAck: &StartAckConfig{
					LogPosition: 0,
					RelevantID:  42,
					Timeout:     tt.timeout,
				},
			})

			// Phase 1: the wait begins at the first tick after Start. A slow
			// Start does not use up the wait. Before the deadline the agent
			// does not mark the Start error.
			require.Equal(t, 1, agent.DoWork(0))
			require.Equal(t, 0, agent.DoWork(startNs))
			require.NoError(t, agent.ServiceFailure())
			require.Equal(t, 0, agent.DoWork(startNs+tt.wantWaitNs-1))
			require.NoError(t, agent.ServiceFailure())

			// Phase 2: at the deadline the tick marks the Start error. The ack
			// stays queued. The ack does not send.
			require.Equal(t, 0, agent.DoWork(startNs+tt.wantWaitNs))
			require.ErrorIs(t, agent.ServiceFailure(), startErr)
			require.Equal(t, int64(0), agent.NextAckID())
			require.Equal(t, 1, agent.PendingCoordinationAcks())

			connected := aeron.NewTestPublicationForHooksWithConnected(t, true)
			proxy.SetPublication(connected)
			require.Equal(t, 0, agent.DoWork(startNs+tt.wantWaitNs))
			require.Empty(t, decodeServiceAcksFromPub(t, connected))
		})
	}
}

func TestControlWork_StartAckStartErrorClosedMarksFailure(t *testing.T) {
	startErr := errors.New("start boom")
	closed := aeron.NewTestPublicationForHooksWithConnected(t, true)
	require.NoError(t, closed.Close())
	proxy := NewConsensusModuleProxy()
	proxy.SetPublication(closed)
	agent := NewAgent(AgentConfig{
		ServiceID:   1,
		Service:     &recordingService{startErr: startErr},
		ModuleProxy: proxy,
		StartAck:    &StartAckConfig{LogPosition: 0, RelevantID: 42},
	})

	require.Equal(t, 1, agent.DoWork(0))
	failure := agent.ServiceFailure()
	require.ErrorIs(t, failure, startErr)
	require.ErrorContains(t, failure, strconv.FormatInt(aeron.Closed, 10))
	require.Equal(t, int64(0), agent.NextAckID())
	require.Equal(t, 1, agent.PendingCoordinationAcks())
}

func TestControlWork_StartAckStartErrorAgentCloseMarksFailure(t *testing.T) {
	startErr := errors.New("start boom")
	proxy := boundModuleProxyWithLimit(t, 0, false)
	agent := NewAgent(AgentConfig{
		ServiceID:   1,
		Service:     &recordingService{startErr: startErr},
		ModuleProxy: proxy,
		StartAck:    &StartAckConfig{LogPosition: 0, RelevantID: 42},
	})

	// Phase 1: the stream is not connected. The agent does not mark the
	// Start error.
	require.Equal(t, 1, agent.DoWork(0))
	require.NoError(t, agent.ServiceFailure())

	// Phase 2: Close marks the Start error. The ack does not send.
	agent.Close()
	require.ErrorIs(t, agent.ServiceFailure(), startErr)
	require.Equal(t, int64(0), agent.NextAckID())

	connected := aeron.NewTestPublicationForHooksWithConnected(t, true)
	proxy.SetPublication(connected)
	require.Equal(t, 0, agent.DoWork(1))
	require.Empty(t, decodeServiceAcksFromPub(t, connected))
}

func TestControlWork_StartAckStartErrorSendsInTheSameTick(t *testing.T) {
	startErr := errors.New("start boom")
	proxy, pub := boundModuleProxy(t)
	agent := NewAgent(AgentConfig{
		ServiceID:   1,
		Service:     &recordingService{startErr: startErr},
		ModuleProxy: proxy,
		StartAck:    &StartAckConfig{LogPosition: 0, RelevantID: 42},
	})

	require.Equal(t, 2, agent.DoWork(0), "Start + start drain")
	require.ErrorIs(t, agent.ServiceFailure(), startErr)
	require.Equal(t, []codecs.ServiceAckFields{
		serviceAckFields(0, 0, nullValue),
	}, decodeServiceAcksFromPub(t, pub))
}

func TestControlWork_StartErrorWithoutAckSenderMarksAtOnce(t *testing.T) {
	startErr := errors.New("start boom")
	tests := []struct {
		name     string
		bound    bool
		startAck *StartAckConfig
	}{
		{name: "no start ack", bound: true},
		{
			name:     "no bound publication",
			startAck: &StartAckConfig{LogPosition: 0, RelevantID: 42},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxy := NewConsensusModuleProxy()
			if tt.bound {
				proxy, _ = boundModuleProxy(t)
			}
			agent := NewAgent(AgentConfig{
				ServiceID:   1,
				Service:     &recordingService{startErr: startErr},
				ModuleProxy: proxy,
				StartAck:    tt.startAck,
			})
			require.Equal(t, 1, agent.DoWork(0))
			require.ErrorIs(t, agent.ServiceFailure(), startErr)
			require.Equal(t, int64(0), agent.NextAckID())
			require.Equal(t, 0, agent.DoWork(0))
		})
	}
}

func TestControlWork_StartAckNegativeLogPositionQueuesNothing(t *testing.T) {
	proxy, pub := boundModuleProxy(t)
	agent := NewAgent(AgentConfig{
		ServiceID:   1,
		Service:     &recordingService{},
		ModuleProxy: proxy,
		StartAck:    &StartAckConfig{LogPosition: -1, RelevantID: 1},
	})
	require.Equal(t, 0, agent.PendingCoordinationAcks())
	require.Equal(t, int64(0), agent.NextAckID())

	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, 0, agent.PendingCoordinationAcks())
	require.Equal(t, int64(0), agent.NextAckID())
	require.Empty(t, decodeServiceAcksFromPub(t, pub))
}

func TestControlWork_StartAckNilServiceSends(t *testing.T) {
	const (
		startPos   int64 = 0
		relevantID int64 = 42
	)
	proxy, pub := boundModuleProxy(t)
	agent := NewAgent(AgentConfig{
		ServiceID:   1,
		ModuleProxy: proxy,
		StartAck: &StartAckConfig{
			LogPosition: startPos,
			RelevantID:  relevantID,
		},
	})
	require.Equal(t, 1, agent.PendingCoordinationAcks())
	require.Equal(t, 0, agent.ReadyCoordinationAcks())

	work := agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.GreaterOrEqual(t, work, 2, "start skip + drain")
	require.Equal(t, int64(1), agent.NextAckID())
	require.Equal(t, 0, agent.PendingCoordinationAcks())
	require.Equal(t, []codecs.ServiceAckFields{
		serviceAckFields(startPos, 0, relevantID),
	}, decodeServiceAcksFromPub(t, pub))
}
