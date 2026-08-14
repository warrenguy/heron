//go:build testhooks

package cluster

import (
	"math"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/service"
)

// In-process consensus-module-plus-service control path unit. Bridges stream
// 104 and 105 through test publications and queue ingresses on one goroutine.
//
// Tick order: consensus module DoWork, then service DoWork, then both bridges.
// The product harness runs runner.DoWork then service.DoWork. This unit uses
// the bare consensus agent for the runner half.

const (
	wireLogChannel   = "aeron:ipc"
	wireLogStreamID  = int32(100)
	wireLogSessionID = int32(42)
	wireMemberID     = int32(7)
	// Matches NewTestImageForSubscriptionHooks partition-0 initial term id.
	wireHookTermID int32 = 7
)

// wireSubAdder returns a fixed subscription from AddSubscription.
type wireSubAdder struct {
	sub *aeron.Subscription
}

func (s *wireSubAdder) AddSubscription(_ int32, _ string) (*aeron.Subscription, error) {
	return s.sub, nil
}

// wireRecordingService is a no-op ClusteredService for the wire unit.
type wireRecordingService struct {
	messages int
}

func (s *wireRecordingService) Start(_ service.Cluster) error {
	return nil
}

func (s *wireRecordingService) SessionOpen(_, _ int64) {}

func (s *wireRecordingService) SessionClose(_, _ int64, _ clusterwire.CloseReason) {}

func (s *wireRecordingService) SessionMessage(_, _ int64, _ []byte) {
	s.messages++
}

func (s *wireRecordingService) TimerEvent(_, _ int64) {}

// wireServiceSide is the service half shared by the wire harnesses.
//
//   - log subscription with one image
//   - control ingress
//   - module proxy on a connected 105 test publication
//   - consensus module service ingress that bridge105 feeds
type wireServiceSide struct {
	serviceIngress *consensus.QueueConsensusIngress

	svc     *service.Agent
	control *service.QueueControlIngress
	pub105  *aeron.Publication
	off105  int

	image       *aeron.Image
	frameOffset int
}

// newWireServiceSide builds the shared service-side fields. The helper wires
// the consensus module service ingress. The image joins at position 0.
func newWireServiceSide(
	t testing.TB,
	cm *consensus.Agent,
	app service.ClusteredService,
) wireServiceSide {
	t.Helper()

	serviceIngress := &consensus.QueueConsensusIngress{}
	cm.SetServiceIngress(serviceIngress)

	sub := aeron.NewSubscriptionForHooks(9001, wireLogStreamID, wireLogChannel)
	image := aeron.NewTestImageForSubscriptionHooksWithSessionID(
		t,
		sub,
		wireLogSessionID,
		9101,
		0,
	)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))

	control := &service.QueueControlIngress{}
	pub105 := aeron.NewTestPublicationForHooksWithConnected(t, true)
	moduleProxy := service.NewConsensusModuleProxy()
	moduleProxy.SetPublication(pub105)

	svc := service.NewAgent(service.AgentConfig{
		// ServiceID 0 matches ServiceCount:1 tracker queue index 0.
		ServiceID:         0,
		Service:           app,
		CommittedPosition: cm.CommitPosition,
		Subscriptions:     &wireSubAdder{sub: sub},
		Control:           control,
		ModuleProxy:       moduleProxy,
	})

	return wireServiceSide{
		serviceIngress: serviceIngress,
		svc:            svc,
		control:        control,
		pub105:         pub105,
		image:          image,
	}
}

// bridge105 walks new DATA frames on the service module proxy publication into
// the consensus module service ingress queue.
func (s *wireServiceSide) bridge105(t testing.TB) {
	t.Helper()
	bridgePub(t, s.pub105, &s.off105, func(msg []byte) {
		s.serviceIngress.Enqueue(msg)
	})
}

// writeFrame commits one DATA frame at frameOffset and advances the offset.
func (s *wireServiceSide) writeFrame(t testing.TB, payload []byte) {
	t.Helper()
	aeron.WriteCommittedDataFrameForHooks(
		aeron.ImageTermBufferForHooks(s.image),
		s.frameOffset,
		s.image.SessionID(),
		s.image.StreamID(),
		wireHookTermID,
		int32(s.frameOffset),
		logbuffer.Unfragmented,
		payload,
	)
	s.frameOffset += logbuffer.AlignFragmentChunkLength(len(payload))
}

// deliverJoinLog enqueues a crafted JoinLog on the service control path. The
// helper does not publish through the consensus module ServiceProxy. That
// path would add an extra consensus module wait.
func (s *wireServiceSide) deliverJoinLog(t testing.TB, joinPos int64) {
	t.Helper()
	s.control.Enqueue(encodeWireJoinLog(t, wireJoinLogFields(joinPos)))
}

// wireHarness holds a consensus agent and a service agent bridged in memory.
type wireHarness struct {
	wireServiceSide

	cm     *consensus.Agent
	pub104 *aeron.Publication
	off104 int

	svcApp *wireRecordingService

	// dropRequest skips RequestServiceAck frames on the 104 bridge. Join
	// pairing sets dropRequest so the consensus module wait pairs with the
	// service join ack.
	dropRequest bool
}

func newWireHarness(t testing.TB) *wireHarness {
	t.Helper()

	commit := &atomic.Int64{}
	cm := consensus.NewAgent(consensus.AgentConfig{
		ServiceCount: 1,
		Commit:       commit,
	})

	pub104 := aeron.NewTestPublicationForHooksWithConnected(t, true)
	proxy104 := consensus.NewServiceProxy()
	proxy104.SetPublication(pub104)
	cm.SetServiceProxy(proxy104)

	svcApp := &wireRecordingService{}

	return &wireHarness{
		wireServiceSide: newWireServiceSide(t, cm, svcApp),
		cm:              cm,
		pub104:          pub104,
		svcApp:          svcApp,
		dropRequest:     true,
	}
}

// tick runs consensus module DoWork, service DoWork, then both bridges.
// Asserts the log poll bound after.
//
// Order: h.cm.DoWork, h.svc.DoWork, bridge104, bridge105. The bare consensus
// agent stands in for runner.DoWork on the product harness.
func (h *wireHarness) tick(t testing.TB, nowNs int64) {
	t.Helper()
	h.cm.DoWork(nowNs)
	h.svc.DoWork(nowNs)
	h.bridge104(t)
	h.bridge105(t)
	require.LessOrEqual(t, h.svc.LogPosition(), h.cm.CommitPosition())
}

// tickN runs n ticks at baseNs, baseNs+1, ...
func (h *wireHarness) tickN(t testing.TB, baseNs int64, n int) {
	t.Helper()
	for i := range n {
		h.tick(t, baseNs+int64(i))
	}
}

// tickUntil ticks until done returns true or maxTicks is exhausted.
// Returns true when done fires.
func (h *wireHarness) tickUntil(t testing.TB, baseNs int64, maxTicks int, done func() bool) bool {
	t.Helper()
	for i := range maxTicks {
		h.tick(t, baseNs+int64(i))
		if done() {
			return true
		}
	}
	return false
}

// bridgeTerm walks new DATA frames on termBuf into onData, advancing *off.
func bridgeTerm(
	t testing.TB,
	termBuf atomicbuffer.AtomicBuffer,
	off *int,
	onData func(msg []byte),
) {
	t.Helper()
	for {
		frameLength := int(logbuffer.FrameLengthVolatile(termBuf, *off))
		if frameLength <= 0 {
			break
		}
		if logbuffer.FrameType(termBuf, *off) == logbuffer.HdrTypeData {
			wire := termBuf.Bytes()[*off+logbuffer.OffsetFramePayload : *off+frameLength]
			onData(append([]byte(nil), wire...))
		}
		*off += logbuffer.AlignFrameLength(frameLength)
		if *off >= termBuf.Capacity() {
			break
		}
	}
}

// bridgePub walks new DATA frames on pub into onData, advancing *off.
func bridgePub(
	t testing.TB,
	pub *aeron.Publication,
	off *int,
	onData func(msg []byte),
) {
	t.Helper()
	bridgeTerm(t, pub.TermBuffer(0), off, onData)
}

// bridge104 walks new DATA frames on the consensus module service proxy
// publication into the service control queue. When dropRequest is set,
// RequestServiceAck frames are skipped (join-pairing harness arrangement).
func (h *wireHarness) bridge104(t testing.TB) {
	t.Helper()
	bridgePub(t, h.pub104, &h.off104, func(msg []byte) {
		if h.dropRequest {
			_, templateID, _, _, ok := codecs.DecodeMessageHeader(msg, 0)
			if ok && templateID == clusterwire.RequestServiceAckSbeTemplateID {
				return
			}
		}
		h.control.Enqueue(msg)
	})
}

func wireJoinLogFields(logPosition int64) codecs.JoinLogFields {
	return codecs.JoinLogFields{
		LogPosition:    logPosition,
		MaxLogPosition: math.MaxInt64,
		MemberID:       wireMemberID,
		LogSessionID:   wireLogSessionID,
		LogStreamID:    wireLogStreamID,
		IsStartup:      clusterwire.BooleanType_TRUE,
		Role:           codecs.RoleLeader.Code(),
		IsStandby:      clusterwire.BooleanType_FALSE,
		LogChannel:     []byte(wireLogChannel),
	}
}

func encodeWireJoinLog(t testing.TB, fields codecs.JoinLogFields) []byte {
	t.Helper()
	buf := make([]byte, 256+len(fields.LogChannel))
	wireEnd, ok := codecs.EncodeJoinLog(
		buf,
		0,
		uint16(clusterwire.JoinLogSbeSchemaVersion),
		fields,
	)
	require.True(t, ok)
	return buf[:wireEnd]
}

func encodeWireSessionMessage(
	t testing.TB,
	fields codecs.SessionMessageHeaderFields,
	appPayload []byte,
) []byte {
	t.Helper()
	headerLen := codecs.SessionMessageHeaderEncodedLen()
	buf := make([]byte, headerLen+len(appPayload))
	wireEnd, ok := codecs.EncodeSessionMessageHeader(
		buf,
		0,
		clusterwire.SessionMessageHeaderSbeSchemaVersion,
		fields,
	)
	require.True(t, ok)
	require.Equal(t, headerLen, wireEnd)
	copy(buf[headerLen:], appPayload)
	return buf
}

func encodeWireTimerEvent(t testing.TB, fields codecs.TimerEventFields) []byte {
	t.Helper()
	buf := make([]byte, codecs.TimerEventEncodedLen())
	wireEnd, ok := codecs.EncodeTimerEvent(
		buf,
		0,
		clusterwire.TimerEventSbeSchemaVersion,
		fields,
	)
	require.True(t, ok)
	return buf[:wireEnd]
}

// commitTimer writes a timer frame, proposes commit at the new end, returns that position.
func (h *wireHarness) commitTimer(t testing.TB, fields codecs.TimerEventFields) int64 {
	t.Helper()
	h.writeFrame(t, encodeWireTimerEvent(t, fields))
	pos := int64(h.frameOffset)
	h.cm.ProposeMaxCommit(pos)
	return pos
}

// pairJoinWait queues a consensus module RequestServiceAck wait at joinPos for
// the service join ack. The request frame is dropped at bridge104 when
// dropRequest is set.
func (h *wireHarness) pairJoinWait(t testing.TB, joinPos int64) {
	t.Helper()
	require.NoError(t, h.cm.RequestServiceAck(joinPos))
	require.Equal(t, 1, h.cm.PendingServiceAckWaits())
	require.Equal(t, joinPos, h.cm.ExpectedAckPosition())
}

// runJoinRound delivers JoinLog, pairs the consensus module wait, and ticks
// until the join ack completes the wait. Clears dropRequest after success so
// later RequestServiceAck frames bridge to the service.
func (h *wireHarness) runJoinRound(t testing.TB, joinPos int64) {
	t.Helper()
	h.dropRequest = true
	h.deliverJoinLog(t, joinPos)
	h.pairJoinWait(t, joinPos)

	// Bridge drops the request on the first tick after pairJoinWait.
	// Service binds join same tick it polls JoinLog; drain lags one tick.
	// Consensus module consumes the bridged ack on the following tick.
	ok := h.tickUntil(t, 1, 8, func() bool {
		return h.cm.ExpectedServiceAckID() == 1 && h.cm.PendingServiceAckWaits() == 0
	})
	if ok {
		// Later requests must reach the service.
		h.dropRequest = false
		return
	}
	t.Fatalf(
		"join round did not complete: expectedAckID=%d waits=%d bound=%v failure=%v svcFailure=%v",
		h.cm.ExpectedServiceAckID(),
		h.cm.PendingServiceAckWaits(),
		h.svc.IsLogBound(),
		h.cm.ServiceAckFailure(),
		h.svc.ServiceFailure(),
	)
}

func TestServiceAckWire_JoinRoundTrip(t *testing.T) {
	h := newWireHarness(t)
	const joinPos int64 = 0

	h.runJoinRound(t, joinPos)

	require.True(t, h.svc.IsLogBound())
	require.Equal(t, joinPos, h.svc.LogPosition())
	require.Equal(t, int64(1), h.cm.ExpectedServiceAckID())
	require.Equal(t, 0, h.cm.PendingServiceAckWaits())
	require.Equal(t, 0, h.cm.ServiceAckQueueDepth(0))
	require.NoError(t, h.cm.ServiceAckFailure())
	require.NoError(t, h.svc.ServiceFailure())
	require.Equal(t, int64(1), h.svc.NextAckID())
}

func TestServiceAckWire_RequestRoundTrip(t *testing.T) {
	h := newWireHarness(t)
	h.runJoinRound(t, 0)
	require.Equal(t, int64(1), h.cm.ExpectedServiceAckID())

	// One committed timer frame; request at its end position.
	reqPos := h.commitTimer(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    1,
		Timestamp:        50,
	})
	require.Equal(t, reqPos, h.cm.CommitPosition())

	require.NoError(t, h.cm.RequestServiceAck(reqPos))
	require.Equal(t, 1, h.cm.PendingServiceAckWaits())
	require.Equal(t, reqPos, h.cm.ExpectedAckPosition())

	// Bridge delivers request one tick after publish (bridges run after DoWork).
	// Service then polls, consumes to reqPos, and drains on a later tick.
	ok := h.tickUntil(t, 100, 10, func() bool {
		return h.cm.ExpectedServiceAckID() == 2 && h.cm.PendingServiceAckWaits() == 0
	})
	if ok {
		require.Equal(t, reqPos, h.svc.LogPosition())
		require.Equal(t, int64(2), h.svc.NextAckID())
		require.Equal(t, 0, h.cm.ServiceAckQueueDepth(0))
		require.NoError(t, h.cm.ServiceAckFailure())
		require.NoError(t, h.svc.ServiceFailure())
		return
	}
	t.Fatalf(
		"request round did not complete: expectedAckID=%d waits=%d logPos=%d nextAck=%d failure=%v",
		h.cm.ExpectedServiceAckID(),
		h.cm.PendingServiceAckWaits(),
		h.svc.LogPosition(),
		h.svc.NextAckID(),
		h.cm.ServiceAckFailure(),
	)
}

func TestServiceAckWire_NoPerMessageAck(t *testing.T) {
	h := newWireHarness(t)
	h.runJoinRound(t, 0)
	require.Equal(t, int64(1), h.cm.ExpectedServiceAckID())
	require.Equal(t, 0, h.cm.PendingServiceAckWaits())

	// Session messages between rounds must not publish on 105 or queue
	// consensus module acks.
	posBefore105 := h.pub105.Position()
	depthBefore := h.cm.ServiceAckQueueDepth(0)

	payload := encodeWireSessionMessage(
		t,
		codecs.SessionMessageHeaderFields{
			LeadershipTermID: 1,
			ClusterSessionID: 1,
			Timestamp:        9,
		},
		[]byte("body"),
	)
	h.writeFrame(t, payload)
	h.cm.ProposeMaxCommit(int64(h.frameOffset))

	h.tickN(t, 200, 4)

	require.Equal(t, int64(h.frameOffset), h.svc.LogPosition())
	// SessionMessage ran; no ServiceAck on 105.
	require.Equal(t, 1, h.svcApp.messages)
	require.Equal(t, posBefore105, h.pub105.Position())
	require.Equal(t, depthBefore, h.cm.ServiceAckQueueDepth(0))
	require.Equal(t, 0, h.cm.PendingServiceAckWaits())
	require.Equal(t, int64(1), h.cm.ExpectedServiceAckID())
	require.Equal(t, int64(1), h.svc.NextAckID())
	require.NoError(t, h.cm.ServiceAckFailure())
}

func TestServiceAckWire_BoundAssert(t *testing.T) {
	// Bound assert runs after every tick inside tick(). This row drives a full
	// join then request path and relies on that check.
	h := newWireHarness(t)
	h.runJoinRound(t, 0)

	reqPos := h.commitTimer(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    2,
		Timestamp:        11,
	})
	require.NoError(t, h.cm.RequestServiceAck(reqPos))

	h.tickUntil(t, 300, 10, func() bool {
		return h.cm.ExpectedServiceAckID() == 2 && h.cm.PendingServiceAckWaits() == 0
	})
	require.Equal(t, int64(2), h.cm.ExpectedServiceAckID())
	require.Equal(t, 0, h.cm.PendingServiceAckWaits())
	require.Equal(t, reqPos, h.svc.LogPosition())
	require.Equal(t, int64(2), h.svc.NextAckID())
}

func TestServiceAckWire_PairingViolation(t *testing.T) {
	// Deliver the join-pairing RequestServiceAck as well. Service emits join
	// ack then request ack for the same position. The consensus module holds
	// one wait from RequestServiceAck. First ack completes the wait; leftover
	// request ack sits as the first queued entry. A later wait at a different
	// position mismatches and marks ServiceAckFailure.
	h := newWireHarness(t)
	h.dropRequest = false

	const joinPos int64 = 0
	h.deliverJoinLog(t, joinPos)
	h.pairJoinWait(t, joinPos)

	// Complete the first wait (join ack) while the request ack is also emitted.
	// Single-ack tick: ExpectedServiceAckID advances from 0 to 1 only.
	h.tickUntil(t, 400, 10, func() bool {
		return h.cm.ExpectedServiceAckID() == 1 && h.cm.PendingServiceAckWaits() == 0
	})
	require.Equal(t, int64(1), h.cm.ExpectedServiceAckID())
	require.Equal(t, 0, h.cm.PendingServiceAckWaits())
	require.NoError(t, h.cm.ServiceAckFailure())
	// Both join and request acks published on the service side.
	require.Equal(t, int64(2), h.svc.NextAckID())

	// Leftover request ack (id 1 at joinPos) remains after the single wait
	// completed (bridged on residual ticks). Advance log and queue a wait at
	// a new position: wait-site hasReached sees first queued entry (1, joinPos)
	// vs reqPos.
	reqPos := h.commitTimer(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    3,
		Timestamp:        22,
	})

	// Drain residual bridges. Leftover first queued entry stays (id 1 at joinPos).
	h.tickN(t, 420, 4)
	require.Equal(t, 0, h.cm.PendingServiceAckWaits())
	require.Equal(t, 1, h.cm.ServiceAckQueueDepth(0))
	require.Equal(t, int64(1), h.cm.ExpectedServiceAckID())
	require.NoError(t, h.cm.ServiceAckFailure())

	require.NoError(t, h.cm.RequestServiceAck(reqPos))
	// Wait work marks out-of-sequence: first queued entry is (1, joinPos), wait
	// is reqPos.
	h.tickUntil(t, 430, 4, func() bool {
		return h.cm.ServiceAckFailure() != nil
	})
	require.ErrorIs(t, h.cm.ServiceAckFailure(), consensus.ErrServiceAckOutOfSequence)
	require.Equal(t, 1, h.cm.PendingServiceAckWaits())
	require.Equal(t, int64(1), h.cm.ExpectedServiceAckID())
	// Residual first queued entry not consumed by the mismatched wait.
	require.Equal(t, 1, h.cm.ServiceAckQueueDepth(0))
}
