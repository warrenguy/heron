//go:build testhooks

package service

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// hookTermID matches NewTestImageForHooks partition-0 initial term id.
const hookTermID int32 = 7

// recordingSink records sink seat calls for adapter unit tests.
type recordingSink struct {
	sessionMessages int
	sessionOpens    int
	sessionCloses   int
	timerEvents     int
	nltEvents       int
	dispatchFails   int

	lastLogPosition int64
	lastSessionID   int64
	lastTimestamp   int64
	lastPayload     []byte
	lastOpen        codecs.SessionOpenEventFields
	lastClose       codecs.SessionCloseEventFields
	lastTimer       codecs.TimerEventFields
	lastNLT         codecs.NewLeadershipTermEventFields
	lastFailure     error

	// action is returned from every seat when actionSet is true.
	// ControlledAbort is iota 0; do not treat zero as "unset".
	action    aeron.ControlledAction
	actionSet bool
}

func (s *recordingSink) handleSessionMessage(
	logPosition, sessionID, timestamp int64,
	payload []byte,
) aeron.ControlledAction {
	s.sessionMessages++
	s.lastLogPosition = logPosition
	s.lastSessionID = sessionID
	s.lastTimestamp = timestamp
	s.lastPayload = append([]byte(nil), payload...)
	return s.seatAction()
}

func (s *recordingSink) handleSessionOpen(
	logPosition int64,
	fields codecs.SessionOpenEventFields,
) aeron.ControlledAction {
	s.sessionOpens++
	s.lastLogPosition = logPosition
	s.lastOpen = fields
	return s.seatAction()
}

func (s *recordingSink) handleSessionClose(
	logPosition int64,
	fields codecs.SessionCloseEventFields,
) aeron.ControlledAction {
	s.sessionCloses++
	s.lastLogPosition = logPosition
	s.lastClose = fields
	return s.seatAction()
}

func (s *recordingSink) handleTimerEvent(
	logPosition int64,
	fields codecs.TimerEventFields,
) aeron.ControlledAction {
	s.timerEvents++
	s.lastLogPosition = logPosition
	s.lastTimer = fields
	return s.seatAction()
}

func (s *recordingSink) handleNewLeadershipTermEvent(
	logPosition int64,
	fields codecs.NewLeadershipTermEventFields,
) aeron.ControlledAction {
	s.nltEvents++
	s.lastLogPosition = logPosition
	s.lastNLT = fields
	return s.seatAction()
}

func (s *recordingSink) handleDispatchFailure(err error) aeron.ControlledAction {
	s.dispatchFails++
	s.lastFailure = err
	// Fail closed: abort so the failing frame stays unconsumed.
	return aeron.ControlledAbort
}

func (s *recordingSink) seatAction() aeron.ControlledAction {
	if !s.actionSet {
		return aeron.ControlledContinue
	}
	return s.action
}

// setAction forces success-path handle methods through seatAction only.
// handleDispatchFailure always returns ControlledAbort. ControlledAbort is a
// valid set value (ControlledAbort is iota 0; zero is not "unset").
func (s *recordingSink) setAction(action aeron.ControlledAction) {
	s.action = action
	s.actionSet = true
}

func requireSinkIdle(tb testing.TB, s *recordingSink) {
	tb.Helper()
	require.Equal(tb, 0, s.sessionMessages)
	require.Equal(tb, 0, s.sessionOpens)
	require.Equal(tb, 0, s.sessionCloses)
	require.Equal(tb, 0, s.timerEvents)
	require.Equal(tb, 0, s.nltEvents)
	require.Equal(tb, 0, s.dispatchFails)
}

func boundAdapter(tb testing.TB) (*BoundedLogAdapter, *aeron.Image, *recordingSink) {
	tb.Helper()
	return boundAdapterWithLimit(tb, 0)
}

func boundAdapterWithLimit(
	tb testing.TB,
	fragmentLimit int,
) (*BoundedLogAdapter, *aeron.Image, *recordingSink) {
	tb.Helper()
	sub := aeron.NewTestSubscriptionForHooks(tb)
	image := aeron.NewTestImageForHooks(tb, sub, 0)
	sink := &recordingSink{}
	a := NewBoundedLogAdapter(sink, fragmentLimit)
	a.SetImage(image)
	return a, image, sink
}

func writeUnfragmentedFrame(
	tb testing.TB,
	image *aeron.Image,
	frameOffset int,
	payload []byte,
) {
	tb.Helper()
	aeron.WriteCommittedDataFrameForHooks(
		aeron.ImageTermBufferForHooks(image),
		frameOffset,
		image.SessionID(),
		image.StreamID(),
		hookTermID,
		int32(frameOffset),
		logbuffer.Unfragmented,
		payload,
	)
}

func writeFrameChain(
	tb testing.TB,
	image *aeron.Image,
	startOffset int,
	frameCount int,
	payload []byte,
) {
	tb.Helper()
	frames := make([]aeron.FragmentedFrameForTest, frameCount)
	for i := range frames {
		frames[i] = aeron.FragmentedFrameForTest{
			Flags:   logbuffer.Unfragmented,
			Payload: payload,
		}
	}
	aeron.WriteCommittedDataFramesForStreamForTest(
		aeron.ImageTermBufferForHooks(image),
		startOffset,
		image.SessionID(),
		image.StreamID(),
		hookTermID,
		frames,
	)
}

func encodeMessageHeader(
	tb testing.TB,
	blockLength, templateID, schemaID, version uint16,
) []byte {
	tb.Helper()
	buf := make([]byte, codecs.MessageHeaderEncodedLen())
	var hdr clusterwire.MessageHeader
	hdr.Wrap(buf, 0, 0, uint64(len(buf)))
	hdr.SetBlockLength(blockLength)
	hdr.SetTemplateId(templateID)
	hdr.SetSchemaId(schemaID)
	hdr.SetVersion(version)
	return buf
}

func encodeSessionMessage(
	tb testing.TB,
	fields codecs.SessionMessageHeaderFields,
	appPayload []byte,
) []byte {
	tb.Helper()
	headerLen := codecs.SessionMessageHeaderEncodedLen()
	buf := make([]byte, headerLen+len(appPayload))
	wireEnd, ok := codecs.EncodeSessionMessageHeader(
		buf,
		0,
		clusterwire.SessionMessageHeaderSbeSchemaVersion,
		fields,
	)
	require.True(tb, ok)
	require.Equal(tb, headerLen, wireEnd)
	copy(buf[headerLen:], appPayload)
	return buf
}

func encodeTimerEvent(tb testing.TB, fields codecs.TimerEventFields) []byte {
	tb.Helper()
	buf := make([]byte, codecs.TimerEventEncodedLen())
	wireEnd, ok := codecs.EncodeTimerEvent(
		buf,
		0,
		clusterwire.TimerEventSbeSchemaVersion,
		fields,
	)
	require.True(tb, ok)
	return buf[:wireEnd]
}

func encodeSessionOpen(tb testing.TB, fields codecs.SessionOpenEventFields) []byte {
	tb.Helper()
	wireLen, ok := codecs.ClusterWireMessageLen(
		clusterwire.SessionOpenEventSbeBlockLength,
		len(fields.ResponseChannel),
		len(fields.EncodedPrincipal),
	)
	require.True(tb, ok)
	buf := make([]byte, wireLen)
	wireEnd, ok := codecs.EncodeSessionOpenEvent(
		buf,
		0,
		clusterwire.SessionOpenEventSbeSchemaVersion,
		fields,
	)
	require.True(tb, ok)
	return buf[:wireEnd]
}

func encodeSessionClose(tb testing.TB, fields codecs.SessionCloseEventFields) []byte {
	tb.Helper()
	buf := make([]byte, codecs.SessionCloseEventEncodedLen())
	wireEnd, ok := codecs.EncodeSessionCloseEvent(
		buf,
		0,
		clusterwire.SessionCloseEventSbeSchemaVersion,
		fields,
	)
	require.True(tb, ok)
	return buf[:wireEnd]
}

func encodeNLT(tb testing.TB, fields codecs.NewLeadershipTermEventFields) []byte {
	tb.Helper()
	buf := make([]byte, codecs.NewLeadershipTermEventEncodedLen())
	wireEnd, ok := codecs.EncodeNewLeadershipTermEvent(
		buf,
		0,
		clusterwire.NewLeadershipTermEventSbeSchemaVersion,
		fields,
	)
	require.True(tb, ok)
	return buf[:wireEnd]
}

func encodeServiceAck(tb testing.TB) []byte {
	tb.Helper()
	buf := make([]byte, codecs.ServiceAckEncodedLen())
	wireEnd, ok := codecs.EncodeServiceAck(
		buf,
		0,
		clusterwire.ServiceAckSbeSchemaVersion,
		codecs.ServiceAckFields{
			LogPosition: 1,
			Timestamp:   2,
			AckID:       3,
			RelevantID:  4,
			ServiceID:   0,
		},
	)
	require.True(tb, ok)
	return buf[:wireEnd]
}

func writeAndPollOne(
	tb testing.TB,
	a *BoundedLogAdapter,
	image *aeron.Image,
	payload []byte,
) int64 {
	tb.Helper()
	span := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	writeUnfragmentedFrame(tb, image, 0, payload)
	require.Equal(tb, 1, a.Poll(span+32))
	require.Equal(tb, span, a.Position())
	return span
}

// writeAndPollAbort writes one unfragmented frame and Polls past it. Asserts
// ControlledAbort: Poll returns 0 and position freezes at the join position.
func writeAndPollAbort(
	tb testing.TB,
	a *BoundedLogAdapter,
	image *aeron.Image,
	payload []byte,
) (join, span int64) {
	tb.Helper()
	span = int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	writeUnfragmentedFrame(tb, image, 0, payload)
	join = image.Position()
	require.Equal(tb, 0, a.Poll(span+32))
	require.Equal(tb, join, a.Position())
	return join, span
}

// writeBeginEndTrain writes whole as a BEGIN/END fragment pair at offset 0.
//
// Returns the BEGIN frame span and the total aligned train span.
func writeBeginEndTrain(
	tb testing.TB,
	image *aeron.Image,
	whole []byte,
) (beginSpan int, totalSpan int64) {
	tb.Helper()
	mid := len(whole) / 2
	require.Positive(tb, mid)
	require.Less(tb, mid, len(whole))
	begin := whole[:mid]
	end := whole[mid:]
	frames := []aeron.FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: begin},
		{Flags: logbuffer.EndFlag, Payload: end},
	}
	aeron.WriteCommittedDataFramesForStreamForTest(
		aeron.ImageTermBufferForHooks(image),
		0,
		image.SessionID(),
		image.StreamID(),
		hookTermID,
		frames,
	)
	beginSpan = logbuffer.AlignFragmentChunkLength(len(begin))
	endSpan := logbuffer.AlignFragmentChunkLength(len(end))
	totalSpan = int64(beginSpan + endSpan)
	return beginSpan, totalSpan
}

// encodeTrivialSessionMessage builds a SessionMessageHeader with empty app
// payload for multi-frame Poll bound tests.
func encodeTrivialSessionMessage(tb testing.TB) []byte {
	tb.Helper()
	return encodeSessionMessage(tb, codecs.SessionMessageHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: 1,
		Timestamp:        1,
	}, nil)
}

func TestNewBoundedLogAdapter_defaults(t *testing.T) {
	sink := &recordingSink{}
	a := NewBoundedLogAdapter(sink, 0)
	require.Equal(t, DefaultLogFragmentLimit, a.fragmentLimit)
	require.Equal(t, int64(math.MaxInt64), a.MaxLogPosition())
	require.Nil(t, a.Image())
	require.Equal(t, int64(0), a.Position())

	aNeg := NewBoundedLogAdapter(sink, -1)
	require.Equal(t, DefaultLogFragmentLimit, aNeg.fragmentLimit)

	a7 := NewBoundedLogAdapter(sink, 7)
	require.Equal(t, 7, a7.fragmentLimit)
}

func TestNewBoundedLogAdapter_nilSinkPanics(t *testing.T) {
	require.Panics(t, func() {
		NewBoundedLogAdapter(nil, 0)
	})
}

func TestBoundedLogAdapter_nilImagePoll(t *testing.T) {
	a := NewBoundedLogAdapter(&recordingSink{}, 0)
	require.Equal(t, 0, a.Poll(1<<20))
}

func TestBoundedLogAdapter_nilReceiver(t *testing.T) {
	var a *BoundedLogAdapter
	require.Equal(t, int64(0), a.Position())
	require.Equal(t, int64(0), a.MaxLogPosition())
	require.Nil(t, a.Image())
	require.Equal(t, 0, a.Poll(1<<20))
	require.NotPanics(t, func() {
		a.SetImage(nil)
		a.SetMaxLogPosition(1)
	})
}

func TestBoundedLogAdapter_SetImage_cachePosition(t *testing.T) {
	a, image, _ := boundAdapter(t)

	const advanced int64 = 128
	aeron.ImageSetSubscriberPositionForHooks(image, advanced)
	require.Equal(t, advanced, a.Position())

	a.SetImage(nil)
	require.Nil(t, a.Image())
	require.Equal(t, advanced, a.Position())
	require.Equal(t, 0, a.Poll(1<<20))

	sub2 := aeron.NewTestSubscriptionForHooks(t)
	image2 := aeron.NewTestImageForHooks(t, sub2, 0)
	a.SetImage(image2)
	require.Same(t, image2, a.Image())
	require.Equal(t, image2.Position(), a.Position())
}

func TestBoundedLogAdapter_freshAdapter_maxPositionDefault(t *testing.T) {
	// Fresh adapter with no SetMaxLogPosition delivers under the commit bound
	// alone (math.MaxInt64 constructor default is not a silent zero cap).
	a, image, sink := boundAdapter(t)
	writeAndPollOne(t, a, image, encodeTrivialSessionMessage(t))
	require.Equal(t, 1, sink.sessionMessages)
	require.Equal(t, int64(math.MaxInt64), a.MaxLogPosition())
}

func TestBoundedLogAdapter_dispatch_SessionMessage(t *testing.T) {
	a, image, sink := boundAdapter(t)

	const (
		leadershipTermID int64 = 11
		clusterSessionID int64 = 22
		timestamp        int64 = 33
	)
	appPayload := []byte("app-body")
	payload := encodeSessionMessage(t, codecs.SessionMessageHeaderFields{
		LeadershipTermID: leadershipTermID,
		ClusterSessionID: clusterSessionID,
		Timestamp:        timestamp,
	}, appPayload)
	span := writeAndPollOne(t, a, image, payload)

	require.Equal(t, 1, sink.sessionMessages)
	require.Equal(t, 0, sink.dispatchFails)
	require.Equal(t, clusterSessionID, sink.lastSessionID)
	require.Equal(t, timestamp, sink.lastTimestamp)
	require.Equal(t, appPayload, sink.lastPayload)
	require.Equal(t, span, sink.lastLogPosition)
}

func TestBoundedLogAdapter_dispatch_SessionMessage_emptyPayload(t *testing.T) {
	a, image, sink := boundAdapter(t)
	payload := encodeSessionMessage(t, codecs.SessionMessageHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		Timestamp:        3,
	}, nil)
	writeAndPollOne(t, a, image, payload)
	require.Equal(t, 1, sink.sessionMessages)
	require.Empty(t, sink.lastPayload)
}

func TestBoundedLogAdapter_dispatch_TimerEvent(t *testing.T) {
	a, image, sink := boundAdapter(t)
	fields := codecs.TimerEventFields{
		LeadershipTermID: 7,
		CorrelationID:    8,
		Timestamp:        9,
	}
	span := writeAndPollOne(t, a, image, encodeTimerEvent(t, fields))

	require.Equal(t, 1, sink.timerEvents)
	require.Equal(t, fields, sink.lastTimer)
	require.Equal(t, span, sink.lastLogPosition)
}

func TestBoundedLogAdapter_dispatch_SessionOpenEvent(t *testing.T) {
	a, image, sink := boundAdapter(t)
	fields := codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		CorrelationID:    2,
		ClusterSessionID: 3,
		Timestamp:        4,
		ResponseStreamID: 102,
		ResponseChannel:  []byte("aeron:ipc"),
		EncodedPrincipal: []byte{0xAB, 0xCD},
	}
	span := writeAndPollOne(t, a, image, encodeSessionOpen(t, fields))

	require.Equal(t, 1, sink.sessionOpens)
	require.Equal(t, fields, sink.lastOpen)
	require.Equal(t, span, sink.lastLogPosition)
}

func TestBoundedLogAdapter_dispatch_SessionCloseEvent(t *testing.T) {
	a, image, sink := boundAdapter(t)
	fields := codecs.SessionCloseEventFields{
		LeadershipTermID: 5,
		ClusterSessionID: 6,
		Timestamp:        7,
		CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
	}
	span := writeAndPollOne(t, a, image, encodeSessionClose(t, fields))

	require.Equal(t, 1, sink.sessionCloses)
	require.Equal(t, fields, sink.lastClose)
	require.Equal(t, span, sink.lastLogPosition)
}

func TestBoundedLogAdapter_dispatch_NewLeadershipTermEvent(t *testing.T) {
	a, image, sink := boundAdapter(t)
	fields := codecs.NewLeadershipTermEventFields{
		LeadershipTermID:    7,
		LogPosition:         128,
		Timestamp:           999,
		TermBaseLogPosition: 64,
		LeaderMemberID:      1,
		LogSessionID:        3,
		TimeUnit:            clusterwire.ClusterTimeUnit_NANOS,
		AppVersion:          42,
	}
	span := writeAndPollOne(t, a, image, encodeNLT(t, fields))

	require.Equal(t, 1, sink.nltEvents)
	require.Equal(t, fields, sink.lastNLT)
	require.Equal(t, span, sink.lastLogPosition)
}

func TestBoundedLogAdapter_unknownTemplate_skips(t *testing.T) {
	a, image, sink := boundAdapter(t)
	// ServiceAck (33) is a known cluster template. Dispatch hits switch
	// default Continue; the adapter does not own ServiceAck. This path is not
	// classifyLayoutFailure for an unknown template.
	payload := encodeServiceAck(t)
	writeAndPollOne(t, a, image, payload)
	requireSinkIdle(t, sink)
}

func TestBoundedLogAdapter_unknownTemplateID_skips(t *testing.T) {
	a, image, sink := boundAdapter(t)
	// Template id 99 is absent from ClusterRootBlockLengthBounds. Layout
	// parse fails. classifyLayoutFailure returns Continue when the template
	// is unknown.
	payload := encodeMessageHeader(
		t,
		0,
		99,
		clusterwire.MessageHeaderSbeSchemaID,
		0,
	)
	writeAndPollOne(t, a, image, payload)
	requireSinkIdle(t, sink)
}

func TestBoundedLogAdapter_SessionOpen_varOverCap_aborts(t *testing.T) {
	a, image, sink := boundAdapter(t)

	// Encode a valid SessionOpen, then set the first var length prefix to
	// ClusterDecodeMaxVarLen+1. Codec unit tests pad the full claimed payload
	// so ReadVarSlice hits maxLen. TermMinLength images cannot hold that pad.
	// An over-cap prefix still fails TryDecode on bounds, so dispatch aborts
	// and never calls handleSessionOpen. FieldsFromLayout alone would not fail
	// closed.
	fields := codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		CorrelationID:    2,
		ClusterSessionID: 3,
		Timestamp:        4,
		ResponseStreamID: 102,
		ResponseChannel:  []byte("aeron:ipc"),
		EncodedPrincipal: []byte{0xAB, 0xCD},
	}
	buf := encodeSessionOpen(t, fields)
	headerAndBlock := codecs.MessageHeaderEncodedLen() +
		int(clusterwire.SessionOpenEventSbeBlockLength)
	require.GreaterOrEqual(t, len(buf), headerAndBlock+codecs.VarLengthPrefixSize)
	binary.LittleEndian.PutUint32(
		buf[headerAndBlock:],
		uint32(codecs.ClusterDecodeMaxVarLen+1),
	)

	writeAndPollAbort(t, a, image, buf)
	require.Equal(t, 1, sink.dispatchFails)
	require.ErrorIs(t, sink.lastFailure, errLogDispatchDecode)
	require.Equal(t, 0, sink.sessionOpens)
}

func TestBoundedLogAdapter_wrongSchema_aborts(t *testing.T) {
	a, image, sink := boundAdapter(t)
	// Schema 112 is not cluster schema 111.
	payload := encodeMessageHeader(t, 0, 1, 112, 0)
	join, span := writeAndPollAbort(t, a, image, payload)
	require.Equal(t, 1, sink.dispatchFails)
	require.ErrorIs(t, sink.lastFailure, errLogDispatchSchema)

	// Repeat poll redelivers the same frame.
	require.Equal(t, 0, a.Poll(span+32))
	require.Equal(t, join, a.Position())
	require.Equal(t, 2, sink.dispatchFails)
}

func TestBoundedLogAdapter_undecodable_aborts(t *testing.T) {
	a, image, sink := boundAdapter(t)
	// MessageHeader only: production blockLength claims a full SessionMessage
	// body not present in the fragment.
	payload := encodeMessageHeader(
		t,
		clusterwire.SessionMessageHeaderSbeBlockLength,
		clusterwire.SessionMessageHeaderSbeTemplateID,
		clusterwire.MessageHeaderSbeSchemaID,
		clusterwire.SessionMessageHeaderSbeSchemaVersion,
	)
	writeAndPollAbort(t, a, image, payload)
	require.Equal(t, 1, sink.dispatchFails)
	require.ErrorIs(t, sink.lastFailure, errLogDispatchDecode)
}

func TestBoundedLogAdapter_shortHeader_aborts(t *testing.T) {
	a, image, sink := boundAdapter(t)
	payload := []byte{0x01, 0x02, 0x03, 0x04}
	require.Less(t, len(payload), codecs.MessageHeaderEncodedLen())
	writeAndPollAbort(t, a, image, payload)
	require.Equal(t, 1, sink.dispatchFails)
	require.ErrorIs(t, sink.lastFailure, errLogDispatchHeader)
}

func TestBoundedLogAdapter_Poll_boundBeforeFirstFrame(t *testing.T) {
	a, image, sink := boundAdapter(t)
	join := image.Position()
	writeUnfragmentedFrame(t, image, 0, []byte("frame"))

	// Bound at join: frame start is at limit; not delivered.
	require.Equal(t, 0, a.Poll(join))
	require.Equal(t, join, a.Position())
	requireSinkIdle(t, sink)
}

func TestBoundedLogAdapter_Poll_twoFrames_midBound(t *testing.T) {
	a, image, sink := boundAdapter(t)
	payload := encodeTrivialSessionMessage(t)
	span := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	writeFrameChain(t, image, 0, 2, payload)

	// Bound at start of B: only A delivered; Position stops at B start.
	require.Equal(t, 1, a.Poll(span))
	require.Equal(t, span, a.Position())
	require.Equal(t, 1, sink.sessionMessages)

	// Raise bound past B: second Poll delivers B.
	require.Equal(t, 1, a.Poll(2*span))
	require.Equal(t, 2*span, a.Position())
	require.Equal(t, 2, sink.sessionMessages)
}

func TestBoundedLogAdapter_Poll_maxLogPositionCap(t *testing.T) {
	a, image, sink := boundAdapter(t)
	payload := encodeTrivialSessionMessage(t)
	span := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	writeFrameChain(t, image, 0, 2, payload)

	// Cap between frames; bound generous. Only first delivers.
	a.SetMaxLogPosition(span)
	require.Equal(t, 1, a.Poll(1<<20))
	require.Equal(t, span, a.Position())
	require.Equal(t, 1, sink.sessionMessages)

	// Cap still blocks the second frame.
	require.Equal(t, 0, a.Poll(1<<20))
	require.Equal(t, 1, sink.sessionMessages)
}

func TestBoundedLogAdapter_Poll_fragmentLimit(t *testing.T) {
	a, image, sink := boundAdapterWithLimit(t, 1)
	payload := encodeTrivialSessionMessage(t)
	span := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	const totalFrames = 5
	writeFrameChain(t, image, 0, totalFrames, payload)

	bound := span * totalFrames
	require.Equal(t, 1, a.Poll(bound))
	require.Equal(t, span, a.Position())
	require.Equal(t, 1, sink.sessionMessages)

	require.Equal(t, 1, a.Poll(bound))
	require.Equal(t, 2*span, a.Position())
	require.Equal(t, 2, sink.sessionMessages)
}

func TestBoundedLogAdapter_fragmented_assemblesAndDispatches(t *testing.T) {
	a, image, sink := boundAdapter(t)

	fields := codecs.SessionMessageHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: 42,
		Timestamp:        99,
	}
	appPayload := []byte("frag-payload")
	whole := encodeSessionMessage(t, fields, appPayload)
	_, totalSpan := writeBeginEndTrain(t, image, whole)

	require.Equal(t, 2, a.Poll(totalSpan+32))
	require.Equal(t, totalSpan, a.Position())
	require.Equal(t, 1, sink.sessionMessages)
	require.Equal(t, fields.ClusterSessionID, sink.lastSessionID)
	require.Equal(t, fields.Timestamp, sink.lastTimestamp)
	require.Equal(t, appPayload, sink.lastPayload)
	// Completed-header position is the end of the END frame.
	require.Equal(t, totalSpan, sink.lastLogPosition)
}

func TestBoundedLogAdapter_fragmented_abortRedelivers(t *testing.T) {
	a, image, sink := boundAdapter(t)
	// Sink aborts on first delivery; next poll redelivers (assembler abort-rewind).
	sink.setAction(aeron.ControlledAbort)

	fields := codecs.SessionMessageHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: 7,
		Timestamp:        8,
	}
	whole := encodeSessionMessage(t, fields, []byte("x"))
	beginSpan, totalSpan := writeBeginEndTrain(t, image, whole)
	join := image.Position()

	// First poll: BEGIN continues (counted, no delegate), END aborts via sink
	// (not counted). Position freezes before the END frame is consumed.
	require.Equal(t, 1, a.Poll(totalSpan+32))
	require.Equal(t, 1, sink.sessionMessages)
	// After abort on END, subscriber position sits at END frame start
	// (BEGIN already consumed).
	require.Equal(t, join+int64(beginSpan), a.Position())

	// Continue on redelivery.
	sink.setAction(aeron.ControlledContinue)
	require.Equal(t, 1, a.Poll(totalSpan+32))
	require.Equal(t, 2, sink.sessionMessages)
	require.Equal(t, totalSpan, a.Position())
	require.Equal(t, fields.ClusterSessionID, sink.lastSessionID)
}
