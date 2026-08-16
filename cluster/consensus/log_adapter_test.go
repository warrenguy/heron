//go:build testhooks

package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// logAdapterHookTermID matches NewTestImageForHooks partition-0 initial term id.
const logAdapterHookTermID int32 = 7

// logAdapterHandlerSpy records handler calls for decode tests.
type logAdapterHandlerSpy struct {
	sessionMessages int
	nltEvents       int

	lastClusterSessionID int64
	lastTimestamp        int64

	lastLeadershipTermID    int64
	lastLogPosition         int64
	lastNLTTimestamp        int64
	lastTermBaseLogPosition int64
	lastTimeUnit            clusterwire.ClusterTimeUnit
	lastAppVersion          int32
}

func (s *logAdapterHandlerSpy) SessionMessage(clusterSessionID, timestamp int64) {
	s.sessionMessages++
	s.lastClusterSessionID = clusterSessionID
	s.lastTimestamp = timestamp
}

func (s *logAdapterHandlerSpy) NewLeadershipTermEvent(
	leadershipTermID int64,
	logPosition int64,
	timestamp int64,
	termBaseLogPosition int64,
	timeUnit clusterwire.ClusterTimeUnit,
	appVersion int32,
) {
	s.nltEvents++
	s.lastLeadershipTermID = leadershipTermID
	s.lastLogPosition = logPosition
	s.lastNLTTimestamp = timestamp
	s.lastTermBaseLogPosition = termBaseLogPosition
	s.lastTimeUnit = timeUnit
	s.lastAppVersion = appVersion
}

func requireHandlerIdle(tb testing.TB, spy *logAdapterHandlerSpy) {
	tb.Helper()
	require.Equal(tb, 0, spy.sessionMessages)
	require.Equal(tb, 0, spy.nltEvents)
}

// encodeLogAdapterSessionMessagePayload builds SessionMessageHeader wire bytes.
func encodeLogAdapterSessionMessagePayload(
	tb testing.TB,
	fields codecs.SessionMessageHeaderFields,
) []byte {
	tb.Helper()
	buf := make([]byte, codecs.SessionMessageHeaderEncodedLen())
	wireEnd, ok := codecs.EncodeSessionMessageHeader(
		buf,
		0,
		clusterwire.SessionMessageHeaderSbeSchemaVersion,
		fields,
	)
	require.True(tb, ok)
	return buf[:wireEnd]
}

// encodeLogAdapterNLTPayload builds NewLeadershipTermEvent wire bytes.
func encodeLogAdapterNLTPayload(
	tb testing.TB,
	fields codecs.NewLeadershipTermEventFields,
) []byte {
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

// encodeLogAdapterMessageHeader writes an SBE MessageHeader only (no body).
func encodeLogAdapterMessageHeader(
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

func boundLogAdapterWithLimit(tb testing.TB, fragmentLimit int) (*LogAdapter, *aeron.Image) {
	tb.Helper()
	sub := aeron.NewTestSubscriptionForHooks(tb)
	image := aeron.NewTestImageForHooks(tb, sub, 0)
	a := NewLogAdapter(fragmentLimit)
	a.SetImage(image)
	return a, image
}

func boundLogAdapter(tb testing.TB) (*LogAdapter, *aeron.Image) {
	return boundLogAdapterWithLimit(tb, 0)
}

func boundLogAdapterWithHandler(tb testing.TB) (*LogAdapter, *aeron.Image, *logAdapterHandlerSpy) {
	tb.Helper()
	a, image := boundLogAdapter(tb)
	spy := &logAdapterHandlerSpy{}
	a.SetHandler(spy)
	return a, image, spy
}

// writeLogAdapterDataFrame commits one DATA frame for LogAdapter Poll tests.
func writeLogAdapterDataFrame(
	tb testing.TB,
	image *aeron.Image,
	frameOffset int,
	flags uint8,
	payload []byte,
) {
	tb.Helper()
	aeron.WriteCommittedDataFrameForHooks(
		aeron.ImageTermBufferForHooks(image),
		frameOffset,
		image.SessionID(),
		image.StreamID(),
		logAdapterHookTermID,
		int32(frameOffset),
		flags,
		payload,
	)
}

func writeLogAdapterUnfragmentedFrame(
	tb testing.TB,
	image *aeron.Image,
	frameOffset int,
	payload []byte,
) {
	tb.Helper()
	writeLogAdapterDataFrame(tb, image, frameOffset, logbuffer.Unfragmented, payload)
}

// writeLogAdapterFrameChain writes frameCount unfragmented DATA frames starting at
// startOffset.
func writeLogAdapterFrameChain(
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
		logAdapterHookTermID,
		frames,
	)
}

// writeAndPollOne writes one unfragmented frame at offset 0 and Polls past its end.
// Returns the aligned frame span.
func writeAndPollOne(
	tb testing.TB,
	a *LogAdapter,
	image *aeron.Image,
	payload []byte,
) int64 {
	tb.Helper()
	span := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	writeLogAdapterUnfragmentedFrame(tb, image, 0, payload)
	require.Equal(tb, 1, a.Poll(span+32))
	require.Equal(tb, span, a.Position())
	return span
}

// writeSessionMessageAtJoin encodes a SessionMessageHeader and writes one frame at 0.
// Returns the aligned frame span.
func writeSessionMessageAtJoin(
	tb testing.TB,
	image *aeron.Image,
	fields codecs.SessionMessageHeaderFields,
) int64 {
	tb.Helper()
	payload := encodeLogAdapterSessionMessagePayload(tb, fields)
	span := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	writeLogAdapterUnfragmentedFrame(tb, image, 0, payload)
	return span
}

// newFollowerLogPollAgent builds a follower Agent for log-poll / propose-max tests.
// Pass logAdapter non-nil to seed AgentConfig.LogAdapter; nil leaves adapter unset.
func newFollowerLogPollAgent(appendPos int64, logAdapter *LogAdapter) *Agent {
	appendV := new(atomic.Int64)
	appendV.Store(appendPos)
	return NewAgent(AgentConfig{
		Role:                      RoleFollower,
		Commit:                    new(atomic.Int64),
		Egress:                    &recordingEgress{},
		AppendSource:              &AtomicAppendPosition{V: appendV},
		LogAdapter:                logAdapter,
		LeadershipTermID:          1,
		LeaderHeartbeatIntervalNs: 1_000_000_000,
		LeaderMemberID:            2,
	})
}

func TestLogAdapter_unbound(t *testing.T) {
	a := NewLogAdapter(0)
	require.Nil(t, a.Image())
	require.Equal(t, int64(0), a.Position())
	require.Equal(t, 0, a.Poll(1<<20))
	require.True(t, a.IsImageClosed())
}

func TestLogAdapter_nilReceiver(t *testing.T) {
	var a *LogAdapter
	require.Equal(t, int64(0), a.Position())
	require.Equal(t, 0, a.Poll(1<<20))
	require.True(t, a.IsImageClosed())
}

func TestLogAdapter_fragmentLimit(t *testing.T) {
	require.Equal(t, DefaultLogFragmentLimit, NewLogAdapter(0).fragmentLimit)
	require.Equal(t, DefaultLogFragmentLimit, NewLogAdapter(-1).fragmentLimit)
	require.Equal(t, 7, NewLogAdapter(7).fragmentLimit)
}

func TestLogAdapter_SetImage_bind(t *testing.T) {
	a, image := boundLogAdapter(t)

	require.Same(t, image, a.Image())
	require.Equal(t, image.Position(), a.Position())
	require.False(t, a.IsImageClosed())
	require.Equal(t, 0, a.Poll(1<<20)) // empty term; bound poll returns 0
}

func TestLogAdapter_SetImage_cachePosition(t *testing.T) {
	a, image := boundLogAdapter(t)

	const advanced int64 = 128
	aeron.ImageSetSubscriberPositionForHooks(image, advanced)
	require.Equal(t, advanced, a.Position())

	// Clear bind: cache previous image position into logPosition.
	a.SetImage(nil)
	require.Nil(t, a.Image())
	require.Equal(t, advanced, a.Position())
	require.True(t, a.IsImageClosed())
	require.Equal(t, 0, a.Poll(1<<20))

	// Replace with a fresh image; Position follows the new bind.
	sub2 := aeron.NewTestSubscriptionForHooks(t)
	image2 := aeron.NewTestImageForHooks(t, sub2, 0)
	a.SetImage(image2)
	require.Same(t, image2, a.Image())
	require.Equal(t, image2.Position(), a.Position())

	// Replacing non-nil caches the outgoing image position, then binds nil.
	aeron.ImageSetSubscriberPositionForHooks(image2, 256)
	a.SetImage(nil)
	require.Nil(t, a.Image())
	require.Equal(t, int64(256), a.Position())
}

func TestLogAdapter_SetHandler(t *testing.T) {
	a := NewLogAdapter(0)
	a.SetHandler(nil)
	require.Nil(t, a.handler)

	spy := &logAdapterHandlerSpy{}
	a.SetHandler(spy)
	require.Same(t, spy, a.handler)

	a.SetHandler(nil)
	require.Nil(t, a.handler)
}

func TestLogAdapter_SetSessionEventSink(t *testing.T) {
	a := NewLogAdapter(0)
	a.SetSessionEventSink(nil)
	require.Nil(t, a.sessionSink)

	spy := &logAdapterSessionSinkSpy{}
	a.SetSessionEventSink(spy)
	require.Same(t, spy, a.sessionSink)

	a.SetSessionEventSink(nil)
	require.Nil(t, a.sessionSink)
}

func TestLogAdapter_Poll_boundBeforeFirstFrame(t *testing.T) {
	a, image := boundLogAdapter(t)
	join := image.Position()
	writeLogAdapterUnfragmentedFrame(t, image, 0, []byte("frame"))

	// Bound at join: frame start is at limit; not delivered.
	require.Equal(t, 0, a.Poll(join))
	require.Equal(t, join, a.Position())
}

func TestLogAdapter_Poll_twoFrames_midBound(t *testing.T) {
	a, image := boundLogAdapter(t)
	payload := []byte("x")
	span := logbuffer.AlignFragmentChunkLength(len(payload))
	writeLogAdapterFrameChain(t, image, 0, 2, payload)

	// Bound at start of B: only A delivered; Position stops at B start.
	boundB := int64(span)
	require.Equal(t, 1, a.Poll(boundB))
	require.Equal(t, boundB, a.Position())

	// Raise bound past B: second Poll delivers B.
	require.Equal(t, 1, a.Poll(boundB+int64(span)))
	require.Equal(t, int64(2*span), a.Position())
}

func TestLogAdapter_Poll_frameStartsBeforeBound_endPastBound(t *testing.T) {
	a, image := boundLogAdapter(t)
	// Long payload so mid-frame bound sits clearly inside the aligned span.
	payload := make([]byte, 100)
	span := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	writeLogAdapterUnfragmentedFrame(t, image, 0, payload)

	bound := span / 2
	require.Positive(t, bound)
	require.Less(t, bound, span)

	// Frame starts before bound: delivered in full; Position may exceed bound.
	require.Equal(t, 1, a.Poll(bound))
	require.Equal(t, span, a.Position())
}

func TestLogAdapter_Poll_fragmentLimit(t *testing.T) {
	a, image := boundLogAdapterWithLimit(t, 1)

	payload := []byte("a")
	span := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	const totalFrames = 5
	writeLogAdapterFrameChain(t, image, 0, totalFrames, payload)

	bound := span * totalFrames
	require.Equal(t, 1, a.Poll(bound))
	require.Equal(t, span, a.Position())

	// Second Poll continues from advanced Position.
	require.Equal(t, 1, a.Poll(bound))
	require.Equal(t, 2*span, a.Position())
}

func TestLogAdapter_IsImageClosed(t *testing.T) {
	a, image := boundLogAdapter(t)
	require.False(t, a.IsImageClosed())

	aeron.ImageInternalCloseForHooks(image)
	require.True(t, a.IsImageClosed())
}

func TestLogAdapter_Poll_closedImage(t *testing.T) {
	a, image := boundLogAdapter(t)
	writeLogAdapterUnfragmentedFrame(t, image, 0, []byte("frame"))
	aeron.ImageInternalCloseForHooks(image)

	require.True(t, a.IsImageClosed())
	require.Equal(t, 0, a.Poll(1<<20))
}

func TestLogAdapter_SessionMessage_handler(t *testing.T) {
	a, image, spy := boundLogAdapterWithHandler(t)

	const (
		leadershipTermID int64 = 11
		clusterSessionID int64 = 22
		timestamp        int64 = 33
	)
	payload := encodeLogAdapterSessionMessagePayload(t, codecs.SessionMessageHeaderFields{
		LeadershipTermID: leadershipTermID,
		ClusterSessionID: clusterSessionID,
		Timestamp:        timestamp,
	})
	writeAndPollOne(t, a, image, payload)

	require.Equal(t, 1, spy.sessionMessages)
	require.Equal(t, 0, spy.nltEvents)
	require.Equal(t, clusterSessionID, spy.lastClusterSessionID)
	require.Equal(t, timestamp, spy.lastTimestamp)
}

func TestLogAdapter_NLT_handler(t *testing.T) {
	a, image, spy := boundLogAdapterWithHandler(t)

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
	writeAndPollOne(t, a, image, encodeLogAdapterNLTPayload(t, fields))

	require.Equal(t, 0, spy.sessionMessages)
	require.Equal(t, 1, spy.nltEvents)
	require.Equal(t, fields.LeadershipTermID, spy.lastLeadershipTermID)
	require.Equal(t, fields.LogPosition, spy.lastLogPosition)
	require.Equal(t, fields.Timestamp, spy.lastNLTTimestamp)
	require.Equal(t, fields.TermBaseLogPosition, spy.lastTermBaseLogPosition)
	require.Equal(t, fields.TimeUnit, spy.lastTimeUnit)
	require.Equal(t, fields.AppVersion, spy.lastAppVersion)
}

func TestLogAdapter_decodeSkip_noHandler(t *testing.T) {
	tests := []struct {
		name    string
		payload func(testing.TB) []byte
	}{
		{
			name: "unknown template",
			payload: func(tb testing.TB) []byte {
				// Dummy cluster-schema template the adapter does not decode.
				return encodeLogAdapterMessageHeader(
					tb, 0, 99, clusterwire.MessageHeaderSbeSchemaID, 0,
				)
			},
		},
		{
			name: "wrong schema",
			payload: func(tb testing.TB) []byte {
				// Schema 112 is not cluster schema 111.
				return encodeLogAdapterMessageHeader(tb, 0, 1, 112, 0)
			},
		},
		{
			name: "short length",
			payload: func(tb testing.TB) []byte {
				// Payload shorter than MessageHeader: decode skipped, position advances.
				p := []byte{0x01, 0x02, 0x03, 0x04}
				require.Less(tb, len(p), codecs.MessageHeaderEncodedLen())
				return p
			},
		},
		{
			name: "header only claimed body",
			payload: func(tb testing.TB) []byte {
				// MessageHeader only: production blockLength claims a full
				// SessionMessage body not present in the fragment. Decode must
				// fail closed on the fragment span.
				return encodeLogAdapterMessageHeader(
					tb,
					clusterwire.SessionMessageHeaderSbeBlockLength,
					clusterwire.SessionMessageHeaderSbeTemplateID,
					clusterwire.MessageHeaderSbeSchemaID,
					clusterwire.SessionMessageHeaderSbeSchemaVersion,
				)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, image, spy := boundLogAdapterWithHandler(t)
			writeAndPollOne(t, a, image, tt.payload(t))
			requireHandlerIdle(t, spy)
		})
	}
}

func TestLogAdapter_nilHandler_stillAdvances(t *testing.T) {
	a, image := boundLogAdapter(t)
	// No SetHandler: nil handler must not panic; position still advances.
	payload := encodeLogAdapterSessionMessagePayload(t, codecs.SessionMessageHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		Timestamp:        3,
	})
	require.NotPanics(t, func() {
		writeAndPollOne(t, a, image, payload)
	})
}

func TestLogAdapter_fragmented_noHandler(t *testing.T) {
	a, image, spy := boundLogAdapterWithHandler(t)

	// Valid SessionMessage body. Begin-only flags. The assembler holds the
	// fragment and does not call the handler until END.
	payload := encodeLogAdapterSessionMessagePayload(t, codecs.SessionMessageHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: 2,
		Timestamp:        3,
	})
	span := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	writeLogAdapterDataFrame(t, image, 0, logbuffer.BeginFlag, payload)

	require.Equal(t, 1, a.Poll(span+32))
	require.Equal(t, span, a.Position())
	requireHandlerIdle(t, spy)
}

func TestFollowerConsensusWork_pollAndProposeMax(t *testing.T) {
	tests := []struct {
		name     string
		newAgent func(adapter *LogAdapter, appendPos int64) *Agent
	}{
		{
			name: "setter",
			newAgent: func(adapter *LogAdapter, appendPos int64) *Agent {
				a := newFollowerLogPollAgent(appendPos, nil)
				a.SetLogAdapter(adapter)
				return a
			},
		},
		{
			name: "config seed",
			newAgent: func(adapter *LogAdapter, appendPos int64) *Agent {
				return newFollowerLogPollAgent(appendPos, adapter)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, image := boundLogAdapter(t)
			span := writeSessionMessageAtJoin(t, image, codecs.SessionMessageHeaderFields{
				LeadershipTermID: 1,
				ClusterSessionID: 2,
				Timestamp:        3,
			})

			a := tt.newAgent(adapter, span+100)
			require.Same(t, adapter, a.logAdapter)
			a.notifiedCommitPosition = span + 32

			require.Positive(t, a.followerConsensusWork(0))
			require.Equal(t, span, adapter.Position())
			// Commit tracks adapter Position (max prior is 0). Do not assert
			// CommitPosition <= NotifiedCommitPosition -- fails on straddle.
			require.Equal(t, adapter.Position(), a.CommitPosition())
		})
	}
}

func TestFollowerConsensusWork_straddleProposeMaxPastNotify(t *testing.T) {
	adapter, image, spy := boundLogAdapterWithHandler(t)
	span := writeSessionMessageAtJoin(t, image, codecs.SessionMessageHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: 9,
		Timestamp:        7,
	})

	// Mid-frame notify: start before bound, end past bound (straddle).
	bound := span / 2
	require.Positive(t, bound)
	require.Less(t, bound, span)

	a := newFollowerLogPollAgent(span+100, nil)
	a.SetLogAdapter(adapter)
	a.notifiedCommitPosition = bound

	require.Positive(t, a.followerConsensusWork(0))
	require.Equal(t, 1, spy.sessionMessages)
	require.Equal(t, int64(9), spy.lastClusterSessionID)
	require.Equal(t, span, adapter.Position())
	require.Equal(t, adapter.Position(), a.CommitPosition())
	// Post-straddle consume may sit past notify.
	require.Greater(t, a.CommitPosition(), a.NotifiedCommitPosition())
}

func TestFollowerConsensusWork_boundUsesMinNotifyAndAppend(t *testing.T) {
	adapter, image, spy := boundLogAdapterWithHandler(t)

	// Two short frames; second starts at span (append bound).
	payload := []byte("x")
	span := logbuffer.AlignFragmentChunkLength(len(payload))
	writeLogAdapterFrameChain(t, image, 0, 2, payload)

	const notifyHigh int64 = 1000
	appendBound := int64(span)

	a := newFollowerLogPollAgent(appendBound, nil)
	a.SetLogAdapter(adapter)
	a.notifiedCommitPosition = notifyHigh

	// Bound = min(notifyHigh, appendBound) = appendBound: only first frame.
	require.Equal(t, appendBound, a.followerLogPollBound(appendBound))
	require.Positive(t, a.followerConsensusWork(0))
	require.Equal(t, appendBound, adapter.Position())
	// Second frame start == appendBound: never delivered.
	requireHandlerIdle(t, spy)
	// Unknown raw payload: position advanced for frame A only.
	require.Equal(t, appendBound, a.CommitPosition())
}

// logAdapterSessionSinkSpy records SessionOpenEvent and SessionCloseEvent
// deliveries from the adapter sink.
type logAdapterSessionSinkSpy struct {
	opens  int
	closes int

	lastOpen    codecs.SessionOpenEventFields
	lastOpenPos int64
	lastClose   codecs.SessionCloseEventFields
}

func (s *logAdapterSessionSinkSpy) handleSessionOpenEvent(
	fields codecs.SessionOpenEventFields,
	logPosition int64,
) {
	s.opens++
	s.lastOpen = fields
	s.lastOpenPos = logPosition
}

func (s *logAdapterSessionSinkSpy) handleSessionCloseEvent(
	fields codecs.SessionCloseEventFields,
) {
	s.closes++
	s.lastClose = fields
}

func requireSessionSinkIdle(tb testing.TB, spy *logAdapterSessionSinkSpy) {
	tb.Helper()
	require.Equal(tb, 0, spy.opens)
	require.Equal(tb, 0, spy.closes)
}

func sampleLogAdapterSessionOpenFields() codecs.SessionOpenEventFields {
	return codecs.SessionOpenEventFields{
		LeadershipTermID: 11,
		CorrelationID:    22,
		ClusterSessionID: 33,
		Timestamp:        44,
		ResponseStreamID: 55,
		ResponseChannel:  []byte("aeron:udp?endpoint=127.0.0.1:40123"),
		EncodedPrincipal: []byte("alice"),
	}
}

func sampleLogAdapterSessionCloseFields() codecs.SessionCloseEventFields {
	return codecs.SessionCloseEventFields{
		LeadershipTermID: 11,
		ClusterSessionID: 33,
		Timestamp:        44,
		CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
	}
}

func encodeLogAdapterSessionOpenPayload(
	tb testing.TB,
	fields codecs.SessionOpenEventFields,
) []byte {
	tb.Helper()
	total, ok := codecs.ClusterWireMessageLen(
		clusterwire.SessionOpenEventSbeBlockLength,
		len(fields.ResponseChannel),
		len(fields.EncodedPrincipal),
	)
	require.True(tb, ok)
	buf := make([]byte, total)
	wireEnd, encOK := codecs.EncodeSessionOpenEvent(
		buf,
		0,
		clusterwire.SessionOpenEventSbeSchemaVersion,
		fields,
	)
	require.True(tb, encOK)
	return buf[:wireEnd]
}

func encodeLogAdapterSessionClosePayload(
	tb testing.TB,
	fields codecs.SessionCloseEventFields,
) []byte {
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

func boundLogAdapterWithSink(
	tb testing.TB,
) (*LogAdapter, *aeron.Image, *logAdapterSessionSinkSpy) {
	tb.Helper()
	a, image := boundLogAdapter(tb)
	spy := &logAdapterSessionSinkSpy{}
	a.SetSessionEventSink(spy)
	return a, image, spy
}

func boundLogAdapterWithSinkAndHandler(
	tb testing.TB,
) (*LogAdapter, *aeron.Image, *logAdapterSessionSinkSpy, *logAdapterHandlerSpy) {
	tb.Helper()
	a, image, sink := boundLogAdapterWithSink(tb)
	handler := &logAdapterHandlerSpy{}
	a.SetHandler(handler)
	return a, image, sink, handler
}

func writeLogAdapterFragmentTrain(
	tb testing.TB,
	image *aeron.Image,
	startOffset int,
	payload []byte,
	chunk int,
) (offsets []int, endPos int64) {
	tb.Helper()
	require.Positive(tb, chunk)
	require.Greater(tb, len(payload), chunk)

	frames := make([]aeron.FragmentedFrameForTest, 0, (len(payload)+chunk-1)/chunk)
	for off := 0; off < len(payload); {
		n := min(chunk, len(payload)-off)
		frames = append(frames, aeron.FragmentedFrameForTest{
			Payload: append([]byte(nil), payload[off:off+n]...),
		})
		off += n
	}
	require.Greater(tb, len(frames), 1)
	frames[0].Flags = logbuffer.BeginFlag
	frames[len(frames)-1].Flags = logbuffer.EndFlag

	offsets = aeron.WriteCommittedDataFramesForStreamForTest(
		aeron.ImageTermBufferForHooks(image),
		startOffset,
		image.SessionID(),
		image.StreamID(),
		logAdapterHookTermID,
		frames,
	)
	last := frames[len(frames)-1]
	lastOff := offsets[len(offsets)-1]
	endPos = int64(lastOff + logbuffer.AlignFragmentChunkLength(len(last.Payload)))
	return offsets, endPos
}

func TestLogAdapter_SessionOpen_reachesSink(t *testing.T) {
	a, image, sink, handler := boundLogAdapterWithSinkAndHandler(t)

	fields := sampleLogAdapterSessionOpenFields()
	payload := encodeLogAdapterSessionOpenPayload(t, fields)
	span := writeAndPollOne(t, a, image, payload)

	require.Equal(t, 1, sink.opens)
	require.Equal(t, 0, sink.closes)
	require.Equal(t, fields, sink.lastOpen)
	require.Equal(t, span, sink.lastOpenPos)
	requireHandlerIdle(t, handler)
}

func TestLogAdapter_SessionClose_reachesSink(t *testing.T) {
	a, image, sink, handler := boundLogAdapterWithSinkAndHandler(t)

	fields := sampleLogAdapterSessionCloseFields()
	payload := encodeLogAdapterSessionClosePayload(t, fields)
	writeAndPollOne(t, a, image, payload)

	require.Equal(t, 0, sink.opens)
	require.Equal(t, 1, sink.closes)
	require.Equal(t, fields, sink.lastClose)
	requireHandlerIdle(t, handler)
}

func TestLogAdapter_nilSinkAndHandler_sessionEventsSafe(t *testing.T) {
	tests := []struct {
		name    string
		payload func(testing.TB) []byte
	}{
		{
			name: "open",
			payload: func(tb testing.TB) []byte {
				return encodeLogAdapterSessionOpenPayload(tb, sampleLogAdapterSessionOpenFields())
			},
		},
		{
			name: "close",
			payload: func(tb testing.TB) []byte {
				return encodeLogAdapterSessionClosePayload(tb, sampleLogAdapterSessionCloseFields())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, image := boundLogAdapter(t)
			require.NotPanics(t, func() {
				writeAndPollOne(t, a, image, tt.payload(t))
			})
		})
	}
}

func TestLogAdapter_sessionEvents_skipped(t *testing.T) {
	tests := []struct {
		name    string
		payload func(testing.TB) []byte
	}{
		{
			name: "open_wrong_schema",
			payload: func(tb testing.TB) []byte {
				return encodeLogAdapterMessageHeader(
					tb,
					clusterwire.SessionOpenEventSbeBlockLength,
					clusterwire.SessionOpenEventSbeTemplateID,
					112,
					clusterwire.SessionOpenEventSbeSchemaVersion,
				)
			},
		},
		{
			name: "close_wrong_schema",
			payload: func(tb testing.TB) []byte {
				return encodeLogAdapterMessageHeader(
					tb,
					clusterwire.SessionCloseEventSbeBlockLength,
					clusterwire.SessionCloseEventSbeTemplateID,
					112,
					clusterwire.SessionCloseEventSbeSchemaVersion,
				)
			},
		},
		{
			name: "open_header_only",
			payload: func(tb testing.TB) []byte {
				return encodeLogAdapterMessageHeader(
					tb,
					clusterwire.SessionOpenEventSbeBlockLength,
					clusterwire.SessionOpenEventSbeTemplateID,
					clusterwire.MessageHeaderSbeSchemaID,
					clusterwire.SessionOpenEventSbeSchemaVersion,
				)
			},
		},
		{
			name: "close_header_only",
			payload: func(tb testing.TB) []byte {
				return encodeLogAdapterMessageHeader(
					tb,
					clusterwire.SessionCloseEventSbeBlockLength,
					clusterwire.SessionCloseEventSbeTemplateID,
					clusterwire.MessageHeaderSbeSchemaID,
					clusterwire.SessionCloseEventSbeSchemaVersion,
				)
			},
		},
		{
			name: "short_length",
			payload: func(tb testing.TB) []byte {
				p := []byte{0x01, 0x02, 0x03, 0x04}
				require.Less(tb, len(p), codecs.MessageHeaderEncodedLen())
				return p
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, image, sink, handler := boundLogAdapterWithSinkAndHandler(t)
			writeAndPollOne(t, a, image, tt.payload(t))
			requireSessionSinkIdle(t, sink)
			requireHandlerIdle(t, handler)
		})
	}
}

func TestLogAdapter_templates1And24_stillReachHandler(t *testing.T) {
	t.Run("session_message", func(t *testing.T) {
		a, image, sink, handler := boundLogAdapterWithSinkAndHandler(t)
		msg := codecs.SessionMessageHeaderFields{
			LeadershipTermID: 11,
			ClusterSessionID: 22,
			Timestamp:        33,
		}
		writeAndPollOne(t, a, image, encodeLogAdapterSessionMessagePayload(t, msg))
		require.Equal(t, 1, handler.sessionMessages)
		require.Equal(t, msg.ClusterSessionID, handler.lastClusterSessionID)
		require.Equal(t, msg.Timestamp, handler.lastTimestamp)
		requireSessionSinkIdle(t, sink)
	})

	t.Run("nlt", func(t *testing.T) {
		a, image, sink, handler := boundLogAdapterWithSinkAndHandler(t)
		nlt := codecs.NewLeadershipTermEventFields{
			LeadershipTermID:    7,
			LogPosition:         128,
			Timestamp:           999,
			TermBaseLogPosition: 64,
			LeaderMemberID:      1,
			LogSessionID:        3,
			TimeUnit:            clusterwire.ClusterTimeUnit_NANOS,
			AppVersion:          42,
		}
		writeAndPollOne(t, a, image, encodeLogAdapterNLTPayload(t, nlt))
		require.Equal(t, 1, handler.nltEvents)
		require.Equal(t, nlt.LeadershipTermID, handler.lastLeadershipTermID)
		require.Equal(t, nlt.LogPosition, handler.lastLogPosition)
		require.Equal(t, nlt.Timestamp, handler.lastNLTTimestamp)
		require.Equal(t, nlt.TermBaseLogPosition, handler.lastTermBaseLogPosition)
		require.Equal(t, nlt.TimeUnit, handler.lastTimeUnit)
		require.Equal(t, nlt.AppVersion, handler.lastAppVersion)
		requireSessionSinkIdle(t, sink)
	})
}

func TestLogAdapter_fragmentedSessionOpen_oneSinkDelivery(t *testing.T) {
	a, image, sink := boundLogAdapterWithSink(t)
	fields := sampleLogAdapterSessionOpenFields()
	payload := encodeLogAdapterSessionOpenPayload(t, fields)
	offsets, endPos := writeLogAdapterFragmentTrain(t, image, 0, payload, 16)

	require.Equal(t, len(offsets), a.Poll(endPos+32))
	require.Equal(t, endPos, a.Position())
	require.Equal(t, 1, sink.opens)
	require.Equal(t, 0, sink.closes)
	require.Equal(t, fields, sink.lastOpen)
	require.Equal(t, endPos, sink.lastOpenPos)
}

func TestLogAdapter_fragmentedSessionOpen_pausedMidTrain(t *testing.T) {
	a, image, sink := boundLogAdapterWithSink(t)
	fields := sampleLogAdapterSessionOpenFields()
	payload := encodeLogAdapterSessionOpenPayload(t, fields)
	offsets, endPos := writeLogAdapterFragmentTrain(t, image, 0, payload, 16)
	require.Greater(t, len(offsets), 1)

	endStart := int64(offsets[len(offsets)-1])
	require.Equal(t, len(offsets)-1, a.Poll(endStart))
	require.Equal(t, endStart, a.Position())
	requireSessionSinkIdle(t, sink)

	require.Equal(t, 1, a.Poll(endPos+32))
	require.Equal(t, endPos, a.Position())
	require.Equal(t, 1, sink.opens)
	require.Equal(t, fields, sink.lastOpen)
	require.Equal(t, endPos, sink.lastOpenPos)
}

func TestLogAdapter_fragmentedSessionMessage_reachesHandler(t *testing.T) {
	a, image, handler := boundLogAdapterWithHandler(t)
	fields := codecs.SessionMessageHeaderFields{
		LeadershipTermID: 11,
		ClusterSessionID: 22,
		Timestamp:        33,
	}
	payload := encodeLogAdapterSessionMessagePayload(t, fields)
	offsets, endPos := writeLogAdapterFragmentTrain(t, image, 0, payload, 8)

	require.Equal(t, len(offsets), a.Poll(endPos+32))
	require.Equal(t, endPos, a.Position())
	require.Equal(t, 1, handler.sessionMessages)
	require.Equal(t, 0, handler.nltEvents)
	require.Equal(t, fields.ClusterSessionID, handler.lastClusterSessionID)
	require.Equal(t, fields.Timestamp, handler.lastTimestamp)
}

func TestLogAdapter_SetImage_clearsAssembler(t *testing.T) {
	a, image, sink := boundLogAdapterWithSink(t)
	fields := sampleLogAdapterSessionOpenFields()
	payload := encodeLogAdapterSessionOpenPayload(t, fields)
	offsets, endPos := writeLogAdapterFragmentTrain(t, image, 0, payload, 16)
	endStart := int64(offsets[len(offsets)-1])

	require.Equal(t, len(offsets)-1, a.Poll(endStart))
	requireSessionSinkIdle(t, sink)

	a.SetImage(image)
	require.Equal(t, 1, a.Poll(endPos+32))
	requireSessionSinkIdle(t, sink)

	fullSpan := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	writeLogAdapterUnfragmentedFrame(t, image, int(endPos), payload)
	require.Equal(t, 1, a.Poll(endPos+fullSpan+32))
	require.Equal(t, 1, sink.opens)
	require.Equal(t, fields, sink.lastOpen)
}
