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
	return NewAgent(followerLogPollConfig(appendPos, logAdapter))
}

func followerLogPollConfig(appendPos int64, logAdapter *LogAdapter) AgentConfig {
	appendV := new(atomic.Int64)
	appendV.Store(appendPos)
	return AgentConfig{
		Role:                      RoleFollower,
		Commit:                    new(atomic.Int64),
		Egress:                    &recordingEgress{},
		AppendSource:              &AtomicAppendPosition{V: appendV},
		LogAdapter:                logAdapter,
		LeadershipTermID:          1,
		LeaderHeartbeatIntervalNs: 1_000_000_000,
		LeaderMemberID:            2,
	}
}

// newFollowerActionAgent builds a log-poll follower with serviceCount services
// and a queue on service ingress.
func newFollowerActionAgent(
	appendPos int64,
	logAdapter *LogAdapter,
	serviceCount int,
) (*Agent, *QueueConsensusIngress) {
	cfg := followerLogPollConfig(appendPos, logAdapter)
	cfg.ServiceCount = serviceCount
	return agentWithServiceIngressConfig(cfg)
}

// encodeLogAdapterClusterActionPayload builds ClusterActionRequest wire bytes.
func encodeLogAdapterClusterActionPayload(
	tb testing.TB,
	fields codecs.ClusterActionRequestFields,
) []byte {
	tb.Helper()
	buf := make([]byte, codecs.ClusterActionRequestEncodedLen())
	wireEnd, ok := codecs.EncodeClusterActionRequest(
		buf,
		0,
		clusterwire.ClusterActionRequestSbeSchemaVersion,
		fields,
	)
	require.True(tb, ok)
	return buf[:wireEnd]
}

// writeClusterActionFrame writes one ClusterActionRequest frame at
// frameOffset. The wire LogPosition is the frame end. Returns the frame end.
func writeClusterActionFrame(
	tb testing.TB,
	image *aeron.Image,
	frameOffset int,
	action clusterwire.ClusterAction,
	flags int32,
) int64 {
	tb.Helper()
	chunk := logbuffer.AlignFragmentChunkLength(codecs.ClusterActionRequestEncodedLen())
	end := int64(frameOffset + chunk)
	fields := snapshotActionFields(end)
	fields.Action = action
	fields.Flags = flags
	payload := encodeLogAdapterClusterActionPayload(tb, fields)
	writeLogAdapterUnfragmentedFrame(tb, image, frameOffset, payload)
	return end
}

// writeSessionMessageFrame writes one SessionMessageHeader frame at
// frameOffset. Returns the frame end.
func writeSessionMessageFrame(
	tb testing.TB,
	image *aeron.Image,
	frameOffset int,
	clusterSessionID int64,
) int64 {
	tb.Helper()
	payload := encodeLogAdapterSessionMessagePayload(tb, codecs.SessionMessageHeaderFields{
		LeadershipTermID: 1,
		ClusterSessionID: clusterSessionID,
		Timestamp:        7,
	})
	writeLogAdapterUnfragmentedFrame(tb, image, frameOffset, payload)
	return int64(frameOffset + logbuffer.AlignFragmentChunkLength(len(payload)))
}

// writeSnapshotActionThenMessage writes a SNAPSHOT action with flags 0 at
// frameOffset, then a session message. Returns both frame ends.
func writeSnapshotActionThenMessage(
	tb testing.TB,
	image *aeron.Image,
	frameOffset int,
) (actionEnd, messageEnd int64) {
	tb.Helper()
	actionEnd = writeClusterActionFrame(tb, image, frameOffset, clusterwire.ClusterAction_SNAPSHOT, 0)
	messageEnd = writeSessionMessageFrame(tb, image, int(actionEnd), 9)
	return actionEnd, messageEnd
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

// TestLogAdapter_endOfStream covers IsLogEndOfStream and IsLogEndOfStreamAt
// on open, closed, and missing images.
func TestLogAdapter_endOfStream(t *testing.T) {
	const endOfStream int64 = 256

	tests := []struct {
		name string
		// Nil adapter leaves the *LogAdapter nil.
		adapter    func(tb testing.TB) *LogAdapter
		atPosition int64
		wantEOS    bool
		wantEOSAt  bool
	}{
		{
			name:       "nil adapter",
			atPosition: endOfStream,
		},
		{
			name: "nil image",
			adapter: func(testing.TB) *LogAdapter {
				return NewLogAdapter(0)
			},
			atPosition: endOfStream,
		},
		{
			name: "open image below the end-of-stream word",
			adapter: func(tb testing.TB) *LogAdapter {
				a, image := boundLogAdapter(tb)
				aeron.ImageSetSubscriberPositionForHooks(image, endOfStream-32)
				aeron.ImageSetEndOfStreamPositionForHooks(image, endOfStream)
				return a
			},
			atPosition: endOfStream,
			wantEOSAt:  true,
		},
		{
			name: "open image at the end-of-stream word",
			adapter: func(tb testing.TB) *LogAdapter {
				a, image := boundLogAdapter(tb)
				aeron.ImageSetSubscriberPositionForHooks(image, endOfStream)
				aeron.ImageSetEndOfStreamPositionForHooks(image, endOfStream)
				return a
			},
			atPosition: endOfStream,
			wantEOS:    true,
			wantEOSAt:  true,
		},
		{
			name: "closed image with end of stream",
			adapter: func(tb testing.TB) *LogAdapter {
				a, image := boundLogAdapter(tb)
				aeron.ImageSetSubscriberPositionForHooks(image, endOfStream)
				aeron.ImageSetEndOfStreamPositionForHooks(image, endOfStream)
				aeron.ImageInternalCloseForHooks(image)
				return a
			},
			atPosition: endOfStream,
			wantEOS:    true,
			wantEOSAt:  true,
		},
		{
			name: "closed image without end of stream",
			adapter: func(tb testing.TB) *LogAdapter {
				a, image := boundLogAdapter(tb)
				aeron.ImageSetSubscriberPositionForHooks(image, endOfStream)
				aeron.ImageInternalCloseForHooks(image)
				return a
			},
			atPosition: endOfStream,
		},
		{
			name: "position unequal to the end-of-stream word",
			adapter: func(tb testing.TB) *LogAdapter {
				a, image := boundLogAdapter(tb)
				aeron.ImageSetSubscriberPositionForHooks(image, endOfStream)
				aeron.ImageSetEndOfStreamPositionForHooks(image, endOfStream)
				return a
			},
			atPosition: endOfStream + 32,
			wantEOS:    true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var a *LogAdapter
			if tc.adapter != nil {
				a = tc.adapter(t)
			}
			require.Equal(t, tc.wantEOS, a.IsLogEndOfStream())
			require.Equal(t, tc.wantEOSAt, a.IsLogEndOfStreamAt(tc.atPosition))
		})
	}
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
			require.Same(t, a, adapter.timerSink)
			require.Same(t, a, adapter.actionSink)
			a.notifiedCommitPosition = span + 32

			require.Positive(t, a.followerConsensusWork(0))
			require.Equal(t, span, adapter.Position())
			// Commit tracks adapter Position (max prior is 0). Do not assert
			// CommitPosition <= NotifiedCommitPosition -- fails on straddle.
			require.Equal(t, adapter.Position(), a.CommitPosition())
		})
	}
}

func TestFollowerConsensusWork_holdsAtSnapshotAction(t *testing.T) {
	adapter, image, spy := boundLogAdapterWithHandler(t)
	actionEnd, messageEnd := writeSnapshotActionThenMessage(t, image, 0)
	a, q := newFollowerActionAgent(messageEnd+100, adapter, 1)
	a.notifiedCommitPosition = messageEnd + 32

	// Tick 1: consume the action, queue the wait, stop the batch.
	a.followerConsensusWork(0)
	require.Equal(t, actionEnd, adapter.Position())
	require.Equal(t, actionEnd, a.CommitPosition())
	require.True(t, a.HasPendingSnapshotAction())
	require.Equal(t, actionEnd, a.ExpectedAckPosition())
	requireHandlerIdle(t, spy)

	// Tick 2: the hold keeps the adapter at the action end.
	a.followerConsensusWork(0)
	require.Equal(t, actionEnd, adapter.Position())
	require.Equal(t, actionEnd, a.CommitPosition())
	require.Equal(t, 1, a.PendingServiceAckWaits())
	requireHandlerIdle(t, spy)

	// Release: the service ack pops the wait. The fixture does not wire a
	// recording control, so the module take resolves on its first step.
	enqueueServiceAck(t, q, actionEnd, 0, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	require.True(t, a.HasPendingSnapshotAction())
	a.snapshotTakeWork(0)
	require.False(t, a.HasPendingSnapshotAction())
	require.Equal(t, 0, a.PendingServiceAckWaits())

	// Tick 3: consume resumes.
	a.followerConsensusWork(0)
	require.Equal(t, 1, spy.sessionMessages)
	require.Equal(t, messageEnd, adapter.Position())
	require.Equal(t, messageEnd, a.CommitPosition())
	require.NoError(t, a.ServiceAckFailure())
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

func TestFollowerConsensusWork_timerEventCancelsTimer(t *testing.T) {
	const correlationID int64 = 41
	adapter, image := boundLogAdapter(t)
	payload := encodeLogAdapterTimerEventPayload(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    correlationID,
		Timestamp:        5,
	})
	span := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	writeLogAdapterUnfragmentedFrame(t, image, 0, payload)

	a := newFollowerLogPollAgent(span+100, adapter)
	scheduleTimer(a, correlationID, timerWheelTickResolutionNs)
	require.Equal(t, int64(1), a.ScheduledTimerCount())
	a.notifiedCommitPosition = span + 32

	require.Positive(t, a.followerConsensusWork(0))
	require.Equal(t, span, a.CommitPosition())
	require.Equal(t, int64(0), a.ScheduledTimerCount())
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

func encodeLogAdapterTimerEventPayload(tb testing.TB, fields codecs.TimerEventFields) []byte {
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

type logAdapterTimerSinkSpy struct {
	events int

	lastCorrelationID int64
	lastLogPosition   int64
}

func (s *logAdapterTimerSinkSpy) handleTimerEvent(correlationID, logPosition int64) {
	s.events++
	s.lastCorrelationID = correlationID
	s.lastLogPosition = logPosition
}

func boundLogAdapterWithTimerSink(
	tb testing.TB,
) (
	*LogAdapter,
	*aeron.Image,
	*logAdapterSessionSinkSpy,
	*logAdapterTimerSinkSpy,
	*logAdapterHandlerSpy,
) {
	tb.Helper()
	a, image, sessionSink, handler := boundLogAdapterWithSinkAndHandler(tb)
	timerSink := &logAdapterTimerSinkSpy{}
	a.setTimerEventSink(timerSink)
	return a, image, sessionSink, timerSink, handler
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

func TestLogAdapter_TimerEvent_reachesSink(t *testing.T) {
	tests := []struct {
		name          string
		timerSinkOnly bool
	}{
		{name: "all_sinks"},
		{name: "timer_sink_only", timerSinkOnly: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				a           *LogAdapter
				image       *aeron.Image
				sessionSink *logAdapterSessionSinkSpy
				timerSink   *logAdapterTimerSinkSpy
				handler     *logAdapterHandlerSpy
			)
			if tt.timerSinkOnly {
				a, image = boundLogAdapter(t)
				timerSink = &logAdapterTimerSinkSpy{}
				a.setTimerEventSink(timerSink)
			} else {
				a, image, sessionSink, timerSink, handler = boundLogAdapterWithTimerSink(t)
			}

			fields := codecs.TimerEventFields{
				LeadershipTermID: 3,
				CorrelationID:    44,
				Timestamp:        55,
			}
			span := writeAndPollOne(t, a, image, encodeLogAdapterTimerEventPayload(t, fields))

			require.Equal(t, 1, timerSink.events)
			require.Equal(t, fields.CorrelationID, timerSink.lastCorrelationID)
			require.Equal(t, span, timerSink.lastLogPosition)
			if !tt.timerSinkOnly {
				requireSessionSinkIdle(t, sessionSink)
				requireHandlerIdle(t, handler)
			}
		})
	}
}

// logAdapterActionSinkSpy records ClusterActionRequest deliveries from the
// adapter sink.
type logAdapterActionSinkSpy struct {
	actions int

	lastFields      codecs.ClusterActionRequestFields
	lastLogPosition int64
}

func (s *logAdapterActionSinkSpy) handleClusterAction(
	fields codecs.ClusterActionRequestFields,
	logPosition int64,
) {
	s.actions++
	s.lastFields = fields
	s.lastLogPosition = logPosition
}

func TestLogAdapter_ClusterAction_reachesSink(t *testing.T) {
	fields := codecs.ClusterActionRequestFields{
		LeadershipTermID: 3,
		LogPosition:      77,
		Timestamp:        55,
		Action:           clusterwire.ClusterAction_SNAPSHOT,
		Flags:            0,
	}
	payload := encodeLogAdapterClusterActionPayload(t, fields)
	tests := []struct {
		name string
		// write writes the action at 0. Returns the fragment count and the
		// action end.
		write func(t *testing.T, image *aeron.Image) (int, int64)
	}{
		{
			name: "unfragmented",
			write: func(t *testing.T, image *aeron.Image) (int, int64) {
				writeLogAdapterUnfragmentedFrame(t, image, 0, payload)
				return 1, int64(logbuffer.AlignFragmentChunkLength(len(payload)))
			},
		},
		{
			name: "fragment_train",
			write: func(t *testing.T, image *aeron.Image) (int, int64) {
				offsets, end := writeLogAdapterFragmentTrain(t, image, 0, payload, len(payload)/2+1)
				require.Len(t, offsets, 2)
				return len(offsets), end
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, image, handler := boundLogAdapterWithHandler(t)
			sink := &logAdapterActionSinkSpy{}
			a.setClusterActionSink(sink)
			fragments, actionEnd := tt.write(t, image)
			messageEnd := writeSessionMessageFrame(t, image, int(actionEnd), 9)

			// The action ends the batch.
			require.Equal(t, fragments, a.Poll(messageEnd+32))
			require.Equal(t, actionEnd, a.Position())
			require.Equal(t, 1, sink.actions)
			require.Equal(t, fields, sink.lastFields)
			require.Equal(t, actionEnd, sink.lastLogPosition)
			requireHandlerIdle(t, handler)

			require.Equal(t, 1, a.Poll(messageEnd+32))
			require.Equal(t, messageEnd, a.Position())
			require.Equal(t, 1, sink.actions)
			require.Equal(t, 1, handler.sessionMessages)
		})
	}
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

func TestLogAdapter_nilSinksAndHandler_eventsSafe(t *testing.T) {
	timerPayload := func(tb testing.TB) []byte {
		return encodeLogAdapterTimerEventPayload(tb, codecs.TimerEventFields{
			LeadershipTermID: 1,
			CorrelationID:    2,
			Timestamp:        3,
		})
	}
	actionPayload := func(tb testing.TB) []byte {
		return encodeLogAdapterClusterActionPayload(tb, codecs.ClusterActionRequestFields{
			LeadershipTermID: 1,
			LogPosition:      2,
			Timestamp:        3,
			Action:           clusterwire.ClusterAction_SNAPSHOT,
		})
	}
	tests := []struct {
		name                      string
		payload                   func(testing.TB) []byte
		withSessionSinkAndHandler bool
		// thenMessage adds a session message frame after the payload. One Poll
		// must consume both frames.
		thenMessage bool
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
		{
			name:    "timer",
			payload: timerPayload,
		},
		{
			name:                      "timer_nil_timer_sink",
			payload:                   timerPayload,
			withSessionSinkAndHandler: true,
		},
		{
			name:                      "action_nil_action_sink",
			payload:                   actionPayload,
			withSessionSinkAndHandler: true,
			thenMessage:               true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, image := boundLogAdapter(t)
			var (
				sessionSink *logAdapterSessionSinkSpy
				handler     *logAdapterHandlerSpy
			)
			if tt.withSessionSinkAndHandler {
				sessionSink = &logAdapterSessionSinkSpy{}
				handler = &logAdapterHandlerSpy{}
				a.SetSessionEventSink(sessionSink)
				a.SetHandler(handler)
			}
			if tt.thenMessage {
				payload := tt.payload(t)
				writeLogAdapterUnfragmentedFrame(t, image, 0, payload)
				span := logbuffer.AlignFragmentChunkLength(len(payload))
				messageEnd := writeSessionMessageFrame(t, image, span, 9)
				require.NotPanics(t, func() {
					require.Equal(t, 2, a.Poll(messageEnd+32))
				})
				require.Equal(t, messageEnd, a.Position())
				requireSessionSinkIdle(t, sessionSink)
				require.Equal(t, 1, handler.sessionMessages)
				return
			}
			require.NotPanics(t, func() {
				writeAndPollOne(t, a, image, tt.payload(t))
			})
			if tt.withSessionSinkAndHandler {
				requireSessionSinkIdle(t, sessionSink)
				requireHandlerIdle(t, handler)
			}
		})
	}
}

func TestLogAdapter_undecodableEvents_skipped(t *testing.T) {
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
			name: "timer_wrong_schema",
			payload: func(tb testing.TB) []byte {
				return encodeLogAdapterMessageHeader(
					tb,
					clusterwire.TimerEventSbeBlockLength,
					clusterwire.TimerEventSbeTemplateID,
					112,
					clusterwire.TimerEventSbeSchemaVersion,
				)
			},
		},
		{
			name: "timer_header_only",
			payload: func(tb testing.TB) []byte {
				return encodeLogAdapterMessageHeader(
					tb,
					clusterwire.TimerEventSbeBlockLength,
					clusterwire.TimerEventSbeTemplateID,
					clusterwire.MessageHeaderSbeSchemaID,
					clusterwire.TimerEventSbeSchemaVersion,
				)
			},
		},
		{
			name: "action_header_only",
			payload: func(tb testing.TB) []byte {
				return encodeLogAdapterMessageHeader(
					tb,
					clusterwire.ClusterActionRequestSbeBlockLength,
					clusterwire.ClusterActionRequestSbeTemplateID,
					clusterwire.MessageHeaderSbeSchemaID,
					clusterwire.ClusterActionRequestSbeSchemaVersion,
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
			a, image, sessionSink, timerSink, handler := boundLogAdapterWithTimerSink(t)
			actionSink := &logAdapterActionSinkSpy{}
			a.setClusterActionSink(actionSink)
			writeAndPollOne(t, a, image, tt.payload(t))
			requireSessionSinkIdle(t, sessionSink)
			require.Equal(t, 0, timerSink.events)
			require.Equal(t, 0, actionSink.actions)
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
