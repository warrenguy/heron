//go:build testhooks

package aeron

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

type testHookPollCapture struct {
	payloads [][]byte
	headers  []Header
}

func (capture *testHookPollCapture) handler(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	header *Header,
) {
	capture.payloads = append(capture.payloads, slices.Clone(buffer.Bytes()[offset:offset+length]))
	headerCopy := *header
	capture.headers = append(capture.headers, headerCopy)
}

func newTestHookSubscriptionWithImage(tb testing.TB) (*Subscription, *Image) {
	tb.Helper()

	sub := NewTestSubscriptionForHooks(tb)
	return sub, NewTestImageForHooks(tb, sub, 0)
}

func testHookTermBuffer(tb testing.TB) atomic.AtomicBuffer {
	tb.Helper()

	log := newTestHookMappedLog(tb, logbuffer.LogBufferTypeConcurrentPublication)
	termBuffers, err := log.TermBuffers()
	require.NoError(tb, err)
	return termBuffers[0]
}

func writeTestHookCommittedDataFrame(
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
	termOffset int32,
	flags uint8,
	payload []byte,
) {
	WriteCommittedDataFrameForHooks(
		termBuffer,
		frameOffset,
		testHookImageSessionID,
		testHookSubscriptionStreamID,
		testHookPublicationInitialTermID,
		termOffset,
		flags,
		payload,
	)
}

func requireHookFrameMatchesExportTest(
	tb testing.TB,
	frameOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
	flags uint8,
	payload []byte,
) {
	tb.Helper()

	fragmented := flags&logbuffer.Unfragmented != logbuffer.Unfragmented
	hookBuffer := testHookTermBuffer(tb)
	exportBuffer := testHookTermBuffer(tb)

	WriteCommittedDataFrameForHooks(
		hookBuffer,
		frameOffset,
		sessionID,
		streamID,
		termID,
		termOffset,
		flags,
		payload,
	)
	writeCommittedDataFrameForTest(
		exportBuffer,
		frameOffset,
		sessionID,
		streamID,
		termID,
		termOffset,
		flags,
		payload,
		fragmented,
	)

	exportFrameLength := logbuffer.FrameLengthVolatile(exportBuffer, frameOffset)
	hookFrameLength := logbuffer.FrameLengthVolatile(hookBuffer, frameOffset)
	require.Equal(tb, exportFrameLength, hookFrameLength)
	committedLen := int(exportFrameLength)
	require.Equal(
		tb,
		exportBuffer.Bytes()[frameOffset:frameOffset+committedLen],
		hookBuffer.Bytes()[frameOffset:frameOffset+committedLen],
	)
}

func TestSubscriptionAddImageForHooks_registration(t *testing.T) {
	sub, image := newTestHookSubscriptionWithImage(t)

	require.True(t, SubscriptionAddImageForHooks(sub, image))
	require.False(t, SubscriptionAddImageForHooks(sub, image))
	require.False(t, SubscriptionAddImageForHooks(sub, nil))
}

func TestNewTestImageForHooks_subscriptionPoll(t *testing.T) {
	sub, image := newTestHookSubscriptionWithImage(t)
	require.True(t, SubscriptionAddImageForHooks(sub, image))

	termBuffer := ImageTermBufferForHooks(image)
	payload := []byte("egress-hook")
	writeTestHookCommittedDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, payload)

	capture := &testHookPollCapture{}
	fragments := sub.Poll(capture.handler, 1)

	require.Equal(t, 1, fragments)
	require.Len(t, capture.payloads, 1)
	require.Equal(t, payload, capture.payloads[0])
	require.Len(t, capture.headers, 1)
	require.Equal(t, testHookImageSessionID, capture.headers[0].SessionID())
	require.Equal(t, testHookSubscriptionStreamID, capture.headers[0].StreamID())
	require.Equal(t, testHookPublicationInitialTermID, capture.headers[0].TermID())
	require.Equal(t, int32(0), capture.headers[0].TermOffset())
	require.Equal(t, byte(logbuffer.Unfragmented), capture.headers[0].Flags())
	require.Equal(
		t,
		int32(len(payload)+logbuffer.DataHeaderLength),
		capture.headers[0].FrameLength(),
	)
}

func TestWriteCommittedDataFrameForHooks_fragmentedBeginFlag(t *testing.T) {
	termBuffer := testHookTermBuffer(t)
	writeTestHookCommittedDataFrame(termBuffer, 0, 0, logbuffer.BeginFlag, []byte("fragment-begin"))

	require.Equal(t, byte(logbuffer.BeginFlag), logbuffer.FrameFlags(termBuffer, 0))
	require.NotEqual(t, byte(logbuffer.Unfragmented), logbuffer.FrameFlags(termBuffer, 0))
}

func TestWriteCommittedDataFrameForHooks_matchesExportTest(t *testing.T) {
	const frameOffset = 64

	cases := []struct {
		name    string
		flags   uint8
		payload []byte
	}{
		{
			name:    "unfragmented",
			flags:   logbuffer.Unfragmented,
			payload: []byte("whole-message"),
		},
		{
			name:    "begin_fragment",
			flags:   logbuffer.BeginFlag,
			payload: []byte("begin-only"),
		},
		{
			name:    "end_fragment",
			flags:   logbuffer.EndFlag,
			payload: []byte("end-only"),
		},
		{
			name:    "middle_fragment",
			flags:   0,
			payload: []byte("middle-only"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireHookFrameMatchesExportTest(
				t,
				frameOffset,
				TestPublicationSessionID,
				TestPublicationStreamID,
				TestPublicationInitialTermID,
				frameOffset,
				tc.flags,
				tc.payload,
			)
		})
	}
}
