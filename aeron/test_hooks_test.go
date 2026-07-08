//go:build testhooks

package aeron

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

type testHookPollCapture struct {
	payloads [][]byte
	headers  []Header
}

func (capture *testHookPollCapture) handler(
	buffer atomicbuffer.AtomicBuffer,
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

func testHookTermBuffer(tb testing.TB) atomicbuffer.AtomicBuffer {
	tb.Helper()

	log := newTestHookMappedLog(tb, logbuffer.LogBufferTypeConcurrentPublication)
	termBuffers, err := log.TermBuffers()
	require.NoError(tb, err)
	return termBuffers[0]
}

func writeTestHookCommittedDataFrame(
	termBuffer atomicbuffer.AtomicBuffer,
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
