//go:build testhooks

package aeron

import (
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/atomicbuffer"
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

// testHookOfferClaim is the Offer+TryClaim surface shared by hook publication builders.
type testHookOfferClaim interface {
	Offer(buffer []byte) int64
	TryClaim(length int) (termIndex, frameOffset int, position int64, err error)
}

var testHookLimitZeroRetriableCases = []struct {
	name      string
	connected bool
	want      int64
}{
	{name: "limit 0 disconnected yields NotConnected", connected: false, want: NotConnected},
	{name: "limit 0 connected yields BackPressured", connected: true, want: BackPressured},
}

func requireTestHookLimitZeroRetriable(
	t *testing.T,
	offerer testHookOfferClaim,
	want int64,
) {
	t.Helper()

	require.Equal(t, want, offerer.Offer([]byte{1}))
	_, _, position, err := offerer.TryClaim(1)
	require.NoError(t, err)
	require.Equal(t, want, position)
}

func TestNewTestExclusivePublicationForHooksWithLimit_retriableCodes(t *testing.T) {
	for _, tt := range testHookLimitZeroRetriableCases {
		t.Run(tt.name, func(t *testing.T) {
			ep := NewTestExclusivePublicationForHooksWithLimit(t, 0, tt.connected)
			requireTestHookLimitZeroRetriable(t, ep, tt.want)
		})
	}
}

func TestNewTestExclusivePublicationForHooksWithConnected_highLimitSucceedsWhenDisconnected(
	t *testing.T,
) {
	// High default limit: disconnected alone is not a NotConnected recipe.
	ep := NewTestExclusivePublicationForHooksWithConnected(t, false)

	got := ep.Offer([]byte{1})
	require.Positive(t, got)
}

func TestNewTestPublicationForHooksWithLimit_retriableCodes(t *testing.T) {
	for _, tt := range testHookLimitZeroRetriableCases {
		t.Run(tt.name, func(t *testing.T) {
			pub := NewTestPublicationForHooksWithLimit(t, 0, tt.connected)
			requireTestHookLimitZeroRetriable(t, pub, tt.want)
		})
	}
}

func TestNewTestPublicationForHooksWithConnected_highLimitSucceeds(t *testing.T) {
	// High default limit: disconnected alone is not a NotConnected recipe.
	tests := []struct {
		name      string
		connected bool
	}{
		{name: "disconnected", connected: false},
		{name: "connected", connected: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := NewTestPublicationForHooksWithConnected(t, tt.connected)
			require.Positive(t, pub.Offer([]byte{1}))
		})
	}
}

// addThroughHarness runs one add on streamID to completion: offer, driver
// response, poll.
func addThroughHarness[T any](
	t *testing.T,
	harness *AsyncConnectTestHarness,
	streamID int32,
	offer func(streamID int32, channel string) (int64, error),
	poll func(correlationID int64) (T, bool, error),
) T {
	t.Helper()

	correlationID, err := offer(streamID, testHookIPCChannel)
	require.NoError(t, err)
	harness.RespondPendingAdds()
	value, done, err := poll(correlationID)
	require.NoError(t, err)
	require.True(t, done)
	return value
}

func TestAsyncConnectTestHarness_transmitAvailableImage(t *testing.T) {
	harness := NewAsyncConnectTestHarness(t)
	sub := addThroughHarness(
		t,
		harness,
		testHookSubscriptionStreamID,
		harness.Client.OfferAddSubscription,
		harness.Client.PollAddSubscription,
	)

	// Phase: the transmit alone does not attach an image.
	harness.TransmitAvailableImage(t, sub)
	require.False(t, sub.IsConnected())
	_, ok := sub.ImageAt(0)
	require.False(t, ok)

	// Phase: one conductor tick attaches the image.
	harness.DoWork()
	require.True(t, sub.IsConnected())
	image, ok := sub.ImageAt(0)
	require.True(t, ok)
	require.Equal(t, testHookImageCorrelationID, image.CorrelationID())

	// Phase: a publication added after the reader swap offers. Its limit
	// counter survives the swap.
	pub := addThroughHarness(
		t,
		harness,
		testHookPublicationStreamID,
		harness.Client.OfferAddExclusivePublication,
		harness.Client.PollAddExclusivePublication,
	)
	require.Positive(t, pub.Offer([]byte{1}))
}

func TestExclusivePublication_sendDestinationHook(t *testing.T) {
	const channel = "aeron:udp?endpoint=127.0.0.1:9104"
	addErr := errors.New("add refused")
	removeErr := errors.New("remove refused")

	tests := []struct {
		name string
		call func(*ExclusivePublication, string) error
		want error
		got  func(*RecordingReceiveDestinationHook) []string
	}{
		{
			name: "AddDestination",
			call: (*ExclusivePublication).AddDestination,
			want: addErr,
			got:  func(h *RecordingReceiveDestinationHook) []string { return h.Adds },
		},
		{
			name: "RemoveDestination",
			call: (*ExclusivePublication).RemoveDestination,
			want: removeErr,
			got:  func(h *RecordingReceiveDestinationHook) []string { return h.Removes },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ep := new(ExclusivePublication)
			hook := &RecordingReceiveDestinationHook{AddErr: addErr, RemoveErr: removeErr}
			ExclusivePublicationSetSendDestinationHookForHooks(ep, hook)

			require.ErrorIs(t, tc.call(ep, channel), tc.want)
			require.Equal(t, []string{channel}, tc.got(hook))
		})
		t.Run(tc.name+" unset", func(t *testing.T) {
			ep := new(ExclusivePublication)

			require.ErrorIs(t, tc.call(ep, channel), ErrClientNotBound)
		})
	}
}
