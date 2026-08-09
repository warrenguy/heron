package aeron_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

var fragmentBytesAdapters = []struct {
	name           string
	adapt          func(aeron.FragmentHandlerBytes) aeron.FragmentHandler
	cloneOnCapture bool
	lifetimeAssert func(
		t *testing.T,
		termBuffer atomicbuffer.AtomicBuffer,
		gotPayload []byte,
	)
}{
	{
		name:           "view",
		adapt:          aeron.AdaptFragmentHandlerBytes,
		cloneOnCapture: true,
		lifetimeAssert: func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, gotPayload []byte) {
			raw := termBuffer.Bytes()
			raw[logbuffer.OffsetFramePayload] = 'X'
			require.Equal(t, byte('X'), gotPayload[0])
		},
	},
	{
		name:           "retain",
		adapt:          aeron.AdaptFragmentHandlerBytesRetain,
		cloneOnCapture: false,
		lifetimeAssert: func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, gotPayload []byte) {
			raw := termBuffer.Bytes()
			raw[logbuffer.OffsetFramePayload] = 'Z'
			require.Equal(t, byte('a'), gotPayload[0])
		},
	},
}

func TestAdaptFragmentHandlerBytes_nilReturnsNil(t *testing.T) {
	sub := testSubscription(t)
	require.Equal(t, 0, sub.Poll(nil, 1))
	for _, adapter := range fragmentBytesAdapters {
		t.Run(adapter.name, func(t *testing.T) {
			require.Nil(t, adapter.adapt(nil))
			require.Equal(t, 0, sub.Poll(adapter.adapt(nil), 1))
		})
	}
}

func TestAdaptFragmentHandlerBytes_roundtripSubscriptionPoll(t *testing.T) {
	for _, adapter := range fragmentBytesAdapters {
		t.Run(adapter.name, func(t *testing.T) {
			sub := testSubscription(t)
			wantPayloads := [][]byte{[]byte("alpha"), []byte("beta")}
			addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, wantPayloads...)

			var gotPayloads [][]byte
			handler := adapter.adapt(func(payload []byte, _ *aeron.Header) {
				if adapter.cloneOnCapture {
					gotPayloads = append(gotPayloads, slices.Clone(payload))
					return
				}
				gotPayloads = append(gotPayloads, payload)
			})

			require.Equal(t, 2, sub.Poll(handler, 10))
			require.Equal(t, wantPayloads, gotPayloads)
		})
	}
}

func TestAdaptFragmentHandlerBytes_roundtripImagePoll(t *testing.T) {
	for _, adapter := range fragmentBytesAdapters {
		t.Run(adapter.name, func(t *testing.T) {
			image, _ := newPollImage(t, 0)
			termBuffer := aeron.ImageTermBuffer0ForTest(image)
			wantPayload := []byte("image-poll")
			writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, wantPayload)

			var gotPayload []byte
			handler := adapter.adapt(func(payload []byte, _ *aeron.Header) {
				if adapter.cloneOnCapture {
					gotPayload = slices.Clone(payload)
					return
				}
				gotPayload = payload
			})

			require.Equal(t, 1, image.Poll(handler, 1))
			require.Equal(t, wantPayload, gotPayload)
		})
	}
}

func TestAdaptFragmentHandlerBytes_zeroLengthPayloadNil(t *testing.T) {
	for _, adapter := range fragmentBytesAdapters {
		t.Run(adapter.name, func(t *testing.T) {
			sub := testSubscription(t)
			addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, []byte{})

			var gotPayload []byte
			var called bool
			handler := adapter.adapt(func(payload []byte, _ *aeron.Header) {
				called = true
				gotPayload = payload
			})

			require.Equal(t, 1, sub.Poll(handler, 1))
			require.True(t, called)
			require.Nil(t, gotPayload)
		})
	}
}

func TestAdaptFragmentHandlerBytes_invalidSpans(t *testing.T) {
	tests := []struct {
		name        string
		offset      int
		length      int
		wantPayload []byte
	}{
		{name: "zero length", offset: 0, length: 0},
		{name: "length past buffer end", offset: 0, length: 99},
		{name: "negative offset", offset: -1, length: 4},
		{name: "negative length", offset: 0, length: -1},
		{name: "offset past buffer end", offset: 7, length: 1},
		{name: "partial overrun past buffer end", offset: 5, length: 4},
		{name: "valid span", offset: 0, length: 7, wantPayload: []byte("payload")},
	}

	for _, adapter := range fragmentBytesAdapters {
		for _, tt := range tests {
			t.Run(adapter.name+"/"+tt.name, func(t *testing.T) {
				buf := atomicbuffer.Wrap([]byte("payload"))

				var gotPayload []byte
				var called bool
				handler := adapter.adapt(func(payload []byte, _ *aeron.Header) {
					called = true
					gotPayload = payload
				})

				handler(buf, tt.offset, tt.length, nil)

				require.True(t, called)
				require.Equal(t, tt.wantPayload, gotPayload)
			})
		}
	}
}

func TestAdaptFragmentHandlerBytes_payloadLifetime(t *testing.T) {
	for _, adapter := range fragmentBytesAdapters {
		t.Run(adapter.name, func(t *testing.T) {
			sub := testSubscription(t)
			image := addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, []byte("abcde"))
			termBuffer := aeron.ImageTermBuffer0ForTest(image)

			var gotPayload []byte
			handler := adapter.adapt(func(payload []byte, _ *aeron.Header) {
				gotPayload = payload
			})

			require.Equal(t, 1, sub.Poll(handler, 1))
			adapter.lifetimeAssert(t, termBuffer, gotPayload)
		})
	}
}

func TestAdaptFragmentHandlerBytes_headerFieldsCopyable(t *testing.T) {
	for _, adapter := range fragmentBytesAdapters {
		t.Run(adapter.name, func(t *testing.T) {
			sub := testSubscription(t)
			addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, []byte("msg"))

			var gotHeader aeron.Header
			handler := adapter.adapt(func(_ []byte, header *aeron.Header) {
				gotHeader = *header
			})

			require.Equal(t, 1, sub.Poll(handler, 1))
			require.Equal(t, aeron.TestPublicationSessionID, gotHeader.SessionID())
			require.Equal(t, aeron.TestPublicationStreamID, gotHeader.StreamID())
		})
	}
}

func TestAdaptControlledFragmentHandlerBytes_nilReturnsNil(t *testing.T) {
	require.Nil(t, aeron.AdaptControlledFragmentHandlerBytes(nil))
	image, _ := newControlledPollImage(t, 0)
	require.Equal(t, 0, image.ControlledPoll(nil, 1))
}

func TestAdaptControlledFragmentHandlerBytes_actionPassthrough(t *testing.T) {
	actions := []struct {
		name   string
		action aeron.ControlledAction
	}{
		{name: "abort", action: aeron.ControlledAbort},
		{name: "break", action: aeron.ControlledBreak},
		{name: "commit", action: aeron.ControlledCommit},
		{name: "continue", action: aeron.ControlledContinue},
	}
	buf := atomicbuffer.Wrap([]byte("payload"))
	for _, tc := range actions {
		t.Run(tc.name, func(t *testing.T) {
			var called bool
			handler := aeron.AdaptControlledFragmentHandlerBytes(
				func(payload []byte, _ *aeron.Header) aeron.ControlledAction {
					called = true
					require.Equal(t, []byte("payload"), payload)
					return tc.action
				},
			)
			require.NotNil(t, handler)
			got := handler(buf, 0, len("payload"), nil)
			require.True(t, called)
			require.Equal(t, tc.action, got)
		})
	}
}

func TestAdaptControlledFragmentHandlerBytes_nilPayloadStillInvokes(t *testing.T) {
	tests := []struct {
		name   string
		offset int
		length int
	}{
		{name: "zero length", offset: 0, length: 0},
		{name: "length past buffer end", offset: 0, length: 99},
		{name: "negative offset", offset: -1, length: 4},
		{name: "negative length", offset: 0, length: -1},
	}
	buf := atomicbuffer.Wrap([]byte("payload"))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var called bool
			var gotPayload []byte
			handler := aeron.AdaptControlledFragmentHandlerBytes(
				func(payload []byte, _ *aeron.Header) aeron.ControlledAction {
					called = true
					gotPayload = payload
					return aeron.ControlledContinue
				},
			)
			got := handler(buf, tt.offset, tt.length, nil)
			require.True(t, called)
			require.Nil(t, gotPayload)
			require.Equal(t, aeron.ControlledContinue, got)
		})
	}
}

func TestAdaptControlledFragmentHandlerBytes_roundtripControlledPoll(t *testing.T) {
	image, _ := newControlledPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	wantPayload := []byte("ctrl-poll")
	writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, wantPayload)

	var gotPayload []byte
	handler := aeron.AdaptControlledFragmentHandlerBytes(
		func(payload []byte, _ *aeron.Header) aeron.ControlledAction {
			gotPayload = slices.Clone(payload)
			return aeron.ControlledContinue
		},
	)

	require.Equal(t, 1, image.ControlledPoll(handler, 1))
	require.Equal(t, wantPayload, gotPayload)
}
