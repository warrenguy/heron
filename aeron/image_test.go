package aeron_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

func TestImage_newImage_recordsJoinPosition(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, aeron.TestSubscriberPositionAtJoin)

	require.Equal(t, aeron.TestSubscriberPositionAtJoin, image.JoinPosition())
	require.Equal(t, aeron.TestSubscriberPositionAtJoin, image.Position())
}

func TestImage_accessorsReturnWiredFields(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, aeron.TestSubscriberPositionAtJoin)

	require.Equal(t, aeron.TestImageCorrelationID, image.CorrelationID())
	require.Equal(t, aeron.TestPublicationSessionID, image.SessionID())
	require.Equal(t, aeron.TestPublicationStreamID, image.StreamID())
	require.Equal(t, aeron.TestImageSourceIdentity, image.SourceIdentity())
	require.Equal(t, "test-image.log", image.LogFileName())
	require.Equal(t, aeron.TestSubscriberPositionCounterID, image.SubscriberPositionID())
	require.False(t, image.IsClosed())
}

func TestImage_Position_tracksSubscriberCounter(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, aeron.TestSubscriberPositionAtJoin)

	const updatedPosition int64 = 512
	aeron.ImageSetSubscriberPositionForTest(image, updatedPosition)

	require.Equal(t, aeron.TestSubscriberPositionAtJoin, image.JoinPosition())
	require.Equal(t, updatedPosition, image.Position())
}

func TestImage_internalClose_closedSemantics(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, aeron.TestSubscriberPositionAtJoin)

	require.Equal(t, 0, image.Poll(nil, 1))

	const positionAtClose int64 = 256
	aeron.ImageSetSubscriberPositionForTest(image, positionAtClose)
	aeron.ImageInternalCloseForTest(image)

	require.True(t, image.IsClosed())
	require.Equal(t, positionAtClose, image.Position())
	require.Zero(t, aeron.ImageTermBuffer0CapacityForTest(image))
	require.Zero(t, aeron.ImageHeaderBufferCapacityForTest(image))

	aeron.ImageSetSubscriberPositionForTest(image, 512)
	require.Equal(t, positionAtClose, image.Position())
	require.Equal(t, 0, image.Poll(nil, 1))
}

func TestImage_internalClose_idempotent(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, 100)

	aeron.ImageSetSubscriberPositionForTest(image, 100)
	aeron.ImageInternalCloseForTest(image)

	aeron.ImageSetSubscriberPositionForTest(image, 200)
	aeron.ImageInternalCloseForTest(image)

	require.Equal(t, int64(100), image.Position())
}

func TestImage_Header_readsCommittedDataFrame(t *testing.T) {
	t.Run("zeroTermOffset", func(t *testing.T) {
		image, frameLength := aeron.NewImageWithCommittedDataFrameForTest(t, 0, 0)

		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		header := aeron.ImageHeaderForTest(image)
		header.SetBuffer(termBuffer)
		header.SetOffset(0)

		require.Equal(t, frameLength, header.FrameLength())
		require.Equal(t, aeron.TestPublicationSessionID, header.SessionID())
		require.Equal(t, aeron.TestPublicationStreamID, header.StreamID())
		require.Equal(t, aeron.TestPublicationInitialTermID, header.TermID())
		require.Equal(t, int32(0), header.TermOffset())
		require.Equal(t, int32(logbuffer.HdrTypeData), header.Type())
		require.Equal(t, byte(logbuffer.Unfragmented), header.Flags())
		require.Equal(t, int64(logbuffer.DefaultReservedValue), header.ReservedValue())
		require.Equal(t, 0, header.Offset())
		require.Equal(
			t,
			int64(logbuffer.AlignFrameLength(int(frameLength))),
			header.Position(),
		)
	})

	t.Run("nonZeroTermOffset", func(t *testing.T) {
		const frameOffset = 64
		image, frameLength := aeron.NewImageWithCommittedDataFrameForTest(t, 0, frameOffset)

		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		header := aeron.ImageHeaderForTest(image)
		header.SetBuffer(termBuffer)
		header.SetOffset(frameOffset)

		expectedPosition := int64(
			logbuffer.AlignFrameLength(int(frameOffset) + int(frameLength)),
		)
		require.Equal(t, expectedPosition, header.Position())
		require.Equal(t, int32(frameOffset), header.TermOffset())
		require.Equal(
			t,
			int32(logbuffer.AlignFrameLength(int(frameOffset)+int(frameLength))),
			header.NextTermOffset(),
		)
	})
}

func TestImage_newImage_errors(t *testing.T) {
	sub := testSubscription(t)
	event := aeron.AvailableImageForTest()

	logBuffers := aeron.NewTestLogBuffersForTest(t, aeron.DefaultTestPublicationLogConfig())

	activeCounters := aeron.NewTestCountersReaderWithSubscriberPositionForTest(
		t,
		aeron.TestSubscriberPositionAtJoin,
	)

	closedLog := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestPublicationLogConfig())
	closedLogBuffers, err := logbuffer.NewLogBuffers(closedLog)
	require.NoError(t, err)
	require.NoError(t, closedLog.Close())

	inactiveCounters := aeron.NewTestCountersReaderLimitOnlyForTest(t, 0)

	tests := []struct {
		name         string
		sub          *aeron.Subscription
		logBuffers   *logbuffer.LogBuffers
		counters     *cnc.CountersReader
		event        conductor.AvailableImage
		wantContains string
	}{
		{
			name:         "nilSubscription",
			sub:          nil,
			logBuffers:   logBuffers,
			counters:     activeCounters,
			event:        event,
			wantContains: "subscription is nil",
		},
		{
			name:         "nilLogBuffers",
			sub:          sub,
			logBuffers:   nil,
			counters:     activeCounters,
			event:        event,
			wantContains: "log buffers is nil",
		},
		{
			name:         "nilCountersReader",
			sub:          sub,
			logBuffers:   logBuffers,
			counters:     nil,
			event:        event,
			wantContains: "counters reader is nil",
		},
		{
			name:         "inactiveSubscriberCounter",
			sub:          sub,
			logBuffers:   logBuffers,
			counters:     inactiveCounters,
			event:        event,
			wantContains: "subscriber position counter",
		},
		{
			name:         "outOfRangeSubscriberCounter",
			sub:          sub,
			logBuffers:   logBuffers,
			counters:     activeCounters,
			event:        aeron.AvailableImageWithSubscriberPositionIDForTest(99),
			wantContains: "subscriber position counter",
		},
		{
			name:         "closedLogBuffers",
			sub:          sub,
			logBuffers:   closedLogBuffers,
			counters:     activeCounters,
			event:        event,
			wantContains: "closed mapped log buffer",
		},
		{
			name:       "mismatchedSubscriberCounterRegistration",
			sub:        sub,
			logBuffers: logBuffers,
			counters: aeron.NewTestCountersReaderWithSubscriberPositionForTest(
				t,
				aeron.TestSubscriberPositionAtJoin,
				aeron.TestSubscriptionRegistrationID+1,
			),
			event:        event,
			wantContains: "subscriber position counter",
		},
		{
			name:       "mismatchedSubscriptionRegistration",
			sub:        sub,
			logBuffers: logBuffers,
			counters:   activeCounters,
			event: func() conductor.AvailableImage {
				mismatch := event
				mismatch.SubscriptionRegistrationID = aeron.TestSubscriptionRegistrationID + 99
				return mismatch
			}(),
			wantContains: "subscription registration",
		},
		{
			name:       "mismatchedSubscriptionStream",
			sub:        sub,
			logBuffers: logBuffers,
			counters:   activeCounters,
			event: func() conductor.AvailableImage {
				mismatch := event
				mismatch.StreamID = aeron.TestPublicationStreamID + 99
				return mismatch
			}(),
			wantContains: "subscription stream",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := aeron.NewImageDirectForTest(
				tc.sub,
				tc.event,
				tc.logBuffers,
				tc.counters,
			)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantContains)
		})
	}
}

func TestImage_newImage_metadataErrors(t *testing.T) {
	sub := testSubscription(t)

	tests := []struct {
		name         string
		mutate       func(atomic.AtomicBuffer)
		wantContains string
	}{
		{
			name: "parseMetadataFailure",
			mutate: func(meta atomic.AtomicBuffer) {
				meta.PutInt32(logbuffer.OffsetPageSize, 3)
			},
			wantContains: "page size",
		},
		{
			name: "metadataTermLengthMismatch",
			mutate: func(meta atomic.AtomicBuffer) {
				meta.PutInt32(logbuffer.OffsetTermLength, aeron.TestPublicationTermLength*2)
			},
			wantContains: "metadata term length",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logBuffers := aeron.NewTestLogBuffersForTest(t, aeron.DefaultTestPublicationLogConfig())

			meta, err := logBuffers.MetaDataBuffer()
			require.NoError(t, err)
			tc.mutate(meta)

			counters := aeron.NewTestCountersReaderWithSubscriberPositionForTest(
				t,
				aeron.TestSubscriberPositionAtJoin,
			)

			_, err = aeron.NewImageDirectForTest(sub, aeron.AvailableImageForTest(), logBuffers, counters)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantContains)
		})
	}
}

func TestImage_Poll_singleDataFrame(t *testing.T) {
	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	payload := []byte("hello")
	writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, payload)

	fragments := image.Poll(capture.handler, 1)

	require.Equal(t, 1, fragments)
	require.Len(t, capture.payloads, 1)
	require.Equal(t, payload, capture.payloads[0])
	require.Len(t, capture.headers, 1)
	require.Equal(t, int32(logbuffer.HdrTypeData), capture.headers[0].Type())
	require.Equal(t, int64(logbuffer.AlignFragmentChunkLength(len(payload))), image.Position())
}

func TestImage_Poll_multipleFrames(t *testing.T) {
	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	payload := []byte("x")
	const frameCount = 5
	endOffset := writeDataFrameChain(t, termBuffer, 0, frameCount, payload)

	fragments := image.Poll(capture.handler, frameCount)

	require.Equal(t, frameCount, fragments)
	require.Len(t, capture.payloads, frameCount)
	for _, got := range capture.payloads {
		require.Equal(t, payload, got)
	}
	require.Equal(t, int64(endOffset), image.Position())
}

func TestImage_Poll_stopsOnUncommitted(t *testing.T) {
	payload := []byte("committed")
	nextOffset := logbuffer.AlignFragmentChunkLength(len(payload))

	tests := []struct {
		name      string
		blockNext func(t *testing.T, termBuffer atomic.AtomicBuffer)
	}{
		{
			name: "zeroFrameLength",
			blockNext: func(t *testing.T, termBuffer atomic.AtomicBuffer) {
				require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuffer, nextOffset))
			},
		},
		{
			name: "negativeFrameLength",
			blockNext: func(_ *testing.T, termBuffer atomic.AtomicBuffer) {
				logbuffer.PutFrameLengthRelease(termBuffer, nextOffset, -int32(nextOffset))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			image, capture := newPollImage(t, 0)
			termBuffer := aeron.ImageTermBuffer0ForTest(image)
			writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, payload)
			tc.blockNext(t, termBuffer)

			fragments := image.Poll(capture.handler, 10)

			require.Equal(t, 1, fragments)
			require.Equal(t, int64(nextOffset), image.Position())
		})
	}
}

func TestImage_Poll_advancesSubscriberCounter(t *testing.T) {
	const joinPosition int64 = 0
	sub := testSubscription(t)
	logBuffers := aeron.NewTestLogBuffersForTest(t, aeron.DefaultTestPublicationLogConfig())
	counters := aeron.NewTestCountersReaderWithSubscriberPositionForTest(t, joinPosition)
	image, err := aeron.NewImageDirectForTest(sub, aeron.AvailableImageForTest(), logBuffers, counters)
	require.NoError(t, err)
	capture := &pollCapture{}
	termBuffer := aeron.ImageTermBuffer0ForTest(image)

	payload := []byte("counter")
	writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, payload)
	advance := int64(logbuffer.AlignFragmentChunkLength(len(payload)))

	fragments := image.Poll(capture.handler, 1)

	require.Equal(t, 1, fragments)
	gotCounter, err := counters.CounterValue(aeron.TestSubscriberPositionCounterID)
	require.NoError(t, err)
	require.Equal(t, joinPosition+advance, gotCounter)
	require.Equal(t, joinPosition+advance, image.Position())
}

func TestImage_Poll_joinPositionImmutable(t *testing.T) {
	const joinPosition int64 = 0
	image, capture := newPollImage(t, joinPosition)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, []byte("x"))

	require.Equal(t, joinPosition, image.JoinPosition())
	_ = image.Poll(capture.handler, 1)
	require.Equal(t, joinPosition, image.JoinPosition())
}

func TestImage_Poll_closedReturnsZero(t *testing.T) {
	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, []byte("closed"))

	positionBeforeClose := image.Position()
	aeron.ImageInternalCloseForTest(image)

	require.True(t, image.IsClosed())
	require.Equal(t, 0, image.Poll(capture.handler, 1))
	require.Empty(t, capture.payloads)
	require.Equal(t, positionBeforeClose, image.Position())
}

func TestImage_Poll_fragmentLimit(t *testing.T) {
	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	const totalFrames = 10
	const fragmentLimit = 3
	payload := []byte("a")
	frameSpan := logbuffer.AlignFragmentChunkLength(len(payload))
	writeDataFrameChain(t, termBuffer, 0, totalFrames, payload)

	fragments := image.Poll(capture.handler, fragmentLimit)

	require.Equal(t, fragmentLimit, fragments)
	require.Len(t, capture.payloads, fragmentLimit)
	require.Equal(t, int64(frameSpan*fragmentLimit), image.Position())
}

func TestImage_Poll_termEnd(t *testing.T) {
	const termLength = aeron.TestPublicationTermLength
	const lastFrameOffset = termLength - pollTestTailFrameLength
	image, capture := newPollImage(t, int64(lastFrameOffset))
	term0 := aeron.ImageTermBuffer0ForTest(image)
	payload := []byte("term0-tail")
	writePollTestDataFrame(
		term0,
		lastFrameOffset,
		int32(lastFrameOffset),
		logbuffer.Unfragmented,
		payload,
	)

	fragments := image.Poll(capture.handler, 1)

	require.Equal(t, 1, fragments)
	require.Equal(t, payload, capture.payloads[0])
	require.Equal(t, int64(termLength), image.Position())

	term1 := aeron.ImageTermBufferForTest(image, 1)
	nextPayload := []byte("term1-head")
	aeron.WriteCommittedDataFrameForTest(
		term1,
		0,
		aeron.TestPublicationSessionID,
		aeron.TestPublicationStreamID,
		aeron.TestPublicationInitialTermID+1,
		0,
		logbuffer.Unfragmented,
		nextPayload,
	)

	capture.reset()
	fragments = image.Poll(capture.handler, 1)

	require.Equal(t, 1, fragments)
	require.Equal(t, nextPayload, capture.payloads[0])
	require.Equal(
		t,
		int64(termLength)+int64(logbuffer.AlignFragmentChunkLength(len(nextPayload))),
		image.Position(),
	)
}

func TestImage_Poll_nilHandlerAndZeroLimit(t *testing.T) {
	image, capture := newPollImage(t, 0)
	writePollTestPaddingThenData(
		aeron.ImageTermBuffer0ForTest(image),
		pollTestPaddingLength,
		[]byte("x"),
	)

	for _, tc := range []struct {
		name    string
		handler func(atomic.AtomicBuffer, int, int, *aeron.Header)
		limit   int
	}{
		{name: "nilHandler", handler: nil, limit: 1},
		{name: "zeroLimit", handler: capture.handler, limit: 0},
		{name: "negativeLimit", handler: capture.handler, limit: -1},
	} {
		require.Equal(t, 0, image.Poll(tc.handler, tc.limit), tc.name)
		require.Zero(t, image.Position(), tc.name)
	}
}

func TestImage_Poll_handlerPanic(t *testing.T) {
	const wantErr = "poll handler failed"
	panicPayload := []byte("panic")
	pollPanicHandler := func(atomic.AtomicBuffer, int, int, *aeron.Header) {
		panic(errors.New(wantErr))
	}

	t.Run("firstFrameWithErrorHandler", func(t *testing.T) {
		var handled error
		image := aeron.NewImageForTestWithErrorHandler(
			t,
			testSubscription(t),
			0,
			func(err error) {
				handled = err
			},
		)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, panicPayload)
		nextOffset := logbuffer.AlignFragmentChunkLength(len(panicPayload))
		writePollTestDataFrame(
			termBuffer,
			nextOffset,
			int32(nextOffset),
			logbuffer.Unfragmented,
			[]byte("after"),
		)

		fragments := image.Poll(pollPanicHandler, 10)

		require.Zero(t, fragments)
		require.Zero(t, image.Position())
		require.Error(t, handled)
		require.Equal(t, wantErr, handled.Error())

		capture := &pollCapture{}
		fragments = image.Poll(capture.handler, 1)
		require.Equal(t, 1, fragments)
		require.Equal(t, panicPayload, capture.payloads[0])
	})

	t.Run("afterPaddingPartialAdvance", func(t *testing.T) {
		var handled error
		image := aeron.NewImageForTestWithErrorHandler(
			t,
			testSubscription(t),
			0,
			func(err error) {
				handled = err
			},
		)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		writePollTestPaddingThenData(termBuffer, pollTestPaddingLength, panicPayload)

		fragments := image.Poll(pollPanicHandler, 10)

		require.Zero(t, fragments)
		require.Equal(t, int64(pollTestPaddingLength), image.Position())
		require.Error(t, handled)
		require.Equal(t, wantErr, handled.Error())
	})

	t.Run("nthFrameAfterSuccess", func(t *testing.T) {
		var handled error
		image := aeron.NewImageForTestWithErrorHandler(
			t,
			testSubscription(t),
			0,
			func(err error) {
				handled = err
			},
		)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		chainPayload := []byte("a")
		writeDataFrameChain(t, termBuffer, 0, 2, chainPayload)
		firstSpan := logbuffer.AlignFragmentChunkLength(len(chainPayload))

		attempts := 0
		nthFramePanicHandler := func(atomic.AtomicBuffer, int, int, *aeron.Header) {
			attempts++
			if attempts == 2 {
				panic(errors.New(wantErr))
			}
		}

		fragments := image.Poll(nthFramePanicHandler, 10)

		require.Equal(t, 1, fragments)
		require.Equal(t, int64(firstSpan), image.Position())
		require.Error(t, handled)
		require.Equal(t, wantErr, handled.Error())

		capture := &pollCapture{}
		fragments = image.Poll(capture.handler, 1)
		require.Equal(t, 1, fragments)
		require.Equal(t, chainPayload, capture.payloads[0])
	})

	t.Run("nilErrorHandler", func(t *testing.T) {
		image := aeron.NewImageForTest(t, testSubscription(t), 0)
		writePollTestDataFrame(
			aeron.ImageTermBuffer0ForTest(image),
			0,
			0,
			logbuffer.Unfragmented,
			panicPayload,
		)

		require.Panics(t, func() {
			_ = image.Poll(pollPanicHandler, 1)
		})
	})

	t.Run("errorHandlerPanicDoesNotEscape", func(t *testing.T) {
		image := aeron.NewImageForTestWithErrorHandler(
			t,
			testSubscription(t),
			0,
			func(error) {
				panic("error handler panic")
			},
		)
		writePollTestDataFrame(
			aeron.ImageTermBuffer0ForTest(image),
			0,
			0,
			logbuffer.Unfragmented,
			panicPayload,
		)

		fragments := image.Poll(pollPanicHandler, 1)

		require.Zero(t, fragments)
		require.Zero(t, image.Position())
	})

	t.Run("nonErrorPanicWrap", func(t *testing.T) {
		const panicValue = "boom"
		var handled error
		image := aeron.NewImageForTestWithErrorHandler(
			t,
			testSubscription(t),
			0,
			func(err error) {
				handled = err
			},
		)
		writePollTestDataFrame(
			aeron.ImageTermBuffer0ForTest(image),
			0,
			0,
			logbuffer.Unfragmented,
			panicPayload,
		)

		nonErrorPanicHandler := func(atomic.AtomicBuffer, int, int, *aeron.Header) {
			panic(panicValue)
		}

		fragments := image.Poll(nonErrorPanicHandler, 1)

		require.Zero(t, fragments)
		require.Error(t, handled)
		require.Equal(t, "aeron: fragment handler panic: boom", handled.Error())
	})
}

func TestImage_Poll_paddingOnlyAdvancesPosition(t *testing.T) {
	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	aeron.WriteCommittedPaddingFrameForTest(
		termBuffer,
		0,
		aeron.TestPublicationInitialTermID,
		0,
		pollTestWidePaddingLength,
	)

	fragments := image.Poll(capture.handler, 1)

	require.Zero(t, fragments)
	require.Empty(t, capture.payloads)
	require.Equal(t, int64(pollTestWidePaddingLength), image.Position())
}

func TestImage_Poll_fragmentLimitWithLeadingPadding(t *testing.T) {
	image, capture := newPollImage(t, 0)
	payload := []byte("data")
	writePollTestPaddingThenData(aeron.ImageTermBuffer0ForTest(image), pollTestPaddingLength, payload)

	fragments := image.Poll(capture.handler, 2)

	require.Equal(t, 1, fragments)
	require.Equal(t, payload, capture.payloads[0])
	require.Equal(
		t,
		int64(pollTestPaddingLength)+int64(logbuffer.AlignFragmentChunkLength(len(payload))),
		image.Position(),
	)
}

func TestImage_Poll_rejectsMidTermShortPadding(t *testing.T) {
	const (
		frameOffset = logbuffer.FrameAlignment * 2
		padLength   = int32(logbuffer.DataHeaderLength - 1)
		wantErr     = "aeron: corrupt committed frame at term offset 64 length 31"
	)
	image, capture, handledErr := newPollImageWithErrorHandler(t, int64(frameOffset))
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	aeron.WriteMinimalTermEndPaddingForTest(termBuffer, frameOffset, padLength)

	fragments := image.Poll(capture.handler, 1)

	require.Zero(t, fragments)
	require.Equal(t, int64(frameOffset), image.Position())
	require.Equal(t, wantErr, handledErr().Error())
}

func TestImage_Poll_closeSkipsCounterStore(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(t *testing.T, image *aeron.Image) aeron.FragmentHandler
		wantFragments int
	}{
		{
			name: "during padding advance",
			setup: func(t *testing.T, image *aeron.Image) aeron.FragmentHandler {
				t.Helper()
				aeron.WriteCommittedPaddingFrameForTest(
					aeron.ImageTermBuffer0ForTest(image),
					0,
					aeron.TestPublicationInitialTermID,
					0,
					pollTestPaddingLength,
				)
				aeron.SetPollAfterPaddingAdvanceForTest(func() {
					aeron.ImageInternalCloseForTest(image)
				})
				t.Cleanup(func() {
					aeron.SetPollAfterPaddingAdvanceForTest(nil)
				})
				capture := &pollCapture{}
				return capture.handler
			},
			wantFragments: 0,
		},
		{
			name: "during handler",
			setup: func(t *testing.T, image *aeron.Image) aeron.FragmentHandler {
				t.Helper()
				writePollTestDataFrame(
					aeron.ImageTermBuffer0ForTest(image),
					0,
					0,
					logbuffer.Unfragmented,
					[]byte("close-in-handler"),
				)
				return func(atomic.AtomicBuffer, int, int, *aeron.Header) {
					aeron.ImageInternalCloseForTest(image)
				}
			},
			wantFragments: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			image, _ := newPollImage(t, 0)
			positionBefore := image.Position()
			handler := tt.setup(t, image)

			fragments := image.Poll(handler, 1)

			require.Equal(t, tt.wantFragments, fragments)
			require.True(t, image.IsClosed())
			require.Equal(t, positionBefore, image.Position())
		})
	}
}

func TestImage_Poll_deliversFragmentedFramesIndividually(t *testing.T) {
	image, capture := newPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	fragments := []struct {
		flags   uint8
		payload []byte
	}{
		{flags: logbuffer.BeginFlag, payload: []byte("begin")},
		{flags: 0, payload: []byte("middle")},
		{flags: logbuffer.EndFlag, payload: []byte("end")},
	}

	offset := 0
	for _, fragment := range fragments {
		aeron.WriteCommittedFragmentedDataFrameForTest(
			termBuffer,
			offset,
			aeron.TestPublicationSessionID,
			aeron.TestPublicationStreamID,
			aeron.TestPublicationInitialTermID,
			int32(offset),
			fragment.flags,
			fragment.payload,
		)
		offset += logbuffer.AlignFragmentChunkLength(len(fragment.payload))
	}

	got := image.Poll(capture.handler, len(fragments))

	require.Equal(t, len(fragments), got)
	require.Len(t, capture.payloads, len(fragments))
	require.Equal(t, fragments[0].payload, capture.payloads[0])
	require.Equal(t, fragments[1].payload, capture.payloads[1])
	require.Equal(t, fragments[2].payload, capture.payloads[2])
	require.Equal(t, byte(logbuffer.BeginFlag), capture.flags[0]&byte(logbuffer.BeginFlag))
	require.Zero(t, capture.flags[0]&byte(logbuffer.EndFlag))
	require.Zero(t, capture.flags[1]&(byte(logbuffer.BeginFlag)|byte(logbuffer.EndFlag)))
	require.Equal(t, byte(logbuffer.EndFlag), capture.flags[2]&byte(logbuffer.EndFlag))
	require.Zero(t, capture.flags[2]&byte(logbuffer.BeginFlag))
}

func TestImage_Poll_singleTermScanDoesNotSpanTerms(t *testing.T) {
	image, capture := newPollImage(t, 0)
	term0Payload := []byte("t0")
	writeDataFrameChain(t, aeron.ImageTermBuffer0ForTest(image), 0, 2, term0Payload)
	writeDataFrameChain(t, aeron.ImageTermBufferForTest(image, 1), 0, 2, []byte("t1"))

	fragments := image.Poll(capture.handler, 100)

	require.Equal(t, 2, fragments)
	require.Len(t, capture.payloads, 2)
	require.Equal(t, term0Payload, capture.payloads[0])
	require.Equal(t, term0Payload, capture.payloads[1])
	require.Equal(t, int64(2*logbuffer.AlignFragmentChunkLength(len(term0Payload))), image.Position())
}

func TestImage_Poll_rejectsOversizedTailPadding(t *testing.T) {
	const termLength = aeron.TestPublicationTermLength
	const tailOffset = termLength - logbuffer.FrameAlignment // 32 bytes remain; PAD claims 64
	image, capture := newPollImage(t, int64(tailOffset))
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	aeron.WriteCommittedPaddingFrameForTest(
		termBuffer,
		tailOffset,
		aeron.TestPublicationInitialTermID,
		int32(tailOffset),
		pollTestTailFrameLength,
	)

	fragments := image.Poll(capture.handler, 1)

	require.Zero(t, fragments)
	require.Equal(t, int64(tailOffset), image.Position())
}

func TestImage_Poll_rejectsCorruptFrameLength(t *testing.T) {
	const wantErr = "aeron: corrupt committed frame at term offset 0 length 31"
	image, capture, handledErr := newPollImageWithErrorHandler(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	logbuffer.PutFrameType(termBuffer, 0, logbuffer.HdrTypeData)
	logbuffer.PutFrameLengthOrdered(termBuffer, 0, int32(logbuffer.DataHeaderLength-1))

	fragments := image.Poll(capture.handler, 1)

	require.Zero(t, fragments)
	require.Zero(t, image.Position())
	require.Error(t, handledErr())
	require.Equal(t, wantErr, handledErr().Error())
}

func TestImage_Poll_consumesSubHeaderTermEndGap(t *testing.T) {
	const termLength = aeron.TestPublicationTermLength
	const slackOffset = termLength - 1

	image, capture := newPollImage(t, int64(slackOffset))
	term0 := aeron.ImageTermBuffer0ForTest(image)
	payload := []byte("term0-before-slack")
	writePollTestDataFrame(term0, 0, 0, logbuffer.Unfragmented, payload)

	fragments := image.Poll(capture.handler, 1)

	require.Zero(t, fragments)
	require.Empty(t, capture.payloads)
	require.Equal(t, int64(termLength), image.Position())

	term1 := aeron.ImageTermBufferForTest(image, 1)
	nextPayload := []byte("term1-after-slack")
	writePollTestDataFrame(
		term1,
		0,
		0,
		logbuffer.Unfragmented,
		nextPayload,
	)

	capture.reset()
	fragments = image.Poll(capture.handler, 1)

	require.Equal(t, 1, fragments)
	require.Equal(t, nextPayload, capture.payloads[0])
	require.Equal(
		t,
		int64(termLength)+int64(logbuffer.AlignFragmentChunkLength(len(nextPayload))),
		image.Position(),
	)
}

func TestImage_Poll_consumesTermEndPadding(t *testing.T) {
	const termLength = aeron.TestPublicationTermLength

	tests := []struct {
		name          string
		padLength     int32
		setup         func(termBuffer atomic.AtomicBuffer, paddingOffset int)
		assertPrePoll func(t *testing.T, termBuffer atomic.AtomicBuffer, paddingOffset int)
	}{
		{
			name:      "slack1",
			padLength: 1,
			setup: func(termBuffer atomic.AtomicBuffer, paddingOffset int) {
				aeron.WriteMinimalTermEndPaddingForTest(termBuffer, paddingOffset, 1)
			},
			assertPrePoll: func(t *testing.T, termBuffer atomic.AtomicBuffer, paddingOffset int) {
				require.Zero(t, logbuffer.FrameLengthVolatile(termBuffer, paddingOffset))
			},
		},
		{
			name:      "slack7",
			padLength: 7,
			setup: func(termBuffer atomic.AtomicBuffer, paddingOffset int) {
				aeron.WriteMinimalTermEndPaddingForTest(termBuffer, paddingOffset, 7)
			},
			assertPrePoll: func(t *testing.T, termBuffer atomic.AtomicBuffer, paddingOffset int) {
				require.Equal(t, int32(7), termBuffer.GetInt32(paddingOffset))
			},
		},
		{
			name:      "minimal",
			padLength: pollTestMinimalPadLength,
			setup: func(termBuffer atomic.AtomicBuffer, paddingOffset int) {
				aeron.WriteMinimalTermEndPaddingForTest(termBuffer, paddingOffset, pollTestMinimalPadLength)
			},
			assertPrePoll: func(t *testing.T, termBuffer atomic.AtomicBuffer, paddingOffset int) {
				require.Equal(t, pollTestMinimalPadLength, termBuffer.GetInt32(paddingOffset))
				require.Equal(t, int16(logbuffer.HdrTypePad), logbuffer.FrameType(termBuffer, paddingOffset))
			},
		},
		{
			name:      "full",
			padLength: pollTestTermEndFullPadLength,
			setup: func(termBuffer atomic.AtomicBuffer, paddingOffset int) {
				aeron.WriteCommittedPaddingFrameForTest(
					termBuffer,
					paddingOffset,
					aeron.TestPublicationInitialTermID,
					int32(paddingOffset),
					pollTestTermEndFullPadLength,
				)
			},
			assertPrePoll: func(t *testing.T, termBuffer atomic.AtomicBuffer, paddingOffset int) {
				require.Equal(t, pollTestTermEndFullPadLength, termBuffer.GetInt32(paddingOffset))
				require.True(t, logbuffer.IsPaddingFrame(termBuffer, paddingOffset))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			paddingOffset := termLength - int(tc.padLength)
			image, capture := newPollImage(t, int64(paddingOffset))
			termBuffer := aeron.ImageTermBuffer0ForTest(image)
			tc.setup(termBuffer, paddingOffset)
			tc.assertPrePoll(t, termBuffer, paddingOffset)

			fragments := image.Poll(capture.handler, 1)

			require.Zero(t, fragments)
			require.Empty(t, capture.payloads)
			require.Equal(t, int64(termLength), image.Position())
		})
	}
}

func TestImage_Poll_stopsAtCorruptFrameAfterCommitted(t *testing.T) {
	payload := []byte("ok")
	corruptOffset := logbuffer.AlignFragmentChunkLength(len(payload))
	const wantCorruptErr = "aeron: corrupt committed frame at term offset 64 length 31"

	assertStopsAtCorruptFrame := func(t *testing.T, image *aeron.Image, capture *pollCapture) {
		fragments := image.Poll(capture.handler, 10)
		require.Equal(t, 1, fragments)
		require.Equal(t, int64(corruptOffset), image.Position())

		fragments = image.Poll(capture.handler, 10)
		require.Zero(t, fragments)
		require.Equal(t, int64(corruptOffset), image.Position())
	}

	t.Run("withoutErrorHandler", func(t *testing.T) {
		image, capture := newPollImage(t, 0)
		writePollTestCommittedThenCorrupt(aeron.ImageTermBuffer0ForTest(image), payload, corruptOffset)
		assertStopsAtCorruptFrame(t, image, capture)
	})

	t.Run("withErrorHandler", func(t *testing.T) {
		var invocations int
		var handled error
		image := aeron.NewImageForTestWithErrorHandler(
			t,
			testSubscription(t),
			0,
			func(err error) {
				invocations++
				handled = err
			},
		)
		capture := &pollCapture{}
		writePollTestCommittedThenCorrupt(aeron.ImageTermBuffer0ForTest(image), payload, corruptOffset)
		assertStopsAtCorruptFrame(t, image, capture)
		require.Equal(t, 1, invocations)
		require.Error(t, handled)
		require.Equal(t, wantCorruptErr, handled.Error())
	})
}

func TestImage_Poll_rejectsAlignedSpanOverflow(t *testing.T) {
	const frameOffset = aeron.TestPublicationTermLength - 40
	const frameLength = int32(40)
	const wantErr = "aeron: corrupt committed frame at term offset 65496 length 40"
	var handled error
	image := aeron.NewImageForTestWithErrorHandler(
		t,
		testSubscription(t),
		int64(frameOffset),
		func(err error) {
			handled = err
		},
	)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	if err := logbuffer.WriteDataHeaderUnfragmented(
		termBuffer,
		frameOffset,
		aeron.TestPublicationSessionID,
		aeron.TestPublicationStreamID,
		aeron.TestPublicationInitialTermID,
		int32(frameOffset),
		logbuffer.Unfragmented,
	); err != nil {
		t.Fatal(err)
	}
	termBuffer.PutBytes(frameOffset+logbuffer.OffsetFramePayload, []byte{1, 2, 3, 4, 5, 6, 7, 8})
	logbuffer.PutFrameLengthOrdered(termBuffer, frameOffset, frameLength)

	capture := &pollCapture{}
	fragments := image.Poll(capture.handler, 1)

	require.Zero(t, fragments)
	require.Equal(t, int64(frameOffset), image.Position())
	require.Error(t, handled)
	require.Equal(t, wantErr, handled.Error())
}

func TestImage_Poll_headerFields(t *testing.T) {
	const frameOffset = pollTestNonZeroFrameOffset
	image, capture := newPollImage(t, int64(frameOffset))
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	payload := []byte("hdr")
	writePollTestDataFrame(
		termBuffer,
		frameOffset,
		int32(frameOffset),
		logbuffer.BeginFlag|logbuffer.EndFlag,
		payload,
	)

	fragments := image.Poll(capture.handler, 1)

	require.Equal(t, 1, fragments)
	header := capture.headers[0]
	require.Equal(t, aeron.TestPublicationSessionID, header.SessionID())
	require.Equal(t, aeron.TestPublicationStreamID, header.StreamID())
	require.Equal(t, aeron.TestPublicationInitialTermID, header.TermID())
	require.Equal(t, int32(frameOffset), header.TermOffset())
	require.Equal(t, byte(logbuffer.Unfragmented), header.Flags())
	require.Equal(
		t,
		int64(frameOffset+logbuffer.AlignFragmentChunkLength(len(payload))),
		header.Position(),
	)
}
