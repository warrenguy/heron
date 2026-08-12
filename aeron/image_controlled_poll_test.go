package aeron_test

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestImage_ControlledPoll_guards(t *testing.T) {
	t.Run("closed", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		writePollTestDataFrame(
			aeron.ImageTermBuffer0ForTest(image),
			0,
			0,
			logbuffer.Unfragmented,
			[]byte("closed"),
		)
		positionBeforeClose := image.Position()
		aeron.ImageInternalCloseForTest(image)

		require.True(t, image.IsClosed())
		require.Equal(t, 0, image.ControlledPoll(capture.handler, 1))
		require.Empty(t, capture.payloads)
		require.Equal(t, positionBeforeClose, image.Position())
	})

	t.Run("nilHandlerAndNonPositiveLimit", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		writePollTestPaddingThenData(
			aeron.ImageTermBuffer0ForTest(image),
			pollTestPaddingLength,
			[]byte("x"),
		)

		for _, tc := range []struct {
			name    string
			handler aeron.ControlledFragmentHandler
			limit   int
		}{
			{name: "nilHandler", handler: nil, limit: 1},
			{name: "zeroLimit", handler: capture.handler, limit: 0},
			{name: "negativeLimit", handler: capture.handler, limit: -1},
		} {
			require.Equal(t, 0, image.ControlledPoll(tc.handler, tc.limit), tc.name)
			require.Zero(t, image.Position(), tc.name)
			require.Empty(t, capture.payloads, tc.name)
		}
	})

	t.Run("emptyTerm", func(t *testing.T) {
		// Non-zero join distinguishes no-store from a store of zero.
		const joinPosition int64 = 128
		image, counters := newDirectPollImage(t, joinPosition)
		capture := &controlledPollCapture{}

		fragments := image.ControlledPoll(capture.handler, 10)

		require.Zero(t, fragments)
		require.Empty(t, capture.payloads)
		require.Equal(t, joinPosition, image.Position())
		gotCounter, err := counters.CounterValue(aeron.TestSubscriberPositionCounterID)
		require.NoError(t, err)
		require.Equal(t, joinPosition, gotCounter)
	})
}

func TestImage_ControlledPoll_continue(t *testing.T) {
	t.Run("singleData", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		payload := []byte("hello")
		writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, payload)

		fragments := image.ControlledPoll(capture.handler, 1)

		require.Equal(t, 1, fragments)
		require.Len(t, capture.payloads, 1)
		require.Equal(t, payload, capture.payloads[0])
		require.Equal(t, int64(logbuffer.AlignFragmentChunkLength(len(payload))), image.Position())
	})

	t.Run("multiData", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		payload := []byte("x")
		const frameCount = 5
		endOffset := writeDataFrameChain(t, termBuffer, 0, frameCount, payload)

		fragments := image.ControlledPoll(capture.handler, frameCount)

		require.Equal(t, frameCount, fragments)
		require.Len(t, capture.payloads, frameCount)
		for _, got := range capture.payloads {
			require.Equal(t, payload, got)
		}
		require.Equal(t, int64(endOffset), image.Position())
	})

	t.Run("fragmentLimitStopsEarly", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		const totalFrames = 10
		const fragmentLimit = 3
		payload := []byte("a")
		frameSpan := logbuffer.AlignFragmentChunkLength(len(payload))
		writeDataFrameChain(t, termBuffer, 0, totalFrames, payload)

		fragments := image.ControlledPoll(capture.handler, fragmentLimit)

		require.Equal(t, fragmentLimit, fragments)
		require.Len(t, capture.payloads, fragmentLimit)
		require.Equal(t, int64(frameSpan*fragmentLimit), image.Position())
	})
}

func TestImage_ControlledPoll_abort(t *testing.T) {
	t.Run("firstFrame", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		payload := []byte("abort-me")
		frameSpan := logbuffer.AlignFragmentChunkLength(len(payload))
		writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, payload)
		writePollTestDataFrame(
			termBuffer,
			frameSpan,
			int32(frameSpan),
			logbuffer.Unfragmented,
			[]byte("after"),
		)
		capture.actions = []aeron.ControlledAction{aeron.ControlledAbort}

		fragments := image.ControlledPoll(capture.handler, 10)

		require.Zero(t, fragments)
		require.Len(t, capture.payloads, 1)
		require.Equal(t, payload, capture.payloads[0])
		require.Zero(t, image.Position())

		capture.reset()
		fragments = image.ControlledPoll(capture.handler, 1)
		require.Equal(t, 1, fragments)
		require.Len(t, capture.payloads, 1)
		require.Equal(t, payload, capture.payloads[0])
		require.Equal(t, int64(frameSpan), image.Position())
	})

	t.Run("secondFrameAfterContinue", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		payload := []byte("a")
		firstSpan := logbuffer.AlignFragmentChunkLength(len(payload))
		writeDataFrameChain(t, termBuffer, 0, 2, payload)
		capture.actions = []aeron.ControlledAction{
			aeron.ControlledContinue,
			aeron.ControlledAbort,
		}

		fragments := image.ControlledPoll(capture.handler, 10)

		require.Equal(t, 1, fragments)
		require.Len(t, capture.payloads, 2)
		require.Equal(t, payload, capture.payloads[0])
		require.Equal(t, payload, capture.payloads[1])
		require.Equal(t, int64(firstSpan), image.Position())

		capture.reset()
		fragments = image.ControlledPoll(capture.handler, 1)
		require.Equal(t, 1, fragments)
		require.Len(t, capture.payloads, 1)
		require.Equal(t, payload, capture.payloads[0])
		require.Equal(t, int64(2*firstSpan), image.Position())
	})
}

func TestImage_ControlledPoll_abortAfterPadding(t *testing.T) {
	image, capture := newControlledPollImage(t, 0)
	payload := []byte("data")
	writePollTestPaddingThenData(aeron.ImageTermBuffer0ForTest(image), pollTestPaddingLength, payload)
	capture.actions = []aeron.ControlledAction{aeron.ControlledAbort}

	fragments := image.ControlledPoll(capture.handler, 10)

	require.Zero(t, fragments)
	require.Len(t, capture.payloads, 1)
	require.Equal(t, payload, capture.payloads[0])
	require.Equal(t, int64(pollTestPaddingLength), image.Position())

	capture.reset()
	fragments = image.ControlledPoll(capture.handler, 1)
	require.Equal(t, 1, fragments)
	require.Len(t, capture.payloads, 1)
	require.Equal(t, payload, capture.payloads[0])
	require.Equal(
		t,
		int64(pollTestPaddingLength)+int64(logbuffer.AlignFragmentChunkLength(len(payload))),
		image.Position(),
	)
}

func TestImage_ControlledPoll_break(t *testing.T) {
	image, capture := newControlledPollImage(t, 0)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	payload := []byte("a")
	firstSpan := logbuffer.AlignFragmentChunkLength(len(payload))
	writeDataFrameChain(t, termBuffer, 0, 2, payload)
	capture.actions = []aeron.ControlledAction{aeron.ControlledBreak}

	fragments := image.ControlledPoll(capture.handler, 10)

	require.Equal(t, 1, fragments)
	require.Len(t, capture.payloads, 1)
	require.Equal(t, payload, capture.payloads[0])
	require.Equal(t, int64(firstSpan), image.Position())
}

func TestImage_ControlledPoll_commitEachFrame(t *testing.T) {
	const joinPosition int64 = 0
	image, counters := newDirectPollImage(t, joinPosition)

	payload := []byte("a")
	frameSpan := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	writeDataFrameChain(t, aeron.ImageTermBuffer0ForTest(image), 0, 2, payload)

	var positionsAtCall []int64
	handler := func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) aeron.ControlledAction {
		positionsAtCall = append(positionsAtCall, image.Position())
		return aeron.ControlledCommit
	}

	fragments := image.ControlledPoll(handler, 10)

	require.Equal(t, 2, fragments)
	require.Len(t, positionsAtCall, 2)
	require.Equal(t, joinPosition, positionsAtCall[0])
	require.Equal(t, joinPosition+frameSpan, positionsAtCall[1])
	require.Equal(t, joinPosition+2*frameSpan, image.Position())
	gotCounter, err := counters.CounterValue(aeron.TestSubscriberPositionCounterID)
	require.NoError(t, err)
	require.Equal(t, joinPosition+2*frameSpan, gotCounter)
}

func TestImage_ControlledPoll_commitMix(t *testing.T) {
	const joinPosition int64 = 0
	image, counters := newDirectPollImage(t, joinPosition)

	payload := []byte("a")
	frameSpan := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	writeDataFrameChain(t, aeron.ImageTermBuffer0ForTest(image), 0, 3, payload)

	var positionsAtCall []int64
	actions := []aeron.ControlledAction{
		aeron.ControlledContinue,
		aeron.ControlledCommit,
		aeron.ControlledContinue,
	}
	call := 0
	handler := func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) aeron.ControlledAction {
		positionsAtCall = append(positionsAtCall, image.Position())
		action := actions[call]
		call++
		return action
	}

	fragments := image.ControlledPoll(handler, 10)

	require.Equal(t, 3, fragments)
	require.Len(t, positionsAtCall, 3)
	require.Equal(t, joinPosition, positionsAtCall[0])
	require.Equal(t, joinPosition, positionsAtCall[1])
	require.Equal(t, joinPosition+2*frameSpan, positionsAtCall[2])
	require.Equal(t, joinPosition+3*frameSpan, image.Position())
	gotCounter, err := counters.CounterValue(aeron.TestSubscriberPositionCounterID)
	require.NoError(t, err)
	require.Equal(t, joinPosition+3*frameSpan, gotCounter)
}

func TestImage_ControlledPoll_padding(t *testing.T) {
	t.Run("paddingThenDataContinue", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		payload := []byte("data")
		writePollTestPaddingThenData(aeron.ImageTermBuffer0ForTest(image), pollTestPaddingLength, payload)

		fragments := image.ControlledPoll(capture.handler, 2)

		require.Equal(t, 1, fragments)
		require.Len(t, capture.payloads, 1)
		require.Equal(t, payload, capture.payloads[0])
		require.Equal(
			t,
			int64(pollTestPaddingLength)+int64(logbuffer.AlignFragmentChunkLength(len(payload))),
			image.Position(),
		)
	})

	t.Run("paddingOnlyAdvancesPosition", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		aeron.WriteCommittedPaddingFrameForTest(
			aeron.ImageTermBuffer0ForTest(image),
			0,
			aeron.TestPublicationInitialTermID,
			0,
			pollTestWidePaddingLength,
		)

		fragments := image.ControlledPoll(capture.handler, 1)

		require.Zero(t, fragments)
		require.Empty(t, capture.payloads)
		require.Equal(t, int64(pollTestWidePaddingLength), image.Position())
	})
}

func TestImage_BoundedControlledPoll_edgeAndBefore(t *testing.T) {
	t.Run("limitEqualsPosition", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		writePollTestDataFrame(
			aeron.ImageTermBuffer0ForTest(image),
			0,
			0,
			logbuffer.Unfragmented,
			[]byte("x"),
		)

		fragments := image.BoundedControlledPoll(capture.handler, image.Position(), 10)

		require.Zero(t, fragments)
		require.Empty(t, capture.payloads)
		require.Zero(t, image.Position())
	})

	t.Run("limitBeforePosition", func(t *testing.T) {
		const joinPosition int64 = 128
		image, capture := newControlledPollImage(t, joinPosition)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		writePollTestDataFrame(
			termBuffer,
			int(joinPosition),
			int32(joinPosition),
			logbuffer.Unfragmented,
			[]byte("x"),
		)

		fragments := image.BoundedControlledPoll(capture.handler, joinPosition-32, 10)

		require.Zero(t, fragments)
		require.Empty(t, capture.payloads)
		require.Equal(t, joinPosition, image.Position())
	})
}

func TestImage_BoundedControlledPoll_frameEdges(t *testing.T) {
	payload := []byte("a")
	frameSpan := logbuffer.AlignFragmentChunkLength(len(payload))

	t.Run("boundAtFrameEdge", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		writeDataFrameChain(t, aeron.ImageTermBuffer0ForTest(image), 0, 2, payload)
		limitPosition := int64(frameSpan)

		fragments := image.BoundedControlledPoll(capture.handler, limitPosition, 10)

		require.Equal(t, 1, fragments)
		require.Len(t, capture.payloads, 1)
		require.Equal(t, payload, capture.payloads[0])
		require.Equal(t, limitPosition, image.Position())
	})

	t.Run("boundMidFirstFrame", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		writeDataFrameChain(t, aeron.ImageTermBuffer0ForTest(image), 0, 2, payload)
		limitPosition := int64(frameSpan / 2)
		require.Positive(t, limitPosition)
		require.Less(t, limitPosition, int64(frameSpan))

		fragments := image.BoundedControlledPoll(capture.handler, limitPosition, 10)

		require.Equal(t, 1, fragments)
		require.Len(t, capture.payloads, 1)
		require.Equal(t, payload, capture.payloads[0])
		require.Equal(t, int64(frameSpan), image.Position())
		require.Greater(t, image.Position(), limitPosition)
	})

	t.Run("boundMidSecondFrame", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		writeDataFrameChain(t, aeron.ImageTermBuffer0ForTest(image), 0, 2, payload)
		limitPosition := int64(frameSpan) + int64(frameSpan/2)
		require.Greater(t, limitPosition, int64(frameSpan))
		require.Less(t, limitPosition, int64(2*frameSpan))

		fragments := image.BoundedControlledPoll(capture.handler, limitPosition, 10)

		require.Equal(t, 2, fragments)
		require.Len(t, capture.payloads, 2)
		require.Equal(t, payload, capture.payloads[0])
		require.Equal(t, payload, capture.payloads[1])
		require.Equal(t, int64(2*frameSpan), image.Position())
		require.Greater(t, image.Position(), limitPosition)
	})
}

func TestImage_BoundedControlledPoll_actions(t *testing.T) {
	payload := []byte("a")
	frameSpan := logbuffer.AlignFragmentChunkLength(len(payload))
	// Mid-second bound delivers both frame starts if CONTINUE; action cases use same layout.
	limitPosition := int64(frameSpan) + int64(frameSpan/2)

	t.Run("abortOnSecond", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		writeDataFrameChain(t, aeron.ImageTermBuffer0ForTest(image), 0, 2, payload)
		capture.actions = []aeron.ControlledAction{
			aeron.ControlledContinue,
			aeron.ControlledAbort,
		}

		fragments := image.BoundedControlledPoll(capture.handler, limitPosition, 10)

		require.Equal(t, 1, fragments)
		require.Len(t, capture.payloads, 2)
		require.Equal(t, payload, capture.payloads[0])
		require.Equal(t, payload, capture.payloads[1])
		require.Equal(t, int64(frameSpan), image.Position())

		capture.reset()
		fragments = image.BoundedControlledPoll(capture.handler, limitPosition, 1)
		require.Equal(t, 1, fragments)
		require.Len(t, capture.payloads, 1)
		require.Equal(t, payload, capture.payloads[0])
		require.Equal(t, int64(2*frameSpan), image.Position())
	})

	t.Run("breakOnFirst", func(t *testing.T) {
		image, capture := newControlledPollImage(t, 0)
		writeDataFrameChain(t, aeron.ImageTermBuffer0ForTest(image), 0, 2, payload)
		capture.actions = []aeron.ControlledAction{aeron.ControlledBreak}

		fragments := image.BoundedControlledPoll(capture.handler, limitPosition, 10)

		require.Equal(t, 1, fragments)
		require.Len(t, capture.payloads, 1)
		require.Equal(t, payload, capture.payloads[0])
		require.Equal(t, int64(frameSpan), image.Position())
	})

	t.Run("commitUnderBound", func(t *testing.T) {
		const joinPosition int64 = 0
		image, counters := newDirectPollImage(t, joinPosition)
		writeDataFrameChain(t, aeron.ImageTermBuffer0ForTest(image), 0, 2, payload)

		var positionsAtCall []int64
		handler := func(
			buffer atomicbuffer.AtomicBuffer,
			offset int,
			length int,
			header *aeron.Header,
		) aeron.ControlledAction {
			positionsAtCall = append(positionsAtCall, image.Position())
			return aeron.ControlledCommit
		}

		fragments := image.BoundedControlledPoll(handler, limitPosition, 10)

		require.Equal(t, 2, fragments)
		require.Len(t, positionsAtCall, 2)
		require.Equal(t, joinPosition, positionsAtCall[0])
		require.Equal(t, joinPosition+int64(frameSpan), positionsAtCall[1])
		require.Equal(t, joinPosition+int64(2*frameSpan), image.Position())
		gotCounter, err := counters.CounterValue(aeron.TestSubscriberPositionCounterID)
		require.NoError(t, err)
		require.Equal(t, joinPosition+int64(2*frameSpan), gotCounter)
	})
}

func TestImage_BoundedControlledPoll_joinOffsetNotZero(t *testing.T) {
	payload := []byte("a")
	frameSpan := logbuffer.AlignFragmentChunkLength(len(payload))
	joinPosition := int64(frameSpan)
	image, capture := newControlledPollImage(t, joinPosition)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	writeDataFrameChain(t, termBuffer, frameSpan, 2, payload)
	limitPosition := joinPosition + int64(frameSpan)

	fragments := image.BoundedControlledPoll(capture.handler, limitPosition, 10)

	require.Equal(t, 1, fragments)
	require.Len(t, capture.payloads, 1)
	require.Equal(t, payload, capture.payloads[0])
	require.Equal(t, limitPosition, image.Position())
}

func TestImage_BoundedControlledPoll_largeLimitAndTermEndPadding(t *testing.T) {
	const termLength = aeron.TestPublicationTermLength
	payload := []byte("tail")
	frameSpan := logbuffer.AlignFragmentChunkLength(len(payload))
	initialOffset := termLength - 2*frameSpan
	require.GreaterOrEqual(t, initialOffset, 0)

	tests := []struct {
		name          string
		limitPosition int64
	}{
		{
			name:          "afterTermEnd",
			limitPosition: int64(initialOffset) + int64(termLength),
		},
		{
			name:          "aboveIntMax",
			limitPosition: int64(math.MaxInt32) + 1000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			image, capture := newControlledPollImage(t, int64(initialOffset))
			termBuffer := aeron.ImageTermBuffer0ForTest(image)
			writePollTestDataFrame(
				termBuffer,
				initialOffset,
				int32(initialOffset),
				logbuffer.Unfragmented,
				payload,
			)
			padOffset := initialOffset + frameSpan
			padLength := int32(termLength - padOffset)
			aeron.WriteCommittedPaddingFrameForTest(
				termBuffer,
				padOffset,
				aeron.TestPublicationInitialTermID,
				int32(padOffset),
				padLength,
			)

			fragments := image.BoundedControlledPoll(capture.handler, tc.limitPosition, 10)

			require.Equal(t, 1, fragments)
			require.Len(t, capture.payloads, 1)
			require.Equal(t, payload, capture.payloads[0])
			require.Equal(t, int64(termLength), image.Position())
		})
	}
}

func TestImage_ControlledPoll_uncommitted(t *testing.T) {
	payload := []byte("committed")
	nextOffset := logbuffer.AlignFragmentChunkLength(len(payload))

	tests := []struct {
		name      string
		blockNext func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer)
	}{
		{
			name: "zeroFrameLength",
			blockNext: func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer) {
				require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuffer, nextOffset))
			},
		},
		{
			name: "negativeFrameLength",
			blockNext: func(_ *testing.T, termBuffer atomicbuffer.AtomicBuffer) {
				logbuffer.PutFrameLengthRelease(termBuffer, nextOffset, -int32(nextOffset))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			image, capture := newControlledPollImage(t, 0)
			termBuffer := aeron.ImageTermBuffer0ForTest(image)
			writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, payload)
			tc.blockNext(t, termBuffer)

			fragments := image.ControlledPoll(capture.handler, 10)

			require.Equal(t, 1, fragments)
			require.Len(t, capture.payloads, 1)
			require.Equal(t, payload, capture.payloads[0])
			require.Equal(t, int64(nextOffset), image.Position())
		})
	}
}

func TestImage_ControlledPoll_corrupt(t *testing.T) {
	t.Run("shortPadding", func(t *testing.T) {
		const (
			frameOffset = logbuffer.FrameAlignment * 2
			padLength   = int32(logbuffer.DataHeaderLength - 1)
			wantErr     = "aeron: corrupt committed frame at term offset 64 length 31"
		)
		image, capture, handledErr := newControlledPollImageWithErrorHandler(t, int64(frameOffset))
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		aeron.WriteMinimalTermEndPaddingForTest(termBuffer, frameOffset, padLength)

		fragments := image.ControlledPoll(capture.handler, 1)

		require.Zero(t, fragments)
		require.Empty(t, capture.payloads)
		require.Equal(t, int64(frameOffset), image.Position())
		handled := handledErr()
		require.Error(t, handled)
		require.Equal(t, wantErr, handled.Error())
	})

	t.Run("badDataLength", func(t *testing.T) {
		const wantErr = "aeron: corrupt committed frame at term offset 0 length 31"
		image, capture, handledErr := newControlledPollImageWithErrorHandler(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		logbuffer.PutFrameType(termBuffer, 0, logbuffer.HdrTypeData)
		logbuffer.PutFrameLengthOrdered(termBuffer, 0, int32(logbuffer.DataHeaderLength-1))

		fragments := image.ControlledPoll(capture.handler, 1)

		require.Zero(t, fragments)
		require.Empty(t, capture.payloads)
		require.Zero(t, image.Position())
		handled := handledErr()
		require.Error(t, handled)
		require.Equal(t, wantErr, handled.Error())
	})

	t.Run("stopsAfterCommitted", func(t *testing.T) {
		payload := []byte("ok")
		corruptOffset := logbuffer.AlignFragmentChunkLength(len(payload))
		const wantCorruptErr = "aeron: corrupt committed frame at term offset 64 length 31"

		assertStopsAtCorruptFrame := func(
			t *testing.T,
			image *aeron.Image,
			capture *controlledPollCapture,
		) {
			fragments := image.ControlledPoll(capture.handler, 10)
			require.Equal(t, 1, fragments)
			require.Len(t, capture.payloads, 1)
			require.Equal(t, payload, capture.payloads[0])
			require.Equal(t, int64(corruptOffset), image.Position())

			fragments = image.ControlledPoll(capture.handler, 10)
			require.Zero(t, fragments)
			require.Len(t, capture.payloads, 1)
			require.Equal(t, int64(corruptOffset), image.Position())
		}

		t.Run("withoutErrorHandler", func(t *testing.T) {
			image, capture := newControlledPollImage(t, 0)
			writePollTestCommittedThenCorrupt(aeron.ImageTermBuffer0ForTest(image), payload, corruptOffset)
			assertStopsAtCorruptFrame(t, image, capture)
		})

		t.Run("withErrorHandlerDebounceAndClear", func(t *testing.T) {
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
			capture := &controlledPollCapture{}
			termBuffer := aeron.ImageTermBuffer0ForTest(image)
			writePollTestCommittedThenCorrupt(termBuffer, payload, corruptOffset)
			assertStopsAtCorruptFrame(t, image, capture)
			require.Equal(t, 1, invocations)
			require.Error(t, handled)
			require.Equal(t, wantCorruptErr, handled.Error())

			require.Zero(t, image.ControlledPoll(capture.handler, 10))
			require.Equal(t, 1, invocations)

			fixedPayload := []byte("fixed")
			writePollTestDataFrame(
				termBuffer,
				corruptOffset,
				int32(corruptOffset),
				logbuffer.Unfragmented,
				fixedPayload,
			)
			capture.reset()
			fragments := image.ControlledPoll(capture.handler, 10)
			require.Equal(t, 1, fragments)
			require.Len(t, capture.payloads, 1)
			require.Equal(t, fixedPayload, capture.payloads[0])
			require.Equal(
				t,
				int64(corruptOffset)+int64(logbuffer.AlignFragmentChunkLength(len(fixedPayload))),
				image.Position(),
			)

			nextCorrupt := int(image.Position())
			logbuffer.PutFrameType(termBuffer, nextCorrupt, logbuffer.HdrTypeData)
			logbuffer.PutFrameLengthOrdered(
				termBuffer,
				nextCorrupt,
				int32(logbuffer.DataHeaderLength-1),
			)
			require.Zero(t, image.ControlledPoll(capture.handler, 10))
			require.Equal(t, 2, invocations)
		})
	})
}

func TestImage_ControlledPoll_handlerPanic(t *testing.T) {
	const wantErr = "controlled poll handler failed"
	panicPayload := []byte("panic")
	pollPanicHandler := func(
		atomicbuffer.AtomicBuffer,
		int,
		int,
		*aeron.Header,
	) aeron.ControlledAction {
		panic(errors.New(wantErr))
	}

	t.Run("firstFrameWithErrorHandler", func(t *testing.T) {
		image, capture, handledErr := newControlledPollImageWithErrorHandler(t, 0)
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

		fragments := image.ControlledPoll(pollPanicHandler, 10)

		require.Zero(t, fragments)
		require.Zero(t, image.Position())
		handled := handledErr()
		require.Error(t, handled)
		require.Equal(t, wantErr, handled.Error())

		fragments = image.ControlledPoll(capture.handler, 1)
		require.Equal(t, 1, fragments)
		require.Len(t, capture.payloads, 1)
		require.Equal(t, panicPayload, capture.payloads[0])
	})

	t.Run("afterPaddingPartialAdvance", func(t *testing.T) {
		image, _, handledErr := newControlledPollImageWithErrorHandler(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		writePollTestPaddingThenData(termBuffer, pollTestPaddingLength, panicPayload)

		fragments := image.ControlledPoll(pollPanicHandler, 10)

		require.Zero(t, fragments)
		require.Equal(t, int64(pollTestPaddingLength), image.Position())
		handled := handledErr()
		require.Error(t, handled)
		require.Equal(t, wantErr, handled.Error())
	})

	t.Run("nthFrameAfterSuccess", func(t *testing.T) {
		image, capture, handledErr := newControlledPollImageWithErrorHandler(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		chainPayload := []byte("a")
		writeDataFrameChain(t, termBuffer, 0, 2, chainPayload)
		firstSpan := logbuffer.AlignFragmentChunkLength(len(chainPayload))

		attempts := 0
		nthFramePanicHandler := func(
			atomicbuffer.AtomicBuffer,
			int,
			int,
			*aeron.Header,
		) aeron.ControlledAction {
			attempts++
			if attempts == 2 {
				panic(errors.New(wantErr))
			}
			return aeron.ControlledContinue
		}

		fragments := image.ControlledPoll(nthFramePanicHandler, 10)

		require.Equal(t, 1, fragments)
		require.Equal(t, int64(firstSpan), image.Position())
		handled := handledErr()
		require.Error(t, handled)
		require.Equal(t, wantErr, handled.Error())

		fragments = image.ControlledPoll(capture.handler, 1)
		require.Equal(t, 1, fragments)
		require.Len(t, capture.payloads, 1)
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
			_ = image.ControlledPoll(pollPanicHandler, 1)
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

		fragments := image.ControlledPoll(pollPanicHandler, 1)

		require.Zero(t, fragments)
		require.Zero(t, image.Position())
	})

	t.Run("nonErrorPanicWrap", func(t *testing.T) {
		const panicValue = "boom"
		image, _, handledErr := newControlledPollImageWithErrorHandler(t, 0)
		writePollTestDataFrame(
			aeron.ImageTermBuffer0ForTest(image),
			0,
			0,
			logbuffer.Unfragmented,
			panicPayload,
		)

		nonErrorPanicHandler := func(
			atomicbuffer.AtomicBuffer,
			int,
			int,
			*aeron.Header,
		) aeron.ControlledAction {
			panic(panicValue)
		}

		fragments := image.ControlledPoll(nonErrorPanicHandler, 1)

		require.Zero(t, fragments)
		handled := handledErr()
		require.Error(t, handled)
		require.Equal(t, "aeron: fragment handler panic: boom", handled.Error())
	})
}

func TestImage_ControlledPoll_closeSkipsCounterStore(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(t *testing.T, image *aeron.Image) aeron.ControlledFragmentHandler
		wantFragments int
	}{
		{
			name: "duringPaddingAdvance",
			setup: func(t *testing.T, image *aeron.Image) aeron.ControlledFragmentHandler {
				t.Helper()
				aeron.WriteCommittedPaddingFrameForTest(
					aeron.ImageTermBuffer0ForTest(image),
					0,
					aeron.TestPublicationInitialTermID,
					0,
					pollTestPaddingLength,
				)
				aeron.SetPollAfterPaddingAdvanceForTest(image, func() {
					aeron.ImageInternalCloseForTest(image)
				})
				t.Cleanup(func() {
					aeron.SetPollAfterPaddingAdvanceForTest(image, nil)
				})
				capture := &controlledPollCapture{}
				return capture.handler
			},
			wantFragments: 0,
		},
		{
			name: "duringHandler",
			setup: func(t *testing.T, image *aeron.Image) aeron.ControlledFragmentHandler {
				t.Helper()
				writePollTestDataFrame(
					aeron.ImageTermBuffer0ForTest(image),
					0,
					0,
					logbuffer.Unfragmented,
					[]byte("close-in-handler"),
				)
				return func(atomicbuffer.AtomicBuffer, int, int, *aeron.Header) aeron.ControlledAction {
					aeron.ImageInternalCloseForTest(image)
					return aeron.ControlledContinue
				}
			},
			wantFragments: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			image, _ := newControlledPollImage(t, 0)
			positionBefore := image.Position()
			handler := tt.setup(t, image)

			fragments := image.ControlledPoll(handler, 1)

			require.Equal(t, tt.wantFragments, fragments)
			require.True(t, image.IsClosed())
			require.Equal(t, positionBefore, image.Position())
		})
	}
}

func TestImage_BoundedControlledPoll_closeSkipsCounterStore(t *testing.T) {
	image, _ := newControlledPollImage(t, 0)
	writePollTestDataFrame(
		aeron.ImageTermBuffer0ForTest(image),
		0,
		0,
		logbuffer.Unfragmented,
		[]byte("close-bounded"),
	)
	positionBefore := image.Position()
	handler := func(atomicbuffer.AtomicBuffer, int, int, *aeron.Header) aeron.ControlledAction {
		aeron.ImageInternalCloseForTest(image)
		return aeron.ControlledContinue
	}

	fragments := image.BoundedControlledPoll(handler, math.MaxInt64, 10)

	require.Equal(t, 1, fragments)
	require.True(t, image.IsClosed())
	require.Equal(t, positionBefore, image.Position())
}

func TestImage_BoundedControlledPoll_guards(t *testing.T) {
	image, capture := newControlledPollImage(t, 0)
	writePollTestDataFrame(
		aeron.ImageTermBuffer0ForTest(image),
		0,
		0,
		logbuffer.Unfragmented,
		[]byte("x"),
	)

	require.Equal(t, 0, image.BoundedControlledPoll(nil, 100, 1))
	require.Equal(t, 0, image.BoundedControlledPoll(capture.handler, 100, 0))
	require.Equal(t, 0, image.BoundedControlledPoll(capture.handler, 100, -1))
	require.Zero(t, image.Position())
	require.Empty(t, capture.payloads)

	aeron.ImageInternalCloseForTest(image)
	require.Equal(t, 0, image.BoundedControlledPoll(capture.handler, 100, 1))
}
