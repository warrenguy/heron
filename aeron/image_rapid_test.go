//go:build rapid

package aeron_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

func writePollTestRandomFrames(
	rt *rapid.T,
	termBuffer atomicbuffer.AtomicBuffer,
	namePrefix string,
	frameCount int,
	maxPayloadLen int,
	flags uint8,
) []byte {
	offset := 0
	expectedFlags := make([]byte, 0, frameCount)
	for range frameCount {
		payloadLen := rapid.IntRange(0, maxPayloadLen).Draw(rt, namePrefix+"payloadLen")
		frameLength := logbuffer.AlignFrameLength(payloadLen + logbuffer.DataHeaderLength)
		if offset+frameLength > termBuffer.Capacity() {
			break
		}
		payload := make([]byte, payloadLen)
		writePollTestDataFrame(termBuffer, offset, int32(offset), flags, payload)
		expectedFlags = append(expectedFlags, byte(flags))
		offset += logbuffer.AlignFragmentChunkLength(len(payload))
	}
	return expectedFlags
}

func TestImage_joinAndLivePosition_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		joinPosition := rapid.Int64().Draw(rt, "joinPosition")

		sub := testSubscription(t)
		image := aeron.NewImageForTest(t, sub, joinPosition)

		require.Equal(rt, joinPosition, image.JoinPosition())
		require.Equal(rt, joinPosition, image.Position())

		livePosition := rapid.Int64().Draw(rt, "livePosition")
		aeron.ImageSetSubscriberPositionForTest(image, livePosition)

		require.Equal(rt, joinPosition, image.JoinPosition())
		require.Equal(rt, livePosition, image.Position())

		aeron.ImageInternalCloseForTest(image)

		postClosePosition := rapid.Int64().Draw(rt, "postClosePosition")
		aeron.ImageSetSubscriberPositionForTest(image, postClosePosition)

		require.Equal(rt, livePosition, image.Position())
		require.Equal(rt, joinPosition, image.JoinPosition())
	})
}

func TestImage_Poll_positionMonotonic_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		image, capture := newPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)

		entryCount := rapid.IntRange(1, 12).Draw(rt, "entryCount")
		offset := 0
		for range entryCount {
			if offset+logbuffer.DataHeaderLength >= termBuffer.Capacity() {
				break
			}
			if rapid.Bool().Draw(rt, "isPadding") {
				paddingUnits := rapid.IntRange(1, 3).Draw(rt, "paddingUnits")
				paddingLength := int32(paddingUnits * logbuffer.FrameAlignment)
				if offset+int(paddingLength) > termBuffer.Capacity() {
					break
				}
				aeron.WriteCommittedPaddingFrameForTest(
					termBuffer,
					offset,
					aeron.TestPublicationInitialTermID,
					int32(offset),
					paddingLength,
				)
				offset += int(paddingLength)
				continue
			}

			payloadLen := rapid.IntRange(0, 32).Draw(rt, "payloadLen")
			frameLength := logbuffer.AlignFrameLength(payloadLen + logbuffer.DataHeaderLength)
			if offset+frameLength > termBuffer.Capacity() {
				break
			}
			payload := make([]byte, payloadLen)
			writePollTestDataFrame(termBuffer, offset, int32(offset), logbuffer.Unfragmented, payload)
			offset += logbuffer.AlignFragmentChunkLength(len(payload))
		}

		fragmentLimit := rapid.IntRange(1, entryCount).Draw(rt, "fragmentLimit")
		positionBefore := image.Position()
		fragments := image.Poll(capture.handler, fragmentLimit)
		positionAfter := image.Position()

		require.GreaterOrEqual(rt, positionAfter, positionBefore)
		if offset > 0 {
			require.Greater(rt, positionAfter, positionBefore)
		}
		require.LessOrEqual(rt, fragments, fragmentLimit)
	})
}

func TestImage_Poll_paddingSkip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		image, capture := newPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)

		paddingLength := int32(rapid.IntRange(1, 4).Draw(rt, "paddingUnits") * logbuffer.FrameAlignment)
		payload := []byte{byte(rapid.Byte().Draw(rt, "payloadByte"))}
		writePollTestPaddingThenData(termBuffer, paddingLength, payload)

		fragments := image.Poll(capture.handler, 2)

		require.Equal(rt, 1, fragments)
		require.Equal(rt, payload, capture.payloads[0])
		require.Equal(
			rt,
			int64(paddingLength)+int64(logbuffer.AlignFragmentChunkLength(len(payload))),
			image.Position(),
		)
	})
}

func TestImage_Poll_fragmentBoundaries_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		image, capture := newPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)

		frameCount := rapid.IntRange(1, 8).Draw(rt, "frameCount")
		flags := uint8(logbuffer.BeginFlag | logbuffer.EndFlag)
		expectedFlags := writePollTestRandomFrames(
			rt,
			termBuffer,
			"",
			frameCount,
			int(aeron.TestPublicationMTULength)-logbuffer.DataHeaderLength,
			flags,
		)

		fragments := image.Poll(capture.handler, frameCount)

		require.Equal(rt, len(expectedFlags), fragments)
		require.Len(rt, capture.payloads, len(expectedFlags))
		require.Len(rt, capture.flags, len(expectedFlags))
		for i, gotFlags := range expectedFlags {
			require.Equal(rt, gotFlags, capture.flags[i])
		}
	})
}

func TestImage_Poll_alignRoundtrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		image, capture := newPollImage(t, 0)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)

		frameCount := rapid.IntRange(1, 6).Draw(rt, "frameCount")
		offset := 0
		for range frameCount {
			payloadLen := rapid.IntRange(0, 48).Draw(rt, "payloadLen")
			frameLength := logbuffer.AlignFrameLength(payloadLen + logbuffer.DataHeaderLength)
			if offset+frameLength > termBuffer.Capacity() {
				break
			}
			payload := make([]byte, payloadLen)
			writePollTestDataFrame(termBuffer, offset, int32(offset), logbuffer.Unfragmented, payload)
			require.Zero(rt, offset%logbuffer.FrameAlignment)
			offset += logbuffer.AlignFragmentChunkLength(len(payload))
			require.Zero(rt, offset%logbuffer.FrameAlignment)
		}

		_ = image.Poll(capture.handler, frameCount)
		require.Equal(rt, int64(offset), image.Position())
	})
}

// newRapidPollImage builds an image at joinPosition. With errorHandlerSet, the
// image error handler increments the returned counter.
func newRapidPollImage(
	t *testing.T,
	joinPosition int64,
	errorHandlerSet bool,
) (*aeron.Image, *int) {
	t.Helper()

	errorCount := new(int)
	if !errorHandlerSet {
		return newTestPollImage(t, joinPosition), errorCount
	}
	image := aeron.NewImageForTestWithErrorHandler(
		t,
		testSubscription(t),
		joinPosition,
		func(error) {
			*errorCount++
		},
	)
	return image, errorCount
}

// pollRecovering recovers a panic from poll. A recovered call reports zero
// fragments.
func pollRecovering(poll func() int) (fragments int, panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	return poll(), false
}

func TestImage_BoundedPoll_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		capacity := aeron.TestPublicationTermLength
		joinOffset := drawJoinOffset(rt, capacity)
		joinPosition := int64(joinOffset)

		errorHandlerSet := rapid.Bool().Draw(rt, "errorHandlerSet")
		image, errorCount := newRapidPollImage(t, joinPosition, errorHandlerSet)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		schedule := drawControlledPollSchedule(rt, termBuffer, joinOffset)

		limitPosition := rapid.Int64Range(joinPosition, int64(capacity)+64).Draw(rt, "limitPosition")
		fragmentLimit := rapid.IntRange(1, 16).Draw(rt, "fragmentLimit")
		panics := drawControlledPollPanics(rt, scheduleDataCount(schedule))

		deliveredStarts := make([]int64, 0, fragmentLimit+1)
		handler := func(_ atomicbuffer.AtomicBuffer, _, _ int, header *aeron.Header) {
			deliveredStarts = append(deliveredStarts, int64(header.TermOffset()))
			index := len(deliveredStarts) - 1
			if index < len(panics) && panics[index] {
				panic("poll handler failed")
			}
		}

		positionBefore := image.Position()
		fragments, panicked := pollRecovering(func() int {
			return image.BoundedPoll(handler, limitPosition, fragmentLimit)
		})
		positionAfter := image.Position()

		requireControlledPollMatchesOracle(
			rt,
			joinPosition,
			positionBefore,
			positionAfter,
			fragments,
			fragmentLimit,
			deliveredStarts,
			limitPosition,
			schedule,
			nil,
			panics,
			controlledPollPanicOutcome{
				errorHandlerSet: errorHandlerSet,
				panicked:        panicked,
				errorCount:      *errorCount,
			},
		)
	})
}

func TestImage_Poll_handlerPanicDrain_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		errorHandlerSet := rapid.Bool().Draw(rt, "errorHandlerSet")
		image, errorCount := newRapidPollImage(t, 0, errorHandlerSet)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		schedule := drawControlledPollSchedule(rt, termBuffer, 0)
		panics := drawControlledPollPanics(rt, scheduleDataCount(schedule))
		fragmentLimit := rapid.IntRange(1, 16).Draw(rt, "fragmentLimit")

		flagged := make(map[int64]bool, len(panics))
		wantStarts := make([]int64, 0, len(panics))
		flagCount := 0
		for _, entry := range schedule {
			if entry.kind != controlledPollFrameData {
				continue
			}
			start := int64(entry.startOffset)
			flagged[start] = panics[len(wantStarts)]
			if flagged[start] {
				flagCount++
			}
			wantStarts = append(wantStarts, start)
		}
		require.NotEmpty(rt, schedule)
		scheduleEnd := int64(schedule[len(schedule)-1].endOffset)

		deliveredStarts := make([]int64, 0, len(wantStarts))
		handler := func(_ atomicbuffer.AtomicBuffer, _, _ int, header *aeron.Header) {
			start := int64(header.TermOffset())
			deliveredStarts = append(deliveredStarts, start)
			if flagged[start] {
				panic("poll handler failed")
			}
		}

		panicCount := 0
		for range len(schedule) + 1 {
			before := image.Position()
			if before >= scheduleEnd {
				break
			}
			deliveredBefore := len(deliveredStarts)
			fragments, panicked := pollRecovering(func() int {
				return image.Poll(handler, fragmentLimit)
			})
			after := image.Position()

			require.Greater(rt, after, before)
			if errorHandlerSet {
				require.False(rt, panicked)
				require.LessOrEqual(rt, fragments, fragmentLimit)
				require.Equal(rt, len(deliveredStarts)-deliveredBefore, fragments)
			} else {
				reachedFlag := slices.ContainsFunc(deliveredStarts[deliveredBefore:], func(start int64) bool {
					return flagged[start]
				})
				require.Equal(rt, reachedFlag, panicked)
			}
			if panicked {
				panicCount++
			}
		}

		require.Equal(rt, wantStarts, deliveredStarts)
		require.Equal(rt, scheduleEnd, image.Position())
		if errorHandlerSet {
			require.Equal(rt, flagCount, *errorCount)
		} else {
			require.Equal(rt, flagCount, panicCount)
		}
	})
}

var receivedPositionShift = logbuffer.PositionBitsToShift(aeron.TestPublicationTermLength)

// receivedPositionRingLimit is the highest position a scan can reach from
// start: the base of start's own term plus the whole term ring.
func receivedPositionRingLimit(start int64) int64 {
	return start - int64(logbuffer.ComputeTermOffsetFromPosition(start, receivedPositionShift)) +
		int64(logbuffer.PartitionCount*aeron.TestPublicationTermLength)
}

func drawReceivedPositionStart(rt *rapid.T) int64 {
	rt.Helper()

	startTerm := rapid.IntRange(0, 5).Draw(rt, "startTerm")
	var offset int
	if rapid.Bool().Draw(rt, "startInTailGap") {
		offset = aeron.TestPublicationTermLength -
			rapid.IntRange(1, logbuffer.DataHeaderLength-1).Draw(rt, "tailGap")
	} else {
		offset = logbuffer.FrameAlignment * rapid.IntRange(0, 16).Draw(rt, "startFrames")
	}
	return int64(startTerm)*aeron.TestPublicationTermLength + int64(offset)
}

type receivedPositionLayoutKind int

const (
	receivedPositionLayoutData receivedPositionLayoutKind = iota
	receivedPositionLayoutPad
	receivedPositionLayoutTermEndPad
	receivedPositionLayoutStop
)

// writeReceivedPositionLayout writes a drawn frame layout from start and
// returns the position it ends at. It never writes to or past limit, so the
// layout stays inside the scan's contract and the unbounded-poll oracle stays
// well defined. The expectation is what the writer laid down, not a second copy
// of the scan.
func writeReceivedPositionLayout(
	rt *rapid.T,
	image *aeron.Image,
	start int64,
	limit int64,
) int64 {
	rt.Helper()

	const termLength = aeron.TestPublicationTermLength

	position := start
	steps := rapid.IntRange(1, 40).Draw(rt, "steps")
	for range steps {
		if limit-position <= int64(logbuffer.DataHeaderLength) {
			return position
		}
		termIndex := logbuffer.IndexByPosition(position, receivedPositionShift)
		termBuffer := aeron.ImageTermBufferForTest(image, termIndex)
		offset := int(logbuffer.ComputeTermOffsetFromPosition(position, receivedPositionShift))
		remaining := termLength - offset

		if remaining < logbuffer.DataHeaderLength {
			// A term-end gap shorter than a header carries at most a length
			// field. Draw it across the whole legal span, not only at the gap
			// width: a committed length that is not the remaining byte count
			// fails the slack guard, and both the scan and Poll stop there.
			gapLength := rapid.IntRange(0, remaining).Draw(rt, "tailGapCommittedLength")
			if gapLength > 0 {
				aeron.WriteMinimalTermEndPaddingForTest(termBuffer, offset, int32(gapLength))
			}
			if gapLength >= atomicbuffer.SizeInt32 && gapLength != remaining {
				return position
			}
			position += int64(remaining)
			continue
		}

		advance := 0
		payloadLength := 0
		isData := false
		kind := rapid.SampledFrom([]receivedPositionLayoutKind{
			receivedPositionLayoutData,
			receivedPositionLayoutPad,
			receivedPositionLayoutTermEndPad,
			receivedPositionLayoutStop,
		}).Draw(rt, "kind")
		switch kind {
		case receivedPositionLayoutStop:
			return position
		case receivedPositionLayoutPad:
			advance = min(
				logbuffer.FrameAlignment*rapid.IntRange(1, 4).Draw(rt, "padUnits"),
				remaining,
			)
		case receivedPositionLayoutTermEndPad:
			advance = remaining
		case receivedPositionLayoutData:
			payloadLength = rapid.IntRange(0, 256).Draw(rt, "payloadLength")
			advance = logbuffer.AlignFragmentChunkLength(payloadLength)
			isData = advance < remaining

			// A data frame never crosses the term boundary; a chunk that would
			// cross becomes term-end padding, as a publication claims it.
			if !isData {
				advance = remaining
			}
		}

		if position+int64(advance) >= limit {
			return position
		}
		if isData {
			writePollTestDataFrame(
				termBuffer,
				offset,
				int32(offset),
				logbuffer.Unfragmented,
				make([]byte, payloadLength),
			)
		} else {
			aeron.WriteCommittedPaddingFrameForTest(
				termBuffer,
				offset,
				aeron.TestPublicationInitialTermID,
				int32(offset),
				int32(advance),
			)
		}
		position += int64(advance)
	}
	return position
}

func TestImage_ReceivedPosition_matchesWrittenLayout_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		start := drawReceivedPositionStart(rt)
		limit := receivedPositionRingLimit(start)
		image, _ := newPollImage(t, 0)
		aeron.ImageSetSubscriberPositionForTest(image, start)
		want := writeReceivedPositionLayout(rt, image, start, limit)

		received := image.ReceivedPosition()

		require.Equal(rt, want, received)
		require.GreaterOrEqual(rt, received, image.Position())
		require.LessOrEqual(rt, received, limit, "the scan never runs past the term ring")
		require.Equal(rt, received, image.ReceivedPosition(), "the scan is idempotent")
		require.Equal(rt, start, image.Position(), "the scan leaves the subscriber alone")
	})
}

func TestImage_ReceivedPosition_matchesUnboundedPoll_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		start := drawReceivedPositionStart(rt)
		image, _ := newPollImage(t, 0)
		aeron.ImageSetSubscriberPositionForTest(image, start)
		want := writeReceivedPositionLayout(rt, image, start, receivedPositionRingLimit(start))

		received := image.ReceivedPosition()
		settled := pollImageToExhaustion(rt, image)

		require.Equal(rt, want, settled)
		require.Equal(rt, received, settled, "the scan predicts where poll settles")
		require.Equal(rt, settled, image.ReceivedPosition())
	})
}

func TestImage_ReceivedPosition_monotonicUnderWrites_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		start := drawReceivedPositionStart(rt)
		limit := receivedPositionRingLimit(start)
		image, _ := newPollImage(t, 0)
		aeron.ImageSetSubscriberPositionForTest(image, start)

		// An empty layout reports the subscriber position, except from a
		// sub-header term-end gap, where the scan advances to the next term
		// base exactly as Poll does.
		previous := image.ReceivedPosition()
		require.GreaterOrEqual(rt, previous, start)
		require.Less(rt, previous-start, int64(logbuffer.DataHeaderLength))

		for range rapid.IntRange(1, 6).Draw(rt, "rounds") {
			written := writeReceivedPositionLayout(rt, image, previous, limit)
			current := image.ReceivedPosition()

			require.GreaterOrEqual(rt, current, previous, "the reported position never retreats")
			require.Equal(rt, written, current)
			if written == previous {
				break
			}
			previous = current
		}
	})
}
