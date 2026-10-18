//go:build rapid

package aeron_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

type controlledPollFrameKind int

const (
	controlledPollFrameData controlledPollFrameKind = iota
	controlledPollFramePadding
)

type controlledPollScheduleEntry struct {
	kind        controlledPollFrameKind
	startOffset int
	endOffset   int
}

// drawControlledPollSchedule writes a contiguous PAD/DATA chain from startOffset.
// Contiguity (no gaps) is an invariant of this generator: each endOffset is the next
// startOffset.
func drawControlledPollSchedule(
	rt *rapid.T,
	termBuffer atomicbuffer.AtomicBuffer,
	startOffset int,
) []controlledPollScheduleEntry {
	rt.Helper()

	entryCount := rapid.IntRange(1, 12).Draw(rt, "entryCount")
	offset := startOffset
	schedule := make([]controlledPollScheduleEntry, 0, entryCount)
	for range entryCount {
		if offset+logbuffer.DataHeaderLength >= termBuffer.Capacity() {
			break
		}
		if rapid.Bool().Draw(rt, "isPadding") {
			paddingUnits := rapid.IntRange(1, 3).Draw(rt, "paddingUnits")
			paddingLength := paddingUnits * logbuffer.FrameAlignment
			if offset+paddingLength > termBuffer.Capacity() {
				break
			}
			aeron.WriteCommittedPaddingFrameForTest(
				termBuffer,
				offset,
				aeron.TestPublicationInitialTermID,
				int32(offset),
				int32(paddingLength),
			)
			schedule = append(schedule, controlledPollScheduleEntry{
				kind:        controlledPollFramePadding,
				startOffset: offset,
				endOffset:   offset + paddingLength,
			})
			offset += paddingLength
			continue
		}

		payloadLen := rapid.IntRange(0, 32).Draw(rt, "payloadLen")
		frameLength := logbuffer.AlignFrameLength(payloadLen + logbuffer.DataHeaderLength)
		if offset+frameLength > termBuffer.Capacity() {
			break
		}
		payload := make([]byte, payloadLen)
		writePollTestDataFrame(termBuffer, offset, int32(offset), logbuffer.Unfragmented, payload)
		endOffset := offset + logbuffer.AlignFragmentChunkLength(len(payload))
		schedule = append(schedule, controlledPollScheduleEntry{
			kind:        controlledPollFrameData,
			startOffset: offset,
			endOffset:   endOffset,
		})
		offset = endOffset
	}
	return schedule
}

// expectedUnderActions walks the schedule under bound + fragment budget + actions.
// starts includes every handler invocation (ABORT frame included). fragments excludes
// ABORT. On ABORT, position is the pre-frame consume point (not past aborted start).
// Empty actions means CONTINUE for every DATA frame. A DATA frame flagged in
// panics acts as BREAK. panicked reports that stop.
func expectedUnderActions(
	joinPosition int64,
	schedule []controlledPollScheduleEntry,
	limitPosition int64,
	fragmentLimit int,
	actions []aeron.ControlledAction,
	panics []bool,
) (
	position int64,
	fragments int,
	starts []int64,
	panicked bool,
) {
	position = joinPosition
	starts = make([]int64, 0)
	actionIndex := 0
	for _, entry := range schedule {
		if fragments >= fragmentLimit {
			break
		}
		if int64(entry.startOffset) >= limitPosition {
			break
		}
		if entry.kind == controlledPollFramePadding {
			position = int64(entry.endOffset)
			continue
		}
		starts = append(starts, int64(entry.startOffset))
		action := aeron.ControlledContinue
		if actionIndex < len(actions) {
			action = actions[actionIndex]
		}
		flagged := actionIndex < len(panics) && panics[actionIndex]
		actionIndex++
		if flagged {
			position = int64(entry.endOffset)
			fragments++
			return position, fragments, starts, true
		}
		switch action {
		case aeron.ControlledAbort:
			return position, fragments, starts, false
		case aeron.ControlledBreak:
			position = int64(entry.endOffset)
			fragments++
			return position, fragments, starts, false
		default:
			// CONTINUE and COMMIT both advance past the frame for final position.
			position = int64(entry.endOffset)
			fragments++
		}
	}
	return position, fragments, starts, false
}

var controlledPollActionChoices = []aeron.ControlledAction{
	aeron.ControlledContinue,
	aeron.ControlledAbort,
	aeron.ControlledBreak,
	aeron.ControlledCommit,
}

func drawControlledPollActions(rt *rapid.T, dataCount int) []aeron.ControlledAction {
	rt.Helper()
	actions := make([]aeron.ControlledAction, 0, dataCount)
	for range dataCount {
		action := rapid.SampledFrom(controlledPollActionChoices).Draw(rt, "action")
		actions = append(actions, action)
	}
	return actions
}

// drawControlledPollPanics draws one handler panic flag per DATA frame.
func drawControlledPollPanics(rt *rapid.T, dataCount int) []bool {
	rt.Helper()
	panics := make([]bool, 0, dataCount)
	for range dataCount {
		panics = append(panics, rapid.Bool().Draw(rt, "panic"))
	}
	return panics
}

func scheduleDataCount(schedule []controlledPollScheduleEntry) int {
	n := 0
	for _, entry := range schedule {
		if entry.kind == controlledPollFrameData {
			n++
		}
	}
	return n
}

// drawJoinOffset picks an aligned subscriber start inside the first half of the term
// so a multi-frame schedule still fits. Term 0 only: join log position == term offset.
func drawJoinOffset(rt *rapid.T, capacity int) int {
	rt.Helper()
	maxUnits := (capacity / 2) / logbuffer.FrameAlignment
	if maxUnits < 1 {
		return 0
	}
	units := rapid.IntRange(0, maxUnits).Draw(rt, "joinUnits")
	return units * logbuffer.FrameAlignment
}

// controlledPollActionHandler returns actions in DATA frame order. The handler
// panics on a frame flagged in panics.
func controlledPollActionHandler(
	actions []aeron.ControlledAction,
	panics []bool,
	deliveredStarts *[]int64,
) aeron.ControlledFragmentHandler {
	actionIndex := 0
	return func(
		_ atomicbuffer.AtomicBuffer,
		_ int,
		_ int,
		header *aeron.Header,
	) aeron.ControlledAction {
		*deliveredStarts = append(*deliveredStarts, int64(header.TermOffset()))
		action := aeron.ControlledContinue
		if actionIndex < len(actions) {
			action = actions[actionIndex]
		}
		flagged := actionIndex < len(panics) && panics[actionIndex]
		actionIndex++
		if flagged {
			panic("controlled poll handler failed")
		}
		return action
	}
}

// controlledPollPanicOutcome records how one poll call surfaced a handler panic.
// Zero value: no error handler, no panic.
type controlledPollPanicOutcome struct {
	errorHandlerSet bool
	panicked        bool
	errorCount      int
}

func requireControlledPollMatchesOracle(
	rt *rapid.T,
	joinPosition int64,
	positionBefore int64,
	positionAfter int64,
	fragments int,
	fragmentLimit int,
	deliveredStarts []int64,
	limitPosition int64,
	schedule []controlledPollScheduleEntry,
	actions []aeron.ControlledAction,
	panics []bool,
	outcome controlledPollPanicOutcome,
) {
	rt.Helper()

	require.Equal(rt, joinPosition, positionBefore)
	require.LessOrEqual(rt, fragments, fragmentLimit)
	require.GreaterOrEqual(rt, positionAfter, positionBefore)

	for _, start := range deliveredStarts {
		require.Less(rt, start, limitPosition)
	}

	wantPos, wantFrags, wantStarts, wantPanicked := expectedUnderActions(
		joinPosition,
		schedule,
		limitPosition,
		fragmentLimit,
		actions,
		panics,
	)
	require.Equal(rt, wantStarts, deliveredStarts)
	require.Equal(rt, wantPos, positionAfter)
	if outcome.errorHandlerSet {
		require.False(rt, outcome.panicked)
		require.Equal(rt, wantFrags, fragments)
		wantErrors := 0
		if wantPanicked {
			wantErrors = 1
		}
		require.Equal(rt, wantErrors, outcome.errorCount)
		return
	}
	require.Equal(rt, wantPanicked, outcome.panicked)
	if !outcome.panicked {
		require.Equal(rt, wantFrags, fragments)
	}
}

func TestImage_BoundedControlledPoll_boundStart_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		capacity := aeron.TestPublicationTermLength
		joinOffset := drawJoinOffset(rt, capacity)
		joinPosition := int64(joinOffset)

		image, _ := newControlledPollImage(t, joinPosition)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		schedule := drawControlledPollSchedule(rt, termBuffer, joinOffset)

		limitPosition := rapid.Int64Range(joinPosition, int64(capacity)+64).Draw(rt, "limitPosition")
		fragmentLimit := rapid.IntRange(1, 16).Draw(rt, "fragmentLimit")

		deliveredStarts := make([]int64, 0, fragmentLimit)
		handler := func(
			_ atomicbuffer.AtomicBuffer,
			_ int,
			_ int,
			header *aeron.Header,
		) aeron.ControlledAction {
			// Term 0 + termOffset==frame offset => start log position == TermOffset.
			deliveredStarts = append(deliveredStarts, int64(header.TermOffset()))
			return aeron.ControlledContinue
		}

		positionBefore := image.Position()
		fragments := image.BoundedControlledPoll(handler, limitPosition, fragmentLimit)
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
			nil,
			controlledPollPanicOutcome{},
		)
		require.Len(rt, deliveredStarts, fragments)
	})
}

func TestImage_BoundedControlledPoll_actions_rapid(t *testing.T) {
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
		actions := drawControlledPollActions(rt, scheduleDataCount(schedule))
		panics := drawControlledPollPanics(rt, scheduleDataCount(schedule))

		deliveredStarts := make([]int64, 0, fragmentLimit+1)
		handler := controlledPollActionHandler(actions, panics, &deliveredStarts)

		positionBefore := image.Position()
		fragments, panicked := pollRecovering(func() int {
			return image.BoundedControlledPoll(handler, limitPosition, fragmentLimit)
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
			actions,
			panics,
			controlledPollPanicOutcome{
				errorHandlerSet: errorHandlerSet,
				panicked:        panicked,
				errorCount:      *errorCount,
			},
		)
	})
}

func TestImage_ControlledPoll_actions_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		capacity := aeron.TestPublicationTermLength
		joinOffset := drawJoinOffset(rt, capacity)
		joinPosition := int64(joinOffset)

		errorHandlerSet := rapid.Bool().Draw(rt, "errorHandlerSet")
		image, errorCount := newRapidPollImage(t, joinPosition, errorHandlerSet)
		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		schedule := drawControlledPollSchedule(rt, termBuffer, joinOffset)

		fragmentLimit := rapid.IntRange(1, 16).Draw(rt, "fragmentLimit")
		actions := drawControlledPollActions(rt, scheduleDataCount(schedule))
		panics := drawControlledPollPanics(rt, scheduleDataCount(schedule))

		deliveredStarts := make([]int64, 0, fragmentLimit+1)
		handler := controlledPollActionHandler(actions, panics, &deliveredStarts)

		positionBefore := image.Position()
		fragments, panicked := pollRecovering(func() int {
			return image.ControlledPoll(handler, fragmentLimit)
		})
		positionAfter := image.Position()

		// Unbounded: model with MaxInt64 limit rather than a second loop shape.
		requireControlledPollMatchesOracle(
			rt,
			joinPosition,
			positionBefore,
			positionAfter,
			fragments,
			fragmentLimit,
			deliveredStarts,
			math.MaxInt64,
			schedule,
			actions,
			panics,
			controlledPollPanicOutcome{
				errorHandlerSet: errorHandlerSet,
				panicked:        panicked,
				errorCount:      *errorCount,
			},
		)
	})
}
