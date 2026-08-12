package aeron

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

// computeLimitOffset maps limitPosition into a term offset upper bound for the
// current single-term scan. Uses int64 arithmetic then clamps to [0, capacity]
// so a huge limit does not truncate via int conversion.
func computeLimitOffset(
	capacity int,
	offset int,
	initialPosition int64,
	limitPosition int64,
) int {
	limitOffset64 := int64(offset) + (limitPosition - initialPosition)
	if limitOffset64 < 0 {
		return 0
	}
	if limitOffset64 > int64(capacity) {
		return capacity
	}
	return int(limitOffset64)
}

func (image *Image) invokeControlledFragmentHandler(
	handler ControlledFragmentHandler,
	termBuffer atomicbuffer.AtomicBuffer,
	frameOffset int,
	frameLength int32,
	header *Header,
) (action ControlledAction, ok bool) {
	payloadOffset, payloadLength := preparePollFragment(
		termBuffer,
		frameOffset,
		frameLength,
		header,
	)
	ok = true
	if image.errorHandler != nil {
		defer image.recoverFragmentHandlerPanic(&ok)
	}

	action = handler(termBuffer, payloadOffset, payloadLength, header)
	return action, ok
}

// ControlledPoll scans one active log term and delivers up to fragmentLimit
// committed DATA fragments via handler. The handler returns a [ControlledAction]
// that decides whether to abort, break, commit mid-loop, or continue.
//
// Padding frames advance the consume position without invoking the handler or
// incrementing the fragment count. A single call does not span terms; the next
// call selects the active term from the updated subscriber position.
//
// Action semantics:
//   - [ControlledAbort]: stop without advancing past the current fragment
//   - [ControlledBreak]: stop after committing the current fragment
//   - [ControlledCommit]: publish position through the current fragment and continue
//   - [ControlledContinue]: continue; position commits at poll end or after Commit
//
// Nil handler, closed image, or fragmentLimit <= 0 return 0.
//
// Handler panic does not advance past the failing DATA frame when an error
// handler is configured. Without an error handler, handler panic propagates to
// the caller.
//
// ControlledPoll is not thread-safe. Same concurrency contract as [Poll].
func (image *Image) ControlledPoll(
	handler ControlledFragmentHandler,
	fragmentLimit int,
) int {
	return image.controlledPollBounded(handler, fragmentLimit, false, 0)
}

// BoundedControlledPoll is [ControlledPoll] with a stream-position upper bound.
// Frames whose start log position is at or past limitPosition are not delivered.
// A frame that starts before the bound is delivered in full even if its end
// exceeds limitPosition.
//
// When the subscriber position is already at or past limitPosition, returns 0
// without scanning.
//
// Same single-term, padding, panic, and close semantics as [ControlledPoll].
//
// BoundedControlledPoll is not thread-safe. Same concurrency contract as [Poll].
func (image *Image) BoundedControlledPoll(
	handler ControlledFragmentHandler,
	limitPosition int64,
	fragmentLimit int,
) int {
	return image.controlledPollBounded(handler, fragmentLimit, true, limitPosition)
}

func (image *Image) controlledPollBounded(
	handler ControlledFragmentHandler,
	fragmentLimit int,
	bounded bool,
	limitPosition int64,
) int {
	if image.isClosed.Load() || handler == nil || fragmentLimit <= 0 {
		return 0
	}

	initialPosition := image.subscriberPos.Get()
	if bounded && initialPosition >= limitPosition {
		return 0
	}

	initialOffset := int(initialPosition) & int(image.termLengthMask)
	offset := initialOffset
	termBuffer := image.activeTermBuffer(initialPosition)
	capacity := termBuffer.Capacity()
	limitOffset := capacity
	if bounded {
		limitOffset = computeLimitOffset(capacity, offset, initialPosition, limitPosition)
	}

	header := &image.header
	fragmentsRead := 0

	for fragmentsRead < fragmentLimit && offset < limitOffset && !image.isClosed.Load() {
		frameLength := logbuffer.CommittedFrameLengthVolatile(termBuffer, offset)
		if advance, ok := termEndSlackAdvance(offset, capacity, frameLength); ok {
			offset += advance
			continue
		}
		if frameLength <= 0 {
			break
		}

		if logbuffer.IsPaddingFrameInTerm(termBuffer, offset) {
			advance, ok := paddingAdvance(offset, frameLength, capacity)
			if !ok {
				image.reportCorruptCommittedFrame(
					initialPosition,
					initialOffset,
					offset,
					frameLength,
				)
				break
			}
			offset += advance
			trackPollAfterPaddingAdvance(image)
			continue
		}

		nextOffset, ok := dataFrameNextOffset(offset, frameLength, capacity)
		if !ok {
			image.reportCorruptCommittedFrame(
				initialPosition,
				initialOffset,
				offset,
				frameLength,
			)
			break
		}

		action, invoked := image.invokeControlledFragmentHandler(
			handler,
			termBuffer,
			offset,
			frameLength,
			header,
		)
		if !invoked {
			break
		}

		if action == ControlledAbort {
			// offset stays so final SetRelease excludes this frame
			break
		}

		offset = nextOffset
		fragmentsRead++
		if action == ControlledBreak {
			break
		}
		if action == ControlledCommit {
			initialPosition = image.publishPollConsumePosition(
				initialPosition,
				initialOffset,
				offset,
			)
			initialOffset = offset
		}
	}

	image.publishPollConsumePosition(initialPosition, initialOffset, offset)
	return fragmentsRead
}
