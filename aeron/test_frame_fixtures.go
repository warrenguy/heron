//go:build testhooks

package aeron

import (
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

type FragmentedFrameForTest struct {
	Flags   uint8
	Payload []byte
}

func writeCommittedDataFrameForTest(
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
	flags uint8,
	payload []byte,
) {
	fragmented := flags&logbuffer.Unfragmented != logbuffer.Unfragmented
	if !fragmented &&
		sessionID == inMemPublicationSessionID &&
		streamID == inMemPublicationStreamID {
		writeInMemCommittedDataFrame(
			termBuffer,
			frameOffset,
			termID,
			termOffset,
			flags,
			payload,
		)
		return
	}

	frameLength := int32(len(payload) + logbuffer.DataHeaderLength)
	var err error
	if fragmented {
		err = logbuffer.WriteDataHeaderFragmented(
			termBuffer,
			frameOffset,
			sessionID,
			streamID,
			termID,
			termOffset,
			flags,
		)
	} else {
		err = logbuffer.WriteDataHeaderUnfragmented(
			termBuffer,
			frameOffset,
			sessionID,
			streamID,
			termID,
			termOffset,
			flags,
		)
	}
	if err != nil {
		panic(err)
	}
	termBuffer.PutBytes(frameOffset+logbuffer.OffsetFramePayload, payload)
	logbuffer.PutFrameLengthOrdered(termBuffer, frameOffset, frameLength)
}

func WriteCommittedDataFrameForTest(
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
	flags uint8,
	payload []byte,
) {
	WriteCommittedDataFrameWithFlagsForTest(
		termBuffer,
		frameOffset,
		sessionID,
		streamID,
		termID,
		termOffset,
		flags,
		payload,
	)
}

func WriteCommittedDataFrameWithFlagsForTest(
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
	flags uint8,
	payload []byte,
) int32 {
	writeCommittedDataFrameForTest(
		termBuffer,
		frameOffset,
		sessionID,
		streamID,
		termID,
		termOffset,
		flags,
		payload,
	)
	return int32(len(payload) + logbuffer.DataHeaderLength)
}

func CommittedDataFramesTermCapacityForTest(frames []FragmentedFrameForTest) int {
	total := 64
	for _, frame := range frames {
		total += logbuffer.AlignFrameLength(len(frame.Payload) + logbuffer.DataHeaderLength)
	}
	return total
}

func WriteCommittedDataFramesForStreamForTest(
	termBuffer atomic.AtomicBuffer,
	startOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	frames []FragmentedFrameForTest,
) []int {
	offsets := make([]int, len(frames))
	offset := startOffset
	for i, frame := range frames {
		offsets[i] = offset
		frameLength := WriteCommittedDataFrameWithFlagsForTest(
			termBuffer,
			offset,
			sessionID,
			streamID,
			termID,
			int32(offset),
			frame.Flags,
			frame.Payload,
		)
		offset += logbuffer.AlignFrameLength(int(frameLength))
	}
	return offsets
}

func WriteCommittedDataFramesForTest(
	termBuffer atomic.AtomicBuffer,
	startOffset int,
	frames []FragmentedFrameForTest,
) []int {
	return WriteCommittedDataFramesForStreamForTest(
		termBuffer,
		startOffset,
		inMemPublicationSessionID,
		inMemPublicationStreamID,
		inMemPublicationInitialTermID,
		frames,
	)
}
