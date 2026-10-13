package aeron

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

var (
	errPublicationLogBufferNil = errors.New("aeron: publication log buffer is nil")
	errPublicationCountersNil  = errors.New("aeron: publication counters view is nil")
)

func newPublicationFromReady(
	ready conductor.PublicationBuffersReady,
	log *logbuffer.MappedLogBuffer,
	counters *cnc.CountersReader,
	channel string,
) (*Publication, error) {
	if log == nil {
		return nil, errPublicationLogBufferNil
	}
	if counters == nil {
		return nil, errPublicationCountersNil
	}

	meta, err := log.MetaDataBuffer()
	if err != nil {
		return nil, err
	}

	if err := requireActiveCounter(
		counters,
		ready.PublicationLimitCounterID,
		ready.RegistrationID,
		cnc.DriverPublisherLimitTypeID,
		"publication limit",
	); err != nil {
		return nil, err
	}
	publicationLimit := cnc.NewCounterSlot(
		ready.PublicationLimitCounterID,
		counters.CounterValuesBuffer(),
	)
	if ready.ChannelStatusCounterID != cnc.NullCounterID {
		if err := requireAllocatedCounter(
			counters,
			ready.ChannelStatusCounterID,
			cnc.DriverSendChannelStatusTypeID,
			"channel status",
		); err != nil {
			return nil, err
		}
	}

	metadata, err := logbuffer.ParseMetadataView(meta)
	if err != nil {
		return nil, err
	}
	if err := requireLogBufferType(
		metadata,
		logbuffer.LogBufferTypeConcurrentPublication,
		"concurrent publication",
	); err != nil {
		return nil, err
	}

	termBufferLength := log.TermLength()
	positionBitsToShift := logbuffer.PositionBitsToShift(termBufferLength)

	pub := &Publication{
		registrationID:         ready.RegistrationID,
		originalRegistrationID: ready.RegistrationID,
		sessionID:              ready.SessionID,
		streamID:               ready.StreamID,
		channel:                channel,
		log:                    log,
		meta:                   meta,
		termBufferLength:       termBufferLength,
		positionBitsToShift:    positionBitsToShift,
		initialTermID:          metadata.InitialTermID,
		maxPayloadLength:       int(metadata.MTULength) - logbuffer.DataHeaderLength,
		maxMessageLength:       logbuffer.ComputeMaxMessageLength(termBufferLength),
		maxPossiblePosition:    int64(termBufferLength) * (1 << 31),
		counters:               counters,
		publicationLimit:       publicationLimit,
		channelStatusCounterID: ready.ChannelStatusCounterID,
	}

	termBuffers, err := log.TermBuffers()
	if err != nil {
		return nil, err
	}
	pub.termBuffers = termBuffers

	pub.headerWriter = logbuffer.NewHeaderWriter(
		logbuffer.DefaultFrameHeaderBuffer(meta),
		ready.SessionID,
		ready.StreamID,
	)

	return pub, nil
}

// fragmentChunk is the frame shape of one BEGIN, middle, or END fragment.
type fragmentChunk struct {
	bytesToWrite  int
	frameLength   int32
	alignedLength int
	flags         uint8
}

// nextFragmentChunk builds the next fragment shape for a message of total
// length with remaining bytes still unwritten.
//
// fullChunkAligned is AlignFragmentChunkLength(maxPayloadLength). The caller
// hoists that value so the loop does not recompute it per fragment.
func nextFragmentChunk(
	remaining int,
	length int,
	maxPayloadLength int,
	fullChunkAligned int,
) fragmentChunk {
	chunk := fragmentChunk{
		bytesToWrite:  min(remaining, maxPayloadLength),
		alignedLength: fullChunkAligned,
	}
	//nolint:gosec // G115: the length is at most the MTU
	chunk.frameLength = int32(chunk.bytesToWrite + logbuffer.DataHeaderLength)
	if chunk.bytesToWrite != maxPayloadLength {
		chunk.alignedLength = logbuffer.AlignFragmentChunkLength(chunk.bytesToWrite)
	}
	if remaining == length {
		chunk.flags = logbuffer.BeginFlag
	}
	if chunk.bytesToWrite == remaining {
		chunk.flags |= logbuffer.EndFlag
	}
	return chunk
}

// writeFragmentChunk writes one fragment header, payload, and ordered frame
// length at frameOffset. payload must be exactly chunk.bytesToWrite long.
//
// The caller owns the term reservation and advances frameOffset by
// chunk.alignedLength.
func writeFragmentChunk(
	headerWriter logbuffer.HeaderWriter,
	termBuf atomicbuffer.AtomicBuffer,
	termID int32,
	frameOffset int,
	chunk fragmentChunk,
	payload []byte,
) {
	headerWriter.WriteWithFlags(
		termBuf,
		frameOffset,
		chunk.frameLength,
		termID,
		chunk.flags,
	)
	termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, payload)
	logbuffer.PutFrameLengthOrdered(termBuf, frameOffset, chunk.frameLength)
}

func commitPublicationTermPadding(
	headerWriter logbuffer.HeaderWriter,
	termBuf atomicbuffer.AtomicBuffer,
	termID int32,
	fromOffset int,
	paddingLength int32,
) {
	if paddingLength <= 0 {
		return
	}
	if paddingLength >= logbuffer.DataHeaderLength {
		headerWriter.Write(termBuf, fromOffset, paddingLength, termID)
		logbuffer.PutFrameType(termBuf, fromOffset, logbuffer.HdrTypePad)
		logbuffer.PutFrameLengthOrdered(termBuf, fromOffset, paddingLength)
		return
	}
	logbuffer.CommitMinimalTermEndPadding(termBuf, fromOffset, paddingLength)
}

func requireLogBufferType(
	metadata logbuffer.MetadataView,
	expected uint8,
	label string,
) error {
	if metadata.Type == expected {
		return nil
	}
	return fmt.Errorf(
		"aeron: expected %s log type %d, got %d",
		label,
		expected,
		metadata.Type,
	)
}

func requireActiveCounter(
	counters *cnc.CountersReader,
	counterID int32,
	registrationID int64,
	typeID int32,
	label string,
) error {
	if counters.IsCounterActive(counterID, registrationID, typeID) {
		return nil
	}
	return fmt.Errorf(
		"aeron: %s counter %d is not active for registration %d",
		label,
		counterID,
		registrationID,
	)
}

// requireAllocatedCounter validates a shared endpoint counter (send/receive channel
// status) by allocation and type only. The counter's registration id is the channel
// endpoint's, not this publication's, so it is not matched: a publication that reuses
// an existing endpoint (e.g. ingress reconnect to a new leader) sees the endpoint's
// original registration. Java does not validate this counter at construction.
func requireAllocatedCounter(
	counters *cnc.CountersReader,
	counterID int32,
	typeID int32,
	label string,
) error {
	if counters.IsCounterAllocatedOfType(counterID, typeID) {
		return nil
	}
	return fmt.Errorf("aeron: %s counter %d is not allocated", label, counterID)
}
