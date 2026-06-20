package conductor

import (
	"testing"

	"github.com/warrenguy/heron/agrona/atomic"
)

const fuzzWireMaxOffset = 8

type wireFuzzKind uint8

const (
	fuzzPublicationBuffersReady wireFuzzKind = iota
	fuzzSubscriptionReady
	fuzzAvailableImage
	fuzzUnavailableImage
	fuzzPublicationError
	fuzzPublicationMessage
	fuzzSubscriptionMessage
	fuzzRemoveMessage
	fuzzRemovePublicationMessage
	fuzzErrorResponse
	fuzzOperationSuccess
	fuzzCorrelated
	fuzzCorrelationAndInt32
	fuzzAdapterDispatch
	wireFuzzKindCount
)

var (
	seedPublicationBuffersReady = PublicationBuffersReadyBytes(PublicationBuffersReady{
		CorrelationID:             1001,
		RegistrationID:            2002,
		SessionID:                 42,
		StreamID:                  7,
		PublicationLimitCounterID: 10,
		ChannelStatusCounterID:    11,
		LogFileName:               "/dev/shm/aeron-publication.log",
	})
	seedSubscriptionReady = SubscriptionReadyBytes(SubscriptionReady{
		CorrelationID:          3003,
		ChannelStatusCounterID: 12,
	})
	seedAvailableImage = AvailableImageBytes(AvailableImage{
		CorrelationID:              4004,
		SessionID:                  42,
		StreamID:                   7,
		SubscriptionRegistrationID: 5005,
		SubscriberPositionID:       13,
		LogFileName:                "/dev/shm/aeron-publication.log",
		SourceIdentity:             "ipc",
	})
	seedUnavailableImage = UnavailableImageBytes(UnavailableImage{
		CorrelationID:              6006,
		SubscriptionRegistrationID: 5005,
		StreamID:                   7,
		Channel:                    "aeron:ipc",
	})
	seedPublicationError = PublicationErrorBytes(PublicationError{
		RegistrationID: 2002,
		SessionID:      42,
		StreamID:       7,
		ErrorCode:      1,
		ErrorMessage:   "simulated",
	})
	seedPublicationMessage  = PublicationMessageBytes(1, 42, 1001, "aeron:ipc")
	seedSubscriptionMessage = SubscriptionMessageBytes(
		1,
		42,
		NullRegistrationCorrelationID,
		1001,
		"aeron:ipc",
	)
	seedRemoveMessage            = RemoveMessageBytes(1, 42, 9001)
	seedRemovePublicationMessage = RemovePublicationMessageBytes(1, 42, 9001, true)
	seedErrorResponse            = ErrorResponseBytes(7, 11, "fail")
	seedOperationSuccess         = OperationSuccessBytes(7)
	seedCorrelated               = CorrelatedMessageBytes(1, -1)
	seedCorrelationAndInt32      = CorrelationAndInt32Bytes(99, 1001)
)

type wireFuzzSeed struct {
	kind      wireFuzzKind
	offset    uint8
	msgTypeID int32
	body      []byte
}

func fuzzWireBodyBuffer(offset int, body []byte) (atomic.AtomicBuffer, int) {
	buf := make([]byte, offset+len(body))
	copy(buf[offset:], body)
	return atomic.Wrap(buf), offset
}

func fuzzWireBuffer(offset int, body []byte) (atomic.AtomicBuffer, int) {
	if offset == 0 {
		return atomic.Wrap(body), 0
	}
	return fuzzWireBodyBuffer(offset, body)
}

func runWireFuzzKind(
	kind wireFuzzKind,
	adapter *DriverEventsAdapter,
	buffer atomic.AtomicBuffer,
	bodyOffset int,
	bodyLen int,
	msgTypeID int32,
) {
	switch kind {
	case fuzzPublicationBuffersReady:
		_, _ = DecodePublicationBuffersReady(buffer, bodyOffset, bodyLen)
	case fuzzSubscriptionReady:
		_, _ = DecodeSubscriptionReady(buffer, bodyOffset, bodyLen)
	case fuzzAvailableImage:
		_, _ = DecodeAvailableImage(buffer, bodyOffset, bodyLen)
	case fuzzUnavailableImage:
		_, _ = DecodeUnavailableImage(buffer, bodyOffset, bodyLen)
	case fuzzPublicationError:
		_, _ = DecodePublicationError(buffer, bodyOffset, bodyLen)
	case fuzzPublicationMessage:
		_, _ = DecodePublicationMessage(buffer, bodyOffset, bodyLen)
	case fuzzSubscriptionMessage:
		_, _ = DecodeSubscriptionMessage(buffer, bodyOffset, bodyLen)
	case fuzzRemoveMessage:
		_, _ = DecodeRemoveMessage(buffer, bodyOffset, bodyLen)
	case fuzzRemovePublicationMessage:
		_, _ = DecodeRemovePublicationMessage(buffer, bodyOffset, bodyLen)
	case fuzzErrorResponse:
		_, _ = DecodeErrorResponse(buffer, bodyOffset, bodyLen)
	case fuzzOperationSuccess:
		_, _ = DecodeOperationSuccess(buffer, bodyOffset)
	case fuzzCorrelated:
		_, _ = DecodeCorrelated(buffer, bodyOffset)
	case fuzzCorrelationAndInt32:
		_, _ = DecodeCorrelationAndInt32(buffer, bodyOffset)
	case fuzzAdapterDispatch:
		adapter.handleMessage(
			int(msgTypeID),
			buffer,
			bodyOffset,
			bodyLen,
			NullCorrelationID,
			nil,
		)
	}
}

func FuzzWireDecode(f *testing.F) {
	seeds := []wireFuzzSeed{
		{fuzzPublicationBuffersReady, 0, 0, seedPublicationBuffersReady},
		{fuzzSubscriptionReady, 0, 0, seedSubscriptionReady},
		{fuzzAvailableImage, 0, 0, seedAvailableImage},
		{fuzzUnavailableImage, 0, 0, seedUnavailableImage},
		{fuzzPublicationError, 0, 0, seedPublicationError},
		{fuzzPublicationMessage, 0, 0, seedPublicationMessage},
		{fuzzSubscriptionMessage, 0, 0, seedSubscriptionMessage},
		{fuzzRemoveMessage, 0, 0, seedRemoveMessage},
		{fuzzRemovePublicationMessage, 0, 0, seedRemovePublicationMessage},
		{fuzzErrorResponse, 0, 0, seedErrorResponse},
		{fuzzOperationSuccess, 0, 0, seedOperationSuccess},
		{fuzzCorrelated, 0, 0, seedCorrelated},
		{fuzzCorrelationAndInt32, 0, 0, seedCorrelationAndInt32},
		{fuzzAdapterDispatch, 0, int32(OnError), seedErrorResponse},
		{fuzzAdapterDispatch, 0, int32(OnPublicationReady), seedPublicationBuffersReady},
		{fuzzAdapterDispatch, 0, int32(OnOperationSuccess), seedOperationSuccess},
		{fuzzAdapterDispatch, 0, 0x12345678, []byte{0x01, 0x02, 0x03, 0x04}},
		{fuzzPublicationBuffersReady, fuzzWireMaxOffset, 0, seedPublicationBuffersReady},
		{fuzzAdapterDispatch, fuzzWireMaxOffset, int32(OnError), seedErrorResponse},
	}
	for _, seed := range seeds {
		f.Add(uint8(seed.kind), seed.offset, seed.msgTypeID, seed.body)
	}

	f.Fuzz(func(t *testing.T, kind, offset uint8, msgTypeID int32, body []byte) {
		if len(body) == 0 || len(body) > ScratchBufferLength {
			return
		}
		buffer, bodyOffset := fuzzWireBuffer(int(offset%(fuzzWireMaxOffset+1)), body)
		adapter := NewDriverEventsAdapter(
			1,
			nil,
			nil,
			nil,
			DriverEventsAdapterHandlers{},
			nil,
		)
		runWireFuzzKind(
			wireFuzzKind(kind%uint8(wireFuzzKindCount)),
			adapter,
			buffer,
			bodyOffset,
			len(body),
			msgTypeID,
		)
	})
}
