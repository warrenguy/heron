package conductor

import (
	"net"

	"github.com/warrenguy/heron/atomicbuffer"
)

const (
	sizeOfInt32  = atomicbuffer.SizeInt32
	sizeOfInt64  = atomicbuffer.SizeInt64
	sizeOfUint16 = atomicbuffer.SizeUint16

	CorrelatedMessageClientIDOffset      = 0
	CorrelatedMessageCorrelationIDOffset = CorrelatedMessageClientIDOffset + sizeOfInt64

	CorrelationAndInt32CorrelationIDOffset = 0
	CorrelationAndInt32Int32ValueOffset    = CorrelationAndInt32CorrelationIDOffset + sizeOfInt64

	SubscriptionReadyCorrelationIDOffset          = 0
	SubscriptionReadyChannelStatusCounterIDOffset = SubscriptionReadyCorrelationIDOffset + sizeOfInt64

	OperationSuccessCorrelationIDOffset = 0
	OperationSuccessBodyLength          = OperationSuccessCorrelationIDOffset + sizeOfInt64
)

const (
	UnavailableImageCorrelationIDOffset = 0
	unavailImageSubRegOffset            = UnavailableImageCorrelationIDOffset + sizeOfInt64
	unavailImageStreamOffset            = unavailImageSubRegOffset + sizeOfInt64

	UnavailableImageSubscriptionRegistrationIDOffset = unavailImageSubRegOffset
	UnavailableImageStreamIDOffset                   = unavailImageStreamOffset
)

const (
	AvailableImageCorrelationIDOffset = 0
	availImageSessionOffset           = AvailableImageCorrelationIDOffset + sizeOfInt64
	availImageStreamOffset            = availImageSessionOffset + sizeOfInt32
	availImageSubRegOffset            = availImageStreamOffset + sizeOfInt32
	availImageSubscriberPosOffset     = availImageSubRegOffset + sizeOfInt64

	AvailableImageSessionIDOffset                  = availImageSessionOffset
	AvailableImageStreamIDOffset                   = availImageStreamOffset
	AvailableImageSubscriptionRegistrationIDOffset = availImageSubRegOffset
	AvailableImageSubscriberPositionIDOffset       = availImageSubscriberPosOffset
)

const (
	pubBufReadyCorrelationOffset  = 0
	pubBufReadyRegistrationOffset = pubBufReadyCorrelationOffset + sizeOfInt64

	PublicationBuffersReadyCorrelationIDOffset = pubBufReadyCorrelationOffset
	pubBufReadySessionOffset                   = pubBufReadyRegistrationOffset + sizeOfInt64
	pubBufReadyStreamOffset                    = pubBufReadySessionOffset + sizeOfInt32
	pubBufReadyLimitCounterOffset              = pubBufReadyStreamOffset + sizeOfInt32
	pubBufReadyChannelStatusOffset             = pubBufReadyLimitCounterOffset + sizeOfInt32

	PublicationBuffersReadyRegistrationIDOffset            = pubBufReadyRegistrationOffset
	PublicationBuffersReadySessionIDOffset                 = pubBufReadySessionOffset
	PublicationBuffersReadyStreamIDOffset                  = pubBufReadyStreamOffset
	PublicationBuffersReadyPublicationLimitCounterIDOffset = pubBufReadyLimitCounterOffset
	PublicationBuffersReadyChannelStatusCounterIDOffset    = pubBufReadyChannelStatusOffset
)

const (
	PublicationErrorRegistrationIDOffset = 0
	pubErrorDestRegOffset                = PublicationErrorRegistrationIDOffset + sizeOfInt64
	pubErrorSessionOffset                = pubErrorDestRegOffset + sizeOfInt64
	pubErrorStreamOffset                 = pubErrorSessionOffset + sizeOfInt32
	pubErrorReceiverOffset               = pubErrorStreamOffset + sizeOfInt32
	pubErrorGroupTagOffset               = pubErrorReceiverOffset + sizeOfInt64
	pubErrorAddressTypeOffset            = pubErrorGroupTagOffset + sizeOfInt64
	pubErrorAddressPortOffset            = pubErrorAddressTypeOffset + sizeOfUint16
	pubErrorAddressOffset                = pubErrorAddressPortOffset + sizeOfUint16
	pubErrorErrorCodeOffset              = pubErrorAddressOffset + net.IPv6len

	PublicationErrorDestinationRegistrationIDOffset = pubErrorDestRegOffset
	PublicationErrorSessionIDOffset                 = pubErrorSessionOffset
	PublicationErrorStreamIDOffset                  = pubErrorStreamOffset
	PublicationErrorReceiverIDOffset                = pubErrorReceiverOffset
	PublicationErrorGroupTagOffset                  = pubErrorGroupTagOffset
	PublicationErrorAddressTypeOffset               = pubErrorAddressTypeOffset
	PublicationErrorAddressPortOffset               = pubErrorAddressPortOffset
	PublicationErrorAddressOffset                   = pubErrorAddressOffset
	PublicationErrorErrorCodeOffset                 = pubErrorErrorCodeOffset
)

const (
	errorResponseCorrelationOffset = 0
	errorResponseErrorCodeOffset   = errorResponseCorrelationOffset + sizeOfInt64

	ErrorResponseOffendingCommandCorrelationIDOffset = errorResponseCorrelationOffset
	errorResponseMessageLenOffset                    = errorResponseErrorCodeOffset + sizeOfInt32

	ErrorResponseErrorCodeOffset          = errorResponseErrorCodeOffset
	ErrorResponseErrorMessageLengthOffset = errorResponseMessageLenOffset
)

const (
	PublicationMessageStreamIDOffset    = CorrelatedMessageLength
	PublicationMessageChannelOffset     = CorrelatedMessageLength + sizeOfInt32
	PublicationMessageMinimumBodyLength = PublicationMessageChannelOffset + sizeOfInt32
)

const (
	subscriptionMessageRegCorrOffset  = CorrelatedMessageLength
	subscriptionMessageStreamIDOffset = subscriptionMessageRegCorrOffset + sizeOfInt64
	subscriptionMessageChannelOffset  = subscriptionMessageStreamIDOffset + sizeOfInt32

	SubscriptionMessageRegistrationCorrelationIDOffset = subscriptionMessageRegCorrOffset
	subscriptionMessageMinimumBodyLength               = subscriptionMessageChannelOffset + sizeOfInt32

	SubscriptionMessageStreamIDOffset    = subscriptionMessageStreamIDOffset
	SubscriptionMessageChannelOffset     = subscriptionMessageChannelOffset
	SubscriptionMessageMinimumBodyLength = subscriptionMessageMinimumBodyLength
)

// DestinationMessage layout is CorrelatedMessage, then registrationCorrelationId,
// then channel. No stream id on the wire (unlike SubscriptionMessage).
const (
	destinationMessageRegCorrOffset = CorrelatedMessageLength
	destinationMessageChannelOffset = destinationMessageRegCorrOffset + sizeOfInt64

	DestinationMessageRegistrationCorrelationIDOffset = destinationMessageRegCorrOffset
	DestinationMessageChannelOffset                   = destinationMessageChannelOffset
	DestinationMessageMinimumBodyLength               = destinationMessageChannelOffset + sizeOfInt32
)

// CounterMessage layout is CorrelatedMessage, then typeId, then key length
// and key bytes. The label (length, then bytes) starts at the next 4-byte
// boundary after the key.
const (
	CounterMessageTypeIDOffset      = CorrelatedMessageLength
	CounterMessageKeyLengthOffset   = CounterMessageTypeIDOffset + sizeOfInt32
	CounterMessageKeyOffset         = CounterMessageKeyLengthOffset + sizeOfInt32
	CounterMessageMinimumBodyLength = CounterMessageKeyOffset + sizeOfInt32
)

const (
	removeMessageRegistrationIDOffset = CorrelatedMessageLength
	RemoveMessageRegistrationIDOffset = removeMessageRegistrationIDOffset
	RemoveMessageBodyLength           = RemoveMessageRegistrationIDOffset + sizeOfInt64
)

const (
	destinationByIDResourceOffset    = CorrelatedMessageLength
	destinationByIDDestinationOffset = destinationByIDResourceOffset + sizeOfInt64
	destinationByIDBodyLength        = destinationByIDDestinationOffset + sizeOfInt64

	DestinationByIDMessageResourceRegistrationIDOffset    = destinationByIDResourceOffset
	DestinationByIDMessageDestinationRegistrationIDOffset = destinationByIDDestinationOffset
	DestinationByIDMessageBodyLength                      = destinationByIDBodyLength
)

const (
	removePublicationFlagsOffset = RemoveMessageBodyLength
	RemovePublicationFlagsOffset = removePublicationFlagsOffset
	RemovePublicationBodyLength  = RemovePublicationFlagsOffset + sizeOfInt64
)

const pubBufReadyPrefixEnd = PublicationBuffersReadyChannelStatusCounterIDOffset + sizeOfInt32

const (
	SubscriptionReadyBodyLength        = SubscriptionReadyChannelStatusCounterIDOffset + sizeOfInt32
	UnavailableImageFixedLength        = UnavailableImageStreamIDOffset + sizeOfInt32
	AvailableImageFixedLength          = AvailableImageSubscriberPositionIDOffset + sizeOfInt32
	PublicationBuffersReadyFixedLength = pubBufReadyPrefixEnd
	PublicationErrorFixedLength        = PublicationErrorErrorCodeOffset + sizeOfInt32
	ErrorResponseFixedBodyLength       = ErrorResponseErrorMessageLengthOffset + sizeOfInt32
)
