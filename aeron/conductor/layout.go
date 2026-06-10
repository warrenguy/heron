package conductor

import (
	"net"

	"github.com/warrenguy/heron/aeron/atomic"
)

const (
	sizeOfInt32  = atomic.SizeInt32
	sizeOfInt64  = atomic.SizeInt64
	sizeOfUint16 = atomic.SizeUint16

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

const (
	removeMessageRegistrationIDOffset = CorrelatedMessageLength
	RemoveMessageRegistrationIDOffset = removeMessageRegistrationIDOffset
	RemoveMessageBodyLength           = RemoveMessageRegistrationIDOffset + sizeOfInt64
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
