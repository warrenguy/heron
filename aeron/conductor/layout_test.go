package conductor_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
)

func TestLayout_goldenOffsets(t *testing.T) {
	tests := []struct {
		name string
		got  int
		want int
	}{
		{"CorrelatedMessageClientIDOffset", conductor.CorrelatedMessageClientIDOffset, 0},
		{"CorrelatedMessageCorrelationIDOffset", conductor.CorrelatedMessageCorrelationIDOffset, 8},
		{
			"CorrelationAndInt32CorrelationIDOffset",
			conductor.CorrelationAndInt32CorrelationIDOffset,
			0,
		},
		{"CorrelationAndInt32Int32ValueOffset", conductor.CorrelationAndInt32Int32ValueOffset, 8},
		{
			"SubscriptionReadyCorrelationIDOffset",
			conductor.SubscriptionReadyCorrelationIDOffset,
			0,
		},
		{
			"SubscriptionReadyChannelStatusCounterIDOffset",
			conductor.SubscriptionReadyChannelStatusCounterIDOffset,
			8,
		},
		{
			"UnavailableImageCorrelationIDOffset",
			conductor.UnavailableImageCorrelationIDOffset,
			0,
		},
		{
			"UnavailableImageSubscriptionRegistrationIDOffset",
			conductor.UnavailableImageSubscriptionRegistrationIDOffset,
			8,
		},
		{"UnavailableImageStreamIDOffset", conductor.UnavailableImageStreamIDOffset, 16},
		{
			"AvailableImageCorrelationIDOffset",
			conductor.AvailableImageCorrelationIDOffset,
			0,
		},
		{"AvailableImageSessionIDOffset", conductor.AvailableImageSessionIDOffset, 8},
		{"AvailableImageStreamIDOffset", conductor.AvailableImageStreamIDOffset, 12},
		{
			"AvailableImageSubscriptionRegistrationIDOffset",
			conductor.AvailableImageSubscriptionRegistrationIDOffset,
			16,
		},
		{
			"AvailableImageSubscriberPositionIDOffset",
			conductor.AvailableImageSubscriberPositionIDOffset,
			24,
		},
		{
			"PublicationBuffersReadyCorrelationIDOffset",
			conductor.PublicationBuffersReadyCorrelationIDOffset,
			0,
		},
		{
			"PublicationBuffersReadyRegistrationIDOffset",
			conductor.PublicationBuffersReadyRegistrationIDOffset,
			8,
		},
		{
			"PublicationBuffersReadySessionIDOffset",
			conductor.PublicationBuffersReadySessionIDOffset,
			16,
		},
		{
			"PublicationBuffersReadyStreamIDOffset",
			conductor.PublicationBuffersReadyStreamIDOffset,
			20,
		},
		{
			"PublicationBuffersReadyPublicationLimitCounterIDOffset",
			conductor.PublicationBuffersReadyPublicationLimitCounterIDOffset,
			24,
		},
		{
			"PublicationBuffersReadyChannelStatusCounterIDOffset",
			conductor.PublicationBuffersReadyChannelStatusCounterIDOffset,
			28,
		},
		{
			"PublicationErrorRegistrationIDOffset",
			conductor.PublicationErrorRegistrationIDOffset,
			0,
		},
		{
			"PublicationErrorDestinationRegistrationIDOffset",
			conductor.PublicationErrorDestinationRegistrationIDOffset,
			8,
		},
		{"PublicationErrorSessionIDOffset", conductor.PublicationErrorSessionIDOffset, 16},
		{"PublicationErrorStreamIDOffset", conductor.PublicationErrorStreamIDOffset, 20},
		{"PublicationErrorReceiverIDOffset", conductor.PublicationErrorReceiverIDOffset, 24},
		{"PublicationErrorGroupTagOffset", conductor.PublicationErrorGroupTagOffset, 32},
		{"PublicationErrorAddressTypeOffset", conductor.PublicationErrorAddressTypeOffset, 40},
		{"PublicationErrorAddressPortOffset", conductor.PublicationErrorAddressPortOffset, 42},
		{"PublicationErrorAddressOffset", conductor.PublicationErrorAddressOffset, 44},
		{"PublicationErrorErrorCodeOffset", conductor.PublicationErrorErrorCodeOffset, 60},
		{
			"ErrorResponseOffendingCommandCorrelationIDOffset",
			conductor.ErrorResponseOffendingCommandCorrelationIDOffset,
			0,
		},
		{"ErrorResponseErrorCodeOffset", conductor.ErrorResponseErrorCodeOffset, 8},
		{
			"ErrorResponseErrorMessageLengthOffset",
			conductor.ErrorResponseErrorMessageLengthOffset,
			12,
		},
		{"PublicationMessageStreamIDOffset", conductor.PublicationMessageStreamIDOffset, 16},
		{"PublicationMessageChannelOffset", conductor.PublicationMessageChannelOffset, 20},
		{
			"SubscriptionMessageRegistrationCorrelationIDOffset",
			conductor.SubscriptionMessageRegistrationCorrelationIDOffset,
			16,
		},
		{"SubscriptionMessageStreamIDOffset", conductor.SubscriptionMessageStreamIDOffset, 24},
		{"SubscriptionMessageChannelOffset", conductor.SubscriptionMessageChannelOffset, 28},
		{
			"RemoveMessageRegistrationIDOffset",
			conductor.RemoveMessageRegistrationIDOffset,
			16,
		},
		{"RemoveMessageBodyLength", conductor.RemoveMessageBodyLength, 24},
		{"RemovePublicationFlagsOffset", conductor.RemovePublicationFlagsOffset, 24},
		{"RemovePublicationBodyLength", conductor.RemovePublicationBodyLength, 32},
		{
			"OperationSuccessCorrelationIDOffset",
			conductor.OperationSuccessCorrelationIDOffset,
			0,
		},
		{"OperationSuccessBodyLength", conductor.OperationSuccessBodyLength, 8},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, test.got)
		})
	}

	bodyLengthTests := []struct {
		name string
		got  int
		want int
	}{
		{"SubscriptionReadyBodyLength", conductor.SubscriptionReadyBodyLength, 12},
		{"UnavailableImageFixedLength", conductor.UnavailableImageFixedLength, 20},
		{"AvailableImageFixedLength", conductor.AvailableImageFixedLength, 28},
		{"PublicationBuffersReadyFixedLength", conductor.PublicationBuffersReadyFixedLength, 32},
		{"PublicationErrorFixedLength", conductor.PublicationErrorFixedLength, 64},
		{"ErrorResponseFixedBodyLength", conductor.ErrorResponseFixedBodyLength, 16},
		{"PublicationMessageMinimumBodyLength", conductor.PublicationMessageMinimumBodyLength, 24},
		{"SubscriptionMessageMinimumBodyLength", conductor.SubscriptionMessageMinimumBodyLength, 32},
		{"CorrelationAndInt32BodyLength", conductor.CorrelationAndInt32BodyLength, 12},
	}

	for _, test := range bodyLengthTests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, test.got)
		})
	}
	require.Equal(t, 2*atomic.SizeInt64, conductor.CorrelatedMessageLength)
	require.Equal(
		t,
		net.IPv6len,
		conductor.PublicationErrorErrorCodeOffset-conductor.PublicationErrorAddressOffset,
	)

	require.Equal(
		t,
		conductor.SubscriptionReadyBodyLength,
		conductor.SubscriptionReadyChannelStatusCounterIDOffset+atomic.SizeInt32,
	)
	require.Equal(
		t,
		conductor.UnavailableImageFixedLength,
		conductor.UnavailableImageStreamIDOffset+atomic.SizeInt32,
	)
	require.Equal(
		t,
		conductor.AvailableImageFixedLength,
		conductor.AvailableImageSubscriberPositionIDOffset+atomic.SizeInt32,
	)
	require.Equal(
		t,
		conductor.PublicationBuffersReadyFixedLength,
		conductor.PublicationBuffersReadyChannelStatusCounterIDOffset+atomic.SizeInt32,
	)
	require.Equal(
		t,
		conductor.PublicationErrorFixedLength,
		conductor.PublicationErrorErrorCodeOffset+atomic.SizeInt32,
	)
	require.Equal(
		t,
		conductor.ErrorResponseFixedBodyLength,
		conductor.ErrorResponseErrorMessageLengthOffset+atomic.SizeInt32,
	)
	require.Equal(
		t,
		conductor.PublicationMessageMinimumBodyLength,
		conductor.PublicationMessageChannelOffset+atomic.SizeInt32,
	)
	require.Equal(
		t,
		conductor.SubscriptionMessageMinimumBodyLength,
		conductor.SubscriptionMessageChannelOffset+atomic.SizeInt32,
	)
}
