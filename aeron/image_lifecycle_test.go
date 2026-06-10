package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
)

type stubSubscriptionHandle struct{}

func (stubSubscriptionHandle) RegistrationID() int64 { return 1 }
func (stubSubscriptionHandle) StreamID() int32       { return 1 }

type stubImageHandle struct{}

func (stubImageHandle) CorrelationID() int64 { return 1 }
func (stubImageHandle) LogFileName() string  { return "stub.log" }
func (stubImageHandle) IsClosed() bool       { return false }

var (
	stubSubHandle = stubSubscriptionHandle{}
	stubImgHandle = stubImageHandle{}
)

func TestConductorImageLifecycle_subscriptionFromHandle_errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sub  conductor.SubscriptionHandle
		want string
	}{
		{
			name: "nil",
			sub:  nil,
			want: "aeron: subscription handle is nil",
		},
		{
			name: "wrong type",
			sub:  stubSubHandle,
			want: "aeron: subscription handle aeron.stubSubscriptionHandle is not *Subscription",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ConductorImageLifecycle.NewImage(
				tt.sub,
				conductor.AvailableImage{},
				nil,
				nil,
				nil,
			)
			require.EqualError(t, err, tt.want)
		})
	}
}

func TestConductorImageLifecycle_noOpOnInvalidHandles(t *testing.T) {
	t.Parallel()

	require.False(t, ConductorImageLifecycle.HasImage(stubSubHandle, 1))
	require.False(t, ConductorImageLifecycle.AddImage(stubSubHandle, nil))
	require.False(t, ConductorImageLifecycle.AddImage(stubSubHandle, stubImgHandle))
	require.Nil(t, ConductorImageLifecycle.RemoveImage(nil, 1))
	require.Nil(t, ConductorImageLifecycle.RemoveImage(stubSubHandle, 1))
	ConductorImageLifecycle.CloseImage(nil)
	ConductorImageLifecycle.CloseImage(stubImgHandle)
	require.Nil(t, ConductorImageLifecycle.AvailableImageCallback(stubSubHandle))
	require.Nil(t, ConductorImageLifecycle.UnavailableImageCallback(stubSubHandle))
}

func TestConductorImageLifecycle_callbackClosuresIgnoreWrongHandleType(t *testing.T) {
	t.Parallel()

	sub := NewSubscriptionForTest(
		TestSubscriptionRegistrationID,
		TestPublicationStreamID,
		TestImageSourceIdentity,
	)
	var availableCalls, unavailableCalls int
	SubscriptionSetAvailableImageHandlerForTest(sub, func(*Image) {
		availableCalls++
	})
	SubscriptionSetUnavailableImageHandlerForTest(sub, func(*Image) {
		unavailableCalls++
	})

	availableCallback := ConductorImageLifecycle.AvailableImageCallback(sub)
	require.NotNil(t, availableCallback)
	availableCallback(stubImgHandle)
	require.Zero(t, availableCalls)

	unavailableCallback := ConductorImageLifecycle.UnavailableImageCallback(sub)
	require.NotNil(t, unavailableCallback)
	unavailableCallback(stubImgHandle)
	require.Zero(t, unavailableCalls)
}

func TestConductorImageLifecycle_HasImage(t *testing.T) {
	t.Parallel()

	sub := NewSubscriptionForTest(
		TestSubscriptionRegistrationID,
		TestPublicationStreamID,
		TestImageSourceIdentity,
	)
	image := NewImageForTest(t, sub, TestSubscriberPositionAtJoin)
	SubscriptionAddImageForTest(sub, image)

	require.True(t, ConductorImageLifecycle.HasImage(sub, image.CorrelationID()))
	require.False(t, ConductorImageLifecycle.HasImage(sub, image.CorrelationID()+1))
}
