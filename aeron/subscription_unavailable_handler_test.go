package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubscription_SetUnavailableImageHandler(t *testing.T) {
	sub := NewSubscriptionForTest(
		TestSubscriptionRegistrationID,
		TestPublicationStreamID,
		TestImageSourceIdentity,
	)
	require.Nil(t, SubscriptionUnavailableImageHandlerForTest(sub))

	var seen *Image
	handler := UnavailableImageHandler(func(image *Image) {
		seen = image
	})
	sub.SetUnavailableImageHandler(handler)

	got := SubscriptionUnavailableImageHandlerForTest(sub)
	require.NotNil(t, got)
	got(nil)
	require.Nil(t, seen)

	// Replace the handler, then clear it.
	sub.SetUnavailableImageHandler(func(image *Image) {
		seen = image
	})
	image := &Image{}
	SubscriptionUnavailableImageHandlerForTest(sub)(image)
	require.Same(t, image, seen)

	sub.SetUnavailableImageHandler(nil)
	require.Nil(t, SubscriptionUnavailableImageHandlerForTest(sub))
}

func TestSubscription_SetUnavailableImageHandler_nilReceiver(t *testing.T) {
	var sub *Subscription
	require.NotPanics(t, func() {
		sub.SetUnavailableImageHandler(func(*Image) {})
	})
}
