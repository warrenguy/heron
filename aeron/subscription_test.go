package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

func TestSubscription_newSubscription_fields(t *testing.T) {
	sub := testSubscription(t)

	require.Equal(t, aeron.TestSubscriptionRegistrationID, sub.RegistrationID())
	require.Equal(t, aeron.TestPublicationStreamID, sub.StreamID())
	require.Equal(t, aeron.TestImageSourceIdentity, sub.Channel())
	require.Equal(t, 0, sub.ImageCount())
	require.False(t, sub.IsConnected())
	require.False(t, sub.IsClosed())
}

func TestSubscription_removeImage_notFoundBeforeAdd(t *testing.T) {
	sub := testSubscription(t)

	require.Nil(t, aeron.SubscriptionRemoveImageForTest(sub, aeron.TestImageCorrelationID))
}

func TestSubscription_addImageRemoveImage(t *testing.T) {
	sub := testSubscription(t)
	image := addTestImage(t, sub)

	require.Equal(t, 1, sub.ImageCount())

	removed := aeron.SubscriptionRemoveImageForTest(sub, aeron.TestImageCorrelationID)
	require.Same(t, image, removed)
	require.Equal(t, 0, sub.ImageCount())
	require.False(t, image.IsClosed())

	require.Nil(t, aeron.SubscriptionRemoveImageForTest(sub, aeron.TestImageCorrelationID))
}

func TestSubscription_addImage_nilIgnored(t *testing.T) {
	sub := testSubscription(t)

	aeron.SubscriptionAddImageForTest(sub, nil)
	require.Equal(t, 0, sub.ImageCount())
	require.False(t, sub.IsConnected())
}

func TestSubscription_IsConnected_allImagesClosed(t *testing.T) {
	sub := testSubscription(t)
	closedImage := addTestImage(t, sub)
	aeron.ImageInternalCloseForTest(closedImage)

	require.Equal(t, 1, sub.ImageCount())
	require.False(t, sub.IsConnected())
}

func TestSubscription_ImageCountIsConnected(t *testing.T) {
	sub := testSubscription(t)
	openImage := testImage(t, sub)
	closedImage := aeron.NewImageForTestWithCorrelationID(
		t,
		sub,
		aeron.TestSubscriberPositionAtJoin+64,
		aeron.TestImageCorrelationID+1,
	)

	aeron.SubscriptionAddImageForTest(sub, openImage)
	aeron.SubscriptionAddImageForTest(sub, closedImage)
	aeron.ImageInternalCloseForTest(closedImage)

	require.Equal(t, 2, sub.ImageCount())
	require.True(t, sub.IsConnected())

	aeron.ImageInternalCloseForTest(openImage)
	require.False(t, sub.IsConnected())
}

func TestSubscription_Close(t *testing.T) {
	sub := testSubscription(t)

	require.NoError(t, sub.Close())
	require.True(t, sub.IsClosed())
	require.Equal(t, 0, sub.Poll(nil, 1))
}

func TestSubscription_addImage_duplicateCorrelationIDSkipped(t *testing.T) {
	sub := testSubscription(t)
	first := testImage(t, sub)
	second := testImage(t, sub)

	aeron.SubscriptionAddImageForTest(sub, first)
	aeron.SubscriptionAddImageForTest(sub, second)

	require.Equal(t, 1, sub.ImageCount())

	removed := aeron.SubscriptionRemoveImageForTest(sub, aeron.TestImageCorrelationID)
	require.NotNil(t, removed)
	require.Equal(t, 0, sub.ImageCount())
}

func TestSubscription_IsConnected_falseAfterClose(t *testing.T) {
	sub := testSubscription(t)
	addTestImage(t, sub)
	require.True(t, sub.IsConnected())

	require.NoError(t, sub.Close())
	require.False(t, sub.IsConnected())
}
