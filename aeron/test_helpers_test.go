package aeron_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
)

func testSubscription(tb testing.TB) *aeron.Subscription {
	tb.Helper()
	return aeron.NewSubscriptionForTest(
		aeron.TestSubscriptionRegistrationID,
		aeron.TestPublicationStreamID,
		aeron.TestImageSourceIdentity,
	)
}

func testImage(tb testing.TB, sub *aeron.Subscription) *aeron.Image {
	tb.Helper()
	return aeron.NewImageForTest(tb, sub, aeron.TestSubscriberPositionAtJoin)
}

func addTestImage(tb testing.TB, sub *aeron.Subscription) *aeron.Image {
	tb.Helper()
	image := testImage(tb, sub)
	aeron.SubscriptionAddImageForTest(sub, image)
	return image
}
