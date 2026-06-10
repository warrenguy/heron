package aeron_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

var (
	imagePollZeroAllocImage         *aeron.Image
	subscriptionPollZeroAllocSub    *aeron.Subscription
	subscriptionPollZeroAllocImage0 *aeron.Image
)

func imagePollZeroAllocIteration() {
	aeron.ImageSetSubscriberPositionForTest(imagePollZeroAllocImage, 0)
	imagePollSink = imagePollZeroAllocImage.Poll(imagePollZeroAllocHandler, 1)
}

func subscriptionPollZeroAllocIteration() {
	aeron.ImageSetSubscriberPositionForTest(subscriptionPollZeroAllocImage0, 0)
	imagePollSink = subscriptionPollZeroAllocSub.Poll(imagePollZeroAllocHandler, 1)
}

func setupZeroAllocPollSubscription(tb testing.TB) *aeron.Subscription {
	tb.Helper()

	sub := testSubscription(tb)
	subscriptionPollZeroAllocImage0 = addSubscriptionPollImage(
		tb,
		sub,
		aeron.TestImageCorrelationID,
		[]byte(pollTestZeroAllocPayload),
	)
	return sub
}

func TestImage_Poll_ZeroAllocs(t *testing.T) {
	imagePollZeroAllocImage = setupZeroAllocPollImage(t)
	testutil.AssertZeroAllocs(t, "Image.Poll", 1000, imagePollZeroAllocIteration)
}

func TestSubscription_Poll_ZeroAllocs(t *testing.T) {
	subscriptionPollZeroAllocSub = setupZeroAllocPollSubscription(t)
	testutil.AssertZeroAllocs(t, "Subscription.Poll", 1000, subscriptionPollZeroAllocIteration)
}
