package aeron_test

import (
	"math"
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

var (
	imagePollZeroAllocImage                  *aeron.Image
	imageControlledPollZeroAllocImage        *aeron.Image
	imageBoundedControlledPollZeroAllocImage *aeron.Image
	subscriptionPollZeroAllocSub             *aeron.Subscription
	subscriptionPollZeroAllocImage0          *aeron.Image
)

func imagePollZeroAllocIteration() {
	aeron.ImageSetSubscriberPositionForTest(imagePollZeroAllocImage, 0)
	imagePollSink = imagePollZeroAllocImage.Poll(imagePollZeroAllocHandler, 1)
}

func imageControlledPollZeroAllocHandler(
	_ atomicbuffer.AtomicBuffer,
	_ int,
	length int,
	_ *aeron.Header,
) aeron.ControlledAction {
	imagePollSink = length
	return aeron.ControlledContinue
}

func imageControlledPollZeroAllocIteration() {
	aeron.ImageSetSubscriberPositionForTest(imageControlledPollZeroAllocImage, 0)
	imagePollSink = imageControlledPollZeroAllocImage.ControlledPoll(
		imageControlledPollZeroAllocHandler,
		1,
	)
}

func imageBoundedControlledPollZeroAllocIteration() {
	aeron.ImageSetSubscriberPositionForTest(imageBoundedControlledPollZeroAllocImage, 0)
	imagePollSink = imageBoundedControlledPollZeroAllocImage.BoundedControlledPoll(
		imageControlledPollZeroAllocHandler,
		math.MaxInt64,
		1,
	)
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

func TestImage_ControlledPoll_ZeroAllocs(t *testing.T) {
	imageControlledPollZeroAllocImage = setupZeroAllocPollImage(t)
	testutil.AssertZeroAllocs(t, "Image.ControlledPoll", 1000, imageControlledPollZeroAllocIteration)
}

func TestImage_BoundedControlledPoll_ZeroAllocs(t *testing.T) {
	imageBoundedControlledPollZeroAllocImage = setupZeroAllocPollImage(t)
	testutil.AssertZeroAllocs(
		t,
		"Image.BoundedControlledPoll",
		1000,
		imageBoundedControlledPollZeroAllocIteration,
	)
}

func TestSubscription_Poll_ZeroAllocs(t *testing.T) {
	subscriptionPollZeroAllocSub = setupZeroAllocPollSubscription(t)
	testutil.AssertZeroAllocs(t, "Subscription.Poll", 1000, subscriptionPollZeroAllocIteration)
}
