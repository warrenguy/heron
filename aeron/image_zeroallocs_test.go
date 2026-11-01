package aeron_test

import (
	"math"
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

var (
	imagePollZeroAllocImage                       *aeron.Image
	imageControlledPollZeroAllocImage             *aeron.Image
	imageBoundedControlledPollZeroAllocImage      *aeron.Image
	imageBoundedPollZeroAllocImage                *aeron.Image
	imagePollErrorHandlerZeroAllocImage           *aeron.Image
	imageControlledPollErrorHandlerZeroAllocImage *aeron.Image
	subscriptionPollZeroAllocSub                  *aeron.Subscription
	subscriptionPollZeroAllocImage0               *aeron.Image
)

func imagePollZeroAllocIteration() {
	aeron.ImageSetSubscriberPositionForTest(imagePollZeroAllocImage, 0)
	imagePollSink = imagePollZeroAllocImage.Poll(imagePollZeroAllocHandler, 1)
}

func imagePollErrorHandlerZeroAllocIteration() {
	aeron.ImageSetSubscriberPositionForTest(imagePollErrorHandlerZeroAllocImage, 0)
	imagePollSink = imagePollErrorHandlerZeroAllocImage.Poll(imagePollZeroAllocHandler, 1)
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

func imageControlledPollErrorHandlerZeroAllocIteration() {
	aeron.ImageSetSubscriberPositionForTest(imageControlledPollErrorHandlerZeroAllocImage, 0)
	imagePollSink = imageControlledPollErrorHandlerZeroAllocImage.ControlledPoll(
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

func imageBoundedPollZeroAllocIteration() {
	aeron.ImageSetSubscriberPositionForTest(imageBoundedPollZeroAllocImage, 0)
	imagePollSink = imageBoundedPollZeroAllocImage.BoundedPoll(
		imagePollZeroAllocHandler,
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

func TestImage_Poll_errorHandler_ZeroAllocs(t *testing.T) {
	imagePollErrorHandlerZeroAllocImage = setupZeroAllocPollImageWithErrorHandler(t)
	testutil.AssertZeroAllocs(
		t,
		"Image.Poll with error handler",
		1000,
		imagePollErrorHandlerZeroAllocIteration,
	)
}

func TestImage_ControlledPoll_errorHandler_ZeroAllocs(t *testing.T) {
	imageControlledPollErrorHandlerZeroAllocImage = setupZeroAllocPollImageWithErrorHandler(t)
	testutil.AssertZeroAllocs(
		t,
		"Image.ControlledPoll with error handler",
		1000,
		imageControlledPollErrorHandlerZeroAllocIteration,
	)
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

func TestImage_BoundedPoll_ZeroAllocs(t *testing.T) {
	imageBoundedPollZeroAllocImage = setupZeroAllocPollImage(t)
	testutil.AssertZeroAllocs(
		t,
		"Image.BoundedPoll",
		1000,
		imageBoundedPollZeroAllocIteration,
	)
}

func TestSubscription_Poll_ZeroAllocs(t *testing.T) {
	subscriptionPollZeroAllocSub = setupZeroAllocPollSubscription(t)
	testutil.AssertZeroAllocs(t, "Subscription.Poll", 1000, subscriptionPollZeroAllocIteration)
}

func TestImage_ReceivedPosition_ZeroAllocs(t *testing.T) {
	image := setupZeroAllocReceivedPositionImage(t)
	testutil.AssertZeroAllocs(t, "Image.ReceivedPosition", 1000, func() {
		imageReceivedPositionSink = image.ReceivedPosition()
	})
}
