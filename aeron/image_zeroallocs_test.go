package aeron_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

var imagePollZeroAllocImage *aeron.Image

func imagePollZeroAllocIteration() {
	aeron.ImageSetSubscriberPositionForTest(imagePollZeroAllocImage, 0)
	imagePollSink = imagePollZeroAllocImage.Poll(imagePollZeroAllocHandler, 1)
}

func TestImage_Poll_ZeroAllocs(t *testing.T) {
	imagePollZeroAllocImage = setupZeroAllocPollImage(t)
	testutil.AssertZeroAllocs(t, "Image.Poll", 1000, imagePollZeroAllocIteration)
}
