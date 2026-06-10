package aeron_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func BenchmarkImage_Poll(b *testing.B) {
	image := setupZeroAllocPollImage(b)
	b.ReportAllocs()
	b.SetBytes(int64(logbuffer.AlignFrameLength(len("zero-alloc") + logbuffer.DataHeaderLength)))
	for b.Loop() {
		aeron.ImageSetSubscriberPositionForTest(image, 0)
		imagePollSink = image.Poll(imagePollZeroAllocHandler, 1)
	}
}
