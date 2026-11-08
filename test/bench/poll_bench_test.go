//go:build bench

package bench_test

import (
	"testing"

	"luas.fast/aeron"
	"luas.fast/atomicbuffer"
	"luas.fast/test/bench/harness"
)

var (
	imagePollLength    int
	imagePollFragments int
)

func benchmarkImagePoll(
	b *testing.B,
	handler func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	),
) {
	image := aeron.DefaultBenchPollImageForBench()
	b.ReportAllocs()
	b.SetBytes(aeron.BenchPollZeroAllocFrameBytes())
	b.ResetTimer()
	for b.Loop() {
		aeron.SetImageSubscriberPositionForBench(image, 0)
		imagePollFragments = image.Poll(handler, 1)
	}
}

func BenchmarkImagePoll(b *testing.B) {
	benchmarkImagePoll(b, harness.PollZeroAllocHandler(&imagePollLength))
}
