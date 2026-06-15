//go:build bench

package bench_test

import (
	"fmt"
	"testing"

	"github.com/warrenguy/heron/test/bench/harness"
)

var (
	endToEndOfferSink     int64
	endToEndPollLength    int
	endToEndPollFragments int
	endToEndPollHandler   = harness.PollZeroAllocHandler(&endToEndPollLength)
)

func benchmarkEndToEndSendReceive(b *testing.B, payload []byte) {
	b.Helper()

	pub, image := newPublicationImagePair()
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	fragmented := harness.IsFragmentedPayload(len(payload))
	for b.Loop() {
		var pos int64
		if fragmented {
			pos = harness.OfferEndToEndRetryWithTermOverflowReset(b, pub, image, payload)
		} else {
			pos = harness.OfferEndToEndWithOverflowReset(b, pub, image, payload)
		}
		if pos < 0 {
			continue
		}
		endToEndOfferSink = pos
		fragmentLimit := 1
		if fragmented {
			maxPayload := pub.MaxPayloadLength()
			fragmentLimit = (len(payload) + maxPayload - 1) / maxPayload
		}
		endToEndPollFragments = image.Poll(endToEndPollHandler, fragmentLimit)
	}
}

func BenchmarkEndToEndSendReceive(b *testing.B) {
	for _, size := range harness.EndToEndPayloadSizes() {
		payload := harness.PayloadForSize(size)
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			benchmarkEndToEndSendReceive(b, payload)
		})
	}
}
