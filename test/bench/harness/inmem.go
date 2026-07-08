//go:build bench

// Package harness composes aeron bench fixtures for test/bench benchmarks.
// Built with //go:build bench; not for production use.
package harness

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/atomicbuffer"
)

func resetTermWithTimer(b *testing.B, reset func()) {
	b.StopTimer()
	reset()
	b.StartTimer()
}

// withAdminActionRetryExcludedFromTimer spins on AdminAction outside bench timing.
// When the first attempt returns AdminAction, the whole spin and the successful
// attempt that ends it are untimed; testing.B cannot exclude only the spin while
// still timing that final attempt.
func withAdminActionRetryExcludedFromTimer(b *testing.B, attempt func() int64) int64 {
	pos := attempt()
	if pos == aeron.AdminAction {
		b.StopTimer()
		for pos == aeron.AdminAction {
			pos = attempt()
		}
		b.StartTimer()
	}
	return pos
}

func resetPublicationTermWithTimer(
	b *testing.B,
	pub *aeron.Publication,
	rewindImage *aeron.Image,
) {
	resetTermWithTimer(b, func() {
		aeron.ResetInMemPublicationTermForBench(pub)
		if rewindImage != nil {
			aeron.SetImageSubscriberPositionForBench(rewindImage, 0)
		}
	})
}

// OfferWithTermOverflowReset calls pub.Offer once per invocation. On any negative
// return (including AdminAction at term-end rotation), it stops the bench timer,
// resets the publication term, and restarts the timer. Contract benches under alloc
// regression use this shape intentionally for steady single-term measurement;
// callers do not retry Offer in the same iteration.
func OfferWithTermOverflowReset(
	b *testing.B,
	pub *aeron.Publication,
	payload []byte,
) int64 {
	return offerOnceWithOverflowReset(b, pub, payload, nil)
}

// OfferEndToEndWithOverflowReset calls pub.Offer once per invocation. On any negative
// return (including AdminAction), it stops the bench timer, resets the publication
// term, rewinds the poll image, and restarts the timer. Same reset-on-any-negative
// contract as OfferWithTermOverflowReset for steady single-term E2E measurement.
func OfferEndToEndWithOverflowReset(
	b *testing.B,
	pub *aeron.Publication,
	image *aeron.Image,
	payload []byte,
) int64 {
	return offerOnceWithOverflowReset(b, pub, payload, image)
}

func offerRetryWithTermOverflowReset(
	b *testing.B,
	pub *aeron.Publication,
	payload []byte,
	rewindImage *aeron.Image,
) int64 {
	pos := withAdminActionRetryExcludedFromTimer(b, func() int64 { return pub.Offer(payload) })
	if pos < 0 {
		resetPublicationTermWithTimer(b, pub, rewindImage)
	}
	return pos
}

// OfferEndToEndRetryWithTermOverflowReset spins on AdminAction during Offer, then resets
// the publication term and poll image on other negative returns. AdminAction retry spin
// is excluded from bench timing. Used for fragmented E2E payloads; unfragmented E2E uses
// OfferEndToEndWithOverflowReset.
func OfferEndToEndRetryWithTermOverflowReset(
	b *testing.B,
	pub *aeron.Publication,
	image *aeron.Image,
	payload []byte,
) int64 {
	return offerRetryWithTermOverflowReset(b, pub, payload, image)
}

func offerOnceWithOverflowReset(
	b *testing.B,
	pub *aeron.Publication,
	payload []byte,
	rewindImage *aeron.Image,
) int64 {
	pos := pub.Offer(payload)
	if pos < 0 {
		resetPublicationTermWithTimer(b, pub, rewindImage)
	}
	return pos
}

func PollZeroAllocHandler(sink *int) func(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) {
	return func(
		_ atomicbuffer.AtomicBuffer,
		_ int,
		length int,
		_ *aeron.Header,
	) {
		*sink = length
	}
}

// OfferRetryWithTermOverflowReset spins on AdminAction during Offer, then resets the
// publication term on other negative returns. AdminAction retry spin is excluded from
// bench timing. Used for fragmented offer payloads and the optional admin-action-retry
// sub-bench; unfragmented contract offer uses OfferWithTermOverflowReset instead.
func OfferRetryWithTermOverflowReset(
	b *testing.B,
	pub *aeron.Publication,
	payload []byte,
) int64 {
	return offerRetryWithTermOverflowReset(b, pub, payload, nil)
}
