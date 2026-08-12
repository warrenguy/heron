//go:build testhooks

package aeron

import "github.com/warrenguy/heron/internal/testhook"

type fragmentedWriteTrackFn func(termIndex, frameOffset int, begin bool)

type fragmentedReservationTrackFn func(termIndex, termOffset int, begin bool)

// fragmentedWriteTracks and fragmentedReservationTracks are keyed by
// *Publication. Concurrent Offer peers must not observe another test's hook.
var (
	fragmentedWriteTracks       = testhook.NewRegistry[*Publication, fragmentedWriteTrackFn]()
	fragmentedReservationTracks = testhook.NewRegistry[*Publication, fragmentedReservationTrackFn]()
)

func trackFragmentedReservationBegin(pub *Publication, termIndex, termOffset int) bool {
	if fn := fragmentedReservationTracks.Load(pub); fn != nil {
		fn(termIndex, termOffset, true)
		return true
	}
	return false
}

func trackFragmentedReservationEnd(pub *Publication, termIndex, termOffset int) {
	if fn := fragmentedReservationTracks.Load(pub); fn != nil {
		fn(termIndex, termOffset, false)
	}
}

func trackFragmentedWrite(pub *Publication, termIndex, frameOffset int, begin bool) {
	if fn := fragmentedWriteTracks.Load(pub); fn != nil {
		fn(termIndex, frameOffset, begin)
	}
}

func setFragmentedWriteTrackHook(pub *Publication, fn fragmentedWriteTrackFn) {
	if fn == nil {
		fragmentedWriteTracks.Delete(pub)
		return
	}
	fragmentedWriteTracks.Store(pub, fn)
}

func setFragmentedReservationTrackHook(pub *Publication, fn fragmentedReservationTrackFn) {
	if fn == nil {
		fragmentedReservationTracks.Delete(pub)
		return
	}
	fragmentedReservationTracks.Store(pub, fn)
}
