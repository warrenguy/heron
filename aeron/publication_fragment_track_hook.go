//go:build testhooks

package aeron

var (
	fragmentedWriteTrack       func(termIndex, frameOffset int, begin bool)
	fragmentedReservationTrack func(termIndex, termOffset int, begin bool)
)

func trackFragmentedReservationBegin(termIndex, termOffset int) bool {
	if fragmentedReservationTrack != nil {
		fragmentedReservationTrack(termIndex, termOffset, true)
		return true
	}
	return false
}

func trackFragmentedReservationEnd(termIndex, termOffset int) {
	if fragmentedReservationTrack != nil {
		fragmentedReservationTrack(termIndex, termOffset, false)
	}
}

func trackFragmentedWrite(termIndex, frameOffset int, begin bool) {
	if fragmentedWriteTrack != nil {
		fragmentedWriteTrack(termIndex, frameOffset, begin)
	}
}

func setFragmentedWriteTrackHook(fn func(termIndex, frameOffset int, begin bool)) {
	fragmentedWriteTrack = fn
}

func setFragmentedReservationTrackHook(fn func(termIndex, termOffset int, begin bool)) {
	fragmentedReservationTrack = fn
}
