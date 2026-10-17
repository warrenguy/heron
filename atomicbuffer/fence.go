package atomicbuffer

import "sync/atomic"

var fencePace atomic.Uint32

func ReleaseFence() {
	fencePace.Store(0)
}
