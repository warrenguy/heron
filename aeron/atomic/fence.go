package atomic

import "sync/atomic"

var fencePace uint32

func ReleaseFence() {
	atomic.StoreUint32(&fencePace, 0)
}

func AcquireFence() {
	atomic.LoadUint32(&fencePace)
}
