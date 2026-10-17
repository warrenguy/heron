package atomicbuffer

import _ "unsafe"

// AcquireFence orders loads before the call ahead of loads and stores after
// the call, like Java VarHandle.acquireFence. Holds on amd64 and arm64.
//
//go:linkname AcquireFence atomicbuffer_acquireFence
func AcquireFence()
