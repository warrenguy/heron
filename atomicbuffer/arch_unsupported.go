//go:build !amd64 && !arm64

package atomicbuffer

// The module builds only for amd64 and arm64. This undefined name stops the
// build on any other GOARCH.
var _ = atomicbufferSupportsOnlyAMD64AndARM64
