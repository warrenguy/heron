package idlestrategy

import _ "unsafe"

//go:linkname procyield runtime.procyield
func procyield(cycles uint32)

// spinWait yields the processor without blocking via runtime.procyield
// (linked through procyield above). The runtime ABI is unsupported; re-verify
// on Go upgrades.
func spinWait() {
	procyield(1)
}
