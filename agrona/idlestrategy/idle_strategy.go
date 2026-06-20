// Package idlestrategy provides Agrona-compatible agent idle pacing.
//
// Implementations are not thread-safe; use one strategy per agent goroutine.
// Busy-spin pacing uses runtime.procyield via //go:linkname; re-verify on Go upgrades.
package idlestrategy

// IdleStrategy paces duty cycles from agent work counts.
// Backoff resets when workCount > 0; other implementations no-op when workCount > 0.
// Idle(0) matches Java idleStrategy.idle() with no work count.
type IdleStrategy interface {
	Idle(workCount int)
	Reset()
}
