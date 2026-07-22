package consensus

import "sync/atomic"

// AppendPositionSource supplies the local log append high-water.
//
// Concurrency: invoker-only reads from [Agent.DoWork] (same as commit).
type AppendPositionSource interface {
	AppendPosition() int64
}

// AtomicAppendPosition is mode-1 inject for unit/sim.
type AtomicAppendPosition struct {
	V *atomic.Int64
}

// AppendPosition returns the current inject value, or 0 when the receiver or
// counter is nil.
func (s *AtomicAppendPosition) AppendPosition() int64 {
	if s == nil || s.V == nil {
		return 0
	}
	return s.V.Load()
}

// SetAppendSource wires the append high-water source for leader rank and
// follower AppendPosition publish.
//
// Concurrency: invoker-only (no concurrent [Agent.DoWork] during wire).
//
// Nil is legal; leader [Agent.consensusWork] returns 0 until rewired.
func (a *Agent) SetAppendSource(src AppendPositionSource) { a.appendSource = src }

// AppendSource returns the wired [AppendPositionSource], or nil if unset.
func (a *Agent) AppendSource() AppendPositionSource { return a.appendSource }
