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

// LogAppendPosition reports the local cluster log append high-water for the
// role the agent currently holds, using the agent's wired log publisher and
// log adapter.
//
// A leader has written the log itself, so its high-water is its own exclusive
// log publication position. A follower has written nothing to its own log
// publication and never will, so its high-water is how far the media driver has
// received on the log image it joined. One value cannot serve both: a follower's
// own log publication stays at 0 for its whole life, and a leader binds no log
// image.
//
// The follower value is what the driver has received, not what this member has
// durably recorded. It does not survive a process or driver restart and it
// cannot be replayed to a peer. That single difference - durability - is the
// whole of the substitution, and it is sound only while no member records the
// log at all, which is the case with no archive in the stack. A member that
// records must rank on its recording position instead.
//
// It also keeps advancing while this member's consumption stalls, because the
// driver's receive path is independent of the duty cycle. That is not part of
// the substitution: upstream ranks on an archive recording counter written by a
// separate recording session, which advances during a stall for the same reason.
// A consumer that must not be outrun by its own member's reported position needs
// its own back pressure in either scheme.
//
// Concurrency: invoker-only. Read from [Agent.DoWork] on the goroutine that owns
// the transport duty cycle.
//
// Lifetime: safe to construct before the log image exists, before the log
// publisher and adapter are wired, and before a role is known. A nil receiver,
// a nil agent, and a nil publisher or adapter report 0.
type LogAppendPosition struct {
	agent *Agent
}

// NewLogAppendPosition returns a role-aware append high-water source over the
// agent's wired log publisher and log adapter.
func NewLogAppendPosition(agent *Agent) *LogAppendPosition {
	return &LogAppendPosition{agent: agent}
}

// AppendPosition returns the leader's log publication position, or the
// non-leader (follower or candidate) log image's received high-water, or 0 when
// the relevant publisher or image is not wired.
func (s *LogAppendPosition) AppendPosition() int64 {
	if s == nil || s.agent == nil {
		return 0
	}
	if s.agent.Role() == RoleLeader {
		return s.agent.logPublisher.Position()
	}
	// Follower and candidate: image received high-water.
	adapter := s.agent.logAdapter
	if adapter == nil {
		return 0
	}
	image := adapter.Image()
	if image == nil {
		return 0
	}
	return image.ReceivedPosition()
}

var _ AppendPositionSource = (*LogAppendPosition)(nil)
