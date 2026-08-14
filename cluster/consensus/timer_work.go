package consensus

import (
	"errors"

	"github.com/warrenguy/heron/cluster/codecs"
)

// errTimerCapacity is the first timer registry failure when the wheel refuses
// a schedule. Marked once on [Agent.timerFailure].
var errTimerCapacity = errors.New("cluster: timer wheel capacity refused")

// TimerFailure returns the first timer registry failure, or nil. Marked once.
// Never cleared in this surface.
//
// Concurrency: invoker-only.
func (a *Agent) TimerFailure() error {
	return a.timerFailure
}

// ScheduledTimerCount returns the live timer count on this member's wheel.
//
// Concurrency: invoker-only.
func (a *Agent) ScheduledTimerCount() int64 {
	return a.timers.count()
}

// handleScheduleTimer schedules the wire correlation id at the wire deadline.
// Runs on every role and while an election is live.
//
// A refused schedule marks timerFailure once and drops the frame. After a
// refusal no timer remains for the correlation id.
func (a *Agent) handleScheduleTimer(fields codecs.ScheduleTimerFields) {
	if a.timers.schedule(fields.CorrelationID, fields.Deadline) {
		return
	}
	if a.timerFailure == nil {
		a.timerFailure = errTimerCapacity
	}
}

// handleCancelTimer cancels the wire correlation id. A miss is a silent no-op.
func (a *Agent) handleCancelTimer(fields codecs.CancelTimerFields) {
	_ = a.timers.cancel(fields.CorrelationID)
}

// handleTimerExpiry appends one TimerEvent for correlationID at nowNs.
//
//   - True: AppendTimer succeeded. poll consumes the timer.
//   - False: AppendTimer failed. The timer stays scheduled. That poll stops.
func (a *Agent) handleTimerExpiry(correlationID, nowNs int64) bool {
	return a.logPublisher.AppendTimer(a.leadershipTermID, correlationID, nowNs)
}

// timerWork polls the timer wheel on a settled leader. Each expiry appends one
// TimerEvent through the log publisher. A failed append keeps the timer and
// stops the poll until the next DoWork call.
//
// Returns 0 when the role is not leader, an election is live, or the log
// publisher is nil or unbound.
func (a *Agent) timerWork(nowNs int64) int {
	if a.role != RoleLeader || a.election != nil {
		return 0
	}
	if a.logPublisher == nil || a.logPublisher.Publication() == nil {
		return 0
	}
	return a.timers.poll(nowNs, a.timerExpiryHandler)
}
