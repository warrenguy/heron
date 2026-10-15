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

// ExpiredTimerCount returns how many correlation ids have a TimerEvent on
// record with no timer to cancel.
//
// Concurrency: invoker-only.
func (a *Agent) ExpiredTimerCount() int {
	return len(a.expiredTimerCountByCorrelationID)
}

// handleScheduleTimer schedules the wire correlation id at the wire deadline.
// Runs on every role and while an election is live.
//
// If a TimerEvent for this id already arrived, the timer has fired and the
// request is ignored. A refused schedule marks timerFailure once and drops
// the frame. After a refusal no timer remains for the correlation id.
func (a *Agent) handleScheduleTimer(fields codecs.ScheduleTimerFields) {
	correlationID := fields.CorrelationID
	if count := a.expiredTimerCountByCorrelationID[correlationID]; count > 0 {
		if count == 1 {
			delete(a.expiredTimerCountByCorrelationID, correlationID)
		} else {
			a.expiredTimerCountByCorrelationID[correlationID] = count - 1
		}
		return
	}
	if a.timers.schedule(correlationID, fields.Deadline) {
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

// handleTimerEvent cancels the local timer named by a TimerEvent from the
// cluster log. The frame ends at logPosition. Runs on every role.
//
// A frame at or below [Agent.CommitPosition] is ignored: the service may
// already have read it and scheduled the timer again.
//
// A frame with no timer to cancel is still recorded. The service sends
// ScheduleTimer only after it reads the log, so that request can arrive
// after the TimerEvent for the same timer. handleScheduleTimer then ignores
// the request.
func (a *Agent) handleTimerEvent(correlationID, logPosition int64) {
	if logPosition <= a.CommitPosition() {
		return
	}
	if !a.timers.cancel(correlationID) {
		a.expiredTimerCountByCorrelationID[correlationID]++
	}
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
