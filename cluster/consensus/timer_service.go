package consensus

import "github.com/warrenguy/heron/timerwheel"

// timerPollLimit caps expiries per timerService.poll call.
const timerPollLimit = 20

// timerWheelTickResolutionNs is one wheel tick in nanoseconds. Cluster time is
// nanoseconds. The constant is 1<<23 ns (about 8.39ms).
const timerWheelTickResolutionNs int64 = 1 << 23

// timerWheelTicksPerWheel is the spoke count. One revolution is about 1.07s.
const timerWheelTicksPerWheel = 128

// timerWheelInitialTickAllocation is the initial slot count per spoke. Initial
// array: 128 * 16 slots.
const timerWheelInitialTickAllocation = 16

// timerWheelMaxSlots caps capacity growth.
const timerWheelMaxSlots = 1 << 30

// Behaviour: timerService maps user correlation ids onto wheel timer ids.
// Reschedule replaces the deadline. Cancel is by correlation id. Expiry is
// exactly once. An aborted expiry is redelivered.
//
// Any int64 correlation id is valid. Go maps have no missing-value sentinel.
// A deadline equal to timerwheel.NullDeadline is clamped to
// timerwheel.NullDeadline-1. The clamp keeps NullDeadline out of the slot
// array.
//
// Concurrency: invoker-only.
type timerService struct {
	wheel                  *timerwheel.Wheel
	timerIDByCorrelationID map[int64]int64
	correlationIDByTimerID map[int64]int64

	// expiryHandler holds the caller handler for one poll. poll clears
	// expiryHandler before poll returns.
	expiryHandler func(correlationID, now int64) bool

	// abort is true when expiryHandler returned false during the current poll.
	abort bool

	// wheelExpiryHandler is handleWheelExpiry bound once at construction. poll
	// passes wheelExpiryHandler into Wheel.Poll. The cache avoids allocating a
	// method value on each duty cycle.
	wheelExpiryHandler func(now, timerID int64) bool
}

// newTimerService returns a timerService with the package wheel constants and
// startTime 0.
func newTimerService() *timerService {
	return newTimerServiceForWheel(timerwheel.New(
		0,
		timerWheelTickResolutionNs,
		timerWheelTicksPerWheel,
		timerWheelInitialTickAllocation,
		timerWheelMaxSlots,
	))
}

// newTimerServiceForWheel returns a timerService over the given wheel. The
// correlation maps start empty. Tests pass small wheel geometries.
func newTimerServiceForWheel(wheel *timerwheel.Wheel) *timerService {
	t := &timerService{
		wheel:                  wheel,
		timerIDByCorrelationID: make(map[int64]int64),
		correlationIDByTimerID: make(map[int64]int64),
	}
	t.wheelExpiryHandler = t.handleWheelExpiry
	return t
}

// schedule stores one timer for correlationID at deadline. An existing timer
// for the same correlationID is cancelled first. A deadline equal to
// timerwheel.NullDeadline is clamped to timerwheel.NullDeadline-1.
//
// Returns false when the wheel refuses capacity growth. After a refusal no
// timer exists for correlationID.
//
//   - A fresh schedule: the maps stay untouched.
//   - A reschedule: the old timer and both map entries are already removed.
func (t *timerService) schedule(correlationID, deadline int64) bool {
	t.cancel(correlationID)
	if deadline == timerwheel.NullDeadline {
		deadline = timerwheel.NullDeadline - 1
	}
	timerID, ok := t.wheel.Schedule(deadline)
	if !ok {
		return false
	}
	t.timerIDByCorrelationID[correlationID] = timerID
	t.correlationIDByTimerID[timerID] = correlationID
	return true
}

// cancel removes the timer for correlationID.
//
// Returns false when none is scheduled.
func (t *timerService) cancel(correlationID int64) bool {
	timerID, ok := t.timerIDByCorrelationID[correlationID]
	if !ok {
		return false
	}
	t.removeMapping(correlationID, timerID)
	t.wheel.Cancel(timerID)
	return true
}

func (t *timerService) removeMapping(correlationID, timerID int64) {
	delete(t.timerIDByCorrelationID, correlationID)
	delete(t.correlationIDByTimerID, timerID)
}

// poll expires timers due at now, up to timerPollLimit per call. handler
// receives each correlation id and now.
//
//   - True consumes the timer and removes both map entries.
//   - False keeps the map entries. The wheel slot stays as the wheel poll
//     restored it. The scan stops.
//
// The scan repeats until one of:
//   - the expiry count reaches timerPollLimit
//   - the handler returns false
//   - currentTickTime is at or past now
//
// A catch-up that lands on the now wheel tick does not scan that spoke in the
// same call. The caller must poll again to scan that spoke.
func (t *timerService) poll(now int64, handler func(correlationID, now int64) bool) int {
	expired := 0
	t.expiryHandler = handler
	t.abort = false
	for {
		expired += t.wheel.Poll(now, t.wheelExpiryHandler, timerPollLimit-expired)
		if t.abort || expired >= timerPollLimit || t.wheel.CurrentTickTime() >= now {
			break
		}
	}
	t.expiryHandler = nil
	return expired
}

// handleWheelExpiry delivers one due wheel timer to expiryHandler.
//
//   - Unknown timer id: returns true. Maps stay untouched.
//   - expiryHandler true: removes both map entries. Returns true.
//   - expiryHandler false: sets abort. Keeps the map entries. Returns false.
//
// Wheel.Poll restores the slot when handleWheelExpiry returns false.
func (t *timerService) handleWheelExpiry(now, timerID int64) bool {
	correlationID, ok := t.correlationIDByTimerID[timerID]
	if !ok {
		return true
	}
	if !t.expiryHandler(correlationID, now) {
		t.abort = true
		return false
	}
	t.removeMapping(correlationID, timerID)
	return true
}

// currentTime winds the wheel forward to now. Expires nothing.
func (t *timerService) currentTime(now int64) {
	t.wheel.CurrentTime(now)
}

func (t *timerService) count() int64 {
	return t.wheel.Count()
}
