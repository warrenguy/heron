// Package timerwheel is an Agrona DeadlineTimerWheel port. [Wheel] states
// behaviour and concurrency.
package timerwheel

import (
	"math"
	"math/bits"
	"slices"
)

// NullDeadline marks a free wheel slot. A stored deadline must not equal
// NullDeadline. Callers that accept an arbitrary deadline must clamp
// NullDeadline before [Wheel.Schedule].
const NullDeadline int64 = math.MaxInt64

// Wheel stores absolute nanosecond deadlines in a power-of-two spoke array.
//
// Behaviour: cancellation is O(1) by timer id. Expiry is never early. A timer
// whose deadline is earlier than a CurrentTime advance expires when Poll next
// scans that timer's spoke. The delay is at most one revolution.
//
// Concurrency: invoker-only.
type Wheel struct {
	startTime       int64
	currentTick     int64
	timerCount      int64
	ticksPerWheel   int
	tickMask        int
	resolutionShift int
	tickAllocation  int
	allocationShift int
	pollIndex       int
	maxSlots        int
	wheel           []int64
}

// New allocates a Wheel whose slots start as NullDeadline.
//
// tickResolution, ticksPerWheel, and initialTickAllocation must be powers of
// two. New panics on a non-power-of-two argument. The panic is a
// construction-time programmer error.
//
// maxSlots caps later [Wheel.Schedule] growth. [Wheel.Schedule] returns ok
// false when a doubled spoke would exceed maxSlots.
func New(
	startTime, tickResolution int64,
	ticksPerWheel, initialTickAllocation, maxSlots int,
) *Wheel {
	if !isPowerOfTwo(tickResolution) {
		panic("timerwheel: tick resolution must be a power of two")
	}
	if !isPowerOfTwo(int64(ticksPerWheel)) {
		panic("timerwheel: ticks per wheel must be a power of two")
	}
	if !isPowerOfTwo(int64(initialTickAllocation)) {
		panic("timerwheel: tick allocation must be a power of two")
	}

	return &Wheel{
		startTime:       startTime,
		ticksPerWheel:   ticksPerWheel,
		tickMask:        ticksPerWheel - 1,
		resolutionShift: bits.TrailingZeros64(uint64(tickResolution)),
		tickAllocation:  initialTickAllocation,
		allocationShift: bits.TrailingZeros(uint(initialTickAllocation)),
		maxSlots:        maxSlots,
		wheel:           makeSlots(ticksPerWheel * initialTickAllocation),
	}
}

// Schedule stores the deadline. Schedule returns a timer id that encodes the
// spoke and the slot. A full spoke doubles the per-spoke allocation.
//
// Returns ok false when the doubled capacity would exceed maxSlots. The wheel
// is untouched on refusal.
func (w *Wheel) Schedule(deadline int64) (timerID int64, ok bool) {
	spokeIndex := int(w.tickAt(deadline)) & w.tickMask
	tickStartIndex := spokeIndex << w.allocationShift

	for i := range w.tickAllocation {
		index := tickStartIndex + i
		if w.wheel[index] == NullDeadline {
			w.wheel[index] = deadline
			w.timerCount++
			return timerIDForSlot(spokeIndex, i), true
		}
	}

	return w.increaseCapacity(deadline, spokeIndex)
}

// Cancel clears the slot named by timerID.
//
// Returns false when timerID does not name an occupied slot.
func (w *Wheel) Cancel(timerID int64) bool {
	index, ok := w.indexForTimerID(timerID)
	if !ok {
		return false
	}
	if w.wheel[index] == NullDeadline {
		return false
	}
	w.wheel[index] = NullDeadline
	w.timerCount--
	return true
}

// Deadline returns the stored deadline for timerID.
//
// Returns false when timerID does not name an occupied slot.
func (w *Wheel) Deadline(timerID int64) (deadline int64, ok bool) {
	index, ok := w.indexForTimerID(timerID)
	if !ok {
		return 0, false
	}
	deadline = w.wheel[index]
	if deadline == NullDeadline {
		return 0, false
	}
	return deadline, true
}

// Poll scans the current spoke for deadlines at or before now. Poll calls
// handler for each expired timer.
//
//   - A false handler return restores the slot and the count. Poll stops the
//     scan. pollIndex is not advanced on abort. The same timer id is presented
//     again on the next Poll.
//   - Poll advances one wheel tick when the spoke is exhausted under
//     expiryLimit and now is at or past CurrentTickTime.
//   - An empty wheel (timerCount == 0) advances one wheel tick when now is at
//     or past CurrentTickTime. Poll does not scan the spoke.
//
// Returns the number of timers the handler accepted. A restored abort is not
// counted.
func (w *Wheel) Poll(
	now int64,
	handler func(now, timerID int64) bool,
	expiryLimit int,
) int {
	if w.timerCount == 0 {
		if now >= w.CurrentTickTime() {
			w.currentTick++
			w.pollIndex = 0
		}
		return 0
	}

	timersExpired := 0
	spokeIndex := int(w.currentTick) & w.tickMask

	for range w.tickAllocation {
		if timersExpired >= expiryLimit {
			break
		}
		wheelIndex := (spokeIndex << w.allocationShift) + w.pollIndex
		deadline := w.wheel[wheelIndex]

		if now >= deadline {
			w.wheel[wheelIndex] = NullDeadline
			w.timerCount--
			timersExpired++

			if !handler(now, timerIDForSlot(spokeIndex, w.pollIndex)) {
				w.wheel[wheelIndex] = deadline
				w.timerCount++
				return timersExpired - 1
			}
		}

		w.pollIndex++
		if w.pollIndex >= w.tickAllocation {
			w.pollIndex = 0
		}
	}

	if expiryLimit > timersExpired && now >= w.CurrentTickTime() {
		w.currentTick++
		w.pollIndex = 0
	}

	return timersExpired
}

// CurrentTime advances currentTick to now. CurrentTime expires nothing.
func (w *Wheel) CurrentTime(now int64) {
	w.currentTick = w.tickAt(now)
}

// CurrentTickTime is the start of the next wheel tick after currentTick.
func (w *Wheel) CurrentTickTime() int64 {
	return ((w.currentTick + 1) << w.resolutionShift) + w.startTime
}

// Count returns the number of scheduled timers.
func (w *Wheel) Count() int64 {
	return w.timerCount
}

// increaseCapacity doubles the per-spoke slot count. increaseCapacity stores
// deadline in the first new slot of spokeIndex.
//
// Returns ok false when the doubled array would exceed maxSlots. The wheel is
// untouched on refusal.
func (w *Wheel) increaseCapacity(
	deadline int64,
	spokeIndex int,
) (timerID int64, ok bool) {
	newTickAllocation := w.tickAllocation << 1
	newCapacity := int64(w.ticksPerWheel) * int64(newTickAllocation)
	if newCapacity > int64(w.maxSlots) {
		return 0, false
	}

	newAllocationShift := w.allocationShift + 1
	newWheel := makeSlots(int(newCapacity))
	for j := range w.ticksPerWheel {
		oldStart := j << w.allocationShift
		newStart := j << newAllocationShift
		copy(newWheel[newStart:newStart+w.tickAllocation], w.wheel[oldStart:oldStart+w.tickAllocation])
	}

	newWheel[(spokeIndex<<newAllocationShift)+w.tickAllocation] = deadline
	timerID = timerIDForSlot(spokeIndex, w.tickAllocation)
	w.timerCount++
	w.tickAllocation = newTickAllocation
	w.allocationShift = newAllocationShift
	w.wheel = newWheel
	return timerID, true
}

// tickAt returns the wheel tick for now. The tick is never before currentTick.
func (w *Wheel) tickAt(now int64) int64 {
	return max((now-w.startTime)>>w.resolutionShift, w.currentTick)
}

func (w *Wheel) indexForTimerID(timerID int64) (index int, ok bool) {
	spokeIndex := spokeForTimerID(timerID)
	slot := slotForTimerID(timerID)
	if spokeIndex < 0 || spokeIndex >= w.ticksPerWheel {
		return 0, false
	}
	if slot < 0 || slot >= w.tickAllocation {
		return 0, false
	}
	return (spokeIndex << w.allocationShift) + slot, true
}

func timerIDForSlot(spoke, slot int) int64 {
	return int64(spoke)<<32 | int64(uint32(slot))
}

func spokeForTimerID(timerID int64) int {
	return int(timerID >> 32)
}

func slotForTimerID(timerID int64) int {
	return int(uint32(timerID))
}

func makeSlots(n int) []int64 {
	return slices.Repeat([]int64{NullDeadline}, n)
}

func isPowerOfTwo(value int64) bool {
	return value > 0 && value&(value-1) == 0
}
