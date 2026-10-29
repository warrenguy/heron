package consensus

import (
	"errors"
	"slices"
)

// ErrServiceAckOutOfSequence reports a non-empty first queued ServiceAck that
// does not match the expected ack id and log position exactly.
//
// Fail-closed. Callers must surface the error. Callers must not retry the error
// as "not reached yet".
var ErrServiceAckOutOfSequence = errors.New(
	"consensus: service ack out of sequence",
)

// serviceAck is one queued ack from a clustered service.
type serviceAck struct {
	ackID       int64
	logPosition int64
	relevantID  int64
}

// serviceAckTracker holds one FIFO queue of acks per configured service. Each
// service has its own expected next ack id.
//
// Concurrency: invoker-only behind Agent.DoWork. No locks.
type serviceAckTracker struct {
	queues [][]serviceAck

	// nextAckIDs holds the ack id each service's first queued entry must carry.
	nextAckIDs []int64

	// droppedAckIDs holds, per service, dropped ack ids above the expected id
	// of that service.
	droppedAckIDs [][]int64

	// popCount counts removeHead calls. expectedAckID reports it with zero
	// services.
	popCount int64
}

// newServiceAckTracker sizes one empty queue, expected id and dropped id list
// per service.
//
// serviceCount must be non-negative. Callers that take AgentConfig.ServiceCount
// must panic on a negative count before construct.
func newServiceAckTracker(serviceCount int) *serviceAckTracker {
	return &serviceAckTracker{
		queues:        make([][]serviceAck, serviceCount),
		nextAckIDs:    make([]int64, serviceCount),
		droppedAckIDs: make([][]int64, serviceCount),
	}
}

// hasService reports whether serviceID indexes a configured queue.
func (t *serviceAckTracker) hasService(serviceID int32) bool {
	return serviceID >= 0 && int(serviceID) < len(t.queues)
}

// offer appends one ack to the queue for serviceID.
//
// Returns false and keeps the tracker untouched when serviceID is outside
// [0, serviceCount).
func (t *serviceAckTracker) offer(serviceID int32, ack serviceAck) bool {
	if !t.hasService(serviceID) {
		return false
	}
	t.queues[serviceID] = append(t.queues[serviceID], ack)
	return true
}

// hasReached walks the queues in service-id order (0, 1, ...). It reports
// whether every first queued entry matches logPosition and the expected id of
// its service exactly. Check-only: never pops.
//
// Returns:
//   - (false, nil) at the first empty queue; later queues are not examined
//   - (false, ErrServiceAckOutOfSequence) at the first non-empty first entry
//     that mismatches on ack id or logPosition
//   - (true, nil) when every first entry matches
//
// With zero services, returns (true, nil).
func (t *serviceAckTracker) hasReached(logPosition int64) (bool, error) {
	for i, q := range t.queues {
		if len(q) == 0 {
			return false, nil
		}
		first := q[0]
		if first.ackID != t.nextAckIDs[i] || first.logPosition != logPosition {
			return false, ErrServiceAckOutOfSequence
		}
	}
	return true, nil
}

// firstRelevantIDs returns each queue's first RelevantID in service-id order,
// or nil with zero queues. Check-only: never pops.
//
// Call only after hasReached returned true, so every queue holds an entry.
func (t *serviceAckTracker) firstRelevantIDs() []int64 {
	var ids []int64
	for _, q := range t.queues {
		ids = append(ids, q[0].relevantID)
	}
	return ids
}

// removeHead pops one entry from every non-empty queue. Each popped service's
// expected id advances past the popped id, then past each id that service
// already dropped.
//
// Call only after hasReached returned true. Empty queues are tolerated: the
// expected id of an empty queue does not move.
func (t *serviceAckTracker) removeHead() {
	t.popCount++
	for i, q := range t.queues {
		if len(q) == 0 {
			continue
		}
		t.queues[i] = q[1:]
		t.nextAckIDs[i]++
		t.skipDroppedAckIDs(i)
	}
}

// afterOfferSequenceError runs the receive-time sequence check through
// hasReached.
//
// Check-only: never pops. Uses the log position of the first entry in queue 0.
// Does not take the incoming frame position. Later ack ids may sit behind that
// first entry. Wait paths use hasReached at the wait position instead.
//
// Returns:
//   - nil when there are zero services, or queue 0 is empty (walk cannot start)
//   - nil when every first entry matches at that log position
//   - ErrServiceAckOutOfSequence when a non-empty first entry mismatches
func (t *serviceAckTracker) afterOfferSequenceError() error {
	if len(t.queues) == 0 {
		return nil
	}
	q := t.queues[0]
	if len(q) == 0 {
		return nil
	}
	_, err := t.hasReached(q[0].logPosition)
	return err
}

// noteDropped records ackID of serviceID as dropped without a wait pop. The
// expected id of the service advances when ackID equals it. A higher id waits
// in droppedAckIDs until the expected id reaches it. An id below the expected
// id, a repeated id, or an out-of-range serviceID does not change the tracker.
func (t *serviceAckTracker) noteDropped(serviceID int32, ackID int64) {
	if !t.hasService(serviceID) {
		return
	}
	t.noteDroppedAt(int(serviceID), ackID)
}

// noteDroppedAt records ackID of service i as dropped. i must index a
// configured queue.
func (t *serviceAckTracker) noteDroppedAt(i int, ackID int64) {
	if ackID < t.nextAckIDs[i] || slices.Contains(t.droppedAckIDs[i], ackID) {
		return
	}
	t.droppedAckIDs[i] = append(t.droppedAckIDs[i], ackID)
	t.skipDroppedAckIDs(i)
}

// skipDroppedAckIDs advances the expected id of service i past each id that
// service already dropped. Removes each skipped id from droppedAckIDs.
func (t *serviceAckTracker) skipDroppedAckIDs(i int) {
	for {
		k := slices.Index(t.droppedAckIDs[i], t.nextAckIDs[i])
		if k < 0 {
			return
		}
		t.droppedAckIDs[i] = slices.Delete(t.droppedAckIDs[i], k, k+1)
		t.nextAckIDs[i]++
	}
}

// expectedAckID returns the lowest expected ack id across services. With zero
// services, returns the removeHead count.
func (t *serviceAckTracker) expectedAckID() int64 {
	if len(t.nextAckIDs) == 0 {
		return t.popCount
	}
	return slices.Min(t.nextAckIDs)
}

// queueDepth returns the queued ack count for serviceID, or 0 when serviceID is
// outside [0, serviceCount).
func (t *serviceAckTracker) queueDepth(serviceID int32) int {
	if !t.hasService(serviceID) {
		return 0
	}
	return len(t.queues[serviceID])
}

// dropAt removes every queued entry whose logPosition equals pos. It notes
// each removed ack id as dropped. onDrop runs once per removed entry. onDrop
// may be nil.
func (t *serviceAckTracker) dropAt(pos int64, onDrop func(serviceID int32)) {
	for i, q := range t.queues {
		n := 0
		for _, ack := range q {
			if ack.logPosition == pos {
				t.noteDroppedAt(i, ack.ackID)
				if onDrop != nil {
					onDrop(int32(i))
				}
				continue
			}
			q[n] = ack
			n++
		}
		t.queues[i] = q[:n]
	}
}

// hasReachedServiceAcks walks first queued entries against logPosition.
// Check-only. See [serviceAckTracker.hasReached].
func (a *Agent) hasReachedServiceAcks(logPosition int64) (bool, error) {
	return a.serviceAcks.hasReached(logPosition)
}

// removeServiceAckHead pops one matched first entry from every queue. See
// [serviceAckTracker.removeHead].
//
// Call only after hasReachedServiceAcks returned true.
func (a *Agent) removeServiceAckHead() {
	a.serviceAcks.removeHead()
}

// ServiceAckQueueDepth returns the queued ack count for serviceID, or 0 when
// serviceID is outside the configured range.
//
// Concurrency: invoker-only.
func (a *Agent) ServiceAckQueueDepth(serviceID int32) int {
	return a.serviceAcks.queueDepth(serviceID)
}

// ExpectedServiceAckID returns the lowest expected ack id across services.
// With zero services, returns the popped wait count.
//
// Concurrency: invoker-only.
func (a *Agent) ExpectedServiceAckID() int64 {
	return a.serviceAcks.expectedAckID()
}

// ServiceAckFailure returns the first marked failure, or nil. An
// out-of-sequence ack or a service start ack with RelevantID NULL_VALUE
// marks the failure.
//
// Behaviour:
//   - marks once: first cause kept; later mismatches do not replace the cause
//   - never cleared on this surface
//   - election transitions leave the mark
//   - the duty cycle keeps running and keeps enqueueing acks after the mark
//
// Callers must poll this value. Out-of-sequence first entries stay observable
// through the queue depth getters.
//
// Concurrency: invoker-only.
func (a *Agent) ServiceAckFailure() error {
	return a.serviceAckFailure
}
