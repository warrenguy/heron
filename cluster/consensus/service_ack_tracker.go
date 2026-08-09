package consensus

import "errors"

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

// serviceAckTracker holds one FIFO queue of acks per configured service.
//
// Concurrency: invoker-only behind Agent.DoWork. No locks.
type serviceAckTracker struct {
	queues [][]serviceAck
}

// newServiceAckTracker sizes one empty queue per service.
//
// serviceCount must be non-negative. Callers that take AgentConfig.ServiceCount
// must panic on a negative count before construct.
func newServiceAckTracker(serviceCount int) *serviceAckTracker {
	return &serviceAckTracker{
		queues: make([][]serviceAck, serviceCount),
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

// hasReached walks the queues in service-id order (0, 1, ...) and reports
// whether every first queued entry matches logPosition and ackID exactly.
// Check-only: never pops.
//
// Returns:
//   - (false, nil) at the first empty queue; later queues are not examined
//   - (false, ErrServiceAckOutOfSequence) at the first non-empty first entry
//     that mismatches on ackID or logPosition
//   - (true, nil) when every first entry matches
//
// With zero services, returns (true, nil).
func (t *serviceAckTracker) hasReached(logPosition, ackID int64) (bool, error) {
	for _, q := range t.queues {
		if len(q) == 0 {
			return false, nil
		}
		first := q[0]
		if first.ackID != ackID || first.logPosition != logPosition {
			return false, ErrServiceAckOutOfSequence
		}
	}
	return true, nil
}

// removeHead pops one entry from every non-empty queue.
//
// Call only after hasReached returned true. Empty queues are tolerated.
func (t *serviceAckTracker) removeHead() {
	for i, q := range t.queues {
		if len(q) > 0 {
			t.queues[i] = q[1:]
		}
	}
}

// afterOfferSequenceError runs the receive-time sequence check for expectedAckID.
//
// Check-only: never pops. Uses the log position of the first entry in queue 0
// as the serviceAckID step under check. Does not take the incoming frame
// position. Multiple serviceAckID steps may sit behind that first entry. Wait
// paths use hasReached at the wait position instead.
//
// Returns:
//   - nil when there are zero services, or queue 0 is empty (walk cannot start)
//   - nil when every first entry matches expectedAckID at that log position
//   - ErrServiceAckOutOfSequence when a non-empty first entry mismatches
func (t *serviceAckTracker) afterOfferSequenceError(expectedAckID int64) error {
	if len(t.queues) == 0 {
		return nil
	}
	q := t.queues[0]
	if len(q) == 0 {
		return nil
	}
	_, err := t.hasReached(q[0].logPosition, expectedAckID)
	return err
}

// queueDepth returns the queued ack count for serviceID, or 0 when serviceID is
// outside [0, serviceCount).
func (t *serviceAckTracker) queueDepth(serviceID int32) int {
	if !t.hasService(serviceID) {
		return 0
	}
	return len(t.queues[serviceID])
}

// hasReachedServiceAcks walks first queued entries against logPosition and the
// expected serviceAckID. Check-only. See [serviceAckTracker.hasReached].
func (a *Agent) hasReachedServiceAcks(logPosition int64) (bool, error) {
	return a.serviceAcks.hasReached(logPosition, a.serviceAckID)
}

// removeServiceAckHead pops one matched first entry from every queue and
// advances serviceAckID by one.
//
// Call only after hasReachedServiceAcks returned true.
func (a *Agent) removeServiceAckHead() {
	a.serviceAcks.removeHead()
	a.serviceAckID++
}

// ServiceAckQueueDepth returns the queued ack count for serviceID, or 0 when
// serviceID is outside the configured range.
//
// Concurrency: invoker-only.
func (a *Agent) ServiceAckQueueDepth(serviceID int32) int {
	return a.serviceAcks.queueDepth(serviceID)
}

// ExpectedServiceAckID returns the expected next service ack id.
//
// Concurrency: invoker-only.
func (a *Agent) ExpectedServiceAckID() int64 {
	return a.serviceAckID
}

// ServiceAckFailure returns the first receive-time out-of-sequence failure, or
// nil.
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
