package service

import (
	"fmt"

	"luas.fast/aeron"
)

// terminationSendAttempts caps the failed sends of one termination drain. One
// send makes up to aeron.SendAttempts offers inside the proxy.
const terminationSendAttempts = 5

// IsTerminated reports whether the agent stopped at a termination position,
// after Terminate and any required ack drain. DoWork then returns 0.
// ServiceFailure distinguishes an expected stop from an unexpected one. A failure
// on an ack still queued at the stop marks at Close.
func (a *Agent) IsTerminated() bool {
	return a.terminated
}

// handleServiceTerminationPosition stores the position and ack flag of the
// latest frame. A nullValue position clears the stored position.
func (a *Agent) handleServiceTerminationPosition(logPosition int64, ackRequired bool) {
	if a.closed {
		return
	}

	a.terminationPosition = logPosition
	a.terminationAckRequired = ackRequired
}

// terminationWork starts the stop once the log reaches the stored termination
// position. Padding does not move logPosition, so the agent adopts the position
// when isConsumedTo holds. A running snapshot take delays the stop. A passed
// request does not end the stop. A log position past the termination position
// makes the stop unexpected.
func (a *Agent) terminationWork() int {
	position := a.terminationPosition
	if position == nullValue || a.take != nil || a.serviceFailure != nil {
		return 0
	}

	if a.logPosition < position && a.adapter.isConsumedTo(position) {
		a.logPosition = position
	}

	if a.logPosition < position {
		return 0
	}

	a.sweepRequestAckReadiness()

	a.terminating = true

	if a.terminationAckRequired {
		a.queueTerminationAck()
	}

	a.callTerminate()
	return 1 + a.terminationDrainWork()
}

// queueTerminationAck readies every queued entry at its own position, including
// a request above the stop and an unbound join.
func (a *Agent) queueTerminationAck() {
	for i := range a.coordinationAcks {
		a.coordinationAcks[i].ready = true
	}

	a.coordinationAcks = append(a.coordinationAcks, coordinationAck{
		logPosition: a.logPosition,
		kind:        ackKindTermination,
		ready:       true,
		relevantID:  nullValue,
	})
}

// terminationDrainWork sends the queued acks in order. A failed send ends the
// call. A later call sends again, up to terminationSendAttempts failed sends. An
// abandoned drain reports ErrTerminationAckPublish and does not mark. Unsent
// entries stay queued.
//
// Returns the number of acks sent.
func (a *Agent) terminationDrainWork() int {
	if a.serviceFailure != nil || !a.terminationAckRequired {
		a.finishTermination()
		return 0
	}

	if !a.canPublish() {
		a.abandonTerminationDrain(aeron.Closed)
		return 0
	}

	work := 0

	for len(a.coordinationAcks) > 0 {
		result := a.sendFirstCoordinationAck()
		if result > 0 {
			work++
		}

		if a.serviceFailure != nil {
			a.finishTermination()
			return work
		}

		if result <= 0 {
			a.countFailedTerminationSend(result)
			return work
		}
	}

	a.finishTermination()
	return work
}

func (a *Agent) countFailedTerminationSend(result int64) {
	a.terminationSendFailures++

	if !aeron.IsOfferRetriable(result) || a.terminationSendFailures >= terminationSendAttempts {
		a.abandonTerminationDrain(result)
	}
}

func (a *Agent) abandonTerminationDrain(result int64) {
	if a.terminationAckRequired {
		a.handleError(fmt.Errorf("%w: %d", ErrTerminationAckPublish, result))
	}

	a.finishTermination()
}

func (a *Agent) finishTermination() {
	a.terminating = false
	a.terminated = true

	if a.logPosition != a.terminationPosition {
		a.handleError(fmt.Errorf(
			"%w: log position %d, termination position %d",
			ErrTerminationPositionPassed,
			a.logPosition,
			a.terminationPosition,
		))
	}
}
