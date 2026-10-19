package aeron

import (
	"fmt"

	"github.com/warrenguy/heron/aeron/conductor"
)

// PollCommand reports the outcome of an async destination command.
// correlationID is the id an Async*Destination method returned.
//
// Behaviour:
//   - Reads the pending entry. Does not call DoWork.
//   - The driver reply arrives during a later Client.DoWork call.
//   - A command with no reply by Config.DriverTimeout times out.
//   - A reply that arrives after the timeout but before the poll still
//     counts. Remove a timed-out add by id before a retry, or the driver can
//     hold the destination twice.
//
// Ownership: poll each id until done, or call Client.CancelPending. An
// abandoned entry stays until Client.Close.
//
// Concurrency: invoker-only with Client.DoWork. A handler inside DoWork must
// not call PollCommand. PollCommand takes clientLock.
//
// Returns:
//   - false, nil: the command is in flight.
//   - true, nil: the driver acknowledged the command. The entry is gone.
//   - true, [*RegistrationError]: the driver rejected the command. The entry
//     is gone.
//   - true, [*AwaitTimeoutError]: no reply by Config.DriverTimeout. The entry
//     is gone.
//   - true, termination cause: the conductor terminated. Termination removed
//     every entry. Driver shutdown and a stale keepalive map to
//     [*DriverTimeoutError]. Later polls return the same cause.
//   - true, [ErrAwaitInterrupted]: no entry holds correlationID. A poll
//     completed the id, a cancel removed it, or no call issued it.
//   - false, [ErrClientClosed]: Client.Close removed every entry.
//   - false, [ErrClientNotReady]: the entry is unchanged.
//   - false, kind error: correlationID names another kind of command, such
//     as an OfferAdd* registration. The entry is unchanged.
func (client *Client) PollCommand(correlationID int64) (done bool, err error) {
	if client == nil {
		return false, ErrClientNotReady
	}
	client.lockClient()
	defer client.unlockClient()

	if err = client.requireReadyWithConductor(); err != nil {
		return false, err
	}
	conductorInstance := client.conductor
	if conductorInstance.IsTerminating() {
		return true, mapConductorTerminationError(conductorInstance)
	}
	pending, ok := conductorInstance.PendingResult(correlationID)
	if !ok {
		return true, ErrAwaitInterrupted
	}
	if pending.Kind != conductor.PendingKindDestination {
		return false, fmt.Errorf(
			"aeron: poll command: correlation id %d is not an async destination command",
			correlationID,
		)
	}
	done, _, pollErr := conductorInstance.PollPending(correlationID)
	if done {
		return true, mapPollPendingError(correlationID, client.driverTimeout, pollErr)
	}
	if client.nanoClock() < pending.DeadlineNs {
		return false, nil
	}
	conductorInstance.CancelPending(correlationID)
	return true, newAwaitTimeoutError(correlationID, client.driverTimeout)
}
