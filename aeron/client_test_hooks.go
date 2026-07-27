//go:build testhooks

package aeron

import "github.com/warrenguy/heron/aeron/conductor"

func (client *Client) awaitPendingPollTickForTest(
	conductorInstance *conductor.Conductor,
	correlationID int64,
	mapPollErr func(error) error,
	skipDoWork bool,
	afterPollPending func(),
) (conductor.PendingRegistration, bool, error) {
	if !skipDoWork && afterPollPending == nil {
		return client.awaitPendingPollTick(conductorInstance, correlationID, mapPollErr)
	}
	if err := client.requireReadyWithConductor(); err != nil {
		return conductor.PendingRegistration{}, false, err
	}
	if conductorInstance.IsTerminating() {
		return conductor.PendingRegistration{}, false, mapConductorTerminationError(conductorInstance)
	}
	if !skipDoWork {
		client.doWorkLocked()
	}
	done, registration, err := conductorInstance.PollPending(correlationID)
	// afterPollPending runs before the !done return; only safe when PollPending
	// completion is already marked under clientLock.
	if afterPollPending != nil {
		afterPollPending()
	}
	if !done {
		return conductor.PendingRegistration{}, false, nil
	}
	return client.completeAwaitPendingPollTick(
		conductorInstance,
		registration,
		mapPollErr,
		err,
	)
}
