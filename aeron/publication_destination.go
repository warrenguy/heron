package aeron

import (
	"github.com/warrenguy/heron/aeron/conductor"
)

// AddDestination adds a send destination to a control-mode=manual
// publication.
//
// Behaviour:
//   - Offers ADD_DESTINATION once on the to-driver ring, then waits for
//     ON_OPERATION_SUCCESS.
//   - Sends OriginalRegistrationID as the publication id.
//   - channel must name a UDP endpoint.
//   - A full ring fails at once. Config.DriverTimeout bounds the driver wait.
//
// Concurrency: invoker-only with Client.DoWork. Same rules as AddPublication;
// see Client godoc. A handler inside DoWork must not call AddDestination.
// AddDestination takes clientLock.
//
// Returns an error when:
//   - the publication is closed
//   - the publication is unbound ([ErrClientNotBound])
//   - channel is empty ("aeron: channel is required")
//   - channel is non-ASCII ("aeron: channel must be ASCII")
//   - the client is closed ([ErrClientClosed]) or not ready
//     ([ErrClientNotReady])
//   - the ring is full ([conductor.ErrRingInsufficientCapacity])
//   - the conductor is terminating ([ErrConductorTerminating])
//   - the correlation id is already pending
//     ([ErrPendingAlreadyRegistered])
//   - the driver rejects the command ([*RegistrationError])
//   - the wait times out ([*AwaitTimeoutError])
//   - the conductor terminates during the wait. Driver shutdown and a stale
//     driver keepalive map to [*DriverTimeoutError]. Other causes pass
//     through unchanged
func (pub *Publication) AddDestination(channel string) error {
	return awaitSendDestination(pub.sendDestinationTarget().channelCommand(
		channel,
		(*conductor.DriverProxy).AddDestination,
	))
}

// RemoveDestination removes a send destination by channel.
//
// Behaviour:
//   - Offers REMOVE_DESTINATION once on the to-driver ring, then waits for
//     ON_OPERATION_SUCCESS.
//   - The driver acknowledges a remove that matches no destination.
//   - Otherwise as AddDestination.
//
// Concurrency: see AddDestination.
//
// Returns an error when: see AddDestination.
func (pub *Publication) RemoveDestination(channel string) error {
	return awaitSendDestination(pub.sendDestinationTarget().channelCommand(
		channel,
		(*conductor.DriverProxy).RemoveDestination,
	))
}

// RemoveDestinationByID removes a send destination by its registration id.
// destinationRegistrationID is the correlation id of the add command.
//
// Behaviour:
//   - Offers REMOVE_DESTINATION_BY_ID once on the to-driver ring, then waits
//     for ON_OPERATION_SUCCESS.
//   - The driver acknowledges a remove that matches no destination.
//   - Otherwise as AddDestination.
//
// Concurrency: see AddDestination.
//
// Returns an error when: see AddDestination. The channel errors do not apply.
func (pub *Publication) RemoveDestinationByID(destinationRegistrationID int64) error {
	return awaitSendDestination(pub.sendDestinationTarget().byIDCommand(destinationRegistrationID))
}

// AsyncAddDestination adds a send destination to a control-mode=manual
// publication without a wait.
//
// Behaviour:
//   - Offers ADD_DESTINATION once on the to-driver ring, then returns the
//     correlation id.
//   - Sends OriginalRegistrationID as the publication id.
//   - channel must name a UDP endpoint.
//   - A full ring fails at once.
//   - Client.PollCommand reports the driver's answer. Config.DriverTimeout
//     bounds the driver wait.
//   - The returned id is also the destination registration id.
//     RemoveDestinationByID and PublicationErrorFrame use the same id.
//
// Ownership: poll the id with Client.PollCommand until done, or call
// Client.CancelPending. An abandoned entry stays until Client.Close.
//
// Concurrency: invoker-only with Client.DoWork. Same rules as AddPublication;
// see Client godoc. A handler inside DoWork must not call AsyncAddDestination.
// AsyncAddDestination takes clientLock.
//
// Returns an error when: see AddDestination. The wait outcomes do not apply:
// driver rejection, timeout and termination during the wait.
// Client.PollCommand reports them. The register step still returns
// [ErrConductorTerminating].
func (pub *Publication) AsyncAddDestination(channel string) (int64, error) {
	return offerSendDestination(pub.sendDestinationTarget().channelCommand(
		channel,
		(*conductor.DriverProxy).AddDestination,
	))
}

// AsyncRemoveDestination removes a send destination by channel without a
// wait.
//
// Behaviour:
//   - Offers REMOVE_DESTINATION once on the to-driver ring, then returns the
//     correlation id. Pass the id to Client.PollCommand.
//   - The driver acknowledges a remove that matches no destination.
//   - Otherwise as AsyncAddDestination.
//
// Ownership: see AsyncAddDestination.
//
// Concurrency: see AsyncAddDestination.
//
// Returns an error when: see AsyncAddDestination.
func (pub *Publication) AsyncRemoveDestination(channel string) (int64, error) {
	return offerSendDestination(pub.sendDestinationTarget().channelCommand(
		channel,
		(*conductor.DriverProxy).RemoveDestination,
	))
}

// AsyncRemoveDestinationByID removes a send destination by its registration
// id without a wait. destinationRegistrationID is the correlation id of the
// add command.
//
// Behaviour:
//   - Offers REMOVE_DESTINATION_BY_ID once on the to-driver ring, then
//     returns the correlation id. Pass the id to Client.PollCommand.
//   - The driver acknowledges a remove that matches no destination.
//   - Otherwise as AsyncAddDestination. The channel rule does not apply.
//
// Ownership: see AsyncAddDestination.
//
// Concurrency: see AsyncAddDestination.
//
// Returns an error when: see AsyncAddDestination. The channel errors do not
// apply.
func (pub *Publication) AsyncRemoveDestinationByID(
	destinationRegistrationID int64,
) (int64, error) {
	return offerSendDestination(pub.sendDestinationTarget().byIDCommand(destinationRegistrationID))
}

// AddDestination adds a send destination to a control-mode=manual exclusive
// publication.
//
// See Publication.AddDestination for Behaviour, Concurrency and errors.
func (ep *ExclusivePublication) AddDestination(channel string) error {
	return awaitSendDestination(ep.sendDestinationTarget().channelCommand(
		channel,
		(*conductor.DriverProxy).AddDestination,
	))
}

// RemoveDestination removes a send destination by channel.
//
// See Publication.RemoveDestination for Behaviour, Concurrency and errors.
func (ep *ExclusivePublication) RemoveDestination(channel string) error {
	return awaitSendDestination(ep.sendDestinationTarget().channelCommand(
		channel,
		(*conductor.DriverProxy).RemoveDestination,
	))
}

// RemoveDestinationByID removes a send destination by its registration id.
// destinationRegistrationID is the correlation id of the add command.
//
// See Publication.RemoveDestinationByID for Behaviour, Concurrency and
// errors.
func (ep *ExclusivePublication) RemoveDestinationByID(destinationRegistrationID int64) error {
	return awaitSendDestination(ep.sendDestinationTarget().byIDCommand(destinationRegistrationID))
}

// AsyncAddDestination adds a send destination to a control-mode=manual
// exclusive publication without a wait.
//
// See Publication.AsyncAddDestination for Behaviour, Ownership, Concurrency
// and errors.
func (ep *ExclusivePublication) AsyncAddDestination(channel string) (int64, error) {
	return offerSendDestination(ep.sendDestinationTarget().channelCommand(
		channel,
		(*conductor.DriverProxy).AddDestination,
	))
}

// AsyncRemoveDestination removes a send destination by channel without a
// wait.
//
// See Publication.AsyncRemoveDestination for Behaviour, Ownership,
// Concurrency and errors.
func (ep *ExclusivePublication) AsyncRemoveDestination(channel string) (int64, error) {
	return offerSendDestination(ep.sendDestinationTarget().channelCommand(
		channel,
		(*conductor.DriverProxy).RemoveDestination,
	))
}

// AsyncRemoveDestinationByID removes a send destination by its registration
// id without a wait. destinationRegistrationID is the correlation id of the
// add command.
//
// See Publication.AsyncRemoveDestinationByID for Behaviour, Ownership,
// Concurrency and errors.
func (ep *ExclusivePublication) AsyncRemoveDestinationByID(
	destinationRegistrationID int64,
) (int64, error) {
	return offerSendDestination(ep.sendDestinationTarget().byIDCommand(destinationRegistrationID))
}

// sendDestinationTarget carries the publication fields the destination
// helpers read.
type sendDestinationTarget struct {
	// closedErr is nil when the publication is open.
	closedErr error
	client    *Client
	// originalRegistrationID keys the driver send endpoint.
	originalRegistrationID int64
}

func (pub *Publication) sendDestinationTarget() sendDestinationTarget {
	if pub == nil || pub.IsClosed() {
		return sendDestinationTarget{closedErr: errPublicationClosed}
	}
	return sendDestinationTarget{
		client:                 pub.client,
		originalRegistrationID: pub.originalRegistrationID,
	}
}

func (ep *ExclusivePublication) sendDestinationTarget() sendDestinationTarget {
	if ep == nil || ep.IsClosed() {
		return sendDestinationTarget{
			closedErr: errExclusivePublicationClosed,
		}
	}
	return sendDestinationTarget{
		client:                 ep.client,
		originalRegistrationID: ep.originalRegistrationID,
	}
}

func (target sendDestinationTarget) boundClient() (*Client, error) {
	if target.closedErr != nil {
		return nil, target.closedErr
	}
	if target.client == nil {
		return nil, ErrClientNotBound
	}
	return target.client, nil
}

// preparedSendDestination is a checked destination command. err holds the
// precondition error.
type preparedSendDestination struct {
	client *Client
	offer  driverOfferFunc
	err    error
}

// channelCommand checks the target and channel. Returns the bound client and
// the offer.
func (target sendDestinationTarget) channelCommand(
	channel string,
	offer func(
		proxy *conductor.DriverProxy,
		correlationID, registrationID int64,
		channel string,
	) error,
) preparedSendDestination {
	client, err := target.boundClient()
	if err != nil {
		return preparedSendDestination{err: err}
	}
	if err := validateChannel(channel); err != nil {
		return preparedSendDestination{err: err}
	}
	return preparedSendDestination{
		client: client,
		offer: func(proxy *conductor.DriverProxy, correlationID int64) error {
			return offer(proxy, correlationID, target.originalRegistrationID, channel)
		},
	}
}

// byIDCommand checks the target. Returns the bound client and the
// REMOVE_DESTINATION_BY_ID offer.
func (target sendDestinationTarget) byIDCommand(
	destinationRegistrationID int64,
) preparedSendDestination {
	client, err := target.boundClient()
	if err != nil {
		return preparedSendDestination{err: err}
	}
	return preparedSendDestination{
		client: client,
		offer: func(proxy *conductor.DriverProxy, correlationID int64) error {
			return proxy.RemoveDestinationByID(
				correlationID,
				target.originalRegistrationID,
				destinationRegistrationID,
			)
		},
	}
}

func awaitSendDestination(command preparedSendDestination) error {
	if command.err != nil {
		return command.err
	}
	client := command.client
	if err := client.requireReadyWithDriverProxy(); err != nil {
		return err
	}

	correlationID := client.driverProxy.NextCorrelationID()
	_, err := client.awaitResponse(
		correlationID,
		conductor.PendingKindDestination,
		client.driverTimeout,
		func() error {
			return command.offer(client.driverProxy, correlationID)
		},
	)
	return err
}

// offerSendDestination registers a PendingKindDestination entry, then offers
// the command once. It sets no active correlation id. Client.PollCommand reads
// the entry.
func offerSendDestination(command preparedSendDestination) (int64, error) {
	if command.err != nil {
		return 0, command.err
	}
	client := command.client
	client.lockClient()
	defer client.unlockClient()

	if err := client.requireReadyWithDriverProxy(); err != nil {
		return 0, err
	}
	conductorInstance := client.conductor
	driverProxy := client.driverProxy
	correlationID := driverProxy.NextCorrelationID()
	if err := conductorInstance.RegisterPendingWithDeadline(
		conductor.PendingKindDestination,
		correlationID,
		client.nanoClock()+client.driverTimeout.Nanoseconds(),
	); err != nil {
		return 0, mapRegistrationError(err)
	}
	if err := command.offer(driverProxy, correlationID); err != nil {
		conductorInstance.CancelPending(correlationID)
		return 0, err
	}
	return correlationID, nil
}
