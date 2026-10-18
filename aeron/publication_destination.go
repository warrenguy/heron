package aeron

import (
	"errors"

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
	return awaitSendDestinationChannel(
		pub.sendDestinationTarget(),
		channel,
		(*conductor.DriverProxy).AddDestination,
	)
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
	return awaitSendDestinationChannel(
		pub.sendDestinationTarget(),
		channel,
		(*conductor.DriverProxy).RemoveDestination,
	)
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
	return awaitSendDestinationByID(pub.sendDestinationTarget(), destinationRegistrationID)
}

// AddDestination adds a send destination to a control-mode=manual exclusive
// publication.
//
// See Publication.AddDestination for Behaviour, Concurrency and errors.
func (ep *ExclusivePublication) AddDestination(channel string) error {
	return awaitSendDestinationChannel(
		ep.sendDestinationTarget(),
		channel,
		(*conductor.DriverProxy).AddDestination,
	)
}

// RemoveDestination removes a send destination by channel.
//
// See Publication.RemoveDestination for Behaviour, Concurrency and errors.
func (ep *ExclusivePublication) RemoveDestination(channel string) error {
	return awaitSendDestinationChannel(
		ep.sendDestinationTarget(),
		channel,
		(*conductor.DriverProxy).RemoveDestination,
	)
}

// RemoveDestinationByID removes a send destination by its registration id.
// destinationRegistrationID is the correlation id of the add command.
//
// See Publication.RemoveDestinationByID for Behaviour, Concurrency and
// errors.
func (ep *ExclusivePublication) RemoveDestinationByID(destinationRegistrationID int64) error {
	return awaitSendDestinationByID(ep.sendDestinationTarget(), destinationRegistrationID)
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
		return sendDestinationTarget{closedErr: errors.New("aeron: publication is closed")}
	}
	return sendDestinationTarget{
		client:                 pub.client,
		originalRegistrationID: pub.originalRegistrationID,
	}
}

func (ep *ExclusivePublication) sendDestinationTarget() sendDestinationTarget {
	if ep == nil || ep.IsClosed() {
		return sendDestinationTarget{
			closedErr: errors.New("aeron: exclusive publication is closed"),
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

func awaitSendDestinationChannel(
	target sendDestinationTarget,
	channel string,
	offer func(
		proxy *conductor.DriverProxy,
		correlationID, registrationID int64,
		channel string,
	) error,
) error {
	client, err := target.boundClient()
	if err != nil {
		return err
	}
	if err := validateChannel(channel); err != nil {
		return err
	}
	return awaitSendDestination(
		client,
		func(proxy *conductor.DriverProxy, correlationID int64) error {
			return offer(proxy, correlationID, target.originalRegistrationID, channel)
		},
	)
}

func awaitSendDestinationByID(
	target sendDestinationTarget,
	destinationRegistrationID int64,
) error {
	client, err := target.boundClient()
	if err != nil {
		return err
	}
	return awaitSendDestination(
		client,
		func(proxy *conductor.DriverProxy, correlationID int64) error {
			return proxy.RemoveDestinationByID(
				correlationID,
				target.originalRegistrationID,
				destinationRegistrationID,
			)
		},
	)
}

func awaitSendDestination(
	client *Client,
	offer func(proxy *conductor.DriverProxy, correlationID int64) error,
) error {
	if err := client.requireReadyWithDriverProxy(); err != nil {
		return err
	}

	correlationID := client.driverProxy.NextCorrelationID()
	_, err := client.awaitResponse(
		correlationID,
		conductor.PendingKindDestination,
		client.driverTimeout,
		func() error {
			return offer(client.driverProxy, correlationID)
		},
	)
	return err
}
