package aeron

import (
	"errors"

	"github.com/warrenguy/heron/aeron/conductor"
)

// AddDestination adds a receive destination to a control-mode=manual
// subscription.
//
// Behaviour: offers ADD_RCV_DESTINATION once on the to-driver ring, then waits
// for ON_OPERATION_SUCCESS. Offer failure, including ring full, fails
// immediately. Config.DriverTimeout bounds only the post-offer driver wait.
//
// Concurrency: invoker-only with Client.DoWork. Concurrent AddDestination or
// RemoveDestination from multiple goroutines is unsupported. Same rules as
// AddSubscription; see Client godoc.
//
// Returns an error when:
//   - the subscription is closed
//   - the subscription is unbound ([ErrClientNotBound])
//   - channel is empty ("aeron: channel is required")
//   - channel is non-ASCII ("aeron: channel must be ASCII")
//   - the driver returns an error or the wait times out
//   - the caller is inside a handler ([ErrReentrantCall])
func (subscription *Subscription) AddDestination(channel string) error {
	return subscription.receiveDestination(
		channel,
		receiveDestinationHook.AddDestination,
		(*conductor.DriverProxy).AddReceiveDestination,
	)
}

// RemoveDestination removes a previously added receive destination.
//
// Behaviour: offers REMOVE_RCV_DESTINATION once on the to-driver ring, then
// waits for ON_OPERATION_SUCCESS. Offer failure, including ring full, fails
// immediately. Config.DriverTimeout bounds only the post-offer driver wait.
//
// Concurrency: invoker-only with Client.DoWork. Concurrent AddDestination or
// RemoveDestination from multiple goroutines is unsupported. Same rules as
// AddSubscription; see Client godoc.
//
// Returns an error when:
//   - the subscription is closed
//   - the subscription is unbound ([ErrClientNotBound])
//   - channel is empty ("aeron: channel is required")
//   - channel is non-ASCII ("aeron: channel must be ASCII")
//   - the driver returns an error or the wait times out
//   - the caller is inside a handler ([ErrReentrantCall])
func (subscription *Subscription) RemoveDestination(channel string) error {
	return subscription.receiveDestination(
		channel,
		receiveDestinationHook.RemoveDestination,
		(*conductor.DriverProxy).RemoveReceiveDestination,
	)
}

// receiveDestination runs the test hook when set. Otherwise offers once on the
// to-driver ring and waits for ON_OPERATION_SUCCESS.
//
// Check order:
//   - driver path: closed, unbound, channel, ready, await
//   - hook path: closed, channel, then the hook (skips client checks)
func (subscription *Subscription) receiveDestination(
	channel string,
	hookCall func(receiveDestinationHook, string) error,
	offer func(
		proxy *conductor.DriverProxy,
		correlationID, registrationID int64,
		channel string,
	) error,
) error {
	if subscription == nil || subscription.IsClosed() {
		return errors.New("aeron: subscription is closed")
	}
	if hook := subscription.receiveDestinationHook; hook != nil {
		if err := validateChannel(channel); err != nil {
			return err
		}
		return hookCall(hook, channel)
	}
	if subscription.client == nil {
		return ErrClientNotBound
	}
	if err := validateChannel(channel); err != nil {
		return err
	}
	client := subscription.client
	if err := client.requireReadyWithDriverProxy(); err != nil {
		return err
	}

	correlationID := client.driverProxy.NextCorrelationID()
	registrationID := subscription.RegistrationID()
	_, err := client.awaitResponse(
		correlationID,
		conductor.PendingKindOperationSuccess,
		client.driverTimeout,
		func() error {
			return offer(client.driverProxy, correlationID, registrationID, channel)
		},
	)
	return err
}
