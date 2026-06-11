package aeron

import (
	"errors"
	"fmt"
	"time"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
)

// AddPublication registers a concurrent publication on channel/streamID.
// Blocks until ON_PUBLICATION_READY is processed, the log is mmap'd, and the
// Publication handle is inserted into the conductor resource map, or until
// Context.DriverTimeout elapses. DriverTimeout covers both to-driver ring offer
// retries and the wait for the driver response; sustained ring backpressure can
// shorten the response poll window.
//
// Concurrent AddPublication from multiple goroutines is unsupported.
// External DoWork from another goroutine during a blocking AddPublication is
// unsupported.
//
// Concurrent Close from another goroutine during a blocking AddPublication is
// unsupported; if Close is observed while awaiting, ErrClientClosed is returned
// on a best-effort basis.
func (client *Client) AddPublication(streamID int32, channel string) (*Publication, error) {
	if err := prepareAdd(client, channel); err != nil {
		return nil, err
	}
	registration, err := client.awaitAdd(
		conductor.PendingKindAddPublication,
		streamID,
		channel,
		func(proxy *conductor.DriverProxy, correlationID int64) error {
			return proxy.AddPublication(correlationID, streamID, channel)
		},
	)
	if err != nil {
		return nil, err
	}
	return publicationFromPending(registration)
}

// AddSubscription registers a subscription on channel/streamID.
// Blocks until ON_SUBSCRIPTION_READY is processed and the Subscription handle
// is inserted into the resource map, or until Context.DriverTimeout elapses.
// DriverTimeout covers both to-driver ring offer retries and the wait for the
// driver response; sustained ring backpressure can shorten the response poll
// window.
//
// ON_AVAILABLE_IMAGE may arrive during or after the await; images attach via
// conductor dispatch, not as a precondition for AddSubscription to return.
//
// Concurrent AddSubscription from multiple goroutines is unsupported.
// External DoWork from another goroutine during a blocking AddSubscription is
// unsupported.
//
// Concurrent Close from another goroutine during a blocking AddSubscription is
// unsupported; if Close is observed while awaiting, ErrClientClosed is returned
// on a best-effort basis.
func (client *Client) AddSubscription(streamID int32, channel string) (*Subscription, error) {
	if err := prepareAdd(client, channel); err != nil {
		return nil, err
	}
	registration, err := client.awaitAdd(
		conductor.PendingKindAddSubscription,
		streamID,
		channel,
		func(proxy *conductor.DriverProxy, correlationID int64) error {
			return proxy.AddSubscription(correlationID, streamID, channel)
		},
	)
	if err != nil {
		return nil, err
	}
	return subscriptionFromPending(registration)
}

// Publication returns the publication registered with regID, if present.
//
// Lookup is O(1) on the conductor map. Second return is false after remove/close,
// if regID was never registered on this client, or if the stored handle is not a
// *Publication.
func (client *Client) Publication(regID int64) (*Publication, bool) {
	return lookupByRegID(
		client,
		regID,
		(*conductor.Conductor).PublicationByRegID,
		publicationFromHandle,
	)
}

// Subscription returns the subscription registered with regID, if present.
//
// Lookup is O(1) on the conductor map. Second return is false after remove/close,
// if regID was never registered on this client, or if the stored handle is not a
// *Subscription.
func (client *Client) Subscription(regID int64) (*Subscription, bool) {
	return lookupByRegID(
		client,
		regID,
		(*conductor.Conductor).SubscriptionByRegID,
		subscriptionFromHandle,
	)
}

const (
	registrationKindPublication  = "publication"
	registrationKindSubscription = "subscription"
)

type addOfferFunc func(proxy *conductor.DriverProxy, correlationID int64) error

func prepareAdd(client *Client, channel string) error {
	if client == nil {
		return ErrClientNotReady
	}
	if client.closed {
		return ErrClientClosed
	}
	return validateChannel(channel)
}

func registrationKindFor(pendingKind conductor.PendingKind) string {
	switch pendingKind {
	case conductor.PendingKindAddPublication:
		return registrationKindPublication
	case conductor.PendingKindAddSubscription:
		return registrationKindSubscription
	default:
		panic(fmt.Sprintf("aeron: unsupported add pending kind %v", pendingKind))
	}
}

func validateChannel(channel string) error {
	if channel == "" {
		return errors.New("aeron: channel is required")
	}
	if err := atomic.ValidateStringASCII(channel); err != nil {
		return errors.New("aeron: channel must be ASCII")
	}
	return nil
}

func lookupByRegID[Handle, T any](
	client *Client,
	regID int64,
	get func(*conductor.Conductor, int64) (Handle, bool),
	convert func(Handle) (T, error),
) (T, bool) {
	var zero T
	conductorInstance := client.conductorForLookup()
	if conductorInstance == nil {
		return zero, false
	}
	handle, ok := get(conductorInstance, regID)
	if !ok {
		return zero, false
	}
	value, err := convert(handle)
	if err != nil {
		return zero, false
	}
	return value, true
}

func (client *Client) conductorForLookup() *conductor.Conductor {
	if client == nil || client.closed || client.conductor == nil {
		return nil
	}
	return client.conductor
}

func (client *Client) awaitAdd(
	pendingKind conductor.PendingKind,
	streamID int32,
	channel string,
	offer addOfferFunc,
) (conductor.PendingRegistration, error) {
	registrationKind := registrationKindFor(pendingKind)
	if err := client.requireReadyWithDriverProxy(); err != nil {
		return conductor.PendingRegistration{}, err
	}

	driverProxy := client.driverProxy
	cncMappedAtOfferStart := client.mapped != nil
	conductorInstance := client.conductor
	defer conductorInstance.SetActiveCorrelationID(conductor.NullCorrelationID)

	correlationID := driverProxy.NextCorrelationID()
	deadlineNs := client.nanoClock() + client.driverTimeout.Nanoseconds()
	if err := conductorInstance.RegisterPendingAddWithDeadline(
		pendingKind,
		correlationID,
		streamID,
		channel,
		deadlineNs,
	); err != nil {
		return conductor.PendingRegistration{}, mapRegistrationError(err)
	}

	pendingCompleted := false
	defer func() {
		if !pendingCompleted {
			conductorInstance.CancelPending(correlationID)
		}
	}()

	conductorInstance.SetActiveCorrelationID(correlationID)
	if err := client.offerAddCommand(
		registrationKind,
		correlationID,
		streamID,
		channel,
		deadlineNs,
		conductorInstance,
		driverProxy,
		cncMappedAtOfferStart,
		offer,
	); err != nil {
		return conductor.PendingRegistration{}, err
	}

	registration, completed, err := client.awaitPendingPoll(
		conductorInstance,
		correlationID,
		deadlineNs,
		func(pollErr error) error {
			return mapAddPollPendingError(
				registrationKind,
				correlationID,
				streamID,
				channel,
				pollErr,
			)
		},
	)
	if completed {
		pendingCompleted = true
	}
	if err != nil {
		return conductor.PendingRegistration{}, err
	}
	if completed {
		return registration, nil
	}

	return conductor.PendingRegistration{}, newRegistrationTimeoutError(
		registrationKind,
		correlationID,
		streamID,
		channel,
	)
}

func (client *Client) guardClosedBeforeOffer(
	driverProxy *conductor.DriverProxy,
	cncMappedAtOfferStart bool,
) error {
	if err := client.requireReadyWithDriverProxy(); err != nil {
		return err
	}
	if client.closed || (cncMappedAtOfferStart && client.mapped == nil) {
		return ErrClientClosed
	}
	if client.driverProxy != driverProxy {
		return ErrClientNotReady
	}
	return nil
}

func (client *Client) offerAddCommand(
	registrationKind string,
	correlationID int64,
	streamID int32,
	channel string,
	deadlineNs int64,
	conductorInstance *conductor.Conductor,
	driverProxy *conductor.DriverProxy,
	cncMappedAtOfferStart bool,
	offer addOfferFunc,
) error {
	var lastOfferErr error
	for client.nanoClock() < deadlineNs {
		if err := client.guardClosedBeforeOffer(driverProxy, cncMappedAtOfferStart); err != nil {
			return err
		}
		if conductorInstance.IsTerminating() {
			return mapConductorTerminationError(conductorInstance)
		}
		client.DoWork()
		if err := client.guardClosedBeforeOffer(driverProxy, cncMappedAtOfferStart); err != nil {
			return err
		}
		offerErr := offer(driverProxy, correlationID)
		if offerErr == nil {
			return nil
		}
		lastOfferErr = offerErr
		if errors.Is(offerErr, conductor.ErrRingInsufficientCapacity) {
			time.Sleep(client.idleSleepDuration)
			continue
		}
		return mapAddOfferError(registrationKind, correlationID, offerErr)
	}
	if errors.Is(lastOfferErr, conductor.ErrRingInsufficientCapacity) {
		return mapAddOfferError(registrationKind, correlationID, conductor.ErrRingInsufficientCapacity)
	}
	return newRegistrationTimeoutError(registrationKind, correlationID, streamID, channel)
}

func fromPendingHandle[Handle, T any](
	raw any,
	fromHandle func(Handle) (T, error),
	kindName string,
	wantTypeName string,
) (T, error) {
	var zero T
	if raw == nil {
		var nilHandle Handle
		return fromHandle(nilHandle)
	}
	handle, ok := raw.(Handle)
	if !ok {
		return zero, fmt.Errorf(
			"aeron: %s handle %T is not %s",
			kindName,
			raw,
			wantTypeName,
		)
	}
	return fromHandle(handle)
}

func publicationFromPending(
	registration conductor.PendingRegistration,
) (*Publication, error) {
	return fromPendingHandle(
		registration.Publication,
		publicationFromHandle,
		"publication",
		"*Publication",
	)
}

func subscriptionFromPending(
	registration conductor.PendingRegistration,
) (*Subscription, error) {
	return fromPendingHandle(
		registration.Subscription,
		subscriptionFromHandle,
		"subscription",
		"*Subscription",
	)
}
