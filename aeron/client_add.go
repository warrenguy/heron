package aeron

import (
	"errors"
	"fmt"
	"time"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
)

// AddPublication registers a concurrent publication on channel/streamID.
//
// Blocks until ON_PUBLICATION_READY is processed, the log is mmap'd, and the
// Publication handle is inserted into the conductor resource map, or until
// Config.DriverTimeout elapses. DriverTimeout covers both to-driver ring offer
// retries and the wait for the driver response; sustained ring backpressure can
// shorten the response poll window.
//
// Concurrent AddPublication from multiple goroutines is unsupported.
// See Client godoc for DoWork, Close, and handle-level concurrency.
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

// AddExclusivePublication registers an exclusive publication on channel/streamID.
//
// Blocks until ON_EXCLUSIVE_PUBLICATION_READY is processed, the log is mmap'd, and
// the ExclusivePublication handle is inserted into the conductor resource map, or
// until Config.DriverTimeout elapses. DriverTimeout covers both to-driver ring
// offer retries and the wait for the driver response; sustained ring backpressure
// can shorten the response poll window.
//
// Concurrent AddExclusivePublication from multiple goroutines is unsupported.
// See Client godoc for DoWork, Close, and handle-level concurrency.
func (client *Client) AddExclusivePublication(
	streamID int32,
	channel string,
) (*ExclusivePublication, error) {
	if err := prepareAdd(client, channel); err != nil {
		return nil, err
	}
	registration, err := client.awaitAdd(
		conductor.PendingKindAddExclusivePublication,
		streamID,
		channel,
		func(proxy *conductor.DriverProxy, correlationID int64) error {
			return proxy.AddExclusivePublication(correlationID, streamID, channel)
		},
	)
	if err != nil {
		return nil, err
	}
	return exclusivePublicationFromPending(registration)
}

// AddSubscription registers a subscription on channel/streamID.
//
// Blocks until ON_SUBSCRIPTION_READY is processed and the Subscription handle
// is inserted into the resource map, or until Config.DriverTimeout elapses.
// DriverTimeout covers both to-driver ring offer retries and the wait for the
// driver response; sustained ring backpressure can shorten the response poll
// window.
//
// ON_AVAILABLE_IMAGE may arrive during or after the await; images attach via
// conductor dispatch, not as a precondition for AddSubscription to return.
//
// Concurrent AddSubscription from multiple goroutines is unsupported.
// Same concurrency rules as AddPublication; see Client godoc.
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

// ExclusivePublication returns the exclusive publication registered with regID, if present.
//
// Lookup is O(1) on the conductor map. Second return is false after remove/close,
// if regID was never registered on this client, or if the stored handle is not a
// *ExclusivePublication.
//
// Concurrent Close during lookup is unsupported. Closed and nil conductor checks
// are best-effort without clientLock, like NextCorrelationID.
func (client *Client) ExclusivePublication(regID int64) (*ExclusivePublication, bool) {
	return lookupByRegID(
		client,
		regID,
		(*conductor.Conductor).PublicationByRegID,
		exclusivePublicationFromHandle,
	)
}

// Publication returns the publication registered with regID, if present.
//
// Lookup is O(1) on the conductor map. Second return is false after remove/close,
// if regID was never registered on this client, or if the stored handle is not a
// *Publication.
//
// Concurrent Close during lookup is unsupported. Closed and nil conductor checks
// are best-effort without clientLock, like NextCorrelationID.
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
//
// Concurrent Close during lookup is unsupported. Closed and nil conductor checks
// are best-effort without clientLock, like NextCorrelationID.
func (client *Client) Subscription(regID int64) (*Subscription, bool) {
	return lookupByRegID(
		client,
		regID,
		(*conductor.Conductor).SubscriptionByRegID,
		subscriptionFromHandle,
	)
}

const (
	registrationKindPublication          = "publication"
	registrationKindExclusivePublication = "exclusive publication"
	registrationKindSubscription         = "subscription"
)

var nullPendingAddOfferContext = pendingAddOfferContext{
	correlationID: conductor.NullCorrelationID,
}

type addOfferFunc func(proxy *conductor.DriverProxy, correlationID int64) error

type pendingAddOfferContext struct {
	correlationID    int64
	deadlineNs       int64
	registrationKind string
}

// registerAndOfferAddLocked registers a pending add and offers the driver command.
// Caller holds clientLock and must clear active correlation when done.
//
// On any error the pending registration is rolled back and the returned
// correlationID is NullCorrelationID; callers return the error directly.
func (client *Client) registerAndOfferAddLocked(
	pendingKind conductor.PendingKind,
	streamID int32,
	channel string,
	offer addOfferFunc,
) (pendingAddOfferContext, error) {
	if err := client.requireReadyWithDriverProxy(); err != nil {
		return nullPendingAddOfferContext, err
	}

	driverProxy := client.driverProxy
	cncMappedAtOfferStart := client.mapped != nil
	conductorInstance := client.conductor

	offerCtx := pendingAddOfferContext{
		correlationID:    driverProxy.NextCorrelationID(),
		deadlineNs:       client.nanoClock() + client.driverTimeout.Nanoseconds(),
		registrationKind: registrationKindFor(pendingKind),
	}
	if err := conductorInstance.RegisterPendingAddWithDeadline(
		pendingKind,
		offerCtx.correlationID,
		streamID,
		channel,
		offerCtx.deadlineNs,
	); err != nil {
		return nullPendingAddOfferContext, mapRegistrationError(err)
	}

	conductorInstance.SetActiveCorrelationID(offerCtx.correlationID)
	if err := client.offerAddCommand(
		offerCtx,
		streamID,
		channel,
		conductorInstance,
		driverProxy,
		cncMappedAtOfferStart,
		offer,
	); err != nil {
		conductorInstance.CancelPending(offerCtx.correlationID)
		return nullPendingAddOfferContext, err
	}

	return offerCtx, nil
}

func prepareAdd(client *Client, channel string) error {
	if client == nil {
		return ErrClientNotReady
	}
	// awaitAdd re-checks closed under clientLock.
	if client.closed.Load() {
		return ErrClientClosed
	}
	return validateChannel(channel)
}

func registrationKindFor(pendingKind conductor.PendingKind) string {
	//exhaustive:ignore panic default rejects non-add kinds
	switch pendingKind {
	case conductor.PendingKindAddPublication:
		return registrationKindPublication
	case conductor.PendingKindAddExclusivePublication:
		return registrationKindExclusivePublication
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
	if err := atomicbuffer.ValidateStringASCII(channel); err != nil {
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
	if client == nil || client.closed.Load() || client.conductor == nil {
		return nil
	}
	return client.conductor
}

// awaitAdd blocks until the add registration completes or times out.
// Poll and offer ticks call doWorkLocked, not DoWork. See Client godoc for concurrency.
func (client *Client) awaitAdd(
	pendingKind conductor.PendingKind,
	streamID int32,
	channel string,
	offer addOfferFunc,
) (conductor.PendingRegistration, error) {
	if client == nil {
		return conductor.PendingRegistration{}, ErrClientNotReady
	}
	client.lockClient()
	defer client.unlockClient()

	if err := client.requireReadyWithConductor(); err != nil {
		return conductor.PendingRegistration{}, err
	}
	conductorInstance := client.conductor
	defer conductorInstance.SetActiveCorrelationID(conductor.NullCorrelationID)

	offerCtx, err := client.registerAndOfferAddLocked(
		pendingKind,
		streamID,
		channel,
		offer,
	)
	if err != nil {
		return conductor.PendingRegistration{}, err
	}

	pendingCompleted := false
	defer func() {
		if !pendingCompleted {
			conductorInstance.CancelPending(offerCtx.correlationID)
		}
	}()

	registration, completed, err := client.awaitPendingPoll(
		conductorInstance,
		offerCtx.correlationID,
		offerCtx.deadlineNs,
		func(pollErr error) error {
			return mapAddPollPendingError(
				offerCtx.registrationKind,
				offerCtx.correlationID,
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
		offerCtx.registrationKind,
		offerCtx.correlationID,
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
	if err := client.errIfClosed(); err != nil {
		return err
	}
	if cncMappedAtOfferStart && client.mapped == nil {
		return ErrClientClosed
	}
	if client.driverProxy != driverProxy {
		return ErrClientNotReady
	}
	return nil
}

func (client *Client) offerAddCommand(
	offerCtx pendingAddOfferContext,
	streamID int32,
	channel string,
	conductorInstance *conductor.Conductor,
	driverProxy *conductor.DriverProxy,
	cncMappedAtOfferStart bool,
	offer addOfferFunc,
) error {
	var lastOfferErr error
	for client.nanoClock() < offerCtx.deadlineNs {
		if err := client.guardClosedBeforeOffer(driverProxy, cncMappedAtOfferStart); err != nil {
			return err
		}
		if conductorInstance.IsTerminating() {
			return mapConductorTerminationError(conductorInstance)
		}
		client.doWorkLocked()
		if err := client.guardClosedBeforeOffer(driverProxy, cncMappedAtOfferStart); err != nil {
			return err
		}
		offerErr := offer(driverProxy, offerCtx.correlationID)
		if offerErr == nil {
			return nil
		}
		lastOfferErr = offerErr
		if errors.Is(offerErr, conductor.ErrRingInsufficientCapacity) {
			time.Sleep(client.idleSleepDuration)
			continue
		}
		return mapAddOfferError(offerCtx.registrationKind, offerCtx.correlationID, offerErr)
	}
	if errors.Is(lastOfferErr, conductor.ErrRingInsufficientCapacity) {
		return mapAddOfferError(
			offerCtx.registrationKind,
			offerCtx.correlationID,
			conductor.ErrRingInsufficientCapacity,
		)
	}
	return newRegistrationTimeoutError(
		offerCtx.registrationKind,
		offerCtx.correlationID,
		streamID,
		channel,
	)
}

func publicationFromPending(
	registration conductor.PendingRegistration,
) (*Publication, error) {
	return typedHandleFrom[*Publication](
		registration.Publication,
		registrationKindPublication,
		"*Publication",
	)
}

func exclusivePublicationFromPending(
	registration conductor.PendingRegistration,
) (*ExclusivePublication, error) {
	return typedHandleFrom[*ExclusivePublication](
		registration.Publication,
		registrationKindExclusivePublication,
		"*ExclusivePublication",
	)
}

func subscriptionFromPending(
	registration conductor.PendingRegistration,
) (*Subscription, error) {
	return typedHandleFrom[*Subscription](
		registration.Subscription,
		registrationKindSubscription,
		"*Subscription",
	)
}
