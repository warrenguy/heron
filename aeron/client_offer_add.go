package aeron

import (
	"fmt"

	"github.com/warrenguy/heron/aeron/conductor"
)

// OfferAddPublication registers a pending add and offers ADD_PUBLICATION once per
// call (retries ring backpressure internally until offer succeeds or deadline).
// Returns correlationID for PollAddPublication.
//
// Does not wait for driver registration to complete; loop PollAddPublication until
// done or terminal error. Pending remains until PollAdd completes, CancelPending,
// Config.DriverTimeout elapses, or Client.Close. To abandon without Close, call
// CancelPending or continue polling until done or a terminal err.
//
// DriverTimeout covers both to-driver ring offer retries and the wait for the
// driver response; sustained ring backpressure can shorten the response poll
// window. Conductor expire during doWorkLocked and the client deadline fallback in
// pollAddWith both yield *RegistrationTimeoutError.
//
// Concurrent OfferAdd* from multiple goroutines is unsupported.
// See Client godoc for DoWork, Close, and handle-level concurrency.
func (client *Client) OfferAddPublication(streamID int32, channel string) (int64, error) {
	if err := prepareAdd(client, channel); err != nil {
		return 0, err
	}
	return client.offerAdd(
		conductor.PendingKindAddPublication,
		streamID,
		channel,
		func(proxy *conductor.DriverProxy, correlationID int64) error {
			return proxy.AddPublication(correlationID, streamID, channel)
		},
	)
}

// OfferAddExclusivePublication registers a pending add and offers
// ADD_EXCLUSIVE_PUBLICATION once per call (retries ring backpressure internally
// until offer succeeds or deadline). Returns correlationID for
// PollAddExclusivePublication.
//
// Pending lifetime, DriverTimeout, and concurrency contract match
// [Client.OfferAddPublication]; poll with PollAddExclusivePublication.
func (client *Client) OfferAddExclusivePublication(
	streamID int32,
	channel string,
) (int64, error) {
	if err := prepareAdd(client, channel); err != nil {
		return 0, err
	}
	return client.offerAdd(
		conductor.PendingKindAddExclusivePublication,
		streamID,
		channel,
		func(proxy *conductor.DriverProxy, correlationID int64) error {
			return proxy.AddExclusivePublication(correlationID, streamID, channel)
		},
	)
}

// OfferAddSubscription registers a pending add and offers ADD_SUBSCRIPTION once per
// call (retries ring backpressure internally until offer succeeds or deadline).
// Returns correlationID for PollAddSubscription.
//
// Pending lifetime, DriverTimeout, and concurrency contract match
// [Client.OfferAddPublication]; poll with PollAddSubscription.
//
// ON_AVAILABLE_IMAGE may arrive during or after polling; images attach via
// conductor dispatch, not as a precondition for PollAddSubscription to return.
func (client *Client) OfferAddSubscription(streamID int32, channel string) (int64, error) {
	if err := prepareAdd(client, channel); err != nil {
		return 0, err
	}
	return client.offerAdd(
		conductor.PendingKindAddSubscription,
		streamID,
		channel,
		func(proxy *conductor.DriverProxy, correlationID int64) error {
			return proxy.AddSubscription(correlationID, streamID, channel)
		},
	)
}

// PollAddPublication performs one conductor tick via doWorkLocked and PollPending.
// When done is true, the publication is ready or a terminal registration error
// is returned in err. When done is false and err is nil, registration is still
// in flight; call PollAddPublication again until done or terminal err.
//
// To abandon without Client.Close, call CancelPending or continue polling until
// done or a terminal err (including *RegistrationTimeoutError from conductor expire
// or the client deadline fallback in pollAddWith). Not safe for concurrent use with
// other Client methods.
//
// An id from another kind of command, such as an async destination command,
// returns done=false and an error. The entry stays.
func (client *Client) PollAddPublication(
	correlationID int64,
) (*Publication, bool, error) {
	return pollAdd(
		client,
		correlationID,
		publicationFromPending,
	)
}

// PollAddExclusivePublication performs one conductor tick via doWorkLocked and
// PollPending. When done is true, the exclusive publication is ready or a
// terminal registration error is returned in err. When done is false and err is
// nil, registration is still in flight; call PollAddExclusivePublication again
// until done or terminal err.
//
// To abandon without Client.Close, call CancelPending or continue polling until
// done or a terminal err (including *RegistrationTimeoutError from conductor expire
// or the client deadline fallback in pollAddWith). Not safe for concurrent use with
// other Client methods.
//
// An id from another kind of command, such as an async destination command,
// returns done=false and an error. The entry stays.
func (client *Client) PollAddExclusivePublication(
	correlationID int64,
) (*ExclusivePublication, bool, error) {
	return pollAdd(
		client,
		correlationID,
		exclusivePublicationFromPending,
	)
}

// PollAddSubscription performs one conductor tick via doWorkLocked and
// PollPending. When done is true, the subscription is ready or a terminal
// registration error is returned in err. When done is false and err is nil,
// registration is still in flight; call PollAddSubscription again until done or
// terminal err.
//
// To abandon without Client.Close, call CancelPending or continue polling until
// done or a terminal err (including *RegistrationTimeoutError from conductor expire
// or the client deadline fallback in pollAddWith). Not safe for concurrent use with
// other Client methods.
//
// An id from another kind of command, such as an async destination command,
// returns done=false and an error. The entry stays.
func (client *Client) PollAddSubscription(
	correlationID int64,
) (*Subscription, bool, error) {
	return pollAdd(
		client,
		correlationID,
		subscriptionFromPending,
	)
}

func (client *Client) offerAdd(
	pendingKind conductor.PendingKind,
	streamID int32,
	channel string,
	offer driverOfferFunc,
) (int64, error) {
	if err := client.lockClient(); err != nil {
		return 0, err
	}
	defer client.unlockClient()

	if err := client.requireReadyWithConductor(); err != nil {
		return 0, err
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
		return 0, err
	}
	return offerCtx.correlationID, nil
}

type pollAddAwaitTickFunc func(
	client *Client,
	conductorInstance *conductor.Conductor,
	correlationID int64,
	mapPollErr func(error) error,
) (conductor.PendingRegistration, bool, error)

func pollAdd[T any](
	client *Client,
	correlationID int64,
	convert func(conductor.PendingRegistration) (T, error),
) (T, bool, error) {
	return pollAddWith(client, correlationID, convert, (*Client).awaitPendingPollTick)
}

func pollAddWith[T any](
	client *Client,
	correlationID int64,
	convert func(conductor.PendingRegistration) (T, error),
	awaitTick pollAddAwaitTickFunc,
) (T, bool, error) {
	var zero T
	if client == nil {
		return zero, false, ErrClientNotReady
	}
	if err := client.lockClient(); err != nil {
		return zero, false, err
	}
	defer client.unlockClient()

	if err := client.requireReadyWithConductor(); err != nil {
		return zero, false, err
	}
	conductorInstance := client.conductor
	defer conductorInstance.SetActiveCorrelationID(conductor.NullCorrelationID)

	pending, hasPending := conductorInstance.PendingResult(correlationID)
	if !hasPending {
		return zero, false, ErrAwaitInterrupted
	}
	registrationKind := registrationKindFor(pending.Kind)
	if registrationKind == "" {
		return zero, false, fmt.Errorf(
			"aeron: poll add: correlation id %d is not an add command",
			correlationID,
		)
	}

	conductorInstance.SetActiveCorrelationID(correlationID)
	registration, completed, err := awaitTick(
		client,
		conductorInstance,
		correlationID,
		func(pollErr error) error {
			return mapAddPollPendingError(
				registrationKind,
				correlationID,
				pending.StreamID,
				pending.Channel,
				pollErr,
			)
		},
	)
	if err != nil {
		return zero, completed, err
	}
	if completed {
		value, convErr := convert(registration)
		return value, true, convErr
	}
	if client.nanoClock() >= pending.DeadlineNs {
		conductorInstance.CancelPending(correlationID)
		return zero, true, newRegistrationTimeoutError(
			registrationKind,
			correlationID,
			pending.StreamID,
			pending.Channel,
		)
	}
	return zero, false, nil
}
