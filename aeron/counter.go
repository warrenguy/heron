package aeron

import (
	"fmt"
	"sync/atomic"

	"github.com/warrenguy/heron/aeron/conductor"
)

// Counter is a driver counter allocated with AddCounter.
//
// Ownership: the caller owns the Counter. Close the Counter or its Client.
//
// Lifetime: after Close or Client.Close the driver may reuse CounterID. Do not
// use CounterID after either call.
//
// Behaviour: Counter has no value operations. Read and write the value through
// Client.CountersReader with CounterID.
type Counter struct {
	client         *Client
	counterID      int32
	registrationID int64
	closed         atomic.Bool
}

// AddCounter asks the driver to allocate a counter. AddCounter blocks until
// the driver answers or DriverTimeout elapses.
//
// Behaviour: AddCounter offers ADD_COUNTER once. A full ring returns at once,
// with no retry. The correlation id becomes the counter registration id.
//
// Concurrency: AddCounter holds clientLock for the whole wait. Concurrent Close
// is unsupported. See the Client godoc.
//
// Returns an error when:
//   - the client is closed: ErrClientClosed
//   - the client has no conductor or driver proxy: ErrClientNotReady
//   - key exceeds cnc.CounterMetadataMaxKeyLength: conductor.ErrCounterKeyTooLong
//   - label exceeds cnc.CounterMetadataMaxLabelLength:
//     conductor.ErrCounterLabelTooLong
//   - label holds a non-ASCII byte: atomicbuffer.ErrStringASCIINonASCII
//   - the to-driver ring is full: conductor.ErrRingInsufficientCapacity
//   - the driver sends ON_ERROR: *RegistrationError
//   - DriverTimeout elapses: *AwaitTimeoutError
//   - the conductor is terminating: ErrConductorTerminating
//   - the conductor terminates during the wait: *DriverTimeoutError or the
//     termination cause
//   - the caller is inside a handler: ErrReentrantCall
//
// On error: no Counter and no pending entry.
func (client *Client) AddCounter(
	typeID int32,
	key []byte,
	label string,
) (*Counter, error) {
	if err := client.requireReadyWithDriverProxy(); err != nil {
		return nil, err
	}
	if err := conductor.ValidateCounterInput(key, label); err != nil {
		return nil, err
	}
	correlationID := client.driverProxy.NextCorrelationID()
	registration, err := client.awaitResponse(
		correlationID,
		conductor.PendingKindAddCounter,
		client.driverTimeout,
		func() error {
			return client.driverProxy.AddCounter(correlationID, typeID, key, label)
		},
	)
	if err != nil {
		return nil, err
	}
	return &Counter{
		client:         client,
		counterID:      registration.Int32Value,
		registrationID: correlationID,
	}, nil
}

// CounterID returns the driver counter id.
func (counter *Counter) CounterID() int32 {
	return counter.counterID
}

// RegistrationID returns the ADD_COUNTER correlation id.
func (counter *Counter) RegistrationID() int64 {
	return counter.registrationID
}

// Close offers REMOVE_COUNTER once. Close does not wait for the driver answer.
//
// Behaviour: Close is idempotent. The first call that passes the
// reentrancy check closes the Counter. A call from a handler on an open
// Counter returns [ErrReentrantCall]. The handler call does not close the
// Counter. An offer failure leaves the Counter closed.
//
// Concurrency: Close holds clientLock for the offer. Close is safe beside
// Client.Close.
//
// Returns:
//   - nil after the offer, or without an offer for a closed client, a missing
//     driver proxy or a terminating conductor
//   - [ErrReentrantCall] when the caller is inside a handler and the
//     Counter is open. The call does not close the Counter.
//   - "aeron: remove counter: %w" when the offer fails, such as a full ring
func (counter *Counter) Close() error {
	if counter == nil || counter.closed.Load() {
		return nil
	}
	client := counter.client
	if err := client.checkReentrant(); err != nil {
		return err
	}
	if !counter.closed.CompareAndSwap(false, true) {
		return nil
	}
	if client == nil {
		return nil
	}
	client.clientLock.Lock()
	defer client.unlockClient()

	driverProxy := client.driverProxy
	conductorInstance := client.conductor
	if client.closed.Load() || driverProxy == nil {
		return nil
	}
	if conductorInstance != nil && conductorInstance.IsTerminating() {
		return nil
	}
	correlationID := driverProxy.NextCorrelationID()
	if err := driverProxy.RemoveCounter(correlationID, counter.registrationID); err != nil {
		return fmt.Errorf("aeron: remove counter: %w", err)
	}
	return nil
}
