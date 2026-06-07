package conductor

import (
	"errors"

	"github.com/warrenguy/heron/aeron/atomic"
)

const ClientTimeoutBodyLength = 8

var ErrDriverErrorEvent = errors.New("conductor: driver error event received")

type DriverEventsAdapter struct {
	copyReceiver        *CopyBroadcastReceiver
	clientID            int64
	errorHandler        func(error)
	onClientTimeout     func()
	unhandledEventCount int64
	isInvalid           bool
}

func NewDriverEventsAdapter(
	clientID int64,
	copyReceiver *CopyBroadcastReceiver,
	errorHandler func(error),
	onClientTimeout func(),
) *DriverEventsAdapter {
	return &DriverEventsAdapter{
		copyReceiver:    copyReceiver,
		clientID:        clientID,
		errorHandler:    errorHandler,
		onClientTimeout: onClientTimeout,
	}
}

func (adapter *DriverEventsAdapter) Receive(activeCorrelationID int64) int {
	// Java matches driver responses to in-flight sync commands; wired when Client adds awaitResponse.
	_ = activeCorrelationID

	if adapter.isInvalid {
		return 0
	}

	count, err := adapter.copyReceiver.Receive(adapter.onMessage)
	if err != nil {
		adapter.isInvalid = true
		invokeErrorHandler(adapter.errorHandler, err)
		return 0
	}
	return count
}

func (adapter *DriverEventsAdapter) IsInvalid() bool {
	return adapter.isInvalid
}

func (adapter *DriverEventsAdapter) UnhandledEventCount() int64 {
	return adapter.unhandledEventCount
}

func (adapter *DriverEventsAdapter) onMessage(
	msgTypeID int,
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
) {
	switch msgTypeID {
	case OnClientTimeout:
		adapter.handleClientTimeout(buffer, offset, length)
	case OnError:
		// Full ErrorResponse decode and correlation routing lands with Client command handling.
		invokeErrorHandler(adapter.errorHandler, ErrDriverErrorEvent)
	default:
		adapter.unhandledEventCount++
	}
}

func (adapter *DriverEventsAdapter) handleClientTimeout(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
) {
	if length < ClientTimeoutBodyLength {
		adapter.unhandledEventCount++
		return
	}

	eventClientID := buffer.GetInt64(offset)
	if eventClientID != adapter.clientID {
		return
	}

	if adapter.onClientTimeout != nil {
		adapter.onClientTimeout()
	}
}
