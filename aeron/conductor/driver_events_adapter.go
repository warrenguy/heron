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

type nextSessionIDCompleter interface {
	completeNextSessionID(correlationID int64, nextSessionID int32) bool
}

func (adapter *DriverEventsAdapter) Receive(
	activeCorrelationID int64,
	completer nextSessionIDCompleter,
) int {
	if adapter.isInvalid {
		return 0
	}

	count, err := adapter.copyReceiver.Receive(func(
		msgTypeID int,
		buffer atomic.AtomicBuffer,
		offset int,
		length int,
	) {
		adapter.onMessage(msgTypeID, buffer, offset, length, activeCorrelationID, completer)
	})
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
	activeCorrelationID int64,
	completer nextSessionIDCompleter,
) {
	switch msgTypeID {
	case OnClientTimeout:
		adapter.handleClientTimeout(buffer, offset, length)
	case OnNextAvailableSessionID:
		adapter.handleNextAvailableSessionID(
			buffer,
			offset,
			length,
			activeCorrelationID,
			completer,
		)
	case OnError:
		// Full ErrorResponse decode and correlation routing lands with Client command handling.
		invokeErrorHandler(adapter.errorHandler, ErrDriverErrorEvent)
	default:
		adapter.unhandledEventCount++
	}
}

func (adapter *DriverEventsAdapter) handleNextAvailableSessionID(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
	completer nextSessionIDCompleter,
) {
	if activeCorrelationID == NullCorrelationID || completer == nil {
		adapter.unhandledEventCount++
		return
	}
	if length < CorrelationAndInt32BodyLength {
		adapter.unhandledEventCount++
		return
	}
	decoded, err := DecodeCorrelationAndInt32(buffer, offset)
	if err != nil {
		adapter.unhandledEventCount++
		return
	}
	if decoded.CorrelationID != activeCorrelationID ||
		!completer.completeNextSessionID(decoded.CorrelationID, decoded.Int32Value) {
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
