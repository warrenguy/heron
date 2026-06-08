package conductor

import (
	"github.com/warrenguy/heron/aeron/atomic"
)

const ClientTimeoutBodyLength = 8

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

type PendingEventHandler interface {
	OnOperationSuccess(correlationID int64) bool
	OnError(resp ErrorResponse) bool
	OnNextAvailableSessionID(correlationID int64, nextSessionID int32) bool
}

func (adapter *DriverEventsAdapter) Receive(
	activeCorrelationID int64,
	pending PendingEventHandler,
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
		adapter.onMessage(msgTypeID, buffer, offset, length, activeCorrelationID, pending)
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
	pending PendingEventHandler,
) {
	switch msgTypeID {
	case OnClientTimeout:
		adapter.handleClientTimeout(buffer, offset, length)
	case OnOperationSuccess:
		adapter.handleOnOperationSuccess(
			buffer,
			offset,
			length,
			activeCorrelationID,
			pending,
		)
	case OnError:
		adapter.handleOnError(buffer, offset, length, activeCorrelationID, pending)
	case OnNextAvailableSessionID:
		adapter.handleNextAvailableSessionID(
			buffer,
			offset,
			length,
			activeCorrelationID,
			pending,
		)
	default:
		adapter.unhandledEventCount++
	}
}

func (adapter *DriverEventsAdapter) rejectUnlessActiveCorrelation(
	activeCorrelationID int64,
	pending PendingEventHandler,
	correlationID int64,
) bool {
	if activeCorrelationID == NullCorrelationID || pending == nil ||
		correlationID != activeCorrelationID {
		adapter.unhandledEventCount++
		return true
	}
	return false
}

func (adapter *DriverEventsAdapter) handleOnOperationSuccess(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
	pending PendingEventHandler,
) {
	if length < OperationSuccessBodyLength {
		adapter.unhandledEventCount++
		return
	}
	decoded, err := DecodeOperationSuccess(buffer, offset)
	if err != nil {
		adapter.unhandledEventCount++
		return
	}
	if adapter.rejectUnlessActiveCorrelation(
		activeCorrelationID,
		pending,
		decoded.CorrelationID,
	) {
		return
	}
	if !pending.OnOperationSuccess(decoded.CorrelationID) {
		adapter.unhandledEventCount++
	}
}

func (adapter *DriverEventsAdapter) handleOnError(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
	pending PendingEventHandler,
) {
	if length < ErrorResponseFixedBodyLength {
		adapter.unhandledEventCount++
		return
	}
	decoded, err := DecodeErrorResponse(buffer, offset, length)
	if err != nil {
		adapter.unhandledEventCount++
		return
	}
	if adapter.rejectUnlessActiveCorrelation(
		activeCorrelationID,
		pending,
		decoded.OffendingCommandCorrelationID,
	) {
		return
	}
	if !pending.OnError(decoded) {
		adapter.unhandledEventCount++
	}
}

func (adapter *DriverEventsAdapter) handleNextAvailableSessionID(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
	pending PendingEventHandler,
) {
	if length < CorrelationAndInt32BodyLength {
		adapter.unhandledEventCount++
		return
	}
	decoded, err := DecodeCorrelationAndInt32(buffer, offset)
	if err != nil {
		adapter.unhandledEventCount++
		return
	}
	if adapter.rejectUnlessActiveCorrelation(
		activeCorrelationID,
		pending,
		decoded.CorrelationID,
	) {
		return
	}
	if !pending.OnNextAvailableSessionID(decoded.CorrelationID, decoded.Int32Value) {
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
