package conductor

import (
	"github.com/warrenguy/heron/agrona/atomic"
)

const ClientTimeoutBodyLength = 8

type (
	PublicationReadyHandler  func(PublicationBuffersReady)
	SubscriptionReadyHandler func(SubscriptionReady)
	AvailableImageHandler    func(AvailableImage)
	UnavailableImageHandler  func(correlationID, subscriptionRegistrationID int64)
	PublicationErrorHandler  func(PublicationError)
	CorrelatedReadyHandler   func(correlationID int64)
)

type DriverEventsAdapterHandlers struct {
	PublicationReady          PublicationReadyHandler
	ExclusivePublicationReady PublicationReadyHandler
	SubscriptionReady         SubscriptionReadyHandler
	AvailableImage            AvailableImageHandler
	UnavailableImage          UnavailableImageHandler
	PublicationError          PublicationErrorHandler
	CorrelatedReady           CorrelatedReadyHandler
}

type DriverEventsAdapter struct {
	copyReceiver         *CopyBroadcastReceiver
	clientID             int64
	errorHandler         func(error)
	clientTimeoutHandler func()
	handlers             DriverEventsAdapterHandlers
	pendingRegistry      *pendingRegistry
	unhandledEventCount  int64
	// Permanent after ErrBroadcastLapped; Receive returns 0 with no recovery.
	invalid bool
}

func NewDriverEventsAdapter(
	clientID int64,
	copyReceiver *CopyBroadcastReceiver,
	errorHandler func(error),
	clientTimeoutHandler func(),
	handlers DriverEventsAdapterHandlers,
	pendingRegistry *pendingRegistry,
) *DriverEventsAdapter {
	return &DriverEventsAdapter{
		copyReceiver:         copyReceiver,
		clientID:             clientID,
		errorHandler:         errorHandler,
		clientTimeoutHandler: clientTimeoutHandler,
		handlers:             handlers,
		pendingRegistry:      pendingRegistry,
	}
}

type PendingEventHandler interface {
	OperationSucceeded(correlationID int64) bool
	DriverError(resp ErrorResponse) bool
	NextSessionID(correlationID int64, nextSessionID int32) bool
}

func (adapter *DriverEventsAdapter) Receive(
	activeCorrelationID int64,
	pending PendingEventHandler,
) int {
	if adapter.invalid {
		return 0
	}

	count, err := adapter.copyReceiver.Receive(func(
		msgTypeID int,
		buffer atomic.AtomicBuffer,
		offset int,
		length int,
	) {
		adapter.handleMessage(msgTypeID, buffer, offset, length, activeCorrelationID, pending)
	})
	if err != nil {
		adapter.invalid = true
		invokeErrorHandler(adapter.errorHandler, err)
		return 0
	}
	return count
}

func (adapter *DriverEventsAdapter) IsInvalid() bool {
	return adapter.invalid
}

func (adapter *DriverEventsAdapter) UnhandledEventCount() int64 {
	return adapter.unhandledEventCount
}

func (adapter *DriverEventsAdapter) handleMessage(
	msgTypeID int,
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
	pending PendingEventHandler,
) {
	switch msgTypeID {
	// Driver events (0x0F01-0x0F0D), ascending event ID.
	case OnError:
		adapter.handleError(buffer, offset, length, activeCorrelationID, pending)
	case OnAvailableImage:
		adapter.handleAvailableImage(buffer, offset, length)
	case OnPublicationReady:
		adapter.handlePublicationBuffersReady(
			buffer,
			offset,
			length,
			activeCorrelationID,
			adapter.handlers.PublicationReady,
		)
	case OnExclusivePublicationReady:
		adapter.handlePublicationBuffersReady(
			buffer,
			offset,
			length,
			activeCorrelationID,
			adapter.handlers.ExclusivePublicationReady,
		)
	case OnOperationSuccess:
		adapter.handleOperationSuccess(
			buffer,
			offset,
			length,
			activeCorrelationID,
			pending,
		)
	case OnUnavailableImage:
		adapter.handleUnavailableImage(buffer, offset, length)
	case OnSubscriptionReady:
		adapter.handleSubscriptionReady(buffer, offset, length, activeCorrelationID)
	case OnClientTimeout:
		adapter.handleClientTimeout(buffer, offset, length)
	case OnPublicationError:
		adapter.handlePublicationError(buffer, offset, length)
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

// correlated gates async ready events: active await OR pending registry entry.
// rejectUnlessActiveCorrelation requires exact active match for correlated responses.
func (adapter *DriverEventsAdapter) correlated(
	activeCorrelationID int64,
	correlationID int64,
) bool {
	return (correlationID == activeCorrelationID && activeCorrelationID != NullCorrelationID) ||
		(adapter.pendingRegistry != nil && adapter.pendingRegistry.Contains(correlationID))
}

// Fixed flyweight: broadcast length may exceed the body;
// decode reads OperationSuccessBodyLength only.
func (adapter *DriverEventsAdapter) handleOperationSuccess(
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
	if !pending.OperationSucceeded(decoded.CorrelationID) {
		adapter.unhandledEventCount++
	}
}

// Variable-length flyweight: decode requires exact body consumption; trailing bytes are rejected.
func (adapter *DriverEventsAdapter) handleError(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
	pending PendingEventHandler,
) {
	if length < ErrorResponseMinBodyLength {
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
	if !pending.DriverError(decoded) {
		adapter.unhandledEventCount++
	}
}

// Fixed flyweight: broadcast length may exceed the body;
// decode reads NextAvailableSessionIDBodyLength only.
func (adapter *DriverEventsAdapter) handleNextAvailableSessionID(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
	pending PendingEventHandler,
) {
	if length < NextAvailableSessionIDBodyLength {
		adapter.unhandledEventCount++
		return
	}
	decoded, err := DecodeNextAvailableSessionID(buffer, offset)
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
	if !pending.NextSessionID(decoded.CorrelationID, decoded.Int32Value) {
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

	if adapter.clientTimeoutHandler != nil {
		adapter.clientTimeoutHandler()
	}
}

func peekEventCorrelationID(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
) (int64, bool) {
	if length < sizeOfInt64 {
		return 0, false
	}
	return buffer.GetInt64(offset), true
}

func handleDecodedEvent[T any](
	adapter *DriverEventsAdapter,
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	decode func(atomic.AtomicBuffer, int, int) (T, error),
	eventHandler func(T),
) {
	decoded, err := decode(buffer, offset, length)
	if err != nil {
		adapter.unhandledEventCount++
		return
	}
	if eventHandler != nil {
		eventHandler(decoded)
	}
}

func handleCorrelatedReady[T any](
	adapter *DriverEventsAdapter,
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
	minimumBodyLength int,
	peekCorrelationID func(atomic.AtomicBuffer, int, int) (int64, bool),
	decode func(atomic.AtomicBuffer, int, int) (T, error),
	correlationID func(T) int64,
	readyHandler func(T),
) {
	if peekCorrelationID != nil && length >= minimumBodyLength {
		eventCorrelationID, ok := peekCorrelationID(buffer, offset, length)
		if ok && !adapter.correlated(activeCorrelationID, eventCorrelationID) {
			return
		}
	}

	decoded, err := decode(buffer, offset, length)
	if err != nil {
		adapter.unhandledEventCount++
		return
	}

	eventCorrelationID := correlationID(decoded)
	if adapter.handlers.CorrelatedReady != nil {
		adapter.handlers.CorrelatedReady(eventCorrelationID)
	}
	if readyHandler != nil {
		readyHandler(decoded)
	}
}

func publicationBuffersReadyCorrelationID(event PublicationBuffersReady) int64 {
	return event.CorrelationID
}

func subscriptionReadyCorrelationID(event SubscriptionReady) int64 {
	return event.CorrelationID
}

func (adapter *DriverEventsAdapter) handleAvailableImage(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
) {
	handleDecodedEvent(
		adapter,
		buffer,
		offset,
		length,
		DecodeAvailableImage,
		adapter.handlers.AvailableImage,
	)
}

func (adapter *DriverEventsAdapter) handlePublicationBuffersReady(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
	handler PublicationReadyHandler,
) {
	handleCorrelatedReady(
		adapter,
		buffer,
		offset,
		length,
		activeCorrelationID,
		PublicationBuffersReadyFixedLength+atomic.StringASCIIHeaderLength,
		peekEventCorrelationID,
		DecodePublicationBuffersReady,
		publicationBuffersReadyCorrelationID,
		handler,
	)
}

func (adapter *DriverEventsAdapter) handleSubscriptionReady(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
) {
	handleCorrelatedReady(
		adapter,
		buffer,
		offset,
		length,
		activeCorrelationID,
		SubscriptionReadyBodyLength,
		peekEventCorrelationID,
		DecodeSubscriptionReady,
		subscriptionReadyCorrelationID,
		adapter.handlers.SubscriptionReady,
	)
}

func (adapter *DriverEventsAdapter) handleUnavailableImage(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
) {
	handleDecodedEvent(
		adapter,
		buffer,
		offset,
		length,
		DecodeUnavailableImage,
		func(event UnavailableImage) {
			if adapter.handlers.UnavailableImage != nil {
				adapter.handlers.UnavailableImage(
					event.CorrelationID,
					event.SubscriptionRegistrationID,
				)
			}
		},
	)
}

func (adapter *DriverEventsAdapter) handlePublicationError(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
) {
	handleDecodedEvent(
		adapter,
		buffer,
		offset,
		length,
		DecodePublicationError,
		adapter.handlers.PublicationError,
	)
}
