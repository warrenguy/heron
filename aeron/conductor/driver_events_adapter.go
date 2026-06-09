package conductor

import (
	"github.com/warrenguy/heron/aeron/atomic"
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
	OnPublicationReady  PublicationReadyHandler
	OnSubscriptionReady SubscriptionReadyHandler
	OnAvailableImage    AvailableImageHandler
	OnUnavailableImage  UnavailableImageHandler
	OnPublicationError  PublicationErrorHandler
	OnCorrelatedReady   CorrelatedReadyHandler
}

type DriverEventsAdapter struct {
	copyReceiver        *CopyBroadcastReceiver
	clientID            int64
	errorHandler        func(error)
	onClientTimeout     func()
	handlers            DriverEventsAdapterHandlers
	pendingRegistry     *pendingRegistry
	unhandledEventCount int64
	// Permanent after ErrBroadcastLapped; Receive returns 0 with no recovery.
	isInvalid bool
}

func NewDriverEventsAdapter(
	clientID int64,
	copyReceiver *CopyBroadcastReceiver,
	errorHandler func(error),
	onClientTimeout func(),
	handlers DriverEventsAdapterHandlers,
	pendingRegistry *pendingRegistry,
) *DriverEventsAdapter {
	return &DriverEventsAdapter{
		copyReceiver:    copyReceiver,
		clientID:        clientID,
		errorHandler:    errorHandler,
		onClientTimeout: onClientTimeout,
		handlers:        handlers,
		pendingRegistry: pendingRegistry,
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
	// Driver events (0x0F01-0x0F0D), ascending event ID.
	case OnError:
		adapter.handleOnError(buffer, offset, length, activeCorrelationID, pending)
	case OnAvailableImage:
		adapter.handleAvailableImage(buffer, offset, length)
	case OnPublicationReady, OnExclusivePublicationReady:
		adapter.handlePublicationReady(buffer, offset, length, activeCorrelationID)
	case OnOperationSuccess:
		adapter.handleOnOperationSuccess(
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

// isCorrelated gates async ready events: active await OR pending registry entry.
// rejectUnlessActiveCorrelation requires exact active match for correlated responses.
func (adapter *DriverEventsAdapter) isCorrelated(
	activeCorrelationID int64,
	correlationID int64,
) bool {
	if correlationID == activeCorrelationID && activeCorrelationID != NullCorrelationID {
		return true
	}
	if adapter.pendingRegistry != nil && adapter.pendingRegistry.Contains(correlationID) {
		return true
	}
	return false
}

// Fixed flyweight: broadcast length may exceed the body;
// decode reads OperationSuccessBodyLength only.
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

// Variable-length flyweight: decode requires exact body consumption; trailing bytes are rejected.
func (adapter *DriverEventsAdapter) handleOnError(
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
	if !pending.OnError(decoded) {
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
	onEvent func(T),
) {
	decoded, err := decode(buffer, offset, length)
	if err != nil {
		adapter.unhandledEventCount++
		return
	}
	if onEvent != nil {
		onEvent(decoded)
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
	onReady func(T),
) {
	if peekCorrelationID != nil && length >= minimumBodyLength {
		if eventCorrelationID, ok := peekCorrelationID(buffer, offset, length); ok &&
			!adapter.isCorrelated(activeCorrelationID, eventCorrelationID) {
			return
		}
	}

	decoded, err := decode(buffer, offset, length)
	if err != nil {
		adapter.unhandledEventCount++
		return
	}

	eventCorrelationID := correlationID(decoded)
	if adapter.handlers.OnCorrelatedReady != nil {
		adapter.handlers.OnCorrelatedReady(eventCorrelationID)
	}
	if onReady != nil {
		onReady(decoded)
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
		adapter.handlers.OnAvailableImage,
	)
}

func (adapter *DriverEventsAdapter) handlePublicationReady(
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
		PublicationBuffersReadyFixedLength+atomic.StringASCIIHeaderLength,
		peekEventCorrelationID,
		DecodePublicationBuffersReady,
		publicationBuffersReadyCorrelationID,
		adapter.handlers.OnPublicationReady,
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
		adapter.handlers.OnSubscriptionReady,
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
			if adapter.handlers.OnUnavailableImage != nil {
				adapter.handlers.OnUnavailableImage(
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
		adapter.handlers.OnPublicationError,
	)
}
