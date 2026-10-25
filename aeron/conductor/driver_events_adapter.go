package conductor

import "github.com/warrenguy/heron/atomicbuffer"

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
	// UnknownRegistration runs for ErrorCodeUnknownSubscription and
	// ErrorCodeUnknownPublication. Invoked before the correlation reject.
	UnknownRegistration func()
}

type DriverEventsAdapter struct {
	copyReceiver         *CopyBroadcastReceiver
	clientID             int64
	clientTimeoutHandler func()
	handlers             DriverEventsAdapterHandlers
	pendingRegistry      *pendingRegistry
	unhandledEventCount  int64
}

func NewDriverEventsAdapter(
	clientID int64,
	copyReceiver *CopyBroadcastReceiver,
	clientTimeoutHandler func(),
	handlers DriverEventsAdapterHandlers,
	pendingRegistry *pendingRegistry,
) *DriverEventsAdapter {
	return &DriverEventsAdapter{
		copyReceiver:         copyReceiver,
		clientID:             clientID,
		clientTimeoutHandler: clientTimeoutHandler,
		handlers:             handlers,
		pendingRegistry:      pendingRegistry,
	}
}

type PendingEventHandler interface {
	OperationSucceeded(correlationID int64) bool
	DriverError(resp ErrorResponse) bool
	NextSessionID(correlationID int64, nextSessionID int32) bool
	CounterReady(correlationID int64, counterID int32) bool
}

// Receive dispatches the next driver event to its handler.
//
// Returns:
//   - 1 and nil after dispatching one event.
//   - 0 and nil when no event is pending.
//   - 0 and ErrBroadcastLapped after a lap or a torn read. The next call
//     resumes at a record boundary.
//   - 0 and ErrBroadcastMessageTooLong for a record that runs past the
//     broadcast bounds. The caller must stop receiving.
//
// Concurrency: invoker-only.
func (adapter *DriverEventsAdapter) Receive(
	activeCorrelationID int64,
	pending PendingEventHandler,
) (int, error) {
	return adapter.copyReceiver.Receive(func(
		msgTypeID int,
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
	) {
		adapter.handleMessage(msgTypeID, buffer, offset, length, activeCorrelationID, pending)
	})
}

func (adapter *DriverEventsAdapter) UnhandledEventCount() int64 {
	return adapter.unhandledEventCount
}

func (adapter *DriverEventsAdapter) handleMessage(
	msgTypeID int,
	buffer atomicbuffer.AtomicBuffer,
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
	case OnCounterReady:
		adapter.handleCounterReady(buffer, offset, length, pending)
	case OnUnavailableCounter:
		adapter.handleUnavailableCounter(buffer, offset, length)
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
		adapter.markUnhandled()
	}
}

func (adapter *DriverEventsAdapter) markUnhandled() {
	adapter.unhandledEventCount++
}

func (adapter *DriverEventsAdapter) markUnhandledIf(reject bool) bool {
	if reject {
		adapter.markUnhandled()
	}
	return reject
}

func (adapter *DriverEventsAdapter) rejectUnlessActiveCorrelation(
	activeCorrelationID int64,
	pending PendingEventHandler,
	correlationID int64,
) bool {
	return adapter.markUnhandledIf(
		pending == nil || !isActiveCorrelation(activeCorrelationID, correlationID),
	)
}

func (adapter *DriverEventsAdapter) rejectUnlessCorrelated(
	activeCorrelationID int64,
	pending PendingEventHandler,
	correlationID int64,
) bool {
	return adapter.markUnhandledIf(
		pending == nil || !adapter.correlated(activeCorrelationID, correlationID),
	)
}

// rejectUnlessActiveOrDestination filters ON_OPERATION_SUCCESS. See
// isOperationSuccessCorrelated.
func (adapter *DriverEventsAdapter) rejectUnlessActiveOrDestination(
	activeCorrelationID int64,
	pending PendingEventHandler,
	correlationID int64,
) bool {
	return adapter.markUnhandledIf(
		pending == nil ||
			!adapter.isOperationSuccessCorrelated(activeCorrelationID, correlationID),
	)
}

// correlated gates async driver responses when the event correlation differs from
// active: active await OR pending registry entry. rejectUnlessActiveCorrelation
// requires an exact active match for next session ID. Operation success uses
// isOperationSuccessCorrelated.
func (adapter *DriverEventsAdapter) correlated(
	activeCorrelationID int64,
	correlationID int64,
) bool {
	return isActiveCorrelation(activeCorrelationID, correlationID) ||
		(adapter.pendingRegistry != nil && adapter.pendingRegistry.Contains(correlationID))
}

// isActiveCorrelation reports whether correlationID is the active await id.
func isActiveCorrelation(activeCorrelationID, correlationID int64) bool {
	return activeCorrelationID != NullCorrelationID && correlationID == activeCorrelationID
}

// isOperationSuccessCorrelated accepts ON_OPERATION_SUCCESS for the active id
// or for a PendingKindDestination registry entry. An async destination reply
// arrives outside any await.
func (adapter *DriverEventsAdapter) isOperationSuccessCorrelated(
	activeCorrelationID int64,
	correlationID int64,
) bool {
	if isActiveCorrelation(activeCorrelationID, correlationID) {
		return true
	}
	if adapter.pendingRegistry == nil {
		return false
	}
	registration, ok := adapter.pendingRegistry.Get(correlationID)
	return ok && registration.Kind == PendingKindDestination
}

// Fixed flyweight: broadcast length may exceed the body;
// decode reads OperationSuccessBodyLength only.
func (adapter *DriverEventsAdapter) handleOperationSuccess(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
	pending PendingEventHandler,
) {
	decoded, ok := decodeFixedLengthEvent(
		adapter,
		buffer,
		offset,
		length,
		OperationSuccessBodyLength,
		DecodeOperationSuccess,
	)
	if !ok {
		return
	}
	if adapter.rejectUnlessActiveOrDestination(activeCorrelationID, pending, decoded.CorrelationID) {
		return
	}
	if !pending.OperationSucceeded(decoded.CorrelationID) {
		adapter.markUnhandled()
	}
}

// isUnknownRegistrationCode reports the error codes that mean the driver no
// longer knows the registration. The driver may have removed this client.
func isUnknownRegistrationCode(code int32) bool {
	return code == ErrorCodeUnknownSubscription || code == ErrorCodeUnknownPublication
}

// Variable-length flyweight: decode requires exact body consumption; trailing bytes are rejected.
func (adapter *DriverEventsAdapter) handleError(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
	pending PendingEventHandler,
) {
	if length < ErrorResponseMinBodyLength {
		adapter.markUnhandled()
		return
	}
	decoded, err := DecodeErrorResponse(buffer, offset, length)
	if err != nil {
		adapter.markUnhandled()
		return
	}
	if adapter.handlers.UnknownRegistration != nil && isUnknownRegistrationCode(decoded.ErrorCode) {
		adapter.handlers.UnknownRegistration()
	}
	if adapter.rejectUnlessCorrelated(
		activeCorrelationID,
		pending,
		decoded.OffendingCommandCorrelationID,
	) {
		return
	}
	if !pending.DriverError(decoded) {
		adapter.markUnhandled()
	}
}

// Fixed flyweight: broadcast length may exceed the body;
// decode reads NextAvailableSessionIDBodyLength only.
func (adapter *DriverEventsAdapter) handleNextAvailableSessionID(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
	pending PendingEventHandler,
) {
	decoded, ok := decodeFixedLengthEvent(
		adapter,
		buffer,
		offset,
		length,
		NextAvailableSessionIDBodyLength,
		DecodeNextAvailableSessionID,
	)
	if !ok {
		return
	}
	if adapter.rejectUnlessActiveCorrelation(activeCorrelationID, pending, decoded.CorrelationID) {
		return
	}
	if !pending.NextSessionID(decoded.CorrelationID, decoded.Int32Value) {
		adapter.markUnhandled()
	}
}

// handleCounterReady completes a pending add-counter entry. The driver
// broadcasts counter ready events to every client, so an unmatched event
// adds no unhandled count.
func (adapter *DriverEventsAdapter) handleCounterReady(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	pending PendingEventHandler,
) {
	decoded, ok := decodeFixedLengthEvent(
		adapter,
		buffer,
		offset,
		length,
		CounterUpdateBodyLength,
		DecodeCounterUpdate,
	)
	if !ok || pending == nil {
		return
	}
	pending.CounterReady(decoded.CorrelationID, decoded.Int32Value)
}

// handleUnavailableCounter decodes the event and drops it. The client has no
// counter handlers.
func (adapter *DriverEventsAdapter) handleUnavailableCounter(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
) {
	decodeFixedLengthEvent(
		adapter,
		buffer,
		offset,
		length,
		CounterUpdateBodyLength,
		DecodeCounterUpdate,
	)
}

func (adapter *DriverEventsAdapter) handleClientTimeout(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
) {
	if length < ClientTimeoutBodyLength {
		adapter.markUnhandled()
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
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
) (int64, bool) {
	if length < sizeOfInt64 {
		return 0, false
	}
	return buffer.GetInt64(offset), true
}

func publicationBuffersReadyEventCorrelationID(event PublicationBuffersReady) int64 {
	return event.CorrelationID
}

func subscriptionReadyEventCorrelationID(event SubscriptionReady) int64 {
	return event.CorrelationID
}

// decodeFixedLengthEvent decodes a fixed-length event body. A short or
// out-of-bounds body adds one unhandled count and returns false.
func decodeFixedLengthEvent[T any](
	adapter *DriverEventsAdapter,
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	minBodyLength int,
	decode func(atomicbuffer.AtomicBuffer, int) (T, error),
) (T, bool) {
	var zero T
	if length < minBodyLength {
		adapter.markUnhandled()
		return zero, false
	}
	decoded, err := decode(buffer, offset)
	if err != nil {
		adapter.markUnhandled()
		return zero, false
	}
	return decoded, true
}

func handleDecodedEvent[T any](
	adapter *DriverEventsAdapter,
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	decode func(atomicbuffer.AtomicBuffer, int, int) (T, error),
	eventHandler func(T),
) {
	decoded, err := decode(buffer, offset, length)
	if err != nil {
		adapter.markUnhandled()
		return
	}
	if eventHandler != nil {
		eventHandler(decoded)
	}
}

func handleCorrelatedReady[T any](
	adapter *DriverEventsAdapter,
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	activeCorrelationID int64,
	minimumBodyLength int,
	decode func(atomicbuffer.AtomicBuffer, int, int) (T, error),
	correlationFrom func(T) int64,
	readyHandler func(T),
) {
	if length >= minimumBodyLength {
		eventCorrelationID, ok := peekEventCorrelationID(buffer, offset, length)
		if ok && !adapter.correlated(activeCorrelationID, eventCorrelationID) {
			return
		}
	}

	decoded, err := decode(buffer, offset, length)
	if err != nil {
		adapter.markUnhandled()
		return
	}

	eventCorrelationID := correlationFrom(decoded)
	if adapter.handlers.CorrelatedReady != nil {
		adapter.handlers.CorrelatedReady(eventCorrelationID)
	}
	if readyHandler != nil {
		readyHandler(decoded)
	}
}

func (adapter *DriverEventsAdapter) handleAvailableImage(
	buffer atomicbuffer.AtomicBuffer,
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
	buffer atomicbuffer.AtomicBuffer,
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
		PublicationBuffersReadyFixedLength+atomicbuffer.StringASCIIHeaderLength,
		DecodePublicationBuffersReady,
		publicationBuffersReadyEventCorrelationID,
		handler,
	)
}

func (adapter *DriverEventsAdapter) handleSubscriptionReady(
	buffer atomicbuffer.AtomicBuffer,
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
		DecodeSubscriptionReady,
		subscriptionReadyEventCorrelationID,
		adapter.handlers.SubscriptionReady,
	)
}

func (adapter *DriverEventsAdapter) handleUnavailableImage(
	buffer atomicbuffer.AtomicBuffer,
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
	buffer atomicbuffer.AtomicBuffer,
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
