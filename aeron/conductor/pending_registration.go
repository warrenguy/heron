package conductor

import (
	"sync"
	"time"
)

type PendingKind int

const (
	PendingKindOperationSuccess PendingKind = iota
	PendingKindNextSessionID
	PendingKindAddPublication
	PendingKindAddSubscription
	PendingKindRemovePublication
	PendingKindRemoveSubscription
)

type PendingRegistration struct {
	CorrelationID int64
	Kind          PendingKind
	DeadlineNs    int64

	StreamID       int32
	Channel        string
	RegistrationID int64
	Int32Value     int32
	Err            error
	// Publication and Subscription are any-typed until ready handlers construct
	// handles; conductor cannot import aeron without an import cycle.
	Publication  any
	Subscription any
}

// pendingRegistry map structure is mutex-protected; Conductor await fields still
// require a single invoker goroutine per client until clientLock lands.
type pendingRegistry struct {
	mu      sync.Mutex
	entries map[int64]*PendingRegistration
}

func newPendingRegistry() pendingRegistry {
	return pendingRegistry{entries: make(map[int64]*PendingRegistration)}
}

func (registry *pendingRegistry) Register(registration PendingRegistration) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if _, ok := registry.entries[registration.CorrelationID]; ok {
		return ErrPendingAlreadyRegistered
	}
	regCopy := registration
	registry.entries[registration.CorrelationID] = &regCopy
	return nil
}

func (registry *pendingRegistry) Get(correlationID int64) (*PendingRegistration, bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	registration, ok := registry.entries[correlationID]
	return registration, ok
}

func (registry *pendingRegistry) Remove(correlationID int64) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	delete(registry.entries, correlationID)
}

func (registry *pendingRegistry) Contains(correlationID int64) bool {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	_, ok := registry.entries[correlationID]
	return ok
}

func (registry *pendingRegistry) IsEmpty() bool {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	return len(registry.entries) == 0
}

func (registry *pendingRegistry) Clear() {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	clear(registry.entries)
}

func (registry *pendingRegistry) ExpireBefore(
	nowNs int64,
	receivedCorrelationID int64,
	activeCorrelationID int64,
) (workCount int, timedOutActiveCorrelationID int64) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	timedOutActiveCorrelationID = NullCorrelationID
	for correlationID, registration := range registry.entries {
		if correlationID == receivedCorrelationID {
			continue
		}
		if nowNs < registration.DeadlineNs {
			continue
		}
		workCount++
		if correlationID == activeCorrelationID {
			registration.Err = ErrAwaitTimeout
			timedOutActiveCorrelationID = correlationID
			continue
		}
		delete(registry.entries, correlationID)
	}
	return workCount, timedOutActiveCorrelationID
}

func (conductor *Conductor) ActiveCorrelationID() int64 {
	return conductor.activeCorrelationID
}

func (conductor *Conductor) ReceivedCorrelationID() int64 {
	return conductor.receivedCorrelationID
}

func (conductor *Conductor) PendingResult(
	correlationID int64,
) (PendingRegistration, bool) {
	registration, ok := conductor.pending.Get(correlationID)
	if !ok {
		return PendingRegistration{}, false
	}
	return *registration, true
}

// RegisterPending runs on the caller goroutine; PollPending and adapter handlers
// run on the conductor service goroutine inside DoWork. Concurrent DoWork on one
// Conductor is undefined.
func (conductor *Conductor) pendingDeadline(timeout time.Duration) int64 {
	return conductor.config.NanoClock() + timeout.Nanoseconds()
}

func (conductor *Conductor) checkNotTerminating() error {
	if conductor.isTerminating {
		return ErrConductorTerminating
	}
	return nil
}

func (conductor *Conductor) RegisterPending(
	kind PendingKind,
	correlationID int64,
	timeout time.Duration,
) error {
	return conductor.RegisterPendingWithDeadline(
		kind,
		correlationID,
		conductor.pendingDeadline(timeout),
	)
}

func (conductor *Conductor) registerPendingEntry(
	registration PendingRegistration,
	wantAdd bool,
) error {
	if err := conductor.checkNotTerminating(); err != nil {
		return err
	}
	if isAddPendingKind(registration.Kind) && !wantAdd {
		return ErrAddPendingKindRequiresAddAPI
	}
	if !isAddPendingKind(registration.Kind) && wantAdd {
		return ErrNonAddPendingKindRequiresRegisterPending
	}
	return conductor.pending.Register(registration)
}

func (conductor *Conductor) RegisterPendingWithDeadline(
	kind PendingKind,
	correlationID int64,
	deadlineNs int64,
) error {
	return conductor.registerPendingEntry(PendingRegistration{
		CorrelationID: correlationID,
		Kind:          kind,
		DeadlineNs:    deadlineNs,
	}, false)
}

func (conductor *Conductor) RegisterPendingAdd(
	kind PendingKind,
	correlationID int64,
	streamID int32,
	channel string,
	timeout time.Duration,
) error {
	return conductor.RegisterPendingAddWithDeadline(
		kind,
		correlationID,
		streamID,
		channel,
		conductor.pendingDeadline(timeout),
	)
}

func (conductor *Conductor) RegisterPendingAddWithDeadline(
	kind PendingKind,
	correlationID int64,
	streamID int32,
	channel string,
	deadlineNs int64,
) error {
	return conductor.registerPendingEntry(PendingRegistration{
		CorrelationID: correlationID,
		Kind:          kind,
		StreamID:      streamID,
		Channel:       channel,
		DeadlineNs:    deadlineNs,
	}, true)
}

func (conductor *Conductor) PollPending(
	correlationID int64,
) (done bool, registration PendingRegistration, err error) {
	if conductor.receivedCorrelationID != correlationID {
		return false, PendingRegistration{}, nil
	}
	registrationPtr, ok := conductor.pending.Get(correlationID)
	if !ok {
		conductor.receivedCorrelationID = NullCorrelationID
		return true, PendingRegistration{}, ErrPendingNotFound
	}
	registration = *registrationPtr
	conductor.pending.Remove(correlationID)
	conductor.receivedCorrelationID = NullCorrelationID
	return true, registration, registration.Err
}

func (conductor *Conductor) CancelPending(correlationID int64) {
	conductor.pending.Remove(correlationID)
	if conductor.receivedCorrelationID == correlationID {
		conductor.receivedCorrelationID = NullCorrelationID
	}
}

func (conductor *Conductor) ClearPending() {
	conductor.pending.Clear()
	conductor.receivedCorrelationID = NullCorrelationID
	conductor.activeCorrelationID = NullCorrelationID
}

func (conductor *Conductor) SetActiveCorrelationID(correlationID int64) {
	conductor.activeCorrelationID = correlationID
}

func (conductor *Conductor) completePendingSuccess(
	registration *PendingRegistration,
	mutate func(*PendingRegistration),
) bool {
	if mutate != nil {
		mutate(registration)
	}
	registration.Err = nil
	conductor.receivedCorrelationID = registration.CorrelationID
	return true
}

func (conductor *Conductor) completePendingError(response ErrorResponse) bool {
	correlationID := response.OffendingCommandCorrelationID
	registration, ok := conductor.pending.Get(correlationID)
	if !ok {
		return false
	}
	registration.Err = &DriverRegistrationError{
		CorrelationID: correlationID,
		ErrorCode:     response.ErrorCode,
		Message:       response.ErrorMessage,
	}
	conductor.receivedCorrelationID = correlationID
	return true
}

func (conductor *Conductor) completePendingIfKind(
	correlationID int64,
	wantKind PendingKind,
	mutate func(*PendingRegistration),
) bool {
	registration, ok := conductor.pending.Get(correlationID)
	if !ok || registration.Kind != wantKind {
		return false
	}
	return conductor.completePendingSuccess(registration, mutate)
}

func isAddPendingKind(kind PendingKind) bool {
	return kind == PendingKindAddPublication || kind == PendingKindAddSubscription
}

// AddPublication/AddSubscription pendings return true on driver OPERATION_SUCCESS as an
// ack only; they do not set receivedCorrelationID; onPublicationReady/onSubscriptionReady
// complete them via completePendingIfKind. RemovePublication, RemoveSubscription, and
// OperationSuccess pendings complete here via completePendingSuccess. NextSessionID
// returns false; OnNextAvailableSessionID handles that kind.
func (conductor *Conductor) OnOperationSuccess(correlationID int64) bool {
	registration, ok := conductor.pending.Get(correlationID)
	if !ok {
		return false
	}
	switch {
	case isAddPendingKind(registration.Kind):
		return true
	case registration.Kind == PendingKindOperationSuccess,
		registration.Kind == PendingKindRemovePublication,
		registration.Kind == PendingKindRemoveSubscription:
		return conductor.completePendingSuccess(registration, nil)
	default:
		return false
	}
}

func (conductor *Conductor) OnError(response ErrorResponse) bool {
	return conductor.completePendingError(response)
}

func (conductor *Conductor) OnNextAvailableSessionID(
	correlationID int64,
	nextSessionID int32,
) bool {
	return conductor.completePendingIfKind(
		correlationID,
		PendingKindNextSessionID,
		func(registration *PendingRegistration) {
			registration.Int32Value = nextSessionID
		},
	)
}

func (conductor *Conductor) SetReceivedCorrelationIDForTest(correlationID int64) {
	conductor.receivedCorrelationID = correlationID
}

func (conductor *Conductor) CompletePendingForTest(
	correlationID int64,
	int32Value int32,
	err error,
) error {
	if err == nil {
		if !conductor.OnNextAvailableSessionID(correlationID, int32Value) {
			return ErrPendingNotFound
		}
		return nil
	}
	registration, ok := conductor.pending.Get(correlationID)
	if !ok {
		return ErrPendingNotFound
	}
	registration.Err = err
	conductor.receivedCorrelationID = correlationID
	return nil
}
