package conductor

import "time"

type PendingKind int

const (
	PendingKindOperationSuccess PendingKind = iota
	PendingKindNextSessionID
)

type PendingRegistration struct {
	CorrelationID int64
	Kind          PendingKind
	DeadlineNs    int64

	Int32Value int32
	Err        error
}

type pendingRegistry struct {
	entries map[int64]*PendingRegistration
}

func newPendingRegistry() pendingRegistry {
	return pendingRegistry{entries: make(map[int64]*PendingRegistration)}
}

func (registry *pendingRegistry) Register(registration PendingRegistration) error {
	if _, ok := registry.entries[registration.CorrelationID]; ok {
		return ErrPendingAlreadyRegistered
	}
	regCopy := registration
	registry.entries[registration.CorrelationID] = &regCopy
	return nil
}

func (registry *pendingRegistry) Get(correlationID int64) (*PendingRegistration, bool) {
	registration, ok := registry.entries[correlationID]
	return registration, ok
}

func (registry *pendingRegistry) Remove(correlationID int64) {
	delete(registry.entries, correlationID)
}

func (registry *pendingRegistry) IsEmpty() bool {
	return len(registry.entries) == 0
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

func (conductor *Conductor) RegisterPending(
	kind PendingKind,
	correlationID int64,
	timeout time.Duration,
) error {
	if conductor.isTerminating {
		return ErrConductorTerminating
	}
	deadlineNs := conductor.config.NanoClock() + timeout.Nanoseconds()
	return conductor.pending.Register(PendingRegistration{
		CorrelationID: correlationID,
		Kind:          kind,
		DeadlineNs:    deadlineNs,
	})
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
	clear(conductor.pending.entries)
	conductor.receivedCorrelationID = NullCorrelationID
	conductor.activeCorrelationID = NullCorrelationID
}

func (conductor *Conductor) SetActiveCorrelationID(correlationID int64) {
	conductor.activeCorrelationID = correlationID
}

func (conductor *Conductor) completeNextSessionID(
	correlationID int64,
	nextSessionID int32,
) bool {
	registration, ok := conductor.pending.Get(correlationID)
	if !ok {
		return false
	}
	if registration.Kind != PendingKindNextSessionID {
		return false
	}
	registration.Int32Value = nextSessionID
	conductor.receivedCorrelationID = correlationID
	return true
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
		if !conductor.completeNextSessionID(correlationID, int32Value) {
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
