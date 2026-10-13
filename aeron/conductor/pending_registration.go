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
	PendingKindAddExclusivePublication
	PendingKindRemovePublication
	PendingKindRemoveSubscription
)

type PendingRegistration struct {
	CorrelationID int64
	Kind          PendingKind
	DeadlineNs    int64

	StreamID   int32
	Channel    string
	Int32Value int32
	// Ready is set when the driver ready event completes an add kind. Other
	// kinds never set it.
	Ready bool
	Err   error
	// Publication and Subscription are any to break the conductor/aeron import cycle.
	Publication  any
	Subscription any
}

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
	_, ok := registry.Get(correlationID)
	return ok
}

func (registry *pendingRegistry) IsEmpty() bool {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	return len(registry.entries) == 0
}

func (registry *pendingRegistry) ClearEach(fn func(PendingRegistration)) {
	registry.mu.Lock()
	snapshot := make([]PendingRegistration, 0, len(registry.entries))
	for _, registration := range registry.entries {
		snapshot = append(snapshot, *registration)
	}
	clear(registry.entries)
	registry.mu.Unlock()

	if fn == nil {
		return
	}
	for _, registration := range snapshot {
		fn(registration)
	}
}

type pendingExpireItem struct {
	registration PendingRegistration
	removed      bool
}

// ExpireBefore scans pending entries whose DeadlineNs has passed, excluding
// receivedCorrelationID (the response just received on this conductor tick).
//
// For activeCorrelationID or add-pending kinds (publication, exclusive
// publication, subscription), expired entries are retained: registration.Err is
// set to ErrAwaitTimeout only while the add is still in-flight (no handle yet
// and no prior error), removed is false, and the caller must complete or cancel
// via PollPending/CancelPending. Succeeded-but-unpolled adds are left intact.
// Non-add pending kinds are deleted from the registry; expireHandler receives
// removed=true for those.
func (registry *pendingRegistry) ExpireBefore(
	nowNs int64,
	receivedCorrelationID int64,
	activeCorrelationID int64,
	expireHandler func(registration PendingRegistration, removed bool),
) (workCount int, timedOutActiveCorrelationID int64) {
	registry.mu.Lock()
	var callbacks []pendingExpireItem
	timedOutActiveCorrelationID = NullCorrelationID
	for correlationID, registration := range registry.entries {
		if correlationID == receivedCorrelationID {
			continue
		}
		if nowNs < registration.DeadlineNs {
			continue
		}
		isActive := correlationID == activeCorrelationID
		if isActive {
			timedOutActiveCorrelationID = correlationID
		}
		isAddPending := addPendingKind(registration.Kind)
		if isActive || isAddPending {
			inFlight := registration.Err == nil && !registration.Ready
			if inFlight {
				registration.Err = ErrAwaitTimeout
				workCount++
				callbacks = append(callbacks, pendingExpireItem{
					registration: *registration,
					removed:      false,
				})
			}
			continue
		}
		workCount++
		delete(registry.entries, correlationID)
		callbacks = append(callbacks, pendingExpireItem{
			registration: *registration,
			removed:      true,
		})
	}
	registry.mu.Unlock()

	if expireHandler != nil {
		for _, item := range callbacks {
			expireHandler(item.registration, item.removed)
		}
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

func (conductor *Conductor) pendingDeadline(timeout time.Duration) int64 {
	return conductor.config.NanoClock() + timeout.Nanoseconds()
}

func (conductor *Conductor) checkNotTerminating() error {
	if conductor.terminating {
		return ErrConductorTerminating
	}
	return nil
}

// RegisterPending runs on the caller goroutine.
//
// Adapter handlers mark completion during DoWork when the client polls via
// doWorkLocked.
//
// PollPending runs on the same caller in awaitPendingPollTick after
// doWorkLocked, not inside DoWork. Client serialization: see aeron.Client
// godoc.
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
	isAddPending := addPendingKind(registration.Kind)
	if isAddPending && !wantAdd {
		return ErrAddPendingKindRequiresAddAPI
	}
	if !isAddPending && wantAdd {
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
	if err := conductor.registerPendingEntry(PendingRegistration{
		CorrelationID: correlationID,
		Kind:          kind,
		StreamID:      streamID,
		Channel:       channel,
		DeadlineNs:    deadlineNs,
	}, true); err != nil {
		return err
	}
	if kind == PendingKindAddSubscription {
		if err := conductor.preInsertSubscription(correlationID, streamID, channel); err != nil {
			conductor.CancelPending(correlationID)
			return err
		}
	}
	return nil
}

// PollPending returns a completed pending registration for correlationID.
//
// Primary path: receivedCorrelationID matches correlationID after adapter
// handlers mark completion during DoWork.
//
// Multi-pending path: when receivedCorrelationID does not match
// because another async add completed in the same DoWork tick, PollPending still
// checks the pending map. If the entry exists and either (a) Err is nil with the
// kind-appropriate handle populated (Publication, ExclusivePublication via
// Publication, or Subscription), or (b) Err is non-nil, PollPending returns
// done=true with the registration (including Err when set), removes the entry,
// and applies RollbackAddPendingResources when Err is non-nil. receivedCorrelationID
// is left unchanged when completion is observed via the map so other completed
// adds remain pollable on subsequent ticks.
//
// Called from the client on the same goroutine as RegisterPending, after
// doWorkLocked; not invoked from inside DoWork.
func (conductor *Conductor) PollPending(
	correlationID int64,
) (done bool, registration PendingRegistration, err error) {
	if conductor.receivedCorrelationID != correlationID {
		return conductor.pollPendingFromMap(correlationID)
	}
	return conductor.pollPendingMarked(correlationID)
}

func (conductor *Conductor) pollPendingFromMap(
	correlationID int64,
) (done bool, registration PendingRegistration, err error) {
	registrationPtr, ok := conductor.pending.Get(correlationID)
	if !ok || !pendingAddResolved(registrationPtr) {
		return false, PendingRegistration{}, nil
	}
	return conductor.completePendingPoll(*registrationPtr, false)
}

func (conductor *Conductor) pollPendingMarked(
	correlationID int64,
) (done bool, registration PendingRegistration, err error) {
	registrationPtr, ok := conductor.pending.Get(correlationID)
	if !ok {
		conductor.receivedCorrelationID = NullCorrelationID
		return true, PendingRegistration{}, ErrPendingNotFound
	}
	return conductor.completePendingPoll(*registrationPtr, true)
}

func (conductor *Conductor) completePendingPoll(
	registration PendingRegistration,
	clearMark bool,
) (bool, PendingRegistration, error) {
	conductor.pending.Remove(registration.CorrelationID)
	if registration.Err != nil {
		conductor.RollbackAddPendingResources(registration)
	}
	if clearMark {
		conductor.receivedCorrelationID = NullCorrelationID
	}
	return true, registration, registration.Err
}

func pendingAddResolved(registration *PendingRegistration) bool {
	return registration.Err != nil || registration.Ready
}

func (conductor *Conductor) CancelPending(correlationID int64) {
	registration, ok := conductor.pending.Get(correlationID)
	if ok {
		readyMarked := conductor.receivedCorrelationID == correlationID &&
			registration.Err == nil
		if !readyMarked {
			conductor.RollbackAddPendingResources(*registration)
		}
	}
	conductor.pending.Remove(correlationID)
	if conductor.receivedCorrelationID == correlationID {
		conductor.receivedCorrelationID = NullCorrelationID
	}
}

func (conductor *Conductor) ClearPending() {
	conductor.clearPendingEntries()
	conductor.activeCorrelationID = NullCorrelationID
}

// ClearPendingEntriesForClose rolls back in-flight add pendings during client
// teardown. Does not clear activeCorrelationID so unsupported concurrent readers
// of ActiveCorrelationID during Close do not race with teardown.
func (conductor *Conductor) ClearPendingEntriesForClose() {
	conductor.clearPendingEntries()
}

// clearPendingEntries rolls back and removes all pending registrations and clears
// receivedCorrelationID. Does not clear activeCorrelationID.
func (conductor *Conductor) clearPendingEntries() {
	conductor.pending.ClearEach(func(registration PendingRegistration) {
		conductor.RollbackAddPendingResources(registration)
	})
	conductor.receivedCorrelationID = NullCorrelationID
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
	conductor.failPending(registration, &DriverRegistrationError{
		CorrelationID: correlationID,
		ErrorCode:     response.ErrorCode,
		Message:       response.ErrorMessage,
	})
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

func addPublicationPendingKind(kind PendingKind) bool {
	return kind == PendingKindAddPublication ||
		kind == PendingKindAddExclusivePublication
}

func addPendingKind(kind PendingKind) bool {
	return addPublicationPendingKind(kind) ||
		kind == PendingKindAddSubscription
}

// OperationSucceeded completes remove and generic success pendings. Add kinds only
// ack here; handlePublicationReady, handleExclusivePublicationReady, and
// handleSubscriptionReady finish them. NextSessionID is completed by
// Conductor.NextSessionID.
func (conductor *Conductor) OperationSucceeded(correlationID int64) bool {
	registration, ok := conductor.pending.Get(correlationID)
	if !ok {
		return false
	}
	switch {
	case addPendingKind(registration.Kind):
		return true
	case registration.Kind == PendingKindOperationSuccess,
		registration.Kind == PendingKindRemovePublication,
		registration.Kind == PendingKindRemoveSubscription:
		return conductor.completePendingSuccess(registration, nil)
	default:
		return false
	}
}

func (conductor *Conductor) DriverError(response ErrorResponse) bool {
	return conductor.completePendingError(response)
}

func (conductor *Conductor) NextSessionID(
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
