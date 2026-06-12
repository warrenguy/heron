package conductor

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/cnc"
)

var (
	ErrDriverShutdown          = errors.New("conductor: media driver has been shutdown")
	ErrClientTimeout           = errors.New("conductor: client timeout from driver")
	ErrConductorServiceTimeout = errors.New(
		"conductor: service interval exceeded",
	)
	ErrResourceLifecycleRequired = errors.New(
		"conductor: ResourceLifecycle is required when ImageLifecycle is set",
	)
	ErrNegativeCloseLingerNs = errors.New(
		"conductor: CloseLingerNs must not be negative",
	)
)

type HeartbeatCounterClosedError struct {
	CounterID int32
}

func (err *HeartbeatCounterClosedError) Error() string {
	return fmt.Sprintf(
		"conductor: unexpected close of heartbeat timestamp counter: %d",
		err.CounterID,
	)
}

type DriverKeepaliveStaleError struct {
	AgeMs     int64
	TimeoutMs int64
}

func (err *DriverKeepaliveStaleError) Error() string {
	return fmt.Sprintf(
		"conductor: media driver keepalive stale: age=%dms timeout=%dms",
		err.AgeMs,
		err.TimeoutMs,
	)
}

type EpochClock func() int64

type NanoClock func() int64

type Config struct {
	ClientID int64
	// DriverTimeout is compared against epoch-millisecond driver heartbeats;
	// sub-millisecond values truncate via Milliseconds().
	DriverTimeout       time.Duration
	KeepAliveInterval   time.Duration
	IdleSleepDuration   time.Duration
	InterServiceTimeout time.Duration
	EpochClock          EpochClock
	NanoClock           NanoClock
	// ErrorHandler receives conductor faults and recovered image handler panics;
	// nil swallows those errors.
	ErrorHandler func(error)
	// SubscriberErrorHandler is optional; nil swallows subscriber-side errors.
	SubscriberErrorHandler func(error)
	// ImageLifecycle is optional; nil drops image attach/detach events.
	ImageLifecycle ImageLifecycle
	// ResourceLifecycle is optional; nil skips handle construction on ready events.
	ResourceLifecycle ResourceLifecycle
	// MediaDriverDir confines log file paths from the driver; required when
	// ImageLifecycle is non-nil.
	MediaDriverDir string
	// CloseLingerNs schedules log unmap after publication or image close.
	CloseLingerNs int64
}

func ValidateConfig(config Config) error {
	if config.ImageLifecycle != nil && config.ResourceLifecycle == nil {
		return ErrResourceLifecycleRequired
	}
	if config.CloseLingerNs < 0 {
		return ErrNegativeCloseLingerNs
	}
	return nil
}

type Conductor struct {
	driverProxy           *DriverProxy
	eventsAdapter         *DriverEventsAdapter
	countersReader        *cnc.CountersReader
	config                Config
	driverTimeoutMs       int64
	idleSleepNs           int64
	interServiceTimeoutNs int64
	keepAliveIntervalNs   int64
	timeOfLastKeepAliveNs int64
	timeOfLastServiceNs   int64
	heartbeatCounterID    int32
	pending               pendingRegistry
	activeCorrelationID   int64
	receivedCorrelationID int64
	publicationErrors     []PublicationError
	// Resource maps; populated by ready handlers and subscription pre-insert.
	publications  map[int64]PublicationHandle
	subscriptions map[int64]SubscriptionHandle
	// logBuffersRegistry is the sole log-buffer map: dedup mmap, refCount, linger.
	logBuffersRegistry *LogBuffersRegistry
	resourceMu         sync.RWMutex
	// resourceWriteLockDepth is updated only under resourceMu write lock;
	// suspend/resume during image handlers lets foreign drain acquire write lock.
	resourceWriteLockDepth int32
	imageLifecycle         ImageLifecycle
	resourceLifecycle      ResourceLifecycle
	// cachedCorrelatedPending avoids a second pending.Get on the adapter ready path.
	cachedCorrelatedPending *PendingRegistration
	// inCallback is agent-only; suppresses nested image handler delivery.
	inCallback bool
	// lingerSweepBlocker is agent-only; optional test hook during linger sweep.
	// Only invoked via invokeLingerSweepBlockerForTest (no-op in !testhooks builds).
	lingerSweepBlocker func()
	terminationCause   error
	terminating        bool
}

func NewConductor(
	driverProxy *DriverProxy,
	broadcastReceiver *broadcast.BroadcastReceiver,
	countersReader *cnc.CountersReader,
	config Config,
) (*Conductor, error) {
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	return newConductor(driverProxy, broadcastReceiver, countersReader, config), nil
}

func newConductor(
	driverProxy *DriverProxy,
	broadcastReceiver *broadcast.BroadcastReceiver,
	countersReader *cnc.CountersReader,
	config Config,
) *Conductor {
	conductor := &Conductor{
		driverProxy:           driverProxy,
		countersReader:        countersReader,
		config:                config,
		driverTimeoutMs:       config.DriverTimeout.Milliseconds(),
		idleSleepNs:           config.IdleSleepDuration.Nanoseconds(),
		interServiceTimeoutNs: config.InterServiceTimeout.Nanoseconds(),
		keepAliveIntervalNs:   config.KeepAliveInterval.Nanoseconds(),
		heartbeatCounterID:    cnc.NullCounterID,
		pending:               newPendingRegistry(),
		publications:          make(map[int64]PublicationHandle),
		subscriptions:         make(map[int64]SubscriptionHandle),
		logBuffersRegistry:    NewLogBuffersRegistry(config.MediaDriverDir),
		imageLifecycle:        config.ImageLifecycle,
		resourceLifecycle:     config.ResourceLifecycle,
		activeCorrelationID:   NullCorrelationID,
		receivedCorrelationID: NullCorrelationID,
	}
	copyReceiver := NewCopyBroadcastReceiver(broadcastReceiver)
	conductor.eventsAdapter = NewDriverEventsAdapter(
		config.ClientID,
		copyReceiver,
		config.ErrorHandler,
		conductor.handleClientTimeout,
		DriverEventsAdapterHandlers{
			PublicationReady:  conductor.handlePublicationReady,
			SubscriptionReady: conductor.handleSubscriptionReady,
			AvailableImage:    conductor.handleAvailableImage,
			UnavailableImage:  conductor.handleUnavailableImage,
			PublicationError:  conductor.handlePublicationError,
			CorrelatedReady:   conductor.handleCorrelatedEvent,
		},
		&conductor.pending,
	)
	if config.NanoClock != nil {
		conductor.SeedServiceTimestamps(config.NanoClock())
	}
	return conductor
}

func (conductor *Conductor) DoWork() int {
	if conductor.terminating {
		return 0
	}
	return conductor.service()
}

func (conductor *Conductor) IsTerminating() bool {
	return conductor.terminating
}

func (conductor *Conductor) TerminationCause() error {
	return conductor.terminationCause
}

func (conductor *Conductor) SeedServiceTimestamps(nowNs int64) {
	conductor.timeOfLastServiceNs = nowNs
	conductor.timeOfLastKeepAliveNs = nowNs
}

func (conductor *Conductor) service() int {
	nowNs := conductor.config.NanoClock()
	workCount := conductor.checkTimeouts(nowNs)
	if conductor.terminating {
		return workCount
	}

	conductor.receivedCorrelationID = NullCorrelationID
	conductor.cachedCorrelatedPending = nil
	workCount += conductor.eventsAdapter.Receive(
		conductor.activeCorrelationID,
		conductor,
	)
	expireWorkCount, timedOutActiveCorrelationID := conductor.pending.ExpireBefore(
		nowNs,
		conductor.receivedCorrelationID,
		conductor.activeCorrelationID,
		conductor.handlePendingExpired,
	)
	workCount += expireWorkCount
	if timedOutActiveCorrelationID != NullCorrelationID {
		conductor.receivedCorrelationID = timedOutActiveCorrelationID
	}
	// Re-sample NanoClock so linger sweep sees time advanced during this tick.
	if conductor.logBuffersRegistry.HasScheduledLinger() {
		workCount += underResourceWriteLockResult(conductor, func() int {
			invokeLingerSweepBlockerForTest(conductor)
			return conductor.sweepLingeringLogBuffers(conductor.config.NanoClock())
		})
	}
	return workCount
}

func (conductor *Conductor) checkTimeouts(nowNs int64) int {
	if (conductor.timeOfLastServiceNs+conductor.idleSleepNs)-nowNs >= 0 {
		return 0
	}

	conductor.checkServiceInterval(nowNs)
	conductor.timeOfLastServiceNs = nowNs
	if conductor.terminating {
		return 1
	}

	return conductor.checkLiveness(nowNs)
}

func (conductor *Conductor) checkServiceInterval(nowNs int64) {
	if (conductor.timeOfLastServiceNs+conductor.interServiceTimeoutNs)-nowNs >= 0 {
		return
	}

	conductor.terminateWithError(ErrConductorServiceTimeout)
}

func (conductor *Conductor) checkLiveness(nowNs int64) int {
	if (conductor.timeOfLastKeepAliveNs+conductor.keepAliveIntervalNs)-nowNs >= 0 {
		return 0
	}

	nowMs := conductor.config.EpochClock()
	lastKeepaliveMs := conductor.driverProxy.LastDriverKeepaliveMs()

	if lastKeepaliveMs == DriverShutdownHeartbeat {
		conductor.terminateWithError(ErrDriverShutdown)
		return 1
	}

	// Unpublished driver heartbeat: wait for Connect/harness freshness gate; do not false-stale.
	if lastKeepaliveMs <= 0 {
		return 1
	}

	if nowMs > lastKeepaliveMs+conductor.driverTimeoutMs {
		conductor.terminateWithError(&DriverKeepaliveStaleError{
			AgeMs:     nowMs - lastKeepaliveMs,
			TimeoutMs: conductor.driverTimeoutMs,
		})
		return 1
	}

	heartbeatCounterUnset := conductor.heartbeatCounterID == cnc.NullCounterID
	if heartbeatCounterUnset {
		counterID, ok := conductor.countersReader.FindHeartbeatCounter(
			conductor.config.ClientID,
			cnc.ClientHeartbeatTypeID,
		)
		if !ok {
			return 1
		}
		conductor.heartbeatCounterID = counterID
	} else if !conductor.countersReader.IsCounterActive(
		conductor.heartbeatCounterID,
		conductor.config.ClientID,
		cnc.ClientHeartbeatTypeID,
	) {
		conductor.terminateWithError(&HeartbeatCounterClosedError{
			CounterID: conductor.heartbeatCounterID,
		})
		return 1
	}

	return conductor.storeClientHeartbeat(nowNs, nowMs)
}

func (conductor *Conductor) storeClientHeartbeat(nowNs, nowMs int64) int {
	// Reclaim may land between the active check above and this store; next tick terminates.
	if err := conductor.countersReader.SetCounterValue(
		conductor.heartbeatCounterID,
		nowMs,
	); err != nil {
		conductor.terminateWithError(err)
		return 1
	}
	conductor.timeOfLastKeepAliveNs = nowNs
	return 1
}

func (conductor *Conductor) handleClientTimeout() {
	if conductor.terminating {
		return
	}
	conductor.terminateWithError(ErrClientTimeout)
}

// Add kinds complete in handlePublicationReady/handleSubscriptionReady; do not set
// receivedCorrelationID here or PollPending would finish before metadata is filled.
func (conductor *Conductor) handleCorrelatedEvent(correlationID int64) {
	registration, ok := conductor.pending.Get(correlationID)
	if ok {
		conductor.cachedCorrelatedPending = registration
	} else {
		conductor.cachedCorrelatedPending = nil
	}
	if !ok || !addPendingKind(registration.Kind) {
		conductor.receivedCorrelationID = correlationID
	}
}

func (conductor *Conductor) pendingForReady(correlationID int64) (*PendingRegistration, bool) {
	cached := conductor.cachedCorrelatedPending
	if cached != nil && cached.CorrelationID == correlationID {
		conductor.cachedCorrelatedPending = nil
		return cached, true
	}
	return conductor.pending.Get(correlationID)
}

func (conductor *Conductor) addReadyRegistration(
	correlationID int64,
	wantKind PendingKind,
) (*PendingRegistration, bool) {
	registration, ok := conductor.pendingForReady(correlationID)
	if !ok || registration.Kind != wantKind {
		return nil, false
	}
	return registration, true
}

func (conductor *Conductor) handlePublicationReady(event PublicationBuffersReady) {
	conductor.completePublicationReady(event)
}

func (conductor *Conductor) handleSubscriptionReady(event SubscriptionReady) {
	conductor.completeSubscriptionReady(event)
}

func (conductor *Conductor) completePublicationReady(event PublicationBuffersReady) bool {
	registration, ok := conductor.addReadyRegistration(
		event.CorrelationID,
		PendingKindAddPublication,
	)
	if !ok {
		return false
	}
	registration.RegistrationID = event.RegistrationID

	var publication PublicationHandle
	if conductor.resourceLifecycle != nil {
		var buildErr error
		publication, buildErr = conductor.buildAndInsertPublication(event, registration.Channel)
		if buildErr != nil {
			conductor.failPending(registration, buildErr)
			return false
		}
	}

	return conductor.completePendingSuccess(registration, func(registration *PendingRegistration) {
		registration.RegistrationID = event.RegistrationID
		registration.StreamID = event.StreamID
		registration.Publication = publication
	})
}

func (conductor *Conductor) completeSubscriptionReady(event SubscriptionReady) bool {
	registration, ok := conductor.addReadyRegistration(
		event.CorrelationID,
		PendingKindAddSubscription,
	)
	if !ok {
		return false
	}

	var subscription SubscriptionHandle
	if conductor.resourceLifecycle != nil {
		var buildErr error
		subscription, buildErr = conductor.applySubscriptionReady(event, registration)
		if buildErr != nil {
			conductor.failPending(registration, buildErr)
			return false
		}
	}

	return conductor.completePendingSuccess(registration, func(registration *PendingRegistration) {
		registration.RegistrationID = event.CorrelationID
		registration.Subscription = subscription
	})
}

func (conductor *Conductor) buildAndInsertPublication(
	event PublicationBuffersReady,
	channel string,
) (PublicationHandle, error) {
	var publication PublicationHandle
	var buildErr error
	conductor.underResourceWriteLock(func() {
		resolvedLogFileKey, err := conductor.logBuffersRegistry.resolveLogFileKey(event.LogFileName)
		if err != nil {
			buildErr = err
			return
		}
		logBuffers, err := conductor.acquireLogBuffersByKey(resolvedLogFileKey)
		if err != nil {
			buildErr = err
			return
		}
		publication, buildErr = conductor.resourceLifecycle.NewPublication(
			event,
			logBuffers,
			conductor.countersReader,
			channel,
		)
		if buildErr != nil {
			conductor.releaseLogBuffersByKey(resolvedLogFileKey, 0)
			return
		}
		conductor.insertPublication(event.RegistrationID, publication)
		if bindErr := conductor.bindPublicationOnInsert(publication); bindErr != nil {
			buildErr = bindErr
			return
		}
	})
	return publication, buildErr
}

func (conductor *Conductor) applySubscriptionReady(
	event SubscriptionReady,
	registration *PendingRegistration,
) (SubscriptionHandle, error) {
	var subscription SubscriptionHandle
	var buildErr error
	conductor.underResourceWriteLock(func() {
		var readyErr error
		subscription, readyErr = conductor.subscriptionForReady(event.CorrelationID, registration)
		if readyErr != nil {
			buildErr = readyErr
			return
		}
		buildErr = conductor.resourceLifecycle.ApplySubscriptionReady(subscription, event)
	})
	return subscription, buildErr
}

func (conductor *Conductor) subscriptionForReady(
	correlationID int64,
	registration *PendingRegistration,
) (SubscriptionHandle, error) {
	if subscription, ok := conductor.subscriptionByRegID(correlationID); ok {
		return subscription, nil
	}
	return conductor.createAndInsertPreSubscription(
		correlationID,
		registration.StreamID,
		registration.Channel,
	)
}

func (conductor *Conductor) createAndInsertPreSubscription(
	correlationID int64,
	streamID int32,
	channel string,
) (SubscriptionHandle, error) {
	subscription := conductor.resourceLifecycle.NewPreSubscription(
		correlationID,
		streamID,
		channel,
	)
	conductor.insertSubscription(correlationID, subscription)
	if bindErr := conductor.bindSubscriptionOnInsert(subscription); bindErr != nil {
		return nil, bindErr
	}
	return subscription, nil
}

// preInsertSubscription inserts subscriptions[correlationID] before AddSubscription
// offer. RegisterPendingAdd invokes this immediately after pending registration;
// callers must offer synchronously so image events do not arrive in the gap.
func (conductor *Conductor) preInsertSubscription(
	correlationID int64,
	streamID int32,
	channel string,
) error {
	if conductor.resourceLifecycle == nil {
		return nil
	}
	var preInsertErr error
	conductor.underResourceWriteLock(func() {
		if _, ok := conductor.subscriptionByRegID(correlationID); ok {
			return
		}
		_, preInsertErr = conductor.createAndInsertPreSubscription(
			correlationID,
			streamID,
			channel,
		)
	})
	return preInsertErr
}

func (conductor *Conductor) RollbackAddPendingResources(registration PendingRegistration) {
	if conductor.resourceLifecycle == nil {
		return
	}
	if !addPendingKind(registration.Kind) {
		return
	}
	conductor.underResourceWriteLock(func() {
		switch registration.Kind {
		case PendingKindAddSubscription:
			sub, ok := conductor.subscriptionByRegID(registration.CorrelationID)
			if !ok {
				return
			}
			registryKeys, err := conductor.resourceLifecycle.RetireSubscription(sub)
			if err != nil {
				invokeErrorHandler(conductor.config.ErrorHandler, err)
				conductor.resourceLifecycle.MarkSubscriptionClosed(sub)
			}
			for _, registryKey := range registryKeys {
				conductor.releaseLogBuffersByKey(registryKey, 0)
			}
			conductor.removeSubscription(registration.CorrelationID)
		case PendingKindAddPublication:
			if registration.RegistrationID == 0 {
				return
			}
			pub, ok := conductor.publicationByRegID(registration.RegistrationID)
			if !ok {
				return
			}
			registryKey, err := conductor.resourceLifecycle.RetirePublication(pub)
			if err != nil {
				invokeErrorHandler(conductor.config.ErrorHandler, err)
				conductor.resourceLifecycle.MarkPublicationClosed(pub)
			}
			if registryKey != "" {
				conductor.releaseLogBuffersByKey(registryKey, 0)
			}
			conductor.removePublication(registration.RegistrationID)
		}
	})
}

func (conductor *Conductor) bindPublicationOnInsert(publication PublicationHandle) error {
	if isAbsentMapValue(publication) {
		return nil
	}
	return conductor.resourceLifecycle.BindPublication(publication, conductor)
}

func (conductor *Conductor) bindSubscriptionOnInsert(subscription SubscriptionHandle) error {
	if isAbsentMapValue(subscription) {
		return nil
	}
	return conductor.resourceLifecycle.BindSubscription(subscription, conductor)
}

func (conductor *Conductor) handlePendingExpired(registration PendingRegistration, _ bool) {
	conductor.RollbackAddPendingResources(registration)
}

func (conductor *Conductor) failPending(registration *PendingRegistration, err error) {
	conductor.RollbackAddPendingResources(*registration)
	registration.Err = err
	conductor.receivedCorrelationID = registration.CorrelationID
}

func (conductor *Conductor) handlePublicationError(event PublicationError) {
	conductor.publicationErrors = append(conductor.publicationErrors, event)
}

func (conductor *Conductor) terminateWithError(err error) {
	conductor.terminating = true
	conductor.terminationCause = err
	conductor.ClearPending()
	invokeErrorHandler(conductor.config.ErrorHandler, err)
}

func invokeErrorHandler(handler func(error), err error) {
	if handler != nil {
		handler(err)
	}
}
