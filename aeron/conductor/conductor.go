package conductor

import (
	"errors"
	"fmt"
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
	// ErrorHandler receives conductor faults and recovered image callback panics;
	// nil swallows those errors.
	ErrorHandler func(error)
	// SubscriberErrorHandler is optional; nil swallows subscriber-side errors.
	SubscriberErrorHandler func(error)
	// ImageLifecycle is optional; nil drops image attach/detach events.
	ImageLifecycle ImageLifecycle
	// MediaDriverDir confines log file paths from the driver; required when
	// ImageLifecycle is non-nil.
	MediaDriverDir string
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
	// Staging only; publication/subscription handles still use last-ready pointers
	// and an append-only error slice.
	lastPublicationReady  *PublicationBuffersReady
	lastSubscriptionReady *SubscriptionReady
	publicationErrors     []PublicationError
	// Image lifecycle state. subscriptions is populated by the Aeron client after
	// subscription ready (not wired yet); tests use RegisterSubscriptionForTest.
	subscriptions      map[int64]SubscriptionHandle
	logBuffersRegistry *LogBuffersRegistry
	imageLifecycle     ImageLifecycle
	isInCallback       bool
	terminationCause   error
	isTerminating      bool
}

func NewConductor(
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
		subscriptions:         make(map[int64]SubscriptionHandle),
		logBuffersRegistry:    NewLogBuffersRegistry(config.MediaDriverDir),
		imageLifecycle:        config.ImageLifecycle,
		activeCorrelationID:   NullCorrelationID,
		receivedCorrelationID: NullCorrelationID,
	}
	copyReceiver := NewCopyBroadcastReceiver(broadcastReceiver)
	conductor.eventsAdapter = NewDriverEventsAdapter(
		config.ClientID,
		copyReceiver,
		config.ErrorHandler,
		conductor.onClientTimeout,
		DriverEventsAdapterHandlers{
			OnPublicationReady:  conductor.onPublicationReady,
			OnSubscriptionReady: conductor.onSubscriptionReady,
			OnAvailableImage:    conductor.onAvailableImage,
			OnUnavailableImage:  conductor.onUnavailableImage,
			OnPublicationError:  conductor.onPublicationError,
			OnCorrelatedReady:   conductor.onCorrelatedEvent,
		},
		&conductor.pending,
	)
	if config.NanoClock != nil {
		conductor.SeedServiceTimestamps(config.NanoClock())
	}
	return conductor
}

func (conductor *Conductor) DoWork() int {
	if conductor.isTerminating {
		return 0
	}
	return conductor.service()
}

func (conductor *Conductor) IsTerminating() bool {
	return conductor.isTerminating
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
	if conductor.isTerminating {
		return workCount
	}

	conductor.receivedCorrelationID = NullCorrelationID
	workCount += conductor.eventsAdapter.Receive(
		conductor.activeCorrelationID,
		conductor,
	)
	expireWorkCount, timedOutActiveCorrelationID := conductor.pending.ExpireBefore(
		nowNs,
		conductor.receivedCorrelationID,
		conductor.activeCorrelationID,
	)
	workCount += expireWorkCount
	if timedOutActiveCorrelationID != NullCorrelationID {
		conductor.receivedCorrelationID = timedOutActiveCorrelationID
	}
	return workCount
}

func (conductor *Conductor) checkTimeouts(nowNs int64) int {
	if (conductor.timeOfLastServiceNs+conductor.idleSleepNs)-nowNs >= 0 {
		return 0
	}

	conductor.checkServiceInterval(nowNs)
	conductor.timeOfLastServiceNs = nowNs
	if conductor.isTerminating {
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
	lastKeepaliveMs := conductor.driverProxy.TimeOfLastDriverKeepaliveMs()

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

func (conductor *Conductor) onClientTimeout() {
	if conductor.isTerminating {
		return
	}
	conductor.terminateWithError(ErrClientTimeout)
}

// Add kinds complete in onPublicationReady/onSubscriptionReady; do not set
// receivedCorrelationID here or PollPending would finish before metadata is filled.
func (conductor *Conductor) onCorrelatedEvent(correlationID int64) {
	registration, ok := conductor.pending.Get(correlationID)
	if !ok || !isAddPendingKind(registration.Kind) {
		conductor.receivedCorrelationID = correlationID
	}
}

func (conductor *Conductor) onPublicationReady(event PublicationBuffersReady) {
	eventCopy := event
	conductor.lastPublicationReady = &eventCopy
	conductor.completePendingIfKind(
		event.CorrelationID,
		PendingKindAddPublication,
		func(registration *PendingRegistration) {
			registration.RegistrationID = event.RegistrationID
			registration.StreamID = event.StreamID
		},
	)
}

func (conductor *Conductor) onSubscriptionReady(event SubscriptionReady) {
	eventCopy := event
	conductor.lastSubscriptionReady = &eventCopy
	conductor.completePendingIfKind(
		event.CorrelationID,
		PendingKindAddSubscription,
		func(registration *PendingRegistration) {
			registration.RegistrationID = event.CorrelationID
		},
	)
}

func (conductor *Conductor) onPublicationError(event PublicationError) {
	conductor.publicationErrors = append(conductor.publicationErrors, event)
}

func (conductor *Conductor) terminateWithError(err error) {
	conductor.isTerminating = true
	conductor.terminationCause = err
	conductor.ClearPending()
	invokeErrorHandler(conductor.config.ErrorHandler, err)
}

func invokeErrorHandler(handler func(error), err error) {
	if handler != nil {
		handler(err)
	}
}
