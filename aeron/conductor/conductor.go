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
	ErrorHandler        func(error)
}

type Conductor struct {
	driverProxy           *DriverProxy
	eventsAdapter         *DriverEventsAdapter
	countersReader        *cnc.CountersReader
	config                Config
	driverTimeoutMs       int64
	timeOfLastKeepAliveNs int64
	timeOfLastServiceNs   int64
	heartbeatCounterID    int32
	isTerminating         bool
}

func NewConductor(
	driverProxy *DriverProxy,
	broadcastReceiver *broadcast.BroadcastReceiver,
	countersReader *cnc.CountersReader,
	config Config,
) *Conductor {
	conductor := &Conductor{
		driverProxy:        driverProxy,
		countersReader:     countersReader,
		config:             config,
		driverTimeoutMs:    config.DriverTimeout.Milliseconds(),
		heartbeatCounterID: cnc.NullCounterID,
	}
	copyReceiver := NewCopyBroadcastReceiver(broadcastReceiver)
	conductor.eventsAdapter = NewDriverEventsAdapter(
		config.ClientID,
		copyReceiver,
		config.ErrorHandler,
		conductor.onClientTimeout,
	)
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

func (conductor *Conductor) service() int {
	workCount := conductor.checkTimeouts(conductor.config.NanoClock())
	if conductor.isTerminating {
		return workCount
	}
	workCount += conductor.eventsAdapter.Receive(NullCorrelationID)
	return workCount
}

func (conductor *Conductor) checkTimeouts(nowNs int64) int {
	if (conductor.timeOfLastServiceNs+conductor.config.IdleSleepDuration.Nanoseconds())-nowNs >= 0 {
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
	if (conductor.timeOfLastServiceNs+conductor.config.InterServiceTimeout.Nanoseconds())-nowNs >= 0 {
		return
	}

	conductor.terminateWithError(ErrConductorServiceTimeout)
}

func (conductor *Conductor) checkLiveness(nowNs int64) int {
	if (conductor.timeOfLastKeepAliveNs+conductor.config.KeepAliveInterval.Nanoseconds())-nowNs >= 0 {
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

	// Reclaim may land between the active check above and this store; next tick terminates.
	_ = conductor.countersReader.SetCounterValue(conductor.heartbeatCounterID, nowMs)
	conductor.timeOfLastKeepAliveNs = nowNs
	return 1
}

func (conductor *Conductor) onClientTimeout() {
	if conductor.isTerminating {
		return
	}
	conductor.terminateWithError(ErrClientTimeout)
}

func (conductor *Conductor) terminateWithError(err error) {
	conductor.isTerminating = true
	invokeErrorHandler(conductor.config.ErrorHandler, err)
}

func invokeErrorHandler(handler func(error), err error) {
	if handler != nil {
		handler(err)
	}
}
