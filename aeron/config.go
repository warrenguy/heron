package aeron

import (
	"fmt"
	"time"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
)

const (
	defaultDriverTimeout       = 10 * time.Second
	defaultKeepAliveInterval   = 500 * time.Millisecond
	defaultIdleSleepDuration   = 16 * time.Millisecond
	defaultCloseLingerDuration = 3 * time.Second
	defaultPollTimeout         = 10 * time.Second

	DefaultIdleSleepDuration = defaultIdleSleepDuration

	// DefaultPollTimeout is the fallback for Config.PollTimeout when it is zero.
	DefaultPollTimeout = defaultPollTimeout
)

type Config struct {
	AeronDir string

	DriverTimeout     time.Duration
	KeepAliveInterval time.Duration

	// InterServiceTimeout bounds the gap between DoWork calls. A longer gap
	// terminates the Client with conductor.ErrConductorServiceTimeout. When
	// the driver removed the Client during the gap, the cause is a
	// conductor.HeartbeatCounterClosedError instead.
	//
	// Zero takes the driver's client liveness timeout at Connect. Connect
	// rejects a value at or below KeepAliveInterval.
	InterServiceTimeout time.Duration

	IdleSleepDuration time.Duration

	// CloseLingerDuration schedules log unmap after publication or image close.
	//
	// Zero uses the default (3s), not immediate unmap.
	CloseLingerDuration time.Duration

	// PollTimeout bounds resource WaitUntil* and receive poll helpers, plus
	// [Client.RunDuringClientWork].
	//
	// A ctx deadline before the timeout bounds the wait instead. A helper
	// returns its typed timeout error while ctx is live. [PreferContextError]
	// returns ctx.Err() once ctx is done. The helpers may allocate. Keep them
	// off hot paths.
	//
	// Zero uses DefaultPollTimeout after Normalize.
	PollTimeout time.Duration

	// PublicationOfferTimeout bounds OfferWithRetry helpers. The PollTimeout
	// ctx and allocation rules apply.
	//
	// Zero uses PollTimeout after Normalize.
	PublicationOfferTimeout time.Duration

	// ErrorHandler receives each conductor error after mapConductorError.
	// Nil drops every report.
	//
	// A fatal error reaches the handler once, before the client terminates.
	// A broadcast lap reaches it as conductor.ErrBroadcastLapped and the
	// client keeps running with the driver events in the lapped range lost.
	ErrorHandler func(error)

	// AvailableImageHandler is copied to each Subscription on the Client
	// returned by Connect. Nil means no callback. See AvailableImageHandler for
	// invocation contract.
	AvailableImageHandler AvailableImageHandler

	// UnavailableImageHandler is copied to each Subscription on the Client
	// returned by Connect. Nil means no callback. See UnavailableImageHandler for
	// invocation contract.
	UnavailableImageHandler UnavailableImageHandler

	// PublicationErrorFrameHandler receives publication error frames for
	// publications on the Client returned by Connect. Nil means no callback.
	// See PublicationErrorFrameHandler for invocation contract.
	PublicationErrorFrameHandler PublicationErrorFrameHandler

	EpochClock conductor.EpochClock
	NanoClock  conductor.NanoClock

	UseConductorInvoker *bool
}

func (cfg Config) useConductorInvokerEnabled() bool {
	if cfg.UseConductorInvoker == nil {
		return true
	}
	return *cfg.UseConductorInvoker
}

func normalizeConfig(cfg Config) Config {
	if cfg.DriverTimeout == 0 {
		cfg.DriverTimeout = defaultDriverTimeout
	}
	if cfg.KeepAliveInterval == 0 {
		cfg.KeepAliveInterval = defaultKeepAliveInterval
	}
	if cfg.IdleSleepDuration == 0 {
		cfg.IdleSleepDuration = defaultIdleSleepDuration
	}
	if cfg.CloseLingerDuration == 0 {
		cfg.CloseLingerDuration = defaultCloseLingerDuration
	}
	if cfg.PollTimeout == 0 {
		cfg.PollTimeout = defaultPollTimeout
	}
	if cfg.PublicationOfferTimeout == 0 {
		cfg.PublicationOfferTimeout = cfg.PollTimeout
	}
	if cfg.EpochClock == nil {
		cfg.EpochClock = func() int64 {
			return time.Now().UnixMilli()
		}
	}
	if cfg.NanoClock == nil {
		cfg.NanoClock = func() int64 {
			return time.Now().UnixNano()
		}
	}
	return cfg
}

// resolveInterServiceTimeout returns InterServiceTimeout, or the driver's
// client liveness timeout when that field is zero. Rejects a driver value at
// or below KeepAliveInterval.
func resolveInterServiceTimeout(cfg Config, metadata cnc.Metadata) (time.Duration, error) {
	if cfg.InterServiceTimeout != 0 {
		return cfg.InterServiceTimeout, nil
	}
	timeout := time.Duration(metadata.ClientLivenessTimeoutNs)
	if timeout <= cfg.KeepAliveInterval {
		return 0, fmt.Errorf(
			"aeron: driver client liveness timeout %v must exceed KeepAliveInterval %v",
			timeout,
			cfg.KeepAliveInterval,
		)
	}
	return timeout, nil
}
