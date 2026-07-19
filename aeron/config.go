package aeron

import (
	"time"

	"github.com/warrenguy/heron/aeron/conductor"
)

const (
	defaultDriverTimeout       = 10 * time.Second
	defaultKeepAliveInterval   = 500 * time.Millisecond
	defaultIdleSleepDuration   = 16 * time.Millisecond
	defaultInterServiceTimeout = time.Second
	defaultCloseLingerDuration = 3 * time.Second
	defaultPollTimeout         = 10 * time.Second

	DefaultIdleSleepDuration = defaultIdleSleepDuration

	// DefaultPollTimeout is the fallback for Config.PollTimeout when it is zero.
	DefaultPollTimeout = defaultPollTimeout
)

type Config struct {
	AeronDir string

	DriverTimeout       time.Duration
	KeepAliveInterval   time.Duration
	InterServiceTimeout time.Duration
	IdleSleepDuration   time.Duration

	// CloseLingerDuration schedules log unmap after publication or image close.
	//
	// Zero uses the default (3s), not immediate unmap.
	CloseLingerDuration time.Duration

	// PollTimeout bounds resource WaitUntil* and receive poll helpers, plus
	// [Client.RunDuringClientWork].
	//
	// Zero uses DefaultPollTimeout after Normalize.
	PollTimeout time.Duration

	// PublicationOfferTimeout bounds OfferWithRetry helpers.
	//
	// Zero uses PollTimeout after Normalize.
	PublicationOfferTimeout time.Duration

	ErrorHandler func(error)

	// AvailableImageHandler is copied to each Subscription on the Client
	// returned by Connect. Nil means no callback. See AvailableImageHandler for
	// invocation contract.
	AvailableImageHandler AvailableImageHandler

	// UnavailableImageHandler is copied to each Subscription on the Client
	// returned by Connect. Nil means no callback. See UnavailableImageHandler for
	// invocation contract.
	UnavailableImageHandler UnavailableImageHandler

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
	if cfg.InterServiceTimeout == 0 {
		cfg.InterServiceTimeout = defaultInterServiceTimeout
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
