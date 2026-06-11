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

	DefaultIdleSleepDuration = defaultIdleSleepDuration
)

type Context struct {
	AeronDir string

	DriverTimeout       time.Duration
	KeepAliveInterval   time.Duration
	InterServiceTimeout time.Duration
	IdleSleepDuration   time.Duration
	// CloseLingerDuration schedules log unmap after publication or image close.
	//
	// Zero uses the default (3s), not immediate unmap.
	CloseLingerDuration time.Duration

	ErrorHandler func(error)

	EpochClock conductor.EpochClock
	NanoClock  conductor.NanoClock

	UseConductorInvoker *bool
}

func (ctx Context) useConductorInvokerEnabled() bool {
	if ctx.UseConductorInvoker == nil {
		return true
	}
	return *ctx.UseConductorInvoker
}

func normalizeContext(ctx Context) Context {
	if ctx.DriverTimeout == 0 {
		ctx.DriverTimeout = defaultDriverTimeout
	}
	if ctx.KeepAliveInterval == 0 {
		ctx.KeepAliveInterval = defaultKeepAliveInterval
	}
	if ctx.InterServiceTimeout == 0 {
		ctx.InterServiceTimeout = defaultInterServiceTimeout
	}
	if ctx.IdleSleepDuration == 0 {
		ctx.IdleSleepDuration = defaultIdleSleepDuration
	}
	if ctx.CloseLingerDuration == 0 {
		ctx.CloseLingerDuration = defaultCloseLingerDuration
	}
	if ctx.EpochClock == nil {
		ctx.EpochClock = func() int64 {
			return time.Now().UnixMilli()
		}
	}
	if ctx.NanoClock == nil {
		ctx.NanoClock = func() int64 {
			return time.Now().UnixNano()
		}
	}
	return ctx
}
