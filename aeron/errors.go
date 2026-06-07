package aeron

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/conductor"
)

var (
	ErrDriverHeartbeatNotReady = errors.New(
		"aeron: driver consumer heartbeat not ready",
	)
	ErrDriverHeartbeatStaleAtConnect = errors.New(
		"aeron: driver consumer heartbeat stale at connect",
	)
)

type DriverTimeoutError struct {
	Cause error
}

func (err *DriverTimeoutError) Error() string {
	return fmt.Sprintf("aeron: media driver timeout: %v", err.Cause)
}

func (err *DriverTimeoutError) Unwrap() error {
	return err.Cause
}

type ControlProtocolVersionError struct {
	Got  int64
	Want int64
}

func (err *ControlProtocolVersionError) Error() string {
	return fmt.Sprintf(
		"aeron: control protocol version mismatch: got %d want %d",
		err.Got,
		err.Want,
	)
}

// mapConductorError maps driver liveness failures to DriverTimeoutError.
// Other conductor termination errors (ErrConductorServiceTimeout, ErrClientTimeout,
// HeartbeatCounterClosedError) pass through unchanged so callers can distinguish them.
func mapConductorError(err error) error {
	if err == nil {
		return nil
	}

	var staleErr *conductor.DriverKeepaliveStaleError
	if errors.As(err, &staleErr) || errors.Is(err, conductor.ErrDriverShutdown) {
		return &DriverTimeoutError{Cause: err}
	}
	return err
}
