package aeron

import (
	"errors"
	"fmt"
	"time"

	"github.com/warrenguy/heron/aeron/conductor"
)

var (
	ErrClientClosed             = errors.New("aeron: client is closed")
	ErrClientNotReady           = errors.New("aeron: client is not ready")
	ErrConductorTerminating     = errors.New("aeron: conductor is terminating")
	ErrPendingAlreadyRegistered = errors.New("aeron: correlation already pending")
	ErrDriverHeartbeatNotReady  = errors.New(
		"aeron: driver consumer heartbeat not ready",
	)
	ErrDriverHeartbeatStaleAtConnect = errors.New(
		"aeron: driver consumer heartbeat stale at connect",
	)
	ErrAwaitInterrupted = errors.New("aeron: await response interrupted")
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

type RegistrationError struct {
	CorrelationID int64
	ErrorCode     int32
	Message       string
}

func (err *RegistrationError) Error() string {
	return fmt.Sprintf(
		"aeron: driver registration error: correlation=%d code=%d message=%s",
		err.CorrelationID,
		err.ErrorCode,
		err.Message,
	)
}

type AwaitTimeoutError struct {
	CorrelationID int64
	Timeout       time.Duration
}

func (err *AwaitTimeoutError) Error() string {
	return fmt.Sprintf(
		"aeron: await response timeout: correlation=%d timeout=%s",
		err.CorrelationID,
		err.Timeout,
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

func mapRegistrationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, conductor.ErrConductorTerminating) {
		return ErrConductorTerminating
	}
	if errors.Is(err, conductor.ErrPendingAlreadyRegistered) {
		return ErrPendingAlreadyRegistered
	}
	return err
}

func mapAwaitError(err error) error {
	if err == nil {
		return nil
	}

	if registrationErr, ok := errors.AsType[*conductor.DriverRegistrationError](err); ok {
		return &RegistrationError{
			CorrelationID: registrationErr.CorrelationID,
			ErrorCode:     registrationErr.ErrorCode,
			Message:       registrationErr.Message,
		}
	}
	if errors.Is(err, conductor.ErrPendingNotFound) {
		return ErrAwaitInterrupted
	}
	return err
}
