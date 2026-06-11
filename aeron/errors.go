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

// RegistrationError reports add/register failures.
//
// Driver ON_ERROR uses ErrorCode and Message (stable awaitResponse string).
//
// Ring backpressure and ctor failures use Kind and Cause.
//
// Kind and Cause are also set on driver errors for errors.Is/Unwrap while Error()
// keeps the code/message form.
type RegistrationError struct {
	Kind          string
	CorrelationID int64
	ErrorCode     int32
	Message       string
	Cause         error
}

func (err *RegistrationError) Error() string {
	if err.Message != "" || err.ErrorCode != 0 {
		return fmt.Sprintf(
			"aeron: driver registration error: correlation=%d code=%d message=%s",
			err.CorrelationID,
			err.ErrorCode,
			err.Message,
		)
	}
	if err.Kind != "" && err.Cause != nil {
		return fmt.Sprintf(
			"aeron: %s registration error: correlation=%d: %v",
			err.Kind,
			err.CorrelationID,
			err.Cause,
		)
	}
	if err.Kind != "" {
		return fmt.Sprintf(
			"aeron: %s registration error: correlation=%d",
			err.Kind,
			err.CorrelationID,
		)
	}
	return fmt.Sprintf("aeron: registration error: correlation=%d", err.CorrelationID)
}

func (err *RegistrationError) Unwrap() error {
	return err.Cause
}

type RegistrationTimeoutError struct {
	Kind          string
	CorrelationID int64
	StreamID      int32
	Channel       string
}

func (err *RegistrationTimeoutError) Error() string {
	return fmt.Sprintf(
		"aeron: %s registration timeout: correlation=%d stream=%d channel=%s",
		err.Kind,
		err.CorrelationID,
		err.StreamID,
		err.Channel,
	)
}

func newRegistrationTimeoutError(
	registrationKind string,
	correlationID int64,
	streamID int32,
	channel string,
) *RegistrationTimeoutError {
	return &RegistrationTimeoutError{
		Kind:          registrationKind,
		CorrelationID: correlationID,
		StreamID:      streamID,
		Channel:       channel,
	}
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

func newAwaitTimeoutError(correlationID int64, timeout time.Duration) *AwaitTimeoutError {
	return &AwaitTimeoutError{
		CorrelationID: correlationID,
		Timeout:       timeout,
	}
}

// mapConductorError maps driver liveness failures to DriverTimeoutError.
//
// DriverRegistrationError and other conductor faults pass through unchanged;
// add await maps registration errors via mapAddPollPendingError / mapAddAwaitError.
//
// ErrConductorServiceTimeout, ErrClientTimeout, and HeartbeatCounterClosedError
// also pass through so callers can distinguish them.
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

func registrationErrorFromDriver(
	registrationErr *conductor.DriverRegistrationError,
) RegistrationError {
	return RegistrationError{
		CorrelationID: registrationErr.CorrelationID,
		ErrorCode:     registrationErr.ErrorCode,
		Message:       registrationErr.Message,
		Cause:         registrationErr,
	}
}

func mapAwaitError(err error) error {
	if err == nil {
		return nil
	}

	if registrationErr, ok := errors.AsType[*conductor.DriverRegistrationError](err); ok {
		mapped := registrationErrorFromDriver(registrationErr)
		return &mapped
	}
	if errors.Is(err, conductor.ErrPendingNotFound) {
		return ErrAwaitInterrupted
	}
	return err
}

func mapPollPendingError(
	correlationID int64,
	timeout time.Duration,
	err error,
) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, conductor.ErrAwaitTimeout) {
		return newAwaitTimeoutError(correlationID, timeout)
	}
	return mapAwaitError(err)
}

func mapAddOfferError(registrationKind string, correlationID int64, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, conductor.ErrRingInsufficientCapacity) {
		return &RegistrationError{
			Kind:          registrationKind,
			CorrelationID: correlationID,
			Cause:         err,
		}
	}
	return err
}

func mapAddDriverRegistrationError(
	registrationKind string,
	registrationErr *conductor.DriverRegistrationError,
) *RegistrationError {
	mapped := registrationErrorFromDriver(registrationErr)
	mapped.Kind = registrationKind
	return &mapped
}

func mapAddAwaitError(registrationKind string, correlationID int64, err error) error {
	if err == nil {
		return nil
	}
	if registrationErr, ok := errors.AsType[*conductor.DriverRegistrationError](err); ok {
		return mapAddDriverRegistrationError(registrationKind, registrationErr)
	}
	if errors.Is(err, conductor.ErrPendingNotFound) {
		return ErrAwaitInterrupted
	}
	return &RegistrationError{
		Kind:          registrationKind,
		CorrelationID: correlationID,
		Cause:         err,
	}
}

func mapAddPollPendingError(
	registrationKind string,
	correlationID int64,
	streamID int32,
	channel string,
	err error,
) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, conductor.ErrAwaitTimeout) {
		return newRegistrationTimeoutError(registrationKind, correlationID, streamID, channel)
	}
	return mapAddAwaitError(registrationKind, correlationID, err)
}
