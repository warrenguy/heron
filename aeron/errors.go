package aeron

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidWant reports a non-positive want argument to a poll helper.
	ErrInvalidWant = errors.New("aeron: invalid want")

	// ErrClientNotBound reports a cold-path helper on an unbound resource.
	//
	// Typical callers: fixtures, in-mem benches.
	ErrClientNotBound = errors.New("aeron: client not bound")

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

// Driver ON_ERROR error codes. Values match Java io.aeron.ErrorCode.
const (
	// ErrorCodeResourceTemporarilyUnavailable matches Java
	// ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE (value 10).
	//
	// The media driver returns this code when a registration resource is still
	// closing, such as a send_channel_endpoint in CLOSING state. Callers may
	// retry the registration with a fresh correlation after a short pause.
	ErrorCodeResourceTemporarilyUnavailable int32 = 10
)

// IsRetriableRegistration reports whether err is a driver registration failure
// that should be retried after a short pause.
//
// Returns true only for a [*RegistrationError] with
// [ErrorCodeResourceTemporarilyUnavailable], including when that error is
// wrapped. Returns false for every other error.
func IsRetriableRegistration(err error) bool {
	var reg *RegistrationError
	if !errors.As(err, &reg) {
		return false
	}
	return reg.ErrorCode == ErrorCodeResourceTemporarilyUnavailable
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
