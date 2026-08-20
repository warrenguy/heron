package aeron

// Mapping layer from conductor errors to public aeron errors.
//
// mapPollPendingError and mapAwaitError handle generic await: ErrAwaitTimeout becomes
// AwaitTimeoutError; other errors pass through mapAwaitError unchanged.
//
// mapAddPollPendingError and mapAddAwaitError handle add registration flows:
// ErrAwaitTimeout becomes RegistrationTimeoutError; unknown errors wrap in RegistrationError.
import (
	"errors"
	"time"

	"github.com/warrenguy/heron/aeron/conductor"
)

// mapConductorError maps driver liveness failures to DriverTimeoutError.
//
// DriverRegistrationError and other conductor faults pass through unchanged.
// Add await maps registration errors via mapAddPollPendingError and
// mapAddAwaitError.
//
// ErrConductorServiceTimeout, ErrClientTimeout, and HeartbeatCounterClosedError
// pass through so callers can distinguish them.
func mapConductorError(err error) error {
	if err == nil {
		return nil
	}

	if _, ok := errors.AsType[*conductor.DriverKeepaliveStaleError](err); ok ||
		errors.Is(err, conductor.ErrDriverShutdown) {
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
) *RegistrationError {
	return &RegistrationError{
		CorrelationID: registrationErr.CorrelationID,
		ErrorCode:     registrationErr.ErrorCode,
		Message:       registrationErr.Message,
		Cause:         registrationErr,
	}
}

// mapAwaitError maps known conductor await errors; unknown errors pass through unchanged.
func mapAwaitError(err error) error {
	if err == nil {
		return nil
	}

	if registrationErr, ok := errors.AsType[*conductor.DriverRegistrationError](err); ok {
		return registrationErrorFromDriver(registrationErr)
	}
	if errors.Is(err, conductor.ErrPendingNotFound) {
		return ErrAwaitInterrupted
	}
	return err
}

// mapPollPendingError forks ErrAwaitTimeout to AwaitTimeoutError before mapAwaitError.
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
	return mapped
}

// mapAddAwaitError maps known conductor await errors; unknown errors wrap in RegistrationError.
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

// mapAddPollPendingError forks ErrAwaitTimeout to RegistrationTimeoutError before mapAddAwaitError.
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
