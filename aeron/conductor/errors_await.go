package conductor

import (
	"errors"
	"fmt"
)

var (
	ErrPendingAlreadyRegistered = errors.New("conductor: correlation already pending")
	ErrPendingNotFound          = errors.New("conductor: no pending registration for correlation")
	// ErrAwaitTimeout is for conductor-side deadline enforcement once expirePending runs in service().
	ErrAwaitTimeout         = errors.New("conductor: await response timeout")
	ErrConductorTerminating = errors.New("conductor: conductor is terminating")
)

type DriverRegistrationError struct {
	CorrelationID int64
	ErrorCode     int32
	Message       string
}

func (err *DriverRegistrationError) Error() string {
	return fmt.Sprintf(
		"conductor: driver registration error: correlation=%d code=%d message=%s",
		err.CorrelationID,
		err.ErrorCode,
		err.Message,
	)
}
