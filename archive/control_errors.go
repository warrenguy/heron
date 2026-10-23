package archive

import (
	"errors"
	"fmt"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/internal/transportfault"
)

// ErrTransportTerminating reports that the connect-owned transport client has
// terminated. Client.TransportFault and AsyncConnect.Poll wrap it with the
// conductor cause.
var ErrTransportTerminating = errors.New("archive: transport client is terminating")

// transportTerminatingError wraps the monitor termination cause for client
// with ErrTransportTerminating. Returns nil when the monitor reports none.
func transportTerminatingError(monitor *transportfault.Monitor, client *aeron.Client) error {
	cause := monitor.TerminationCause(client)
	if cause == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrTransportTerminating, cause)
}

// ControlTerminalError reports a non-retriable control offer result from a retry loop.
type ControlTerminalError struct {
	Status int64
}

func (err *ControlTerminalError) Error() string {
	return fmt.Sprintf(
		"archive: control offer failed: status=%s",
		aeron.PositionCodeName(err.Status),
	)
}

// ControlOfferTimeoutError reports control offer retry deadline expiry after retriable codes.
type ControlOfferTimeoutError struct {
	Status  int64
	Timeout time.Duration
}

func (err *ControlOfferTimeoutError) Error() string {
	return fmt.Sprintf(
		"archive: control offer timed out within %s: status=%s",
		err.Timeout,
		aeron.PositionCodeName(err.Status),
	)
}

// ControlOfferAttemptsError reports a control offer retry that reached
// Config.MessageRetryAttempts. Status holds the last retriable code.
// Attempts counts the offers made.
type ControlOfferAttemptsError struct {
	Status   int64
	Attempts int
}

func (err *ControlOfferAttemptsError) Error() string {
	return fmt.Sprintf(
		"archive: control offer failed after %d attempts: status=%s",
		err.Attempts,
		aeron.PositionCodeName(err.Status),
	)
}

// ControlResponseTimeoutError reports response/descriptor await deadline expiry.
type ControlResponseTimeoutError struct {
	Operation     string
	CorrelationID int64
	Timeout       time.Duration
}

func (err *ControlResponseTimeoutError) Error() string {
	return fmt.Sprintf(
		"archive: %s timeout within %s: correlation_id=%d",
		err.Operation,
		err.Timeout,
		err.CorrelationID,
	)
}

// Operation labels for ControlResponseTimeoutError.Operation.
const (
	ControlResponseAwaitOperation         = "control response"
	ListRecordingDescriptorAwaitOperation = "list recording descriptor"
	StartRecordingAwaitOperation          = "start recording"
	StopRecordingAwaitOperation           = "stop recording"
	ExtendRecordingAwaitOperation         = "extend recording"
)
