package client

import (
	"fmt"
	"time"

	"github.com/warrenguy/heron/aeron"
)

// ControlTerminalError reports a non-retriable control offer result from a retry loop.
type ControlTerminalError struct {
	Status int64
}

func (err *ControlTerminalError) Error() string {
	return fmt.Sprintf(
		"archive client: control offer failed: status=%s",
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
		"archive client: control offer timed out within %s: status=%s",
		err.Timeout,
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
		"archive client: %s timeout within %s: correlation_id=%d",
		err.Operation,
		err.Timeout,
		err.CorrelationID,
	)
}

// Operation labels for ControlResponseTimeoutError.Operation.
const (
	ControlResponseAwaitOperation         = "control response"
	ListRecordingDescriptorAwaitOperation = "list recording descriptor"
	StopRecordingAwaitOperation           = "stop recording"
)
