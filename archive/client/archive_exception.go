package client

import (
	"fmt"

	"github.com/warrenguy/heron/aeron/conductor"
)

const (
	// ArchiveErrorGeneric is the default archive error code.
	ArchiveErrorGeneric = 0

	// ArchiveErrorUnknownSubscription matches Java ArchiveException.UNKNOWN_SUBSCRIPTION.
	ArchiveErrorUnknownSubscription = 4

	// ArchiveErrorUnknownRecording matches Java ArchiveException.UNKNOWN_RECORDING.
	ArchiveErrorUnknownRecording = 5

	// ArchiveErrorAuthenticationRejected matches Java
	// ArchiveException.AUTHENTICATION_REJECTED.
	ArchiveErrorAuthenticationRejected = 10

	// NotConnectedMsg is returned by future session APIs when the control session is
	// not connected.
	NotConnectedMsg = "not connected"
)

// ArchiveException reports an archive control response error.
type ArchiveException struct {
	Message       string
	ErrorCode     int32
	CorrelationID int64
}

func (e *ArchiveException) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		if e.CorrelationID != conductor.NullCorrelationID {
			return fmt.Sprintf("errorCode=%d correlationId=%d", e.ErrorCode, e.CorrelationID)
		}
		return fmt.Sprintf("errorCode=%d", e.ErrorCode)
	}
	if e.CorrelationID != conductor.NullCorrelationID {
		return fmt.Sprintf(
			"%s (errorCode=%d correlationId=%d)",
			e.Message,
			e.ErrorCode,
			e.CorrelationID,
		)
	}
	return fmt.Sprintf("%s (errorCode=%d)", e.Message, e.ErrorCode)
}
