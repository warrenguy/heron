package client

import (
	"errors"
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
)

// NotConnectedMsg is the ErrNotConnected message string for wire/API parity.
const NotConnectedMsg = "not connected"

// ErrNotConnected reports that the archive control session is not connected.
var ErrNotConnected = errors.New(NotConnectedMsg)

// ArchiveException reports an archive control response error.
type ArchiveException struct {
	msg           string
	ErrorCode     int32
	CorrelationID int64
}

func (e *ArchiveException) Error() string {
	if e == nil {
		return ""
	}
	suffix := archiveExceptionSuffix(e.ErrorCode, e.CorrelationID)
	if e.msg == "" {
		return "archive client: control response error" + suffix
	}
	return fmt.Sprintf("archive client: %s%s", e.msg, suffix)
}

func archiveExceptionSuffix(errorCode int32, correlationID int64) string {
	if correlationID != conductor.NullCorrelationID {
		return fmt.Sprintf(" (code=%d, correlation_id=%d)", errorCode, correlationID)
	}
	return fmt.Sprintf(" (code=%d)", errorCode)
}

// AsArchiveException reports whether err is or wraps an ArchiveException.
func AsArchiveException(err error) (*ArchiveException, bool) {
	return errors.AsType[*ArchiveException](err)
}

// ArchiveErrorCode reports the archive error code when err is an ArchiveException.
func ArchiveErrorCode(err error) (int32, bool) {
	archiveErr, ok := AsArchiveException(err)
	if !ok {
		return 0, false
	}
	return archiveErr.ErrorCode, true
}
