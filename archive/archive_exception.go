package archive

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/conductor"
)

const (
	// ErrorGeneric is the default archive error code.
	ErrorGeneric = 0

	// ErrorUnknownSubscription matches Java UNKNOWN_SUBSCRIPTION (code 4).
	ErrorUnknownSubscription = 4

	// ErrorUnknownRecording matches Java UNKNOWN_RECORDING (code 5).
	ErrorUnknownRecording = 5

	// ErrorAuthenticationRejected matches Java AUTHENTICATION_REJECTED (code 10).
	ErrorAuthenticationRejected = 10
)

// NotConnectedMsg is the ErrNotConnected message string for wire/API parity.
const NotConnectedMsg = "not connected"

// ErrNotConnected reports that the archive control session is not connected.
var ErrNotConnected = errors.New(NotConnectedMsg)

// Exception reports an archive control response error.
type Exception struct {
	msg           string
	ErrorCode     int32
	CorrelationID int64
}

func (e *Exception) Error() string {
	if e == nil {
		return ""
	}
	suffix := archiveExceptionSuffix(e.ErrorCode, e.CorrelationID)
	if e.msg == "" {
		return "archive: control response error" + suffix
	}
	return fmt.Sprintf("archive: %s%s", e.msg, suffix)
}

func archiveExceptionSuffix(errorCode int32, correlationID int64) string {
	if correlationID != conductor.NullCorrelationID {
		return fmt.Sprintf(" (code=%d, correlation_id=%d)", errorCode, correlationID)
	}
	return fmt.Sprintf(" (code=%d)", errorCode)
}

// AsException reports whether err is or wraps an Exception.
func AsException(err error) (*Exception, bool) {
	return errors.AsType[*Exception](err)
}

// ExceptionCode reports the archive error code when err is an Exception.
func ExceptionCode(err error) (int32, bool) {
	archiveErr, ok := AsException(err)
	if !ok {
		return 0, false
	}
	return archiveErr.ErrorCode, true
}
