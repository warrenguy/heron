package archive

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/conductor"
)

const (
	// ErrorGeneric is the default archive error code.
	ErrorGeneric = 0

	// ErrorActiveListing matches Java ACTIVE_LISTING (code 1).
	ErrorActiveListing = 1

	// ErrorActiveRecording matches Java ACTIVE_RECORDING (code 2).
	ErrorActiveRecording = 2

	// ErrorActiveSubscription matches Java ACTIVE_SUBSCRIPTION (code 3).
	ErrorActiveSubscription = 3

	// ErrorUnknownSubscription matches Java UNKNOWN_SUBSCRIPTION (code 4).
	ErrorUnknownSubscription = 4

	// ErrorUnknownRecording matches Java UNKNOWN_RECORDING (code 5).
	ErrorUnknownRecording = 5

	// ErrorUnknownReplay matches Java UNKNOWN_REPLAY (code 6).
	ErrorUnknownReplay = 6

	// ErrorMaxReplays matches Java MAX_REPLAYS (code 7).
	ErrorMaxReplays = 7

	// ErrorMaxRecordings matches Java MAX_RECORDINGS (code 8).
	ErrorMaxRecordings = 8

	// ErrorInvalidExtension matches Java INVALID_EXTENSION (code 9).
	ErrorInvalidExtension = 9

	// ErrorAuthenticationRejected matches Java AUTHENTICATION_REJECTED (code 10).
	ErrorAuthenticationRejected = 10

	// ErrorStorageSpace matches Java STORAGE_SPACE (code 11).
	ErrorStorageSpace = 11

	// ErrorUnknownReplication matches Java UNKNOWN_REPLICATION (code 12).
	ErrorUnknownReplication = 12

	// ErrorUnauthorisedAction matches Java UNAUTHORISED_ACTION (code 13).
	ErrorUnauthorisedAction = 13

	// ErrorReplicationConnectionFailure matches Java REPLICATION_CONNECTION_FAILURE (code 14).
	ErrorReplicationConnectionFailure = 14

	// ErrorEmptyRecording matches Java EMPTY_RECORDING (code 15).
	ErrorEmptyRecording = 15

	// ErrorInvalidPosition matches Java INVALID_POSITION (code 16).
	ErrorInvalidPosition = 16
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
