package client

import (
	"fmt"
	"strings"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

const authenticationRejectedMessage = "authentication rejected"

// MapControlResponse maps archive ControlResponse wire fields to a Go error.
func MapControlResponse(resp codecs.ControlResponseFields) error {
	switch resp.Code {
	case gensbe.ControlResponseCode_OK:
		return nil
	case gensbe.ControlResponseCode_ERROR:
		return archiveExceptionFromErrorResponse(resp)
	case gensbe.ControlResponseCode_RECORDING_UNKNOWN:
		return archiveExceptionUnknownID(
			"recording",
			resp.RelevantID,
			ArchiveErrorUnknownRecording,
			resp.CorrelationID,
		)
	case gensbe.ControlResponseCode_SUBSCRIPTION_UNKNOWN:
		return archiveExceptionUnknownID(
			"subscription",
			resp.RelevantID,
			ArchiveErrorUnknownSubscription,
			resp.CorrelationID,
		)
	default:
		return &ArchiveException{
			Message: fmt.Sprintf(
				"unexpected control response code: %d",
				resp.Code,
			),
			ErrorCode:     ArchiveErrorGeneric,
			CorrelationID: resp.CorrelationID,
		}
	}
}

func archiveExceptionUnknownID(
	entity string,
	relevantID int64,
	errorCode int32,
	correlationID int64,
) error {
	return &ArchiveException{
		Message:       fmt.Sprintf("unknown %s id: %d", entity, relevantID),
		ErrorCode:     errorCode,
		CorrelationID: correlationID,
	}
}

func archiveExceptionFromErrorResponse(resp codecs.ControlResponseFields) error {
	message := strings.TrimSpace(string(resp.ErrorMessage))
	if resp.RelevantID == ArchiveErrorAuthenticationRejected {
		switch {
		case message == "":
			message = authenticationRejectedMessage
		case !strings.Contains(strings.ToLower(message), authenticationRejectedMessage):
			message = authenticationRejectedMessage + ": " + message
		}
	}
	return &ArchiveException{
		Message:       message,
		ErrorCode:     int32(resp.RelevantID),
		CorrelationID: resp.CorrelationID,
	}
}
