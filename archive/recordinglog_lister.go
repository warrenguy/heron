package archive

import (
	"context"
	"errors"
	"fmt"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// LogRecordingExtentsFromDescriptor maps replay geometry from a recording descriptor.
//
// Mapped fields: StartPosition, StopPosition, InitialTermID, TermBufferLength, MtuLength,
// and SessionID (see recordinglog.LogRecordingExtents).
func LogRecordingExtentsFromDescriptor(
	fields codecs.RecordingDescriptorFields,
) recordinglog.LogRecordingExtents {
	return recordinglog.LogRecordingExtents{
		StartPosition:    fields.StartPosition,
		StopPosition:     fields.StopPosition,
		InitialTermID:    fields.InitialTermID,
		TermBufferLength: fields.TermBufferLength,
		MtuLength:        fields.MtuLength,
		SessionID:        fields.SessionID,
	}
}

type archiveLister struct {
	listFn func(recordingID int64) (codecs.RecordingDescriptorFields, error)
}

var _ recordinglog.ArchiveLister = archiveLister{}

func (a archiveLister) ListRecording(recordingID int64) (recordinglog.LogRecordingExtents, error) {
	fields, err := a.listFn(recordingID)
	if err != nil {
		mapped := MapListRecordingError(err)
		if errors.Is(mapped, recordinglog.ErrUnknownRecordingID) {
			return recordinglog.LogRecordingExtents{}, fmt.Errorf(
				"%w: recording id %d",
				mapped,
				recordingID,
			)
		}
		return recordinglog.LogRecordingExtents{}, mapped
	}
	return LogRecordingExtentsFromDescriptor(fields), nil
}

// NewLister wraps Client.ListRecording for recordinglog.PlanRecovery.
//
// client must be non-nil. ctx bounds every subsequent ListRecording call made through
// the returned lister; the closure retains ctx beyond this constructor call.
func NewLister(
	ctx context.Context,
	client *Client,
) recordinglog.ArchiveLister {
	if client == nil {
		panic("archive: NewLister: client must be non-nil")
	}
	return archiveLister{listFn: func(recordingID int64) (codecs.RecordingDescriptorFields, error) {
		return client.ListRecording(ctx, recordingID)
	}}
}

// MapListRecordingError translates archive errors into recordinglog sentinels.
//
// ErrorUnknownRecording (code 5) maps to recordinglog.ErrUnknownRecordingID;
// all other errors propagate unchanged.
func MapListRecordingError(err error) error {
	if code, ok := ExceptionCode(err); ok && code == ErrorUnknownRecording {
		return recordinglog.ErrUnknownRecordingID
	}
	return err
}
