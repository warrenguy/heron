package client

import (
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

// NewArchiveLister wraps AeronArchive.ListRecording for recordinglog.PlanRecovery.
//
// archive must be non-nil.
func NewArchiveLister(archive *AeronArchive) recordinglog.ArchiveLister {
	if archive == nil {
		panic("archive/client: NewArchiveLister: archive must be non-nil")
	}
	return archiveLister{listFn: archive.ListRecording}
}

// MapListRecordingError translates archive/client errors into recordinglog sentinels.
//
// ArchiveErrorUnknownRecording (code 5) maps to recordinglog.ErrUnknownRecordingID;
// all other errors propagate unchanged.
func MapListRecordingError(err error) error {
	if code, ok := ArchiveErrorCode(err); ok && code == ArchiveErrorUnknownRecording {
		return recordinglog.ErrUnknownRecordingID
	}
	return err
}
