package archive

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func TestMapListRecordingError(t *testing.T) {
	t.Parallel()

	otherErr := errors.New("connect failed")
	genericArchiveErr := &Exception{
		msg:       "archive unavailable",
		ErrorCode: ErrorGeneric,
	}

	tests := []struct {
		name            string
		err             error
		want            error
		wantIs          error
		wantNot         error
		wantNoException bool
	}{
		{
			name: "nil_error",
			err:  nil,
			want: nil,
		},
		{
			name: "unknown_recording",
			err: &Exception{
				msg:       "unknown recording id: 42",
				ErrorCode: ErrorUnknownRecording,
			},
			want:            recordinglog.ErrUnknownRecordingID,
			wantIs:          recordinglog.ErrUnknownRecordingID,
			wantNoException: true,
		},
		{
			name: "wrapped_unknown_recording",
			err: fmt.Errorf(
				"list recording: %w",
				&Exception{ErrorCode: ErrorUnknownRecording},
			),
			want:            recordinglog.ErrUnknownRecordingID,
			wantIs:          recordinglog.ErrUnknownRecordingID,
			wantNot:         otherErr,
			wantNoException: true,
		},
		{
			name:    "generic_archive_error",
			err:     genericArchiveErr,
			want:    genericArchiveErr,
			wantNot: recordinglog.ErrUnknownRecordingID,
		},
		{
			name:    "non_archive_error",
			err:     otherErr,
			want:    otherErr,
			wantNot: recordinglog.ErrUnknownRecordingID,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := MapListRecordingError(tc.err)
			if tc.err == nil {
				require.NoError(t, got)
				return
			}
			require.Error(t, got)
			if tc.want != nil {
				require.Equal(t, tc.want, got)
			}
			if tc.wantIs != nil {
				require.ErrorIs(t, got, tc.wantIs)
			}
			if tc.wantNot != nil {
				require.NotErrorIs(t, got, tc.wantNot)
			}
			if tc.wantNoException {
				require.NotErrorAs(t, got, new(*Exception))
			}
		})
	}
}

func TestLogRecordingExtentsFromDescriptor_mapsReplayFields(t *testing.T) {
	t.Parallel()

	fields := codecs.RecordingDescriptorFields{
		RecordingID:       7,
		StartTimestamp:    100,
		StopTimestamp:     200,
		StartPosition:     4096,
		StopPosition:      8192,
		InitialTermID:     3,
		SegmentFileLength: 1 << 20,
		TermBufferLength:  128 << 20,
		MtuLength:         256 << 10,
		SessionID:         1408,
		StreamID:          42,
		StrippedChannel:   []byte("aeron:ipc"),
		OriginalChannel:   []byte("aeron:ipc?alias=original"),
		SourceIdentity:    []byte("source"),
	}

	got := LogRecordingExtentsFromDescriptor(fields)
	require.Equal(t, recordinglog.LogRecordingExtents{
		StartPosition:    fields.StartPosition,
		StopPosition:     fields.StopPosition,
		InitialTermID:    fields.InitialTermID,
		TermBufferLength: fields.TermBufferLength,
		MtuLength:        fields.MtuLength,
		SessionID:        fields.SessionID,
	}, got)
}

func TestArchiveLister_listRecording(t *testing.T) {
	t.Parallel()

	descriptor := codecs.RecordingDescriptorFields{
		StartPosition:    1024,
		StopPosition:     2048,
		InitialTermID:    5,
		TermBufferLength: 1 << 16,
		MtuLength:        1408,
		SessionID:        17,
	}
	otherErr := errors.New("descriptor timeout")

	tests := []struct {
		name       string
		listFn     func(recordingID int64) (codecs.RecordingDescriptorFields, error)
		recording  int64
		want       recordinglog.LogRecordingExtents
		wantErr    error
		wantErrIs  error
		wantErrNot error
	}{
		{
			name: "happy_path",
			listFn: func(recordingID int64) (codecs.RecordingDescriptorFields, error) {
				require.Equal(t, int64(42), recordingID)
				return descriptor, nil
			},
			recording: 42,
			want:      LogRecordingExtentsFromDescriptor(descriptor),
		},
		{
			name: "other_error_propagates",
			listFn: func(int64) (codecs.RecordingDescriptorFields, error) {
				return codecs.RecordingDescriptorFields{}, otherErr
			},
			recording:  1,
			wantErr:    otherErr,
			wantErrNot: recordinglog.ErrUnknownRecordingID,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			lister := archiveLister{listFn: tc.listFn}
			got, err := lister.ListRecording(tc.recording)
			if tc.wantErr != nil || tc.wantErrIs != nil {
				require.Error(t, err)
				require.Equal(t, recordinglog.LogRecordingExtents{}, got)
				if tc.wantErr != nil {
					require.Equal(t, tc.wantErr, err)
				}
				if tc.wantErrIs != nil {
					require.ErrorIs(t, err, tc.wantErrIs)
				}
				if tc.wantErrNot != nil {
					require.NotErrorIs(t, err, tc.wantErrNot)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestNewLister_delegatesToArchiveListRecording(t *testing.T) {
	t.Parallel()

	lister := NewLister(t.Context(), &Client{})
	got, err := lister.ListRecording(1)
	require.Error(t, err)
	require.Equal(t, recordinglog.LogRecordingExtents{}, got)
	require.ErrorIs(t, err, errNilDescriptorPoller)
	require.NotErrorIs(t, err, recordinglog.ErrUnknownRecordingID)
}

func TestNewLister_nilClientPanics(t *testing.T) {
	t.Parallel()

	require.Panics(t, func() {
		NewLister(t.Context(), nil)
	})
}

func TestArchiveLister_unknownRecordingIncludesRecordingID(t *testing.T) {
	t.Parallel()

	lister := archiveLister{
		listFn: func(int64) (codecs.RecordingDescriptorFields, error) {
			return codecs.RecordingDescriptorFields{}, &Exception{
				msg:       "unknown recording id: 99",
				ErrorCode: ErrorUnknownRecording,
			}
		},
	}

	got, err := lister.ListRecording(99)
	require.Error(t, err)
	require.Equal(t, recordinglog.LogRecordingExtents{}, got)
	require.ErrorIs(t, err, recordinglog.ErrUnknownRecordingID)
	require.Contains(t, err.Error(), "recording id 99")
}
