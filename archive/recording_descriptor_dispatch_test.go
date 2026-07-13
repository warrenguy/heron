package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/internal/testutil"
)

const recordingDescriptorTestCorrelationID int64 = 5150

func recordingDescriptorTestFields(correlationID int64) codecs.RecordingDescriptorFields {
	return codecs.RecordingDescriptorFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    correlationID,
		RecordingID:      7001,
		StartPosition:    4096,
		StopPosition:     8192,
		InitialTermID:    9,
		TermBufferLength: 1 << 16,
		MtuLength:        1408,
		SessionID:        13,
		StreamID:         archiveProxyTestRecordingStreamID,
		StrippedChannel:  []byte("aeron:ipc?alias=stripped"),
		OriginalChannel:  []byte("aeron:ipc?alias=original"),
		SourceIdentity:   []byte("source-identity"),
	}
}

func recordingDescriptorControlResponseWire(
	t *testing.T,
	correlationID int64,
	code archivewire.ControlResponseCode,
) []byte {
	t.Helper()
	return encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    correlationID,
		Code:             code,
		Version:          520,
	})
}

func TestHandleRecordingDescriptorFragment(t *testing.T) {
	cases := []struct {
		name        string
		wire        func(t *testing.T) []byte
		wantCapture bool
	}{
		{
			name: "captures matching descriptor",
			wire: func(t *testing.T) []byte {
				return encodeRecordingDescriptorWire(
					t,
					recordingDescriptorTestFields(recordingDescriptorTestCorrelationID),
				)
			},
			wantCapture: true,
		},
		{
			name: "wrong template ignored",
			wire: func(t *testing.T) []byte {
				return recordingDescriptorControlResponseWire(
					t,
					recordingDescriptorTestCorrelationID,
					archivewire.ControlResponseCode_OK,
				)
			},
			wantCapture: false,
		},
		{
			name: "foreign schema ignored",
			wire: func(t *testing.T) []byte {
				return overrideWireSchemaID(
					encodeRecordingDescriptorWire(
						t,
						recordingDescriptorTestFields(recordingDescriptorTestCorrelationID),
					),
					0xFFFF,
				)
			},
			wantCapture: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			poller := NewRecordingDescriptorPoller()
			poller.ResetAwait(recordingDescriptorTestCorrelationID)

			require.NotPanics(t, func() {
				handleRecordingDescriptorFragment(poller, tc.wire(t))
			})

			got := poller.Descriptor()
			if tc.wantCapture {
				require.Equal(t, int64(7001), got.RecordingID)
				require.Equal(t, "aeron:ipc?alias=stripped", string(got.StrippedChannel))
				require.Equal(t, "aeron:ipc?alias=original", string(got.OriginalChannel))
				require.Equal(t, "source-identity", string(got.SourceIdentity))
				require.True(t, poller.IsPollComplete())
				_, ok := poller.ControlResponse()
				require.False(t, ok, "descriptor-only capture must not report ControlResponse")
			} else {
				require.Zero(t, got.RecordingID)
				require.Empty(t, got.StrippedChannel)
				require.False(t, poller.IsPollComplete())
			}
		})
	}
}

func TestHandleDescriptorControlResponseFragment(t *testing.T) {
	cases := []struct {
		name         string
		wire         func(t *testing.T) []byte
		wantResponse bool
		wantComplete bool
		wantMatchErr bool
	}{
		{
			name: "captures matching ok response",
			wire: func(t *testing.T) []byte {
				return recordingDescriptorControlResponseWire(
					t,
					recordingDescriptorTestCorrelationID,
					archivewire.ControlResponseCode_OK,
				)
			},
			wantResponse: true,
			wantComplete: false,
			wantMatchErr: false,
		},
		{
			name: "error response latches terminal",
			wire: func(t *testing.T) []byte {
				return recordingDescriptorControlResponseWire(
					t,
					recordingDescriptorTestCorrelationID,
					archivewire.ControlResponseCode_ERROR,
				)
			},
			wantResponse: true,
			wantComplete: true,
			wantMatchErr: true,
		},
		{
			name: "correlation mismatch ignored",
			wire: func(t *testing.T) []byte {
				return recordingDescriptorControlResponseWire(
					t,
					recordingDescriptorTestCorrelationID+1,
					archivewire.ControlResponseCode_OK,
				)
			},
			wantResponse: false,
		},
		{
			name: "wrong template ignored",
			wire: func(t *testing.T) []byte {
				return encodeRecordingDescriptorWire(
					t,
					recordingDescriptorTestFields(recordingDescriptorTestCorrelationID),
				)
			},
			wantResponse: false,
		},
		{
			name: "foreign schema ignored",
			wire: func(t *testing.T) []byte {
				return overrideWireSchemaID(
					recordingDescriptorControlResponseWire(
						t,
						recordingDescriptorTestCorrelationID,
						archivewire.ControlResponseCode_OK,
					),
					0xFFFF,
				)
			},
			wantResponse: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			poller := NewRecordingDescriptorPoller()
			poller.ResetAwait(recordingDescriptorTestCorrelationID)

			require.NotPanics(t, func() {
				handleDescriptorControlResponseFragment(poller, tc.wire(t))
			})

			fields, ok := poller.ControlResponse()
			require.Equal(t, tc.wantResponse, ok)
			require.Equal(t, tc.wantComplete, poller.IsPollComplete())
			if !ok {
				return
			}
			require.Equal(t, recordingDescriptorTestCorrelationID, fields.CorrelationID)
			err := MapControlResponse(fields)
			if tc.wantMatchErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRecordingDescriptorPoller_descriptorFirstRetainsChannelAfterBufferOverwrite(t *testing.T) {
	const (
		strippedChannel = "aeron:ipc?alias=desc-first-stripped"
		originalChannel = "aeron:ipc?alias=desc-first-original"
		sourceIdentity  = "desc-first-source"
	)
	_, termBuffer := setupControlResponsePollFixtures(t)
	descriptorWire := encodeRecordingDescriptorWire(
		t,
		codecs.RecordingDescriptorFields{
			ControlSessionID: controlResponseTestControlSessionID,
			CorrelationID:    recordingDescriptorTestCorrelationID,
			RecordingID:      7001,
			StrippedChannel:  []byte(strippedChannel),
			OriginalChannel:  []byte(originalChannel),
			SourceIdentity:   []byte(sourceIdentity),
		},
	)
	writeUnfragmentedWireToTerm(t, termBuffer, descriptorWire)

	poller := NewRecordingDescriptorPoller()
	poller.ResetAwait(recordingDescriptorTestCorrelationID)
	handleRecordingDescriptorFragment(poller, descriptorWire)

	overwrite := make([]byte, 512)
	for i := range overwrite {
		overwrite[i] = 0x5A
	}
	termBuffer.PutBytes(0, overwrite)

	handleDescriptorControlResponseFragment(
		poller,
		recordingDescriptorControlResponseWire(
			t,
			recordingDescriptorTestCorrelationID,
			archivewire.ControlResponseCode_OK,
		),
	)

	require.True(t, poller.IsPollComplete())
	got := poller.Descriptor()
	require.Equal(t, strippedChannel, string(got.StrippedChannel))
	require.Equal(t, originalChannel, string(got.OriginalChannel))
	require.Equal(t, sourceIdentity, string(got.SourceIdentity))
}

func TestRecordingDescriptorPoller_completesOnResponseAndDescriptor(t *testing.T) {
	cases := []struct {
		name           string
		descriptorLast bool
	}{
		{name: "response then descriptor", descriptorLast: true},
		{name: "descriptor then response", descriptorLast: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			poller := NewRecordingDescriptorPoller()
			poller.ResetAwait(recordingDescriptorTestCorrelationID)

			responseWire := recordingDescriptorControlResponseWire(
				t,
				recordingDescriptorTestCorrelationID,
				archivewire.ControlResponseCode_OK,
			)
			descriptorWire := encodeRecordingDescriptorWire(
				t,
				recordingDescriptorTestFields(recordingDescriptorTestCorrelationID),
			)

			if tc.descriptorLast {
				handleDescriptorControlResponseFragment(poller, responseWire)
				require.False(t, poller.IsPollComplete())
				handleRecordingDescriptorFragment(poller, descriptorWire)
			} else {
				handleRecordingDescriptorFragment(poller, descriptorWire)
				require.True(t, poller.IsPollComplete())
				// Java list-by-id completes on descriptor only; a trailing OK is ignored.
				handleDescriptorControlResponseFragment(poller, responseWire)
			}

			require.True(t, poller.IsPollComplete())
			require.Equal(t, int64(7001), poller.Descriptor().RecordingID)
		})
	}
}

func TestRecordingDescriptorPoller_foreignDescriptorLeavesIncomplete(t *testing.T) {
	poller := NewRecordingDescriptorPoller()
	poller.ResetAwait(recordingDescriptorTestCorrelationID)

	handleDescriptorControlResponseFragment(
		poller,
		recordingDescriptorControlResponseWire(
			t,
			recordingDescriptorTestCorrelationID,
			archivewire.ControlResponseCode_OK,
		),
	)
	handleRecordingDescriptorFragment(
		poller,
		encodeRecordingDescriptorWire(
			t,
			recordingDescriptorTestFields(recordingDescriptorTestCorrelationID+1),
		),
	)

	require.False(t, poller.IsPollComplete())
}

func TestHandleDescriptorControlResponseFragment_ZeroAllocs(t *testing.T) {
	poller := NewRecordingDescriptorPoller()
	poller.ResetAwait(recordingDescriptorTestCorrelationID)
	wire := recordingDescriptorControlResponseWire(
		t,
		recordingDescriptorTestCorrelationID,
		archivewire.ControlResponseCode_OK,
	)

	testutil.AssertZeroAllocs(t, "handleDescriptorControlResponseFragment", 100, func() {
		handleDescriptorControlResponseFragment(poller, wire)
	})
	_, ok := poller.ControlResponse()
	require.True(t, ok)
}
