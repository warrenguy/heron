package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

type capturedRecordingSignal struct {
	controlSessionID int64
	correlationID    int64
	recordingID      int64
	subscriptionID   int64
	position         int64
	signal           archivewire.RecordingSignal
	calls            int
}

func recordingSignalCapture(captured *capturedRecordingSignal) RecordingSignalHandlerFunc {
	return func(event codecs.RecordingSignalEventFields) {
		captured.controlSessionID = event.ControlSessionID
		captured.correlationID = event.CorrelationID
		captured.recordingID = event.RecordingID
		captured.subscriptionID = event.SubscriptionID
		captured.position = event.Position
		captured.signal = event.Signal
		captured.calls++
	}
}

func recordingSignalWire(t *testing.T, signal archivewire.RecordingSignal) []byte {
	t.Helper()
	return encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    909,
		RecordingID:      1001,
		SubscriptionID:   2002,
		Position:         3003,
		Signal:           signal,
	})
}

func invokeHandleArchiveControlFragment(
	t *testing.T,
	targets ControlPollTargets,
	wire []byte,
) {
	t.Helper()
	handleControlFragment(targets, atomicbuffer.Wrap(wire), 0, len(wire), &aeron.Header{})
}

func TestHandleArchiveControlFragment_signalDispatch(t *testing.T) {
	cases := []struct {
		name   string
		signal archivewire.RecordingSignal
	}{
		{name: "start", signal: archivewire.RecordingSignal_START},
		{name: "stop", signal: archivewire.RecordingSignal_STOP},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var captured capturedRecordingSignal
			targets := ControlPollTargets{SignalHandler: recordingSignalCapture(&captured)}

			invokeHandleArchiveControlFragment(t, targets, recordingSignalWire(t, tc.signal))

			require.Equal(t, 1, captured.calls)
			require.Equal(t, controlResponseTestControlSessionID, captured.controlSessionID)
			require.Equal(t, int64(1001), captured.recordingID)
			require.Equal(t, int64(2002), captured.subscriptionID)
			require.Equal(t, int64(3003), captured.position)
			require.Equal(t, tc.signal, captured.signal)
		})
	}
}

func TestHandleArchiveControlFragment_signalSessionFilter(t *testing.T) {
	var captured capturedRecordingSignal
	targets := ControlPollTargets{
		SignalHandler:    recordingSignalCapture(&captured),
		ControlSessionID: controlResponseTestControlSessionID + 1,
	}

	wire := recordingSignalWire(t, archivewire.RecordingSignal_START)
	invokeHandleArchiveControlFragment(t, targets, wire)

	require.Zero(t, captured.calls)
}

func TestHandleArchiveControlFragment_signalRoutesToHandlerNotPoller(t *testing.T) {
	var captured capturedRecordingSignal
	poller := NewControlResponsePoller(nil, 1)
	targets := ControlPollTargets{
		SignalHandler:  recordingSignalCapture(&captured),
		ResponsePoller: poller,
	}

	wire := recordingSignalWire(t, archivewire.RecordingSignal_START)
	invokeHandleArchiveControlFragment(t, targets, wire)

	require.Equal(t, 1, captured.calls)
	require.False(t, poller.IsPollComplete())
}

func TestHandleArchiveControlFragment_controlResponseToPoller(t *testing.T) {
	var captured capturedRecordingSignal
	poller := NewControlResponsePoller(nil, 1)
	targets := ControlPollTargets{
		SignalHandler:  recordingSignalCapture(&captured),
		ResponsePoller: poller,
	}

	wire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       7700,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	invokeHandleArchiveControlFragment(t, targets, wire)

	require.Zero(t, captured.calls)
	require.True(t, poller.IsPollComplete())
	capture, ok := poller.Capture()
	require.True(t, ok)
	require.Equal(t, int32(archivewire.ControlResponseSbeTemplateID), capture.TemplateID)
	resp, ok := poller.Response()
	require.True(t, ok)
	require.Equal(t, int64(7700), resp.RelevantID)
}

func TestHandleArchiveControlFragment_challengeToPoller(t *testing.T) {
	poller := NewControlResponsePoller(nil, 1)
	targets := ControlPollTargets{ResponsePoller: poller}

	wire := encodeChallengeWire(t, controlResponseTestControlSessionID, 404, []byte("challenge-bytes"))
	invokeHandleArchiveControlFragment(t, targets, wire)

	require.True(t, poller.IsChallenged())
}

func TestHandleArchiveControlFragment_dropsUnroutable(t *testing.T) {
	cases := []struct {
		name    string
		targets ControlPollTargets
		wire    func(t *testing.T) []byte
	}{
		{
			name:    "nil_signal_handler",
			targets: ControlPollTargets{},
			wire: func(t *testing.T) []byte {
				return recordingSignalWire(t, archivewire.RecordingSignal_START)
			},
		},
		{
			name:    "nil_response_poller",
			targets: ControlPollTargets{},
			wire: func(t *testing.T) []byte {
				return encodeControlResponseWire(t, codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    controlResponseTestCorrelationID,
					Code:             archivewire.ControlResponseCode_OK,
				})
			},
		},
		{
			name:    "unknown_template",
			targets: ControlPollTargets{ResponsePoller: NewControlResponsePoller(nil, 1)},
			wire: func(t *testing.T) []byte {
				return encodeKeepAliveRequestWire(t, controlResponseTestControlSessionID)
			},
		},
		{
			name:    "short_fragment",
			targets: ControlPollTargets{ResponsePoller: NewControlResponsePoller(nil, 1)},
			wire: func(_ *testing.T) []byte {
				return []byte{1, 2, 3}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				invokeHandleArchiveControlFragment(t, tc.targets, tc.wire(t))
			})
			if tc.targets.ResponsePoller != nil {
				require.False(t, tc.targets.ResponsePoller.IsPollComplete())
			}
		})
	}
}

func TestPollControlEvents_listRecording_OKAndDescriptor(t *testing.T) {
	poller := NewRecordingDescriptorPoller()
	poller.ResetAwait(recordingDescriptorTestCorrelationID)
	targets := ControlPollTargets{DescriptorPoller: poller}

	invokeHandleArchiveControlFragment(
		t,
		targets,
		recordingDescriptorControlResponseWire(
			t,
			recordingDescriptorTestCorrelationID,
			archivewire.ControlResponseCode_OK,
		),
	)
	require.False(t, poller.IsPollComplete())

	invokeHandleArchiveControlFragment(
		t,
		targets,
		encodeRecordingDescriptorWire(
			t,
			recordingDescriptorTestFields(recordingDescriptorTestCorrelationID),
		),
	)

	require.True(t, poller.IsPollComplete())
	require.Equal(t, int64(7001), poller.Descriptor().RecordingID)
	require.Equal(t, "source-identity", string(poller.Descriptor().SourceIdentity))
}

func TestPollControlEvents_descriptorMode_ControlResponseToDescriptorPoller(t *testing.T) {
	descriptorPoller := NewRecordingDescriptorPoller()
	descriptorPoller.ResetAwait(recordingDescriptorTestCorrelationID)
	targets := ControlPollTargets{DescriptorPoller: descriptorPoller}

	invokeHandleArchiveControlFragment(
		t,
		targets,
		recordingDescriptorControlResponseWire(
			t,
			recordingDescriptorTestCorrelationID,
			archivewire.ControlResponseCode_OK,
		),
	)

	resp, ok := descriptorPoller.ControlResponse()
	require.True(t, ok)
	require.Equal(t, recordingDescriptorTestCorrelationID, resp.CorrelationID)
}

func TestHandleArchiveControlFragment_signalTruncatedBodyDropped(t *testing.T) {
	var captured capturedRecordingSignal
	targets := ControlPollTargets{SignalHandler: recordingSignalCapture(&captured)}

	// Header claims RecordingSignalEvent, but the body is truncated to zero bytes, so the
	// typed decode rejects it after the template routes to the signal handler.
	fullSignal := recordingSignalWire(t, archivewire.RecordingSignal_START)
	headerOnly := fullSignal[:codecs.MessageHeaderEncodedLen()]
	require.NotPanics(t, func() {
		invokeHandleArchiveControlFragment(t, targets, headerOnly)
	})
	require.Zero(t, captured.calls)
}

func TestHandleArchiveControlFragment_foreignSchemaDropped(t *testing.T) {
	const foreignSchemaID uint16 = 0xFFFF
	cases := []struct {
		name string
		wire func(t *testing.T) []byte
	}{
		{
			name: "control_response_template",
			wire: func(t *testing.T) []byte {
				return encodeWireWithSchemaID(t, foreignSchemaID)
			},
		},
		{
			name: "recording_signal_template",
			wire: func(t *testing.T) []byte {
				return overrideWireSchemaID(
					recordingSignalWire(t, archivewire.RecordingSignal_START),
					foreignSchemaID,
				)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var captured capturedRecordingSignal
			poller := NewControlResponsePoller(nil, 1)
			targets := ControlPollTargets{
				SignalHandler:  recordingSignalCapture(&captured),
				ResponsePoller: poller,
			}

			invokeHandleArchiveControlFragment(t, targets, tc.wire(t))

			require.Zero(t, captured.calls)
			require.False(t, poller.IsPollComplete())
		})
	}
}

func TestHandleArchiveControlFragment_DispatchSTART_ZeroAllocs(t *testing.T) {
	wire := recordingSignalWire(t, archivewire.RecordingSignal_START)
	buffer := atomicbuffer.Wrap(wire)
	header := &aeron.Header{}
	var captured capturedRecordingSignal
	targets := ControlPollTargets{SignalHandler: recordingSignalCapture(&captured)}

	testutil.AssertZeroAllocs(t, "handleControlFragment", 100, func() {
		handleControlFragment(targets, buffer, 0, len(wire), header)
	})
	require.Positive(t, captured.calls)
}

func TestStopSignalTracker(t *testing.T) {
	cases := []struct {
		name             string
		trackerSessionID int64
		trackerRecording int64
		signalSessionID  int64
		signalRecording  int64
		signal           archivewire.RecordingSignal
		resetCapture     bool
		wantStopped      bool
	}{
		{
			name:             "stop_matches",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: 6001,
			signalSessionID:  controlResponseTestControlSessionID,
			signalRecording:  6001,
			signal:           archivewire.RecordingSignal_STOP,
			wantStopped:      true,
		},
		{
			name:             "start_ignored",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: 6001,
			signalSessionID:  controlResponseTestControlSessionID,
			signalRecording:  6001,
			signal:           archivewire.RecordingSignal_START,
			wantStopped:      false,
		},
		{
			name:             "session_mismatch_ignored",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: 6001,
			signalSessionID:  controlResponseTestControlSessionID + 1,
			signalRecording:  6001,
			signal:           archivewire.RecordingSignal_STOP,
			wantStopped:      false,
		},
		{
			name:             "recording_mismatch_ignored",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: 6001,
			signalSessionID:  controlResponseTestControlSessionID,
			signalRecording:  6002,
			signal:           archivewire.RecordingSignal_STOP,
			wantStopped:      false,
		},
		{
			name:             "any_session_any_recording",
			trackerSessionID: 0,
			trackerRecording: -1,
			signalSessionID:  controlResponseTestControlSessionID,
			signalRecording:  9999,
			signal:           archivewire.RecordingSignal_STOP,
			wantStopped:      true,
		},
		{
			name:             "reset_clears_capture",
			trackerSessionID: controlResponseTestControlSessionID,
			trackerRecording: 6001,
			signalSessionID:  controlResponseTestControlSessionID,
			signalRecording:  6001,
			signal:           archivewire.RecordingSignal_STOP,
			resetCapture:     true,
			wantStopped:      false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewStopSignalTracker(tc.trackerSessionID, tc.trackerRecording)
			tracker.RecordingSignal(codecs.RecordingSignalEventFields{
				ControlSessionID: tc.signalSessionID,
				CorrelationID:    707,
				RecordingID:      tc.signalRecording,
				SubscriptionID:   2002,
				Position:         3003,
				Signal:           tc.signal,
			})
			if tc.resetCapture {
				require.True(t, tracker.Stopped())
				tracker.Reset()
			}
			require.Equal(t, tc.wantStopped, tracker.Stopped())
			if tc.wantStopped {
				require.Equal(t, tc.signalRecording, tracker.StoppedRecordingID())
				require.Equal(t, int64(3003), tracker.StoppedPosition())
			}
		})
	}
}

func TestStopSignalTracker_secondStopIgnored(t *testing.T) {
	tracker := NewStopSignalTracker(controlResponseTestControlSessionID, 6001)
	tracker.RecordingSignal(codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    707,
		RecordingID:      6001,
		SubscriptionID:   2002,
		Position:         100,
		Signal:           archivewire.RecordingSignal_STOP,
	})
	tracker.RecordingSignal(codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    708,
		RecordingID:      6001,
		SubscriptionID:   2002,
		Position:         999,
		Signal:           archivewire.RecordingSignal_STOP,
	})
	require.True(t, tracker.Stopped())
	require.Equal(t, int64(6001), tracker.StoppedRecordingID())
	require.Equal(t, int64(100), tracker.StoppedPosition())
}
