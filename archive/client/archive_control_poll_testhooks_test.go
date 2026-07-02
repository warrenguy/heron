//go:build testhooks

package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestPollArchiveControlEvents_signalDispatch(t *testing.T) {
	cases := []struct {
		name   string
		signal gensbe.RecordingSignal
	}{
		{name: "start", signal: gensbe.RecordingSignal_START},
		{name: "stop", signal: gensbe.RecordingSignal_STOP},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub, termBuffer := setupControlResponsePollFixtures(t)
			writeUnfragmentedWireToTerm(t, termBuffer, recordingSignalWire(t, tc.signal))

			var captured capturedRecordingSignal
			poller := newControlResponsePollerForTest(sub)
			assembler := NewFragmentAssembler(nil)

			n := PollArchiveControlEvents(
				sub,
				DefaultControlResponseFragmentLimit,
				assembler,
				ArchiveControlPollTargets{
					SignalHandler:  recordingSignalCapture(&captured),
					ResponsePoller: poller,
				},
			)
			require.Equal(t, 1, n)
			require.Equal(t, 1, captured.calls)
			require.Equal(t, tc.signal, captured.signal)
			require.False(t, poller.IsPollComplete())
		})
	}
}

func TestPollArchiveControlEvents_controlResponseToResponsePoller(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)
	responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       7700,
		Code:             gensbe.ControlResponseCode_OK,
		Version:          520,
	})
	writeUnfragmentedWireToTerm(t, termBuffer, responseWire)

	var captured capturedRecordingSignal
	poller := newControlResponsePollerForTest(sub)
	assembler := NewFragmentAssembler(nil)

	n := PollArchiveControlEvents(
		sub,
		DefaultControlResponseFragmentLimit,
		assembler,
		ArchiveControlPollTargets{
			SignalHandler:  recordingSignalCapture(&captured),
			ResponsePoller: poller,
		},
	)
	require.Equal(t, 1, n)
	require.Zero(t, captured.calls)
	require.True(t, poller.IsPollComplete())
	resp, ok := poller.Response()
	require.True(t, ok)
	require.Equal(t, int64(7700), resp.RelevantID)
}

// TestPollArchiveControlEvents_ZeroAllocs gates the steady-state demux wrapper on the
// real poll path: after the delegate is wired, each poll stores targets by value and
// polls the subscription without allocating. Fragment dispatch is gated by
// TestHandleArchiveControlFragment_DispatchSTART_ZeroAllocs.
func TestPollArchiveControlEvents_ZeroAllocs(t *testing.T) {
	sub, _ := setupControlResponsePollFixtures(t)
	poller := newControlResponsePollerForTest(sub)
	assembler := NewFragmentAssembler(nil)
	var captured capturedRecordingSignal
	targets := ArchiveControlPollTargets{
		SignalHandler:    recordingSignalCapture(&captured),
		ControlSessionID: controlResponseTestControlSessionID,
		ResponsePoller:   poller,
	}

	// Warm the once-wired delegate bind so AllocsPerRun measures only steady-state polls.
	PollArchiveControlEvents(sub, DefaultControlResponseFragmentLimit, assembler, targets)

	testutil.AssertZeroAllocs(t, "PollArchiveControlEvents", 100, func() {
		PollArchiveControlEvents(sub, DefaultControlResponseFragmentLimit, assembler, targets)
	})
}

// TestStandalonePollers_loseFragments documents the footgun the demux fixes: a
// ControlResponsePoller.Poll consuming a batch that also holds a signal advances past
// the signal, so a later signal poll on the same subscription never sees it.
func TestStandalonePollers_loseFragments(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)
	writeTwoSequentialUnfragmentedWiresToTerm(
		t,
		termBuffer,
		encodeControlResponsePollerTestOKWire(t),
		recordingSignalWire(t, gensbe.RecordingSignal_START),
	)

	poller := NewControlResponsePoller(sub, DefaultControlResponseFragmentLimit)
	require.Equal(t, 2, poller.Poll())
	require.True(t, poller.IsPollComplete())

	var captured capturedRecordingSignal
	assembler := NewFragmentAssembler(nil)
	n := PollArchiveControlEvents(
		sub,
		DefaultControlResponseFragmentLimit,
		assembler,
		ArchiveControlPollTargets{SignalHandler: recordingSignalCapture(&captured)},
	)
	require.Zero(t, n)
	require.Zero(t, captured.calls)
}
