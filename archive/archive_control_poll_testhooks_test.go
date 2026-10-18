//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestPollControlEvents_signalDispatch(t *testing.T) {
	cases := []struct {
		name   string
		signal archivewire.RecordingSignal
	}{
		{name: "start", signal: archivewire.RecordingSignal_START},
		{name: "stop", signal: archivewire.RecordingSignal_STOP},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub, termBuffer := setupControlResponsePollFixtures(t)
			writeUnfragmentedWireToTerm(t, termBuffer, recordingSignalWire(t, tc.signal))

			var captured capturedRecordingSignal
			poller := newControlResponsePollerForTest(sub)
			assembler := NewFragmentAssembler(nil)

			n := PollControlEvents(
				sub,
				DefaultControlResponseFragmentLimit,
				assembler,
				ControlPollTargets{
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

func TestPollControlEvents_controlResponseToResponsePoller(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)
	responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       7700,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	writeUnfragmentedWireToTerm(t, termBuffer, responseWire)

	var captured capturedRecordingSignal
	poller := newControlResponsePollerForTest(sub)
	assembler := NewFragmentAssembler(nil)

	n := PollControlEvents(
		sub,
		DefaultControlResponseFragmentLimit,
		assembler,
		ControlPollTargets{
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

// TestPollControlEvents_ZeroAllocs gates the steady-state demux wrapper on the
// real poll path: after the delegate is wired, each poll stores targets by value and
// polls the subscription without allocating. Fragment dispatch is gated by
// TestHandleControlFragment_DispatchSTART_ZeroAllocs.
func TestPollControlEvents_ZeroAllocs(t *testing.T) {
	sub, _ := setupControlResponsePollFixtures(t)
	poller := newControlResponsePollerForTest(sub)
	assembler := NewFragmentAssembler(nil)
	var captured capturedRecordingSignal
	targets := ControlPollTargets{
		SignalHandler:    recordingSignalCapture(&captured),
		ControlSessionID: controlResponseTestControlSessionID,
		ResponsePoller:   poller,
	}

	// Warm the once-wired delegate bind so AllocsPerRun measures only steady-state polls.
	PollControlEvents(sub, DefaultControlResponseFragmentLimit, assembler, targets)

	testutil.AssertZeroAllocs(t, "PollControlEvents", 100, func() {
		PollControlEvents(sub, DefaultControlResponseFragmentLimit, assembler, targets)
	})
}

// TestPollControlEvents_ArmAwait_staleThenMatchSameBatch asserts the dispatch-time
// filter. A stale ControlResponse ahead of the awaited one in one demux batch
// must not latch. A stale latch would hold the match in the image past this poll.
func TestPollControlEvents_ArmAwait_staleThenMatchSameBatch(t *testing.T) {
	sub, termBuffer := setupControlResponsePollFixtures(t)
	stale := codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID + 1,
		RelevantID:       1,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	}
	match := codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       7700,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	}
	writeTwoSequentialUnfragmentedWiresToTerm(
		t,
		termBuffer,
		encodeControlResponseWire(t, stale),
		encodeControlResponseWire(t, match),
	)

	poller := newControlResponsePollerForTest(sub)
	poller.ArmAwait(controlResponseTestCorrelationID)
	assembler := NewFragmentAssembler(nil)

	n := PollControlEvents(
		sub,
		DefaultControlResponseFragmentLimit,
		assembler,
		ControlPollTargets{ResponsePoller: poller},
	)
	require.Equal(t, 2, n)
	require.True(t, poller.IsPollComplete())
	resp, ok := poller.Response()
	require.True(t, ok)
	require.Equal(t, controlResponseTestCorrelationID, resp.CorrelationID)
	require.Equal(t, int64(7700), resp.RelevantID)
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
		recordingSignalWire(t, archivewire.RecordingSignal_START),
	)

	poller := NewControlResponsePoller(sub, DefaultControlResponseFragmentLimit)
	require.Equal(t, 2, poller.Poll())
	require.True(t, poller.IsPollComplete())

	var captured capturedRecordingSignal
	assembler := NewFragmentAssembler(nil)
	n := PollControlEvents(
		sub,
		DefaultControlResponseFragmentLimit,
		assembler,
		ControlPollTargets{SignalHandler: recordingSignalCapture(&captured)},
	)
	require.Zero(t, n)
	require.Zero(t, captured.calls)
}

func heldResponseWire(t *testing.T, correlationID, relevantID int64) []byte {
	t.Helper()

	code := archivewire.ControlResponseCode_ERROR
	if relevantID == 0 {
		code = archivewire.ControlResponseCode_OK
	}
	return encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    correlationID,
		RelevantID:       relevantID,
		Code:             code,
		Version:          520,
	})
}

// newResponseDemuxPoll returns a demux poll on a dedicated assembler that
// routes responses to poller.
func newResponseDemuxPoll(sub *aeron.Subscription, poller *ControlResponsePoller) func() int {
	assembler := NewFragmentAssembler(nil)
	return func() int {
		return PollControlEvents(
			sub,
			DefaultControlResponseFragmentLimit,
			assembler,
			ControlPollTargets{ResponsePoller: poller},
		)
	}
}

func requireDemuxPoll(
	t *testing.T,
	poll func() int,
	poller *ControlResponsePoller,
	wantFragments int,
	wantRelevantID int64,
) {
	t.Helper()

	require.Equal(t, wantFragments, poll())
	requireLatchedRelevantID(t, poller, wantRelevantID)
}

// TestPollControlEvents_heldResponseRelease asserts that each poll releases one
// held response once the latch clears.
func TestPollControlEvents_heldResponseRelease(t *testing.T) {
	const correlationID = controlResponseTestCorrelationID

	t.Run("disarmed", func(t *testing.T) {
		sub, termBuffer := setupControlResponsePollFixtures(t)
		for _, relevantID := range []int64{0, 1, 2} {
			appendUnfragmentedWireToTerm(t, termBuffer, heldResponseWire(t, correlationID, relevantID))
		}
		poller := newControlResponsePollerForTest(sub)
		poll := newResponseDemuxPoll(sub, poller)

		// Poll 1 latches OK and holds ERROR 1.
		requireDemuxPoll(t, poll, poller, 1, 0)

		// Poll 2 clears OK and latches ERROR 1.
		requireDemuxPoll(t, poll, poller, 1, 1)

		// Poll 3 clears ERROR 1 and latches ERROR 2.
		requireDemuxPoll(t, poll, poller, 1, 2)

		// Poll 4 clears ERROR 2.
		require.Zero(t, poll())
		require.False(t, poller.IsPollComplete())
	})

	t.Run("armedSameID", func(t *testing.T) {
		sub, termBuffer := setupControlResponsePollFixtures(t)
		for _, relevantID := range []int64{0, 1, 2, 3} {
			appendUnfragmentedWireToTerm(t, termBuffer, heldResponseWire(t, correlationID, relevantID))
		}
		stopWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
			ControlSessionID: controlResponseTestControlSessionID,
			CorrelationID:    correlationID,
			RecordingID:      1001,
			SubscriptionID:   2002,
			Position:         3003,
			Signal:           archivewire.RecordingSignal_STOP,
		})
		appendUnfragmentedWireToTerm(t, termBuffer, stopWire)
		poller := newControlResponsePollerForTest(sub)
		poller.ResetAwait(correlationID)
		assembler := NewFragmentAssembler(nil)
		var captured capturedRecordingSignal
		poll := func() int {
			return PollControlEvents(
				sub,
				DefaultControlResponseFragmentLimit,
				assembler,
				ControlPollTargets{
					SignalHandler:    recordingSignalCapture(&captured),
					ControlSessionID: controlResponseTestControlSessionID,
					ResponsePoller:   poller,
				},
			)
		}

		// Poll 1 latches OK and holds ERROR 1. ResetCapture consumes the OK, as
		// PollSignal does.
		requireDemuxPoll(t, poll, poller, 1, 0)
		poller.ResetCapture()

		// Poll 2 latches ERROR 1 and holds ERROR 2. STOP waits.
		requireDemuxPoll(t, poll, poller, 1, 1)
		require.Zero(t, captured.calls)

		// Poll 3 keeps the armed latch. STOP still waits.
		requireDemuxPoll(t, poll, poller, 0, 1)
		require.Zero(t, captured.calls)

		// A handle Close clears the latch and disarms the filter. Poll 4 latches
		// ERROR 2 and holds ERROR 3.
		poller.ResetCapture()
		poller.DisarmAwait(correlationID)
		requireDemuxPoll(t, poll, poller, 1, 2)
		require.Zero(t, captured.calls)

		// Poll 5 clears ERROR 2, latches ERROR 3 and passes STOP.
		requireDemuxPoll(t, poll, poller, 2, 3)
		require.Equal(t, 1, captured.calls)
		require.Equal(t, archivewire.RecordingSignal_STOP, captured.signal)
	})
}

// TestPollControlEvents_captureSurvivesNextPoll asserts that a kept capture
// owns its ErrorMessage and EncodedChallenge bytes after the term frame changes.
func TestPollControlEvents_captureSurvivesNextPoll(t *testing.T) {
	cases := []struct {
		name     string
		wire     func(t *testing.T, text string) []byte
		captured func(capture ControlResponseCapture) []byte
	}{
		{
			name: "errorMessage",
			wire: func(t *testing.T, text string) []byte {
				return encodeControlResponseWire(t, codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    controlResponseTestCorrelationID,
					RelevantID:       ErrorGeneric,
					Code:             archivewire.ControlResponseCode_ERROR,
					Version:          520,
					ErrorMessage:     []byte(text),
				})
			},
			captured: func(capture ControlResponseCapture) []byte {
				return capture.ErrorMessage
			},
		},
		{
			name: "encodedChallenge",
			wire: func(t *testing.T, text string) []byte {
				return encodeChallengeWire(
					t,
					controlResponseTestControlSessionID,
					controlResponseTestCorrelationID,
					[]byte(text),
				)
			},
			captured: func(capture ControlResponseCapture) []byte {
				return capture.EncodedChallenge
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const (
				firstText  = "first-bytes"
				secondText = "other-bytes"
			)
			sub, termBuffer := setupControlResponsePollFixtures(t)
			writeUnfragmentedWireToTermAt(t, termBuffer, 0, tc.wire(t, firstText))
			poller := newControlResponsePollerForTest(sub)
			poll := newResponseDemuxPoll(sub, poller)

			require.Equal(t, 1, poll())
			kept, ok := poller.Capture()
			require.True(t, ok)
			require.Equal(t, []byte(firstText), tc.captured(kept))

			// The in-place frame rewrite simulates term reuse.
			rewrite := tc.wire(t, secondText)
			require.Len(t, rewrite, len(tc.wire(t, firstText)))
			writeUnfragmentedWireToTermAt(t, termBuffer, 0, rewrite)

			require.Zero(t, poll())
			require.Equal(t, []byte(firstText), tc.captured(kept))
		})
	}
}

// TestFragmentAssembler_forms asserts the shared methods and both poll paths
// for each assembler form. A plain poll cannot hold a frame, so the demux form
// consumes a response behind its latched OK. A delegate-form assembler polls
// through its delegate under PollControlEvents.
func TestFragmentAssembler_forms(t *testing.T) {
	const correlationID = controlResponseTestCorrelationID
	cases := []struct {
		name             string
		delegate         bool
		directAssemblers bool
	}{
		{name: "demux"},
		{name: "demuxDirect", directAssemblers: true},
		{name: "delegate", delegate: true, directAssemblers: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub, termBuffer := setupControlResponsePollFixtures(t)
			var delegated int
			var delegate aeron.FragmentHandler
			if tc.delegate {
				delegate = func(atomicbuffer.AtomicBuffer, int, int, *aeron.Header) {
					delegated++
				}
			}
			assembler := NewFragmentAssemblerWithOptions(delegate, 0, tc.directAssemblers)
			require.Equal(t, tc.directAssemblers, assembler.DirectAssemblers())

			// Phase 1: a BEGIN frame opens a session builder.
			writeControlResponseHookCommittedFragmentFrames(
				t,
				termBuffer,
				0,
				logbuffer.BeginFlag,
				heldResponseWire(t, correlationID, 0),
			)
			require.Equal(t, 1, sub.Poll(assembler.Handler(), DefaultControlResponseFragmentLimit))
			require.True(t, assembler.FreeSessionBuffer(controlResponseHookFrameSessionID))
			require.False(t, assembler.FreeSessionBuffer(controlResponseHookFrameSessionID))
			assembler.Clear()

			// Phase 2: a plain poll consumes OK and ERROR.
			appendUnfragmentedWireToTerm(t, termBuffer, heldResponseWire(t, correlationID, 0))
			appendUnfragmentedWireToTerm(t, termBuffer, heldResponseWire(t, correlationID, 1))
			poller := newControlResponsePollerForTest(sub)
			assembler.demuxTargets = ControlPollTargets{ResponsePoller: poller}
			require.Equal(t, 2, sub.Poll(assembler.Handler(), DefaultControlResponseFragmentLimit))
			if tc.delegate {
				require.Equal(t, 2, delegated)
				require.False(t, poller.IsPollComplete())
			} else {
				requireLatchedRelevantID(t, poller, 0)
			}

			// Phase 3: PollControlEvents holds ERROR behind OK on the demux form.
			appendUnfragmentedWireToTerm(t, termBuffer, heldResponseWire(t, correlationID, 0))
			appendUnfragmentedWireToTerm(t, termBuffer, heldResponseWire(t, correlationID, 2))
			poller = newControlResponsePollerForTest(sub)
			n := PollControlEvents(
				sub,
				DefaultControlResponseFragmentLimit,
				assembler,
				ControlPollTargets{ResponsePoller: poller},
			)
			if tc.delegate {
				require.Equal(t, 2, n)
				require.Equal(t, 4, delegated)
				require.False(t, poller.IsPollComplete())
				return
			}
			require.Equal(t, 1, n)
			requireLatchedRelevantID(t, poller, 0)
		})
	}
}

func requireLatchedRelevantID(t *testing.T, poller *ControlResponsePoller, want int64) {
	t.Helper()

	resp, ok := poller.Response()
	require.True(t, ok, "poller must hold a latch")
	require.Equal(t, want, resp.RelevantID)
}
