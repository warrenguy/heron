//go:build testhooks

package archive

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/internal/testutil"
	"github.com/warrenguy/heron/internal/transportfault"
)

func TestNewClient_transfersOwnsAeronClient(t *testing.T) {
	t.Parallel()

	shared := newClient(&ConnectResult{
		OwnsAeronClient: false,
	})
	require.False(t, shared.ownsAeronClient)
	require.Nil(t, shared.transportFault)

	monitor := &transportfault.Monitor{}
	owned := newClient(&ConnectResult{
		OwnsAeronClient: true,
		transportFault:  monitor,
	})
	require.True(t, owned.ownsAeronClient)
	require.Same(t, monitor, owned.transportFault)
}

func TestNewClient_wiresConfigRecordingSignalHandler(t *testing.T) {
	var gotSignal archivewire.RecordingSignal
	handlers := &RecordingSignalHandlers{
		SignalFunc: func(event codecs.RecordingSignalEventFields) {
			gotSignal = event.Signal
		},
	}

	sub, termBuffer := setupControlResponsePollFixtures(t)
	archive := newClient(&ConnectResult{
		ControlSessionID:    controlResponseTestControlSessionID,
		Poller:              newControlResponsePollerForTest(sub),
		ControlSubscription: sub,
		Config: Config{
			RecordingSignalHandler: handlers,
		},
	})

	appendUnfragmentedWireToTerm(
		t,
		termBuffer,
		recordingSignalWire(t, archivewire.RecordingSignal_START),
	)
	archive.Poll()
	require.Equal(t, archivewire.RecordingSignal_START, gotSignal)
}

func newClientForDemuxTest(
	t *testing.T,
	handler RecordingSignalHandler,
) (*Client, atomicbuffer.AtomicBuffer) {
	t.Helper()

	sub, termBuffer := setupControlResponsePollFixtures(t)
	archive := &Client{
		controlSessionID:    controlResponseTestControlSessionID,
		controlPoller:       newControlResponsePollerForTest(sub),
		descriptorPoller:    NewRecordingDescriptorPoller(),
		signalHandler:       handler,
		controlSubscription: sub,
		controlAssembler:    NewFragmentAssembler(nil),
	}
	return archive, termBuffer
}

// TestClient_handoffClearsConnectLatch proves post-connect Poll captures a new
// control response after the connect latch is cleared at session construction.
func TestClient_handoffClearsConnectLatch(t *testing.T) {
	archive, termBuffer := newClientForDemuxTest(t, nil)
	priorWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       1,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	writeUnfragmentedWireToTerm(t, termBuffer, priorWire)
	require.Equal(t, 1, archive.Poll())
	require.True(t, archive.controlPoller.IsPollComplete())

	archive.controlPoller.ResetCapture()

	const nextCorrelationID int64 = 88001
	nextWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    nextCorrelationID,
		RelevantID:       88002,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	appendUnfragmentedWireToTerm(t, termBuffer, nextWire)

	require.Equal(t, 1, archive.Poll())
	require.True(t, archive.controlPoller.IsPollComplete())
	resp, ok := archive.controlPoller.Response()
	require.True(t, ok)
	require.Equal(t, nextCorrelationID, resp.CorrelationID)
}

// TestClientPoll_routesThroughDemux proves Poll multiplexes one subscription: a
// control response routes to the session poller and a recording signal to the handler in
// a single poll, while the descriptor poller stays idle.
func TestClientPoll_routesThroughDemux(t *testing.T) {
	var captured capturedRecordingSignal
	archive, termBuffer := newClientForDemuxTest(t, recordingSignalCapture(&captured))

	responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       7700,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	writeUnfragmentedWireToTerm(t, termBuffer, responseWire)
	appendUnfragmentedWireToTerm(
		t,
		termBuffer,
		recordingSignalWire(t, archivewire.RecordingSignal_START),
	)

	n := archive.Poll()
	require.Equal(t, 2, n)
	require.True(t, archive.controlPoller.IsPollComplete())
	resp, ok := archive.controlPoller.Response()
	require.True(t, ok)
	require.Equal(t, int64(7700), resp.RelevantID)
	require.Equal(t, 1, captured.calls)
	require.Equal(t, archivewire.RecordingSignal_START, captured.signal)
	require.False(t, archive.descriptorPoller.IsPollComplete())
}

// TestClientPoll_signalsPassLatchedTerminal asserts that a recording signal
// reaches its handler while the poller holds a latched terminal. The next
// control response waits for a later poll.
func TestClientPoll_signalsPassLatchedTerminal(t *testing.T) {
	var signals []archivewire.RecordingSignal
	handler := RecordingSignalHandlerFunc(func(event codecs.RecordingSignalEventFields) {
		signals = append(signals, event.Signal)
	})
	archive, termBuffer := newClientForDemuxTest(t, handler)
	archive.controlPoller.ArmAwait(controlResponseTestCorrelationID)

	response := codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	}
	errorResponse := response
	errorResponse.Code = archivewire.ControlResponseCode_ERROR
	errorResponse.RelevantID = ErrorInvalidExtension
	errorResponse.ErrorMessage = []byte("invalid extension")
	startWire := recordingSignalWire(t, archivewire.RecordingSignal_START)
	stopWire := recordingSignalWire(t, archivewire.RecordingSignal_STOP)
	writeUnfragmentedWireToTerm(t, termBuffer, encodeControlResponseWire(t, response))
	appendUnfragmentedWireToTerm(t, termBuffer, startWire)
	appendUnfragmentedWireToTerm(t, termBuffer, encodeControlResponseWire(t, errorResponse))
	appendUnfragmentedWireToTerm(t, termBuffer, stopWire)

	// Poll 1 latches OK, passes START, and holds ERROR.
	require.Equal(t, 2, archive.Poll())
	resp, ok := archive.controlPoller.Response()
	require.True(t, ok)
	require.Equal(t, archivewire.ControlResponseCode_OK, resp.Code)
	require.Equal(t, []archivewire.RecordingSignal{archivewire.RecordingSignal_START}, signals)

	// Poll 2 latches ERROR and passes STOP.
	archive.controlPoller.ResetCapture()
	require.Equal(t, 2, archive.Poll())
	resp, ok = archive.controlPoller.Response()
	require.True(t, ok)
	require.Equal(t, archivewire.ControlResponseCode_ERROR, resp.Code)
	require.Equal(t, []byte("invalid extension"), resp.ErrorMessage)
	require.Equal(
		t,
		[]archivewire.RecordingSignal{
			archivewire.RecordingSignal_START,
			archivewire.RecordingSignal_STOP,
		},
		signals,
	)
}

// TestClientListRecording_usesDescriptorMode proves the list poll routes the OK
// control response to the descriptor poller, leaving the session control-response poller
// untouched.
func TestClientListRecording_usesDescriptorMode(t *testing.T) {
	archive, termBuffer := newClientForDemuxTest(t, nil)
	const correlationID int64 = 51515
	archive.descriptorPoller.ResetAwait(correlationID)

	responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    correlationID,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	writeUnfragmentedWireToTerm(t, termBuffer, responseWire)

	n := archive.pollListRecording()
	require.Equal(t, 1, n)

	resp, ok := archive.descriptorPoller.ControlResponse()
	require.True(t, ok)
	require.Equal(t, correlationID, resp.CorrelationID)
	require.False(t, archive.controlPoller.IsPollComplete())
}

// TestClientListRecording_forwardsSignalsDuringAwait proves list-recording polls
// deliver recording signals to the session handler while descriptor routing stays intact.
func TestClientListRecording_forwardsSignalsDuringAwait(t *testing.T) {
	h := newLifecycleHarness(t)
	var captured capturedRecordingSignal
	archive := &Client{
		controlSessionID:    controlResponseTestControlSessionID,
		proxy:               h.proxy,
		descriptorPoller:    NewRecordingDescriptorPoller(),
		signalHandler:       recordingSignalCapture(&captured),
		controlSubscription: h.sub,
		controlAssembler:    h.assembler,
		aeronClient:         h.client,
		cfg:                 h.cfg,
	}

	correlationID := h.client.NextCorrelationID() + 1
	responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    correlationID,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	descriptorWire := encodeRecordingDescriptorWire(t, codecs.RecordingDescriptorFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    correlationID,
		RecordingID:      lifecycleRecordingID,
		StartPosition:    4096,
		StopPosition:     8192,
		StrippedChannel:  []byte("aeron:ipc?alias=stripped"),
		OriginalChannel:  []byte("aeron:ipc?alias=original"),
		SourceIdentity:   []byte("source-identity"),
	})
	writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
	appendUnfragmentedWireToTerm(t, h.termBuffer, descriptorWire)
	appendUnfragmentedWireToTerm(
		t,
		h.termBuffer,
		recordingSignalWire(t, archivewire.RecordingSignal_EXTEND),
	)

	got, err := archive.ListRecording(t.Context(), lifecycleRecordingID)
	require.NoError(t, err)
	require.Equal(t, lifecycleRecordingID, got.RecordingID)
	require.Equal(t, 1, captured.calls)
	require.Equal(t, archivewire.RecordingSignal_EXTEND, captured.signal)
}

// TestAsyncListRecording_PollDescriptor_throughClientPoll asserts one
// Client.Poll batch sends a STOP signal to the session handler and the
// descriptor to an open AsyncListRecording. The control poller latches
// nothing.
func TestAsyncListRecording_PollDescriptor_throughClientPoll(t *testing.T) {
	h := newLifecycleHarness(t)
	client := h.newClient()
	var captured capturedRecordingSignal
	client.SetRecordingSignalHandler(recordingSignalCapture(&captured))

	handle := client.NewAsyncListRecording(lifecycleRecordingID)
	defer handle.Close()
	offered, err := handle.TryOffer()
	require.NoError(t, err)
	require.True(t, offered)

	signalWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    909,
		RecordingID:      lifecycleRecordingID,
		SubscriptionID:   2002,
		Position:         8192,
		Signal:           archivewire.RecordingSignal_STOP,
	})
	want := codecs.RecordingDescriptorFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    handle.correlationID,
		RecordingID:      lifecycleRecordingID,
		StartPosition:    4096,
		StopPosition:     8192,
		StrippedChannel:  []byte("aeron:ipc"),
		OriginalChannel:  []byte("aeron:ipc?alias=log"),
		SourceIdentity:   []byte("source-identity"),
	}
	writeUnfragmentedWireToTerm(t, h.termBuffer, signalWire)
	appendUnfragmentedWireToTerm(t, h.termBuffer, encodeRecordingDescriptorWire(t, want))

	require.Equal(t, 2, client.Poll())
	require.Equal(t, 1, captured.calls)
	require.Equal(t, archivewire.RecordingSignal_STOP, captured.signal)
	require.Equal(t, int64(8192), captured.position)

	done, got, err := handle.PollDescriptor()
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, want, got)
	require.False(t, client.controlPoller.IsPollComplete(), "control poller must latch nothing")
}

// TestAsyncStopRecordingByIdentity_PollResponse_throughClientPoll asserts one
// Client.Poll batch latches the OK for an open AsyncStopRecordingByIdentity.
// A STOP signal from another control session in the same batch reaches no
// handler.
func TestAsyncStopRecordingByIdentity_PollResponse_throughClientPoll(t *testing.T) {
	h := newLifecycleHarness(t)
	client := h.newClient()
	var captured capturedRecordingSignal
	client.SetRecordingSignalHandler(recordingSignalCapture(&captured))

	handle := client.NewAsyncStopRecordingByIdentity(lifecycleRecordingID)
	defer handle.Close()
	offered, err := handle.TryOffer()
	require.NoError(t, err)
	require.True(t, offered)

	signalWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID + 1,
		CorrelationID:    909,
		RecordingID:      lifecycleRecordingID,
		SubscriptionID:   2002,
		Position:         8192,
		Signal:           archivewire.RecordingSignal_STOP,
	})
	responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    handle.correlationID,
		RelevantID:       1,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	writeUnfragmentedWireToTerm(t, h.termBuffer, signalWire)
	appendUnfragmentedWireToTerm(t, h.termBuffer, responseWire)

	require.Equal(t, 2, client.Poll())
	require.Zero(t, captured.calls, "session handler must get no STOP from another session")

	done, err := handle.PollResponse()
	require.NoError(t, err)
	require.True(t, done)
	require.True(t, handle.IsStopped())
	require.False(t, client.controlPoller.IsPollComplete(), "matching OK must be consumed")
}

// TestClientStopRecordingAwaitSignal_routesSTOPToTracker proves the session method
// routes STOP signals to the per-call tracker without SetRecordingSignalHandler.
func TestClientStopRecordingAwaitSignal_routesSTOPToTracker(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := &Client{
		controlSessionID:    controlResponseTestControlSessionID,
		proxy:               h.proxy,
		controlPoller:       h.poller,
		descriptorPoller:    NewRecordingDescriptorPoller(),
		controlSubscription: h.sub,
		controlAssembler:    h.assembler,
		aeronClient:         h.client,
		cfg:                 h.cfg,
	}
	tracker := NewStopSignalTracker(controlResponseTestControlSessionID, lifecycleRecordingID)
	signalWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    909,
		RecordingID:      lifecycleRecordingID,
		SubscriptionID:   2002,
		Position:         3003,
		Signal:           archivewire.RecordingSignal_STOP,
	})
	appendUnfragmentedWireToTerm(t, h.termBuffer, signalWire)

	err := archive.StopRecordingAwaitSignal(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
		tracker,
	)
	require.NoError(t, err)
	require.True(t, tracker.Stopped())
	require.Equal(t, lifecycleRecordingID, tracker.StoppedRecordingID())
	require.Equal(t, int64(3003), tracker.StoppedPosition())
}

var clientAutoStopCases = []struct {
	name         string
	autoStop     bool
	wantAutoStop archivewire.BooleanType
}{
	{name: "autoStop_true", autoStop: true, wantAutoStop: archivewire.BooleanType_TRUE},
	{name: "autoStop_false", autoStop: false, wantAutoStop: archivewire.BooleanType_FALSE},
}

// TestClientExtendRecordingAwaitSignal_OKThenEXTEND asserts the public Client
// method maps bool autoStop to BooleanType, completes on EXTEND, and returns
// the requested id.
func TestClientExtendRecordingAwaitSignal_OKThenEXTEND(t *testing.T) {
	for _, tc := range clientAutoStopCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newLifecycleHarness(t)
			client := h.newClient()
			tracker := NewExtendSignalTracker(
				controlResponseTestControlSessionID,
				lifecycleRecordingID,
			)

			correlationID := h.client.NextCorrelationID() + 1
			responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
				ControlSessionID: controlResponseTestControlSessionID,
				CorrelationID:    correlationID,
				Code:             archivewire.ControlResponseCode_OK,
				Version:          520,
			})
			signalWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
				ControlSessionID: controlResponseTestControlSessionID,
				CorrelationID:    correlationID,
				RecordingID:      lifecycleRecordingID,
				SubscriptionID:   2002,
				Position:         3003,
				Signal:           archivewire.RecordingSignal_EXTEND,
			})
			writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
			appendUnfragmentedWireToTerm(t, h.termBuffer, signalWire)

			got, err := client.ExtendRecordingAwaitSignal(
				t.Context(),
				lifecycleRecordingID,
				archiveProxyTestRecordingStreamID,
				archivewire.SourceLocation_LOCAL,
				tc.autoStop,
				[]byte(lifecycleRecordingChannel),
				tracker,
			)
			require.NoError(t, err)
			require.Equal(t, lifecycleRecordingID, got)
			require.True(t, tracker.Extended())
			require.Equal(t, lifecycleRecordingID, tracker.ExtendedRecordingID())

			fields := requireProxyWireFields[codecs.ExtendRecordingRequest2Fields](
				t,
				h.pub.lastOffer(),
				archivewire.ExtendRecordingRequest2SbeTemplateID,
			)
			require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)
			require.Equal(t, correlationID, fields.CorrelationID)
			require.Equal(t, lifecycleRecordingID, fields.RecordingID)
			require.Equal(t, archiveProxyTestRecordingStreamID, fields.StreamID)
			require.Equal(t, archivewire.SourceLocation_LOCAL, fields.SourceLocation)
			require.Equal(t, tc.wantAutoStop, fields.AutoStop)
			require.Equal(t, []byte(lifecycleRecordingChannel), fields.Channel)
		})
	}
}

// TestClientExtendRecordingAwaitSignal_OKThenERROR asserts that an ERROR behind
// the OK fails the call. The await consumes the OK. The next poll latches the
// ERROR.
func TestClientExtendRecordingAwaitSignal_OKThenERROR(t *testing.T) {
	h := newLifecycleHarness(t)
	client := h.newClient()
	tracker := NewExtendSignalTracker(controlResponseTestControlSessionID, lifecycleRecordingID)

	correlationID := h.client.NextCorrelationID() + 1
	okWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    correlationID,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	errorWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    correlationID,
		RelevantID:       ErrorInvalidExtension,
		Code:             archivewire.ControlResponseCode_ERROR,
		Version:          520,
		ErrorMessage:     []byte("invalid extension"),
	})
	writeUnfragmentedWireToTerm(t, h.termBuffer, okWire)
	appendUnfragmentedWireToTerm(t, h.termBuffer, errorWire)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := client.ExtendRecordingAwaitSignal(
		ctx,
		lifecycleRecordingID,
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		false,
		[]byte(lifecycleRecordingChannel),
		tracker,
	)
	require.Error(t, err)
	code, ok := ExceptionCode(err)
	require.True(t, ok, "err=%v", err)
	require.Equal(t, int32(ErrorInvalidExtension), code)
	require.False(t, tracker.Extended())
}

func TestClientStartRecording_OK(t *testing.T) {
	const subscriptionID = int64(2002)

	for _, tc := range clientAutoStopCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newLifecycleHarness(t)
			client := h.newClient()

			correlationID := h.client.NextCorrelationID() + 1
			responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
				ControlSessionID: controlResponseTestControlSessionID,
				CorrelationID:    correlationID,
				RelevantID:       subscriptionID,
				Code:             archivewire.ControlResponseCode_OK,
				Version:          520,
			})
			writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)

			got, err := client.StartRecording(
				t.Context(),
				archiveProxyTestRecordingStreamID,
				archivewire.SourceLocation_LOCAL,
				tc.autoStop,
				[]byte(lifecycleRecordingChannel),
			)
			require.NoError(t, err)
			require.Equal(t, subscriptionID, got)

			fields := requireProxyWireFields[codecs.StartRecordingRequest2Fields](
				t,
				h.pub.lastOffer(),
				archivewire.StartRecordingRequest2SbeTemplateID,
			)
			require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)
			require.Equal(t, correlationID, fields.CorrelationID)
			require.Equal(t, archiveProxyTestRecordingStreamID, fields.StreamID)
			require.Equal(t, archivewire.SourceLocation_LOCAL, fields.SourceLocation)
			require.Equal(t, tc.wantAutoStop, fields.AutoStop)
			require.Equal(t, []byte(lifecycleRecordingChannel), fields.Channel)
		})
	}
}

func TestClientReplicate_OK(t *testing.T) {
	h := newLifecycleHarness(t)
	client := h.newClient()
	params := NewReplicationParams()
	params.DestinationRecordingID = lifecycleRecordingID
	params.StopPosition = 8192
	params.ReplicationChannel = []byte("aeron:udp?endpoint=127.0.0.1:0")
	params.ChannelTagID = 3
	params.SubscriptionTagID = 4
	params.FileIOMaxLength = 4096
	params.ReplicationSessionID = 9
	params.EncodedCredentials = []byte("credentials")
	params.SourceResponseChannel = []byte("aeron:udp?endpoint=127.0.0.1:0|control-mode=response")

	correlationID := h.writeNextControlResponse(
		t,
		asyncReplicateTestReplicationID,
		archivewire.ControlResponseCode_OK,
	)

	got, err := client.Replicate(
		t.Context(),
		asyncReplicateTestSourceRecordingID,
		asyncReplicateTestSourceControlStreamID,
		[]byte(asyncReplicateTestSourceControlChannel),
		params,
	)
	require.NoError(t, err)
	require.Equal(t, asyncReplicateTestReplicationID, got)
	requireOfferedReplicateWire(t, h.pub.lastOffer(), correlationID, params)
}

func TestClientReplicate_errorResponse(t *testing.T) {
	cases := []struct {
		name string
		code int64
	}{
		{name: "unknown_recording", code: ErrorUnknownRecording},
		{name: "active_recording", code: ErrorActiveRecording},
		{name: "generic", code: ErrorGeneric},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newLifecycleHarness(t)
			client := h.newClient()
			h.writeNextControlResponse(t, tc.code, archivewire.ControlResponseCode_ERROR)

			got, err := client.Replicate(
				t.Context(),
				asyncReplicateTestSourceRecordingID,
				asyncReplicateTestSourceControlStreamID,
				[]byte(asyncReplicateTestSourceControlChannel),
				NewReplicationParams(),
			)
			require.Zero(t, got)
			code, ok := ExceptionCode(err)
			require.True(t, ok, "err=%v", err)
			require.Equal(t, int32(tc.code), code)
		})
	}
}

func TestClientReplicate_invalidParams(t *testing.T) {
	h := newLifecycleHarness(t)
	client := h.newClient()

	_, err := client.Replicate(
		t.Context(),
		asyncReplicateTestSourceRecordingID,
		asyncReplicateTestSourceControlStreamID,
		[]byte(asyncReplicateTestSourceControlChannel),
		ReplicationParams{},
	)
	require.ErrorIs(t, err, ErrInvalidReplicationParams)
	require.Empty(t, h.pub.offered, "invalid params must not reach the publication")
}

func TestClientStopReplication(t *testing.T) {
	cases := []struct {
		name        string
		code        archivewire.ControlResponseCode
		relevantID  int64
		wantStopped bool
		wantErrCode int32
		wantErr     bool
	}{
		{
			name:        "ok_relevant_id_zero",
			code:        archivewire.ControlResponseCode_OK,
			wantStopped: true,
		},
		{
			name:       "unknown_replication",
			code:       archivewire.ControlResponseCode_ERROR,
			relevantID: ErrorUnknownReplication,
		},
		{
			name:        "generic_error",
			code:        archivewire.ControlResponseCode_ERROR,
			relevantID:  ErrorGeneric,
			wantErr:     true,
			wantErrCode: ErrorGeneric,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newLifecycleHarness(t)
			client := h.newClient()
			correlationID := h.writeNextControlResponse(t, tc.relevantID, tc.code)

			stopped, err := client.StopReplication(t.Context(), asyncReplicateTestReplicationID)
			require.Equal(t, tc.wantStopped, stopped)
			if tc.wantErr {
				code, ok := ExceptionCode(err)
				require.True(t, ok, "err=%v", err)
				require.Equal(t, tc.wantErrCode, code)
			} else {
				require.NoError(t, err)
			}

			fields := requireProxyWireFields[codecs.StopReplicationRequestFields](
				t,
				h.pub.lastOffer(),
				archivewire.StopReplicationRequestSbeTemplateID,
			)
			require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)
			require.Equal(t, correlationID, fields.CorrelationID)
			require.Equal(t, asyncReplicateTestReplicationID, fields.ReplicationID)
		})
	}
}

func TestClientStartRecordingAwaitSignal_OKThenSTART(t *testing.T) {
	for _, tc := range clientAutoStopCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newLifecycleHarness(t)
			client := h.newClient()
			tracker := NewStartSignalTracker(controlResponseTestControlSessionID)

			correlationID := h.client.NextCorrelationID() + 1
			responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
				ControlSessionID: controlResponseTestControlSessionID,
				CorrelationID:    correlationID,
				Code:             archivewire.ControlResponseCode_OK,
				Version:          520,
			})
			signalWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
				ControlSessionID: controlResponseTestControlSessionID,
				CorrelationID:    correlationID,
				RecordingID:      lifecycleRecordingID,
				SubscriptionID:   2002,
				Position:         0,
				Signal:           archivewire.RecordingSignal_START,
			})
			writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
			appendUnfragmentedWireToTerm(t, h.termBuffer, signalWire)

			got, err := client.StartRecordingAwaitSignal(
				t.Context(),
				archiveProxyTestRecordingStreamID,
				archivewire.SourceLocation_LOCAL,
				tc.autoStop,
				[]byte(lifecycleRecordingChannel),
				tracker,
			)
			require.NoError(t, err)
			require.Equal(t, lifecycleRecordingID, got)
			require.True(t, tracker.Started())
			require.Equal(t, lifecycleRecordingID, tracker.StartedRecordingID())

			fields := requireProxyWireFields[codecs.StartRecordingRequest2Fields](
				t,
				h.pub.lastOffer(),
				archivewire.StartRecordingRequest2SbeTemplateID,
			)
			require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)
			require.Equal(t, correlationID, fields.CorrelationID)
			require.Equal(t, archiveProxyTestRecordingStreamID, fields.StreamID)
			require.Equal(t, archivewire.SourceLocation_LOCAL, fields.SourceLocation)
			require.Equal(t, tc.wantAutoStop, fields.AutoStop)
			require.Equal(t, []byte(lifecycleRecordingChannel), fields.Channel)
		})
	}
}

// TestClientListRecording_forwardsSignalsToPendingTracker asserts the
// ListRecording poll picks its signal target like Poll. The session handler
// and the open handle's tracker each see START once.
func TestClientListRecording_forwardsSignalsToPendingTracker(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	var captured capturedRecordingSignal
	f.client.SetRecordingSignalHandler(recordingSignalCapture(&captured))

	listCorrelationID := f.harness.writeNextControlResponse(
		t,
		0,
		archivewire.ControlResponseCode_OK,
	)
	descriptorWire := encodeRecordingDescriptorWire(t, codecs.RecordingDescriptorFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    listCorrelationID,
		RecordingID:      lifecycleRecordingID,
		StartPosition:    4096,
		StopPosition:     8192,
	})
	appendUnfragmentedWireToTerm(t, f.harness.termBuffer, descriptorWire)
	appendUnfragmentedWireToTerm(
		t,
		f.harness.termBuffer,
		startWire(t, f.handle),
	)

	got, err := f.client.ListRecording(t.Context(), lifecycleRecordingID)
	require.NoError(t, err)
	require.Equal(t, lifecycleRecordingID, got.RecordingID)
	require.Equal(t, 1, captured.calls)
	require.Equal(t, archivewire.RecordingSignal_START, captured.signal)
	require.True(t, f.handle.tracker.Started())
	requirePollSignal(t, f.handle, true)
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())
}

// TestClientStartRecordingAwaitSignal_bypassesPendingTracker asserts a
// blocking *AwaitSignal call routes signals to its own tracker only. The open
// handle keeps the slot for later Poll calls.
func TestClientStartRecordingAwaitSignal_bypassesPendingTracker(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	var captured capturedRecordingSignal
	f.client.SetRecordingSignalHandler(recordingSignalCapture(&captured))
	awaitRecordingID := lifecycleRecordingID + 1
	startWire := startWire(t, f.handle)

	// Phase: the blocking call sees its own START only.
	awaitCorrelationID := f.harness.writeNextControlResponse(
		t,
		0,
		archivewire.ControlResponseCode_OK,
	)
	awaitSignalWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    awaitCorrelationID,
		RecordingID:      awaitRecordingID,
		Signal:           archivewire.RecordingSignal_START,
	})
	appendUnfragmentedWireToTerm(t, f.harness.termBuffer, startWire)
	appendUnfragmentedWireToTerm(t, f.harness.termBuffer, awaitSignalWire)

	tracker := NewStartSignalTracker(controlResponseTestControlSessionID)
	got, err := f.client.StartRecordingAwaitSignal(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		true,
		[]byte(lifecycleRecordingChannel),
		tracker,
	)
	require.NoError(t, err)
	require.Equal(t, awaitRecordingID, got)
	require.Zero(t, captured.calls, "session handler must not see signals during the await")
	require.False(t, f.handle.tracker.Started(), "open handle must not see signals during the await")
	require.Same(t, &f.handle.tracker, f.client.pendingSignalTracker)

	// Phase: Poll feeds the slot again.
	f.deliver(t, startWire)
	require.Equal(t, 1, captured.calls)
	requirePollSignal(t, f.handle, true)
	require.Equal(t, lifecycleRecordingID, f.handle.RecordingID())
}

// TestClientPoll_fullSlot_ZeroAllocs asserts Client.Poll stays zero-alloc
// while an open handle holds the slot. The fixture holds one signal, so the
// check measures empty polls.
// TestHandleControlFragment_DispatchSTARTThroughFanout_ZeroAllocs covers the
// fanout dispatch.
func TestClientPoll_fullSlot_ZeroAllocs(t *testing.T) {
	f := newAsyncStartRecordingFixture(t)
	var captured capturedRecordingSignal
	f.client.SetRecordingSignalHandler(recordingSignalCapture(&captured))

	f.deliver(t, startWire(t, f.handle))
	require.Equal(t, 1, captured.calls)
	require.True(t, f.handle.tracker.Started())

	testutil.AssertZeroAllocs(t, "Client.Poll", 100, func() {
		f.client.Poll()
	})
}

// TestClientPoll_replicateHandle_ZeroAllocs asserts Client.Poll stays
// zero-alloc with an AsyncReplicate as session handler. Each measured poll
// folds signals of the replication into the handle.
func TestClientPoll_replicateHandle_ZeroAllocs(t *testing.T) {
	const runs = 10
	cases := []struct {
		name       string
		code       archivewire.ControlResponseCode
		relevantID int64
	}{
		{
			name:       "ok_latched_then_consumed",
			code:       archivewire.ControlResponseCode_OK,
			relevantID: asyncReplicateTestReplicationID,
		},
		{
			name:       "error_latched",
			code:       archivewire.ControlResponseCode_ERROR,
			relevantID: ErrorGeneric,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncReplicateFixture(t)
			f.client.SetRecordingSignalHandler(f.handle)
			offered, err := f.handle.TryOffer()
			require.NoError(t, err)
			require.True(t, offered)

			appendUnfragmentedWireToTerm(
				t,
				f.h.termBuffer,
				encodeControlResponseWire(t, f.responseFor(f.handle, tc.code, tc.relevantID)),
			)
			extend := encodeRecordingSignalEventWire(
				t,
				replicationSignal(archivewire.RecordingSignal_EXTEND),
			)
			signalCount := 2 * (runs + 1) * DefaultControlResponseFragmentLimit
			for range signalCount {
				appendUnfragmentedWireToTerm(t, f.h.termBuffer, extend)
			}

			// Phase: the response latches. Measured polls pass signals behind it.
			require.Equal(t, DefaultControlResponseFragmentLimit, f.client.Poll())
			require.True(t, f.h.poller.IsPollComplete())
			testutil.AssertZeroAllocs(t, "Client.Poll with latched response", runs, func() {
				f.client.Poll()
				f.handle.ReplicationID()
			})
			require.True(t, f.h.poller.IsPollComplete(), "armed filter keeps the latch")

			if tc.code == archivewire.ControlResponseCode_ERROR {
				require.False(t, f.handle.HasExtended(), "a handle with an ERROR does not fold a signal")
				require.Equal(t, cnc.NullValue, f.handle.ReplicationID())
				return
			}
			require.True(t, f.handle.HasExtended())

			// Phase: PollResponse consumes the OK. Measured polls keep folding.
			done, err := f.handle.PollResponse()
			require.NoError(t, err)
			require.True(t, done)
			testutil.AssertZeroAllocs(t, "Client.Poll after consumed OK", runs, func() {
				f.client.Poll()
			})
			require.Equal(t, asyncReplicateTestReplicationID, f.handle.ReplicationID())
		})
	}
}

// TestClientPoll_ownedTransport_ZeroAllocs asserts Client.Poll stays
// zero-alloc with a transport fault monitor that holds no pending mark.
func TestClientPoll_ownedTransport_ZeroAllocs(t *testing.T) {
	client, _ := newClientForDemuxTest(t, nil)
	client.transportFault = &transportfault.Monitor{}

	testutil.AssertZeroAllocs(t, "Client.Poll", 100, func() {
		client.Poll()
	})
	require.False(t, client.IsClosed())
}

// TestClientClose_releasesResourcesAndIsIdempotent proves Close offers one
// best-effort CloseSessionRequest, closes the control publication and subscription,
// closes the Connect-owned client, and no-ops on a second call.
func TestClientClose_releasesResourcesAndIsIdempotent(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := &Client{
		controlSessionID:    controlResponseTestControlSessionID,
		proxy:               h.proxy,
		controlSubscription: h.sub,
		controlAssembler:    h.assembler,
		aeronClient:         h.client,
		ownsAeronClient:     true,
		cfg:                 h.cfg,
	}

	require.NoError(t, archive.Close())

	require.True(t, archive.IsClosed())
	require.True(t, h.pub.closed)
	require.True(t, h.sub.IsClosed())
	require.Nil(t, archive.controlSubscription)
	require.Nil(t, archive.aeronClient)

	require.Len(t, h.pub.offered, 1)
	fields := requireProxyWireFields[codecs.CloseSessionRequestFields](
		t,
		h.pub.offered[0],
		archivewire.CloseSessionRequestSbeTemplateID,
	)
	require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)

	require.NoError(t, archive.Close())
	require.Len(t, h.pub.offered, 1)
	require.True(t, archive.IsClosed())
}

// TestClientClose_leavesUserSuppliedClientOpen proves Close does not close a client
// the caller supplied via Config.Aeron (ownsAeronClient false).
func TestClientClose_leavesUserSuppliedClientOpen(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := &Client{
		controlSessionID: controlResponseTestControlSessionID,
		proxy:            h.proxy,
		controlAssembler: h.assembler,
		aeronClient:      h.client,
		ownsAeronClient:  false,
		cfg:              h.cfg,
	}

	require.NoError(t, archive.Close())

	require.True(t, archive.IsClosed())
	require.Same(t, h.client, archive.aeronClient)
}
