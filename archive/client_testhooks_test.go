//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestNewClient_transfersOwnsAeronClient(t *testing.T) {
	t.Parallel()

	shared := newClient(&ConnectResult{
		OwnsAeronClient: false,
	})
	require.False(t, shared.ownsAeronClient)

	owned := newClient(&ConnectResult{
		OwnsAeronClient: true,
	})
	require.True(t, owned.ownsAeronClient)
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

// TestClientPoll_nilSessionHandlerFullSlot asserts Poll feeds the tracker of
// an open handle when Client has no session handler.
func TestClientPoll_nilSessionHandlerFullSlot(t *testing.T) {
	h := newLifecycleHarness(t)
	client := h.newClient()
	require.Nil(t, client.signalHandler)
	handle := newAsyncStartRecordingForTest(client)

	appendUnfragmentedWireToTerm(
		t,
		h.termBuffer,
		startSignalWireFor(t, handle, lifecycleRecordingID),
	)
	require.Equal(t, 1, client.Poll())
	require.True(t, handle.tracker.Started())
	requirePollSignal(t, handle, true)
	require.Equal(t, lifecycleRecordingID, handle.RecordingID())

	handle.Close()
	require.Nil(t, client.signalTarget(), "empty slot and nil handler must skip the decode")
}

// TestClientListRecording_forwardsSignalsToPendingTracker asserts the
// ListRecording poll picks its signal target like Poll. The session handler
// and the open handle's tracker each see START once.
func TestClientListRecording_forwardsSignalsToPendingTracker(t *testing.T) {
	h := newLifecycleHarness(t)
	client := h.newClient()
	var captured capturedRecordingSignal
	client.SetRecordingSignalHandler(recordingSignalCapture(&captured))
	handle := newAsyncStartRecordingForTest(client)

	listCorrelationID := h.client.NextCorrelationID() + 1
	responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    listCorrelationID,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	descriptorWire := encodeRecordingDescriptorWire(t, codecs.RecordingDescriptorFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    listCorrelationID,
		RecordingID:      lifecycleRecordingID,
		StartPosition:    4096,
		StopPosition:     8192,
	})
	writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
	appendUnfragmentedWireToTerm(t, h.termBuffer, descriptorWire)
	appendUnfragmentedWireToTerm(
		t,
		h.termBuffer,
		startSignalWireFor(t, handle, lifecycleRecordingID),
	)

	got, err := client.ListRecording(t.Context(), lifecycleRecordingID)
	require.NoError(t, err)
	require.Equal(t, lifecycleRecordingID, got.RecordingID)
	require.Equal(t, 1, captured.calls)
	require.Equal(t, archivewire.RecordingSignal_START, captured.signal)
	require.True(t, handle.tracker.Started())
	requirePollSignal(t, handle, true)
	require.Equal(t, lifecycleRecordingID, handle.RecordingID())
}

// TestClientStartRecordingAwaitSignal_bypassesPendingTracker asserts a
// blocking *AwaitSignal call routes signals to its own tracker only. The open
// handle keeps the slot for later Poll calls.
func TestClientStartRecordingAwaitSignal_bypassesPendingTracker(t *testing.T) {
	h := newLifecycleHarness(t)
	client := h.newClient()
	var captured capturedRecordingSignal
	client.SetRecordingSignalHandler(recordingSignalCapture(&captured))
	handle := newAsyncStartRecordingForTest(client)
	awaitRecordingID := lifecycleRecordingID + 1

	// Phase: the blocking call sees its own START only.
	awaitCorrelationID := h.client.NextCorrelationID() + 1
	responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    awaitCorrelationID,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	awaitSignalWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    awaitCorrelationID,
		RecordingID:      awaitRecordingID,
		Signal:           archivewire.RecordingSignal_START,
	})
	writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
	appendUnfragmentedWireToTerm(
		t,
		h.termBuffer,
		startSignalWireFor(t, handle, lifecycleRecordingID),
	)
	appendUnfragmentedWireToTerm(t, h.termBuffer, awaitSignalWire)

	tracker := NewStartSignalTracker(controlResponseTestControlSessionID)
	got, err := client.StartRecordingAwaitSignal(
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
	require.False(t, handle.tracker.Started(), "open handle must not see signals during the await")
	require.Same(t, handle.tracker, client.pendingSignalTracker)

	// Phase: Poll feeds the slot again.
	appendUnfragmentedWireToTerm(
		t,
		h.termBuffer,
		startSignalWireFor(t, handle, lifecycleRecordingID),
	)
	require.Equal(t, 1, client.Poll())
	require.Equal(t, 1, captured.calls)
	requirePollSignal(t, handle, true)
	require.Equal(t, lifecycleRecordingID, handle.RecordingID())
}

// TestClientPoll_fullSlot_ZeroAllocs asserts Client.Poll stays zero-alloc
// while an open handle holds the slot. The fixture holds one signal, so the
// check measures empty polls.
// TestHandleControlFragment_DispatchSTARTThroughFanout_ZeroAllocs covers the
// fanout dispatch.
func TestClientPoll_fullSlot_ZeroAllocs(t *testing.T) {
	h := newLifecycleHarness(t)
	client := h.newClient()
	var captured capturedRecordingSignal
	client.SetRecordingSignalHandler(recordingSignalCapture(&captured))
	handle := newAsyncStartRecordingForTest(client)
	appendUnfragmentedWireToTerm(
		t,
		h.termBuffer,
		startSignalWireFor(t, handle, lifecycleRecordingID),
	)

	require.Equal(t, 1, client.Poll())
	require.Equal(t, 1, captured.calls)
	require.True(t, handle.tracker.Started())

	testutil.AssertZeroAllocs(t, "Client.Poll", 100, func() {
		client.Poll()
	})
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
