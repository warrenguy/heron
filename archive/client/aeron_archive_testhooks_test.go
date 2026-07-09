//go:build testhooks

package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestNewAeronArchive_transfersOwnsAeronClient(t *testing.T) {
	t.Parallel()

	shared := newAeronArchive(&ConnectResult{
		OwnsAeronClient: false,
	})
	require.False(t, shared.ownsAeronClient)

	owned := newAeronArchive(&ConnectResult{
		OwnsAeronClient: true,
	})
	require.True(t, owned.ownsAeronClient)
}

func TestNewAeronArchive_wiresContextRecordingSignalHandler(t *testing.T) {
	var gotSignal archivewire.RecordingSignal
	handlers := &RecordingSignalHandlers{
		SignalFunc: func(event codecs.RecordingSignalEventFields) {
			gotSignal = event.Signal
		},
	}

	sub, termBuffer := setupControlResponsePollFixtures(t)
	archive := newAeronArchive(&ConnectResult{
		ControlSessionID:    controlResponseTestControlSessionID,
		Poller:              newControlResponsePollerForTest(sub),
		ControlSubscription: sub,
		Context: ArchiveContext{
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

func newAeronArchiveForDemuxTest(
	t *testing.T,
	handler RecordingSignalHandler,
) (*AeronArchive, atomicbuffer.AtomicBuffer) {
	t.Helper()

	sub, termBuffer := setupControlResponsePollFixtures(t)
	archive := &AeronArchive{
		controlSessionID:    controlResponseTestControlSessionID,
		controlPoller:       newControlResponsePollerForTest(sub),
		descriptorPoller:    NewRecordingDescriptorPoller(),
		signalHandler:       handler,
		controlSubscription: sub,
		controlAssembler:    NewFragmentAssembler(nil),
	}
	return archive, termBuffer
}

// TestAeronArchive_handoffClearsConnectLatch proves post-connect Poll captures a new
// control response after the connect latch is cleared at session construction.
func TestAeronArchive_handoffClearsConnectLatch(t *testing.T) {
	archive, termBuffer := newAeronArchiveForDemuxTest(t, nil)
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

// TestAeronArchivePoll_routesThroughDemux proves Poll multiplexes one subscription: a
// control response routes to the session poller and a recording signal to the handler in
// a single poll, while the descriptor poller stays idle.
func TestAeronArchivePoll_routesThroughDemux(t *testing.T) {
	var captured capturedRecordingSignal
	archive, termBuffer := newAeronArchiveForDemuxTest(t, recordingSignalCapture(&captured))

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

// TestAeronArchiveListRecording_usesDescriptorMode proves the list poll routes the OK
// control response to the descriptor poller, leaving the session control-response poller
// untouched.
func TestAeronArchiveListRecording_usesDescriptorMode(t *testing.T) {
	archive, termBuffer := newAeronArchiveForDemuxTest(t, nil)
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

// TestAeronArchiveListRecording_forwardsSignalsDuringAwait proves list-recording polls
// deliver recording signals to the session handler while descriptor routing stays intact.
func TestAeronArchiveListRecording_forwardsSignalsDuringAwait(t *testing.T) {
	h := newLifecycleHarness(t)
	var captured capturedRecordingSignal
	archive := &AeronArchive{
		controlSessionID:    controlResponseTestControlSessionID,
		proxy:               h.proxy,
		descriptorPoller:    NewRecordingDescriptorPoller(),
		signalHandler:       recordingSignalCapture(&captured),
		controlSubscription: h.sub,
		controlAssembler:    h.assembler,
		aeronClient:         h.client,
		ctx:                 h.ctx,
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

// TestAeronArchiveStopRecordingAwaitSignal_routesSTOPToTracker proves the session method
// routes STOP signals to the per-call tracker without SetRecordingSignalHandler.
func TestAeronArchiveStopRecordingAwaitSignal_routesSTOPToTracker(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := &AeronArchive{
		controlSessionID:    controlResponseTestControlSessionID,
		proxy:               h.proxy,
		controlPoller:       h.poller,
		descriptorPoller:    NewRecordingDescriptorPoller(),
		controlSubscription: h.sub,
		controlAssembler:    h.assembler,
		aeronClient:         h.client,
		ctx:                 h.ctx,
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

// TestAeronArchiveClose_releasesResourcesAndIsIdempotent proves Close offers one
// best-effort CloseSessionRequest, closes the control publication and subscription,
// closes the Connect-owned client, and no-ops on a second call.
func TestAeronArchiveClose_releasesResourcesAndIsIdempotent(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := &AeronArchive{
		controlSessionID:    controlResponseTestControlSessionID,
		proxy:               h.proxy,
		controlSubscription: h.sub,
		controlAssembler:    h.assembler,
		aeronClient:         h.client,
		ownsAeronClient:     true,
		ctx:                 h.ctx,
	}

	require.NoError(t, archive.Close())

	require.True(t, archive.IsClosed())
	require.True(t, h.pub.closed)
	require.True(t, h.sub.IsClosed())
	require.Nil(t, archive.controlSubscription)
	require.Nil(t, archive.aeronClient)

	require.Len(t, h.pub.offered, 1)
	fields := requireArchiveProxyWireFields[codecs.CloseSessionRequestFields](
		t,
		h.pub.offered[0],
		archivewire.CloseSessionRequestSbeTemplateID,
	)
	require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)

	require.NoError(t, archive.Close())
	require.Len(t, h.pub.offered, 1)
	require.True(t, archive.IsClosed())
}

// TestAeronArchiveClose_leavesUserSuppliedClientOpen proves Close does not close a client
// the caller supplied via ArchiveContext.Aeron (ownsAeronClient false).
func TestAeronArchiveClose_leavesUserSuppliedClientOpen(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := &AeronArchive{
		controlSessionID: controlResponseTestControlSessionID,
		proxy:            h.proxy,
		controlAssembler: h.assembler,
		aeronClient:      h.client,
		ownsAeronClient:  false,
		ctx:              h.ctx,
	}

	require.NoError(t, archive.Close())

	require.True(t, archive.IsClosed())
	require.Same(t, h.client, archive.aeronClient)
}
