//go:build testhooks

package client

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	agatomic "github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

const (
	lifecycleMessageTimeout         = 500 * time.Millisecond
	lifecycleRecordingID      int64 = 5150
	lifecycleArchiveID        int64 = 919191
	lifecycleRecordingChannel       = "aeron:ipc"
)

type lifecycleHarness struct {
	client     *aeron.Client
	nowNs      *atomic.Int64
	sub        *aeron.Subscription
	termBuffer agatomic.AtomicBuffer
	poller     *ControlResponsePoller
	assembler  *FragmentAssembler
	pub        *stubControlPublication
	proxy      *ArchiveProxy
	ctx        ArchiveContext
}

func newLifecycleHarness(t *testing.T) *lifecycleHarness {
	t.Helper()

	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
	sub, termBuffer := setupControlResponsePollFixtures(t)
	ctx := ArchiveContext{
		MessageTimeout: lifecycleMessageTimeout,
		IdleStrategy:   NewDefaultIdleStrategy(),
		AeronContext: aeron.Context{
			NanoClock: func() int64 { return nowNs.Load() },
		},
	}
	pub, proxy := newTestArchiveProxy(ctx)
	return &lifecycleHarness{
		client:     harness.Client,
		nowNs:      nowNs,
		sub:        sub,
		termBuffer: termBuffer,
		poller:     newControlResponsePollerForTest(sub),
		assembler:  NewFragmentAssembler(nil),
		pub:        pub,
		proxy:      proxy,
		ctx:        ctx,
	}
}

func (h *lifecycleHarness) advanceClock() {
	h.nowNs.Add(int64(h.ctx.MessageTimeout) / 8)
}

func (h *lifecycleHarness) demuxPoll(targets ArchiveControlPollTargets) int {
	return PollArchiveControlEvents(h.sub, DefaultControlResponseFragmentLimit, h.assembler, targets)
}

func (h *lifecycleHarness) responseTargets() ArchiveControlPollTargets {
	return ArchiveControlPollTargets{
		ResponsePoller:   h.poller,
		ControlSessionID: controlResponseTestControlSessionID,
	}
}

func (h *lifecycleHarness) signalTargets(handler RecordingSignalHandler) ArchiveControlPollTargets {
	return ArchiveControlPollTargets{
		SignalHandler:    handler,
		ControlSessionID: controlResponseTestControlSessionID,
		ResponsePoller:   h.poller,
	}
}

func (h *lifecycleHarness) controlOps(pollFn func() int) archiveControlOps {
	return archiveControlOps{
		aeronClient:      h.client,
		ctx:              h.ctx,
		proxy:            h.proxy,
		poll:             pollFn,
		responsePoller:   h.poller,
		controlSessionID: controlResponseTestControlSessionID,
	}
}

func offeredArchiveCorrelationID(t *testing.T, wire []byte) int64 {
	t.Helper()

	decoded, ok := codecs.TryDecodeByTemplate(wire, 0, codecs.ArchiveDecodeMaxVarLen)
	require.True(t, ok)
	switch fields := decoded.Fields.(type) {
	case codecs.StartRecordingRequestFields:
		return fields.CorrelationID
	case codecs.StopRecordingRequestFields:
		return fields.CorrelationID
	case codecs.ListRecordingRequestFields:
		return fields.CorrelationID
	case codecs.ReplayRequestFields:
		return fields.CorrelationID
	case codecs.StopReplayRequestFields:
		return fields.CorrelationID
	default:
		require.Failf(t, "unexpected offered template", "template=%d", decoded.TemplateID)
		return 0
	}
}

func (h *lifecycleHarness) listRecordingOps(
	pollFn func() int,
	descriptorPoller *RecordingDescriptorPoller,
) archiveControlOps {
	ops := h.controlOps(pollFn)
	ops.descriptorPoller = descriptorPoller
	return ops
}

// respondListRecordingOnce writes list-recording wire the first time the request has been
// offered, then polls descriptor-mode targets. withOK adds a preceding OK ControlResponse.
func (h *lifecycleHarness) respondListRecordingOnce(
	t *testing.T,
	descriptor codecs.RecordingDescriptorFields,
	withOK bool,
	written *bool,
	targets ArchiveControlPollTargets,
) func() int {
	return func() int {
		if !*written {
			if offered := h.pub.lastOffer(); offered != nil {
				correlationID := offeredArchiveCorrelationID(t, offered)
				if withOK {
					responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
						ControlSessionID: controlResponseTestControlSessionID,
						CorrelationID:    correlationID,
						Code:             gensbe.ControlResponseCode_OK,
						Version:          520,
					})
					writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
				}
				descriptor.ControlSessionID = controlResponseTestControlSessionID
				descriptor.CorrelationID = correlationID
				appendUnfragmentedWireToTerm(
					t,
					h.termBuffer,
					encodeRecordingDescriptorWire(t, descriptor),
				)
				*written = true
			}
		}
		h.advanceClock()
		return h.demuxPoll(targets)
	}
}

func TestListRecording_nilDescriptorPollerReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)

	_, err := h.controlOps(func() int { return 0 }).listRecording(lifecycleRecordingID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-nil")
}

func TestListRecording_descriptorOnlyCompletes(t *testing.T) {
	h := newLifecycleHarness(t)
	descriptorPoller := NewRecordingDescriptorPoller()
	targets := ArchiveControlPollTargets{DescriptorPoller: descriptorPoller}
	written := false
	pollFn := h.respondListRecordingOnce(
		t,
		codecs.RecordingDescriptorFields{
			RecordingID:   lifecycleRecordingID,
			StartPosition: 0,
			StopPosition:  2048,
			StreamID:      archiveProxyTestRecordingStreamID,
		},
		false,
		&written,
		targets,
	)

	got, err := h.listRecordingOps(pollFn, descriptorPoller).listRecording(lifecycleRecordingID)
	require.NoError(t, err)
	require.Equal(t, lifecycleRecordingID, got.RecordingID)
	require.Equal(t, int64(0), got.StartPosition)
	require.Equal(t, int64(2048), got.StopPosition)
	_, ok := descriptorPoller.ControlResponse()
	require.False(t, ok, "descriptor-only list must not latch a ControlResponse")
}

func TestListRecording_returnsExtents(t *testing.T) {
	h := newLifecycleHarness(t)
	descriptorPoller := NewRecordingDescriptorPoller()
	targets := ArchiveControlPollTargets{DescriptorPoller: descriptorPoller}
	written := false
	pollFn := h.respondListRecordingOnce(
		t,
		codecs.RecordingDescriptorFields{
			RecordingID:      lifecycleRecordingID,
			StartPosition:    4096,
			StopPosition:     8192,
			InitialTermID:    7,
			TermBufferLength: 1 << 16,
			MtuLength:        1408,
			SessionID:        13,
			StreamID:         archiveProxyTestRecordingStreamID,
			StrippedChannel:  []byte("aeron:ipc?alias=stripped"),
			OriginalChannel:  []byte("aeron:ipc?alias=original"),
			SourceIdentity:   []byte("source-identity"),
		},
		true,
		&written,
		targets,
	)

	got, err := h.listRecordingOps(pollFn, descriptorPoller).listRecording(lifecycleRecordingID)
	require.NoError(t, err)
	require.Equal(t, lifecycleRecordingID, got.RecordingID)
	require.Equal(t, int64(4096), got.StartPosition)
	require.Equal(t, int64(8192), got.StopPosition)
	require.Equal(t, int32(7), got.InitialTermID)
	require.Equal(t, int32(1<<16), got.TermBufferLength)
	require.Equal(t, int32(1408), got.MtuLength)
	require.Equal(t, int32(13), got.SessionID)
	require.Equal(t, archiveProxyTestRecordingStreamID, got.StreamID)
	require.Equal(t, "aeron:ipc?alias=stripped", string(got.StrippedChannel))
	require.Equal(t, "aeron:ipc?alias=original", string(got.OriginalChannel))
	require.Equal(t, "source-identity", string(got.SourceIdentity))
}

func TestListRecording_retainedDescriptorBytesValidAfterPoll(t *testing.T) {
	h := newLifecycleHarness(t)
	descriptorPoller := NewRecordingDescriptorPoller()
	targets := ArchiveControlPollTargets{DescriptorPoller: descriptorPoller}
	const (
		strippedChannel = "aeron:ipc?alias=retained-stripped"
		originalChannel = "aeron:ipc?alias=retained-original"
		sourceIdentity  = "retained-source-identity"
	)
	written := false
	pollFn := h.respondListRecordingOnce(
		t,
		codecs.RecordingDescriptorFields{
			RecordingID:     lifecycleRecordingID,
			StrippedChannel: []byte(strippedChannel),
			OriginalChannel: []byte(originalChannel),
			SourceIdentity:  []byte(sourceIdentity),
		},
		true,
		&written,
		targets,
	)

	got, err := h.listRecordingOps(pollFn, descriptorPoller).listRecording(lifecycleRecordingID)
	require.NoError(t, err)
	require.NotEmpty(t, got.StrippedChannel)
	require.Equal(t, strippedChannel, string(got.StrippedChannel))

	// Overwrite the source term buffer where the descriptor was written; the returned
	// descriptor must retain its own copy of the var-ascii bytes.
	overwrite := make([]byte, 512)
	for i := range overwrite {
		overwrite[i] = 0x5A
	}
	h.termBuffer.PutBytes(0, overwrite)

	require.Equal(t, strippedChannel, string(got.StrippedChannel))
	require.Equal(t, originalChannel, string(got.OriginalChannel))
	require.Equal(t, sourceIdentity, string(got.SourceIdentity))
}

// respondListRecordingControlResponseOnce writes one ControlResponse (and no descriptor)
// matching the offered list request the first time it has been offered, then polls
// descriptor-mode targets.
func (h *lifecycleHarness) respondListRecordingControlResponseOnce(
	t *testing.T,
	code gensbe.ControlResponseCode,
	written *bool,
	targets ArchiveControlPollTargets,
) func() int {
	return func() int {
		if !*written {
			if offered := h.pub.lastOffer(); offered != nil {
				responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    offeredArchiveCorrelationID(t, offered),
					Code:             code,
					Version:          520,
				})
				writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
				*written = true
			}
		}
		h.advanceClock()
		return h.demuxPoll(targets)
	}
}

func TestListRecording_errorResponseReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	descriptorPoller := NewRecordingDescriptorPoller()
	targets := ArchiveControlPollTargets{DescriptorPoller: descriptorPoller}
	written := false
	pollFn := h.respondListRecordingControlResponseOnce(
		t,
		gensbe.ControlResponseCode_ERROR,
		&written,
		targets,
	)

	_, err := h.listRecordingOps(pollFn, descriptorPoller).listRecording(lifecycleRecordingID)
	require.Error(t, err)
}

func TestListRecording_descriptorTimeoutReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	descriptorPoller := NewRecordingDescriptorPoller()
	targets := ArchiveControlPollTargets{DescriptorPoller: descriptorPoller}
	written := false
	// OK latched without descriptor; unified await times out.
	pollFn := h.respondListRecordingControlResponseOnce(
		t,
		gensbe.ControlResponseCode_OK,
		&written,
		targets,
	)

	_, err := h.listRecordingOps(pollFn, descriptorPoller).listRecording(lifecycleRecordingID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "descriptor")
}

// TestAeronArchiveListRecording_methodReturnsDescriptor drives the AeronArchive.ListRecording
// method through a fixture subscription and asserts the ControlSessionID and ArchiveID
// accessors. Connect and awaitArchiveConnect need a live driver handshake and are covered
// by the live integration step.
func TestAeronArchiveListRecording_methodReturnsDescriptor(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := &AeronArchive{
		controlSessionID:    controlResponseTestControlSessionID,
		archiveID:           lifecycleArchiveID,
		proxy:               h.proxy,
		descriptorPoller:    NewRecordingDescriptorPoller(),
		controlSubscription: h.sub,
		controlAssembler:    h.assembler,
		aeronClient:         h.client,
		ctx:                 h.ctx,
	}
	require.Equal(t, controlResponseTestControlSessionID, archive.ControlSessionID())
	require.Equal(t, lifecycleArchiveID, archive.ArchiveID())

	// The ring correlation counter is sequential and this test is single-threaded, so the
	// id consumed here is one below the id the method allocates next.
	correlationID := h.client.NextCorrelationID() + 1
	responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    correlationID,
		Code:             gensbe.ControlResponseCode_OK,
		Version:          520,
	})
	writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
	appendUnfragmentedWireToTerm(
		t,
		h.termBuffer,
		encodeRecordingDescriptorWire(t, codecs.RecordingDescriptorFields{
			ControlSessionID: controlResponseTestControlSessionID,
			CorrelationID:    correlationID,
			RecordingID:      lifecycleRecordingID,
			StartPosition:    4096,
			StopPosition:     8192,
			StrippedChannel:  []byte("aeron:ipc?alias=stripped"),
			OriginalChannel:  []byte("aeron:ipc?alias=original"),
			SourceIdentity:   []byte("source-identity"),
		}),
	)

	got, err := archive.ListRecording(lifecycleRecordingID)
	require.NoError(t, err)
	require.Equal(t, lifecycleRecordingID, got.RecordingID)
	require.Equal(t, int64(4096), got.StartPosition)
	require.Equal(t, int64(8192), got.StopPosition)
	require.Equal(t, "source-identity", string(got.SourceIdentity))
}

// respondControlResponseAppendedOnce writes one ControlResponse after prior term
// consumption, matching the offered request on first offer, then polls demux targets.
func (h *lifecycleHarness) respondControlResponseAppendedOnce(
	t *testing.T,
	relevantID int64,
	code gensbe.ControlResponseCode,
	written *bool,
	targets ArchiveControlPollTargets,
) func() int {
	return func() int {
		if !*written {
			if offered := h.pub.lastOffer(); offered != nil {
				responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    offeredArchiveCorrelationID(t, offered),
					RelevantID:       relevantID,
					Code:             code,
					Version:          520,
				})
				appendUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
				*written = true
			}
		}
		h.advanceClock()
		return h.demuxPoll(targets)
	}
}

// respondControlResponseOnce writes one ControlResponse matching the offered request
// the first time the request has been offered, then polls response-mode demux targets.
func (h *lifecycleHarness) respondControlResponseOnce(
	t *testing.T,
	relevantID int64,
	code gensbe.ControlResponseCode,
	written *bool,
	targets ArchiveControlPollTargets,
) func() int {
	return func() int {
		if !*written {
			if offered := h.pub.lastOffer(); offered != nil {
				responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    offeredArchiveCorrelationID(t, offered),
					RelevantID:       relevantID,
					Code:             code,
					Version:          520,
				})
				writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
				*written = true
			}
		}
		h.advanceClock()
		return h.demuxPoll(targets)
	}
}

func TestStartRecording_clearsPriorDemuxLatch(t *testing.T) {
	h := newLifecycleHarness(t)
	targets := h.responseTargets()
	priorWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       1,
		Code:             gensbe.ControlResponseCode_OK,
		Version:          520,
	})
	handleControlResponseFromFragment(
		h.poller,
		priorWire,
		gensbe.ControlResponseSbeTemplateID,
	)
	require.True(t, h.poller.IsPollComplete())

	written := false
	pollFn := h.respondControlResponseOnce(
		t,
		lifecycleRecordingID,
		gensbe.ControlResponseCode_OK,
		&written,
		targets,
	)
	got, err := h.controlOps(pollFn).startRecording(
		archiveProxyTestRecordingStreamID,
		gensbe.SourceLocation_LOCAL,
		[]byte(lifecycleRecordingChannel),
	)
	require.NoError(t, err)
	require.Equal(t, lifecycleRecordingID, got)
}

func TestStartRecording_sequentialRequestsReusePoller(t *testing.T) {
	h := newLifecycleHarness(t)
	targets := h.responseTargets()

	for _, wantID := range []int64{100, 200} {
		written := false
		pollFn := h.respondControlResponseAppendedOnce(
			t,
			wantID,
			gensbe.ControlResponseCode_OK,
			&written,
			targets,
		)
		got, err := h.controlOps(pollFn).startRecording(
			archiveProxyTestRecordingStreamID,
			gensbe.SourceLocation_LOCAL,
			[]byte(lifecycleRecordingChannel),
		)
		require.NoError(t, err)
		require.Equal(t, wantID, got)
	}
}

func TestStartRecording_awaitReturnsRecordingId(t *testing.T) {
	h := newLifecycleHarness(t)
	written := false
	targets := h.responseTargets()
	pollFn := h.respondControlResponseOnce(
		t,
		lifecycleRecordingID,
		gensbe.ControlResponseCode_OK,
		&written,
		targets,
	)

	got, err := h.controlOps(pollFn).startRecording(
		archiveProxyTestRecordingStreamID,
		gensbe.SourceLocation_LOCAL,
		[]byte(lifecycleRecordingChannel),
	)
	require.NoError(t, err)
	require.Equal(t, lifecycleRecordingID, got)
}

func TestStartRecording_errorResponseReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	written := false
	targets := h.responseTargets()
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		gensbe.ControlResponseCode_ERROR,
		&written,
		targets,
	)

	got, err := h.controlOps(pollFn).startRecording(
		archiveProxyTestRecordingStreamID,
		gensbe.SourceLocation_LOCAL,
		[]byte(lifecycleRecordingChannel),
	)
	require.Error(t, err)
	require.Zero(t, got)
}

func TestStopRecording_awaitOK(t *testing.T) {
	h := newLifecycleHarness(t)
	written := false
	targets := h.responseTargets()
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		gensbe.ControlResponseCode_OK,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).stopRecording(
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
	)
	require.NoError(t, err)
}

func TestStopRecording_awaitSTOPSignal(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewStopSignalTracker(controlResponseTestControlSessionID, lifecycleRecordingID)
	targets := h.signalTargets(tracker)
	written := false
	pollFn := func() int {
		if !written && h.pub.lastOffer() != nil {
			signalWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
				ControlSessionID: controlResponseTestControlSessionID,
				CorrelationID:    909,
				RecordingID:      lifecycleRecordingID,
				SubscriptionID:   2002,
				Position:         3003,
				Signal:           gensbe.RecordingSignal_STOP,
			})
			writeUnfragmentedWireToTerm(t, h.termBuffer, signalWire)
			written = true
		}
		h.advanceClock()
		return h.demuxPoll(targets)
	}

	err := h.controlOps(pollFn).stopRecordingAwaitSignal(
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.NoError(t, err)
	require.True(t, tracker.Stopped())
	require.Equal(t, lifecycleRecordingID, tracker.StoppedRecordingID())
	require.Equal(t, int64(3003), tracker.StoppedPosition())
}

func TestStopRecordingAwaitSignal_awaitOKResponse(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewStopSignalTracker(controlResponseTestControlSessionID, -1)
	targets := h.signalTargets(tracker)
	written := false
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		gensbe.ControlResponseCode_OK,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).stopRecordingAwaitSignal(
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.NoError(t, err)
	require.False(t, tracker.Stopped())
}

func TestStopRecordingAwaitSignal_errorResponseReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewStopSignalTracker(controlResponseTestControlSessionID, -1)
	targets := h.signalTargets(tracker)
	written := false
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		gensbe.ControlResponseCode_ERROR,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).stopRecordingAwaitSignal(
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.Error(t, err)
	require.False(t, tracker.Stopped())
}

func TestStopRecordingAwaitSignal_reusedTrackerWaitsForNewProof(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewStopSignalTracker(controlResponseTestControlSessionID, lifecycleRecordingID)
	tracker.RecordingSignal(codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    808,
		RecordingID:      lifecycleRecordingID,
		SubscriptionID:   2002,
		Position:         3003,
		Signal:           gensbe.RecordingSignal_STOP,
	})
	require.True(t, tracker.Stopped())

	targets := h.signalTargets(tracker)
	written := false
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		gensbe.ControlResponseCode_OK,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).stopRecordingAwaitSignal(
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.NoError(t, err)
	require.True(t, written)
	require.False(t, tracker.Stopped())
}

func TestStopRecordingAwaitSignal_nilTrackerReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	targets := h.responseTargets()

	err := h.controlOps(func() int { return h.demuxPoll(targets) }).stopRecordingAwaitSignal(
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
		targets,
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-nil")
}

func TestStopRecordingAwaitSignal_timeout(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewStopSignalTracker(controlResponseTestControlSessionID, -1)
	targets := h.signalTargets(tracker)
	pollFn := func() int {
		h.nowNs.Add(int64(h.ctx.MessageTimeout))
		return h.demuxPoll(targets)
	}

	err := h.controlOps(pollFn).stopRecordingAwaitSignal(
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout")
}
