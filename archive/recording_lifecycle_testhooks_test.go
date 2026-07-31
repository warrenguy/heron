//go:build testhooks

package archive

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
)

const (
	lifecycleControlResponseTimeout       = 500 * time.Millisecond
	lifecycleRecordingID            int64 = 5150
	lifecycleArchiveID              int64 = 919191
	lifecycleRecordingChannel             = "aeron:ipc"
)

type lifecycleHarness struct {
	client     *aeron.Client
	nowNs      *atomic.Int64
	sub        *aeron.Subscription
	termBuffer atomicbuffer.AtomicBuffer
	poller     *ControlResponsePoller
	assembler  *FragmentAssembler
	pub        *stubControlPublication
	proxy      *Proxy
	cfg        Config
}

func newLifecycleHarness(t *testing.T) *lifecycleHarness {
	t.Helper()

	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
	sub, termBuffer := setupControlResponsePollFixtures(t)
	cfg := Config{
		ControlResponseTimeout: lifecycleControlResponseTimeout,
		IdleStrategy:           NewDefaultIdleStrategy(),
		AeronConfig: aeron.Config{
			NanoClock: func() int64 { return nowNs.Load() },
		},
	}.Normalize()
	pub, proxy := newTestProxy(cfg)
	return &lifecycleHarness{
		client:     harness.Client,
		nowNs:      nowNs,
		sub:        sub,
		termBuffer: termBuffer,
		poller:     newControlResponsePollerForTest(sub),
		assembler:  NewFragmentAssembler(nil),
		pub:        pub,
		proxy:      proxy,
		cfg:        cfg,
	}
}

func (h *lifecycleHarness) advanceClock() {
	h.nowNs.Add(int64(h.cfg.ControlResponseTimeout) / 8)
}

func (h *lifecycleHarness) demuxPoll(targets ControlPollTargets) int {
	return PollControlEvents(h.sub, DefaultControlResponseFragmentLimit, h.assembler, targets)
}

func (h *lifecycleHarness) responseTargets() ControlPollTargets {
	return ControlPollTargets{
		ResponsePoller:   h.poller,
		ControlSessionID: controlResponseTestControlSessionID,
	}
}

func (h *lifecycleHarness) signalTargets(handler RecordingSignalHandler) ControlPollTargets {
	return ControlPollTargets{
		SignalHandler:    handler,
		ControlSessionID: controlResponseTestControlSessionID,
		ResponsePoller:   h.poller,
	}
}

func (h *lifecycleHarness) controlOps(pollFn func() int) archiveControlOps {
	return archiveControlOps{
		aeronClient:      h.client,
		cfg:              h.cfg,
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
	case codecs.ExtendRecordingRequest2Fields:
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
	targets ControlPollTargets,
) func() int {
	return func() int {
		if !*written {
			if offered := h.pub.lastOffer(); offered != nil {
				correlationID := offeredArchiveCorrelationID(t, offered)
				if withOK {
					responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
						ControlSessionID: controlResponseTestControlSessionID,
						CorrelationID:    correlationID,
						Code:             archivewire.ControlResponseCode_OK,
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

	_, err := h.controlOps(func() int { return 0 }).listRecording(t.Context(), lifecycleRecordingID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-nil")
}

func TestListRecording_descriptorOnlyCompletes(t *testing.T) {
	h := newLifecycleHarness(t)
	descriptorPoller := NewRecordingDescriptorPoller()
	targets := ControlPollTargets{DescriptorPoller: descriptorPoller}
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

	got, err := h.listRecordingOps(pollFn, descriptorPoller).
		listRecording(t.Context(), lifecycleRecordingID)
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
	targets := ControlPollTargets{DescriptorPoller: descriptorPoller}
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

	got, err := h.listRecordingOps(pollFn, descriptorPoller).
		listRecording(t.Context(), lifecycleRecordingID)
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
	targets := ControlPollTargets{DescriptorPoller: descriptorPoller}
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

	got, err := h.listRecordingOps(pollFn, descriptorPoller).
		listRecording(t.Context(), lifecycleRecordingID)
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
	code archivewire.ControlResponseCode,
	written *bool,
	targets ControlPollTargets,
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
	targets := ControlPollTargets{DescriptorPoller: descriptorPoller}
	written := false
	pollFn := h.respondListRecordingControlResponseOnce(
		t,
		archivewire.ControlResponseCode_ERROR,
		&written,
		targets,
	)

	_, err := h.listRecordingOps(pollFn, descriptorPoller).
		listRecording(t.Context(), lifecycleRecordingID)
	require.Error(t, err)
	require.True(t, written)
	var archiveErr *Exception
	require.ErrorAs(t, err, &archiveErr)
}

func TestListRecording_descriptorTimeoutReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	descriptorPoller := NewRecordingDescriptorPoller()
	targets := ControlPollTargets{DescriptorPoller: descriptorPoller}
	written := false
	// OK latched without descriptor; unified await times out.
	pollFn := h.respondListRecordingControlResponseOnce(
		t,
		archivewire.ControlResponseCode_OK,
		&written,
		targets,
	)

	_, err := h.listRecordingOps(pollFn, descriptorPoller).
		listRecording(t.Context(), lifecycleRecordingID)
	require.Error(t, err)
	var timeoutErr *ControlResponseTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.Equal(t, ListRecordingDescriptorAwaitOperation, timeoutErr.Operation)
	require.Contains(t, err.Error(), "descriptor")
}

// TestClientListRecording_methodReturnsDescriptor drives the Client.ListRecording
// method through a fixture subscription and asserts the ControlSessionID and ArchiveID
// accessors. Connect and awaitConnect need a live driver handshake and are covered
// by the live integration step.
func TestClientListRecording_methodReturnsDescriptor(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := &Client{
		controlSessionID:    controlResponseTestControlSessionID,
		archiveID:           lifecycleArchiveID,
		proxy:               h.proxy,
		descriptorPoller:    NewRecordingDescriptorPoller(),
		controlSubscription: h.sub,
		controlAssembler:    h.assembler,
		aeronClient:         h.client,
		cfg:                 h.cfg,
	}
	require.Equal(t, controlResponseTestControlSessionID, archive.ControlSessionID())
	require.Equal(t, lifecycleArchiveID, archive.ArchiveID())

	// The ring correlation counter is sequential and this test is single-threaded, so the
	// id consumed here is one below the id the method allocates next.
	correlationID := h.client.NextCorrelationID() + 1
	responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    correlationID,
		Code:             archivewire.ControlResponseCode_OK,
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

	got, err := archive.ListRecording(t.Context(), lifecycleRecordingID)
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
	code archivewire.ControlResponseCode,
	written *bool,
	targets ControlPollTargets,
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
	code archivewire.ControlResponseCode,
	written *bool,
	targets ControlPollTargets,
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
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	handleControlResponseFromFragment(
		h.poller,
		priorWire,
		archivewire.ControlResponseSbeTemplateID,
	)
	require.True(t, h.poller.IsPollComplete())

	written := false
	pollFn := h.respondControlResponseOnce(
		t,
		lifecycleRecordingID,
		archivewire.ControlResponseCode_OK,
		&written,
		targets,
	)
	got, err := h.controlOps(pollFn).startRecording(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
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
			archivewire.ControlResponseCode_OK,
			&written,
			targets,
		)
		got, err := h.controlOps(pollFn).startRecording(
			t.Context(),
			archiveProxyTestRecordingStreamID,
			archivewire.SourceLocation_LOCAL,
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
		archivewire.ControlResponseCode_OK,
		&written,
		targets,
	)

	got, err := h.controlOps(pollFn).startRecording(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
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
		archivewire.ControlResponseCode_ERROR,
		&written,
		targets,
	)

	got, err := h.controlOps(pollFn).startRecording(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		[]byte(lifecycleRecordingChannel),
	)
	require.Error(t, err)
	require.Zero(t, got)
	require.True(t, written)
	var archiveErr *Exception
	require.ErrorAs(t, err, &archiveErr)
}

func TestStartRecordingAwaitSignal_nilTrackerReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	targets := h.responseTargets()

	err := h.controlOps(func() int { return h.demuxPoll(targets) }).startRecordingAwaitSignal(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		[]byte(lifecycleRecordingChannel),
		targets,
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-nil")
}

func TestStartRecordingAwaitSignal_errorResponseReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewStartSignalTracker(controlResponseTestControlSessionID)
	targets := h.signalTargets(tracker)
	written := false
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		archivewire.ControlResponseCode_ERROR,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).startRecordingAwaitSignal(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.Error(t, err)
	require.False(t, tracker.Started())
	require.True(t, written)
	var archiveErr *Exception
	require.ErrorAs(t, err, &archiveErr)
}

func TestStartRecordingAwaitSignal_OKAloneDoesNotComplete(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewStartSignalTracker(controlResponseTestControlSessionID)
	targets := h.signalTargets(tracker)
	written := false
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		archivewire.ControlResponseCode_OK,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).startRecordingAwaitSignal(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.Error(t, err)
	var timeoutErr *ControlResponseTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.Equal(t, StartRecordingAwaitOperation, timeoutErr.Operation)
	require.True(t, written)
	require.False(t, tracker.Started())
}

func TestExtendRecordingAwaitSignal_nilTrackerReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	targets := h.responseTargets()

	err := h.controlOps(func() int { return h.demuxPoll(targets) }).extendRecordingAwaitSignal(
		t.Context(),
		lifecycleRecordingID,
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		archivewire.BooleanType_TRUE,
		[]byte(lifecycleRecordingChannel),
		targets,
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-nil")
}

func TestExtendRecordingAwaitSignal_errorResponseReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewExtendSignalTracker(controlResponseTestControlSessionID, lifecycleRecordingID)
	targets := h.signalTargets(tracker)
	written := false
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		archivewire.ControlResponseCode_ERROR,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).extendRecordingAwaitSignal(
		t.Context(),
		lifecycleRecordingID,
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		archivewire.BooleanType_TRUE,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.Error(t, err)
	require.False(t, tracker.Extended())
	require.True(t, written)
	var archiveErr *Exception
	require.ErrorAs(t, err, &archiveErr)
}

func TestExtendRecordingAwaitSignal_OKAloneDoesNotComplete(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewExtendSignalTracker(controlResponseTestControlSessionID, lifecycleRecordingID)
	targets := h.signalTargets(tracker)
	written := false
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		archivewire.ControlResponseCode_OK,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).extendRecordingAwaitSignal(
		t.Context(),
		lifecycleRecordingID,
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		archivewire.BooleanType_TRUE,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.Error(t, err)
	var timeoutErr *ControlResponseTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.Equal(t, ExtendRecordingAwaitOperation, timeoutErr.Operation)
	require.True(t, written)
	require.False(t, tracker.Extended())
}

func TestExtendRecordingAwaitSignal_OKThenEXTEND(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewExtendSignalTracker(controlResponseTestControlSessionID, lifecycleRecordingID)
	targets := h.signalTargets(tracker)
	okWritten := false
	extendWritten := false
	pollFn := func() int {
		if offered := h.pub.lastOffer(); offered != nil {
			correlationID := offeredArchiveCorrelationID(t, offered)
			if !okWritten {
				responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    correlationID,
					Code:             archivewire.ControlResponseCode_OK,
					Version:          520,
				})
				writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
				okWritten = true
			} else if !extendWritten {
				signalWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    correlationID,
					RecordingID:      lifecycleRecordingID,
					SubscriptionID:   2002,
					Position:         3003,
					Signal:           archivewire.RecordingSignal_EXTEND,
				})
				appendUnfragmentedWireToTerm(t, h.termBuffer, signalWire)
				extendWritten = true
			}
		}
		h.advanceClock()
		return h.demuxPoll(targets)
	}

	err := h.controlOps(pollFn).extendRecordingAwaitSignal(
		t.Context(),
		lifecycleRecordingID,
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		archivewire.BooleanType_TRUE,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.NoError(t, err)
	require.True(t, okWritten)
	require.True(t, extendWritten)
	require.True(t, tracker.Extended())
	require.Equal(t, lifecycleRecordingID, tracker.ExtendedRecordingID())
	require.False(t, h.poller.IsPollComplete())
}

func TestStartRecordingAwaitSignal_OKThenSTART(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewStartSignalTracker(controlResponseTestControlSessionID)
	targets := h.signalTargets(tracker)
	okWritten := false
	startWritten := false
	pollFn := func() int {
		if offered := h.pub.lastOffer(); offered != nil {
			correlationID := offeredArchiveCorrelationID(t, offered)
			if !okWritten {
				responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    correlationID,
					Code:             archivewire.ControlResponseCode_OK,
					Version:          520,
				})
				writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
				okWritten = true
			} else if !startWritten {
				signalWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    correlationID,
					RecordingID:      lifecycleRecordingID,
					SubscriptionID:   2002,
					Position:         3003,
					Signal:           archivewire.RecordingSignal_START,
				})
				appendUnfragmentedWireToTerm(t, h.termBuffer, signalWire)
				startWritten = true
			}
		}
		h.advanceClock()
		return h.demuxPoll(targets)
	}

	err := h.controlOps(pollFn).startRecordingAwaitSignal(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.NoError(t, err)
	require.True(t, okWritten)
	require.True(t, startWritten)
	require.True(t, tracker.Started())
	require.Equal(t, lifecycleRecordingID, tracker.StartedRecordingID())
	require.False(t, h.poller.IsPollComplete())
}

func TestStopRecording_awaitOK(t *testing.T) {
	h := newLifecycleHarness(t)
	written := false
	targets := h.responseTargets()
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		archivewire.ControlResponseCode_OK,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).stopRecording(
		t.Context(),
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
				Signal:           archivewire.RecordingSignal_STOP,
			})
			writeUnfragmentedWireToTerm(t, h.termBuffer, signalWire)
			written = true
		}
		h.advanceClock()
		return h.demuxPoll(targets)
	}

	err := h.controlOps(pollFn).stopRecordingAwaitSignal(
		t.Context(),
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

func TestStopRecordingAwaitSignal_OKAloneDoesNotComplete(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewStopSignalTracker(controlResponseTestControlSessionID, -1)
	targets := h.signalTargets(tracker)
	written := false
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		archivewire.ControlResponseCode_OK,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).stopRecordingAwaitSignal(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.Error(t, err)
	var timeoutErr *ControlResponseTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.Equal(t, StopRecordingAwaitOperation, timeoutErr.Operation)
	require.True(t, written)
	require.False(t, tracker.Stopped())
}

func TestStopRecordingAwaitSignal_OKThenSTOP(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewStopSignalTracker(controlResponseTestControlSessionID, lifecycleRecordingID)
	targets := h.signalTargets(tracker)
	okWritten := false
	stopWritten := false
	pollFn := func() int {
		if offered := h.pub.lastOffer(); offered != nil {
			if !okWritten {
				responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    offeredArchiveCorrelationID(t, offered),
					Code:             archivewire.ControlResponseCode_OK,
					Version:          520,
				})
				writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
				okWritten = true
			} else if !stopWritten {
				signalWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
					ControlSessionID: controlResponseTestControlSessionID,
					CorrelationID:    909,
					RecordingID:      lifecycleRecordingID,
					SubscriptionID:   2002,
					Position:         3003,
					Signal:           archivewire.RecordingSignal_STOP,
				})
				appendUnfragmentedWireToTerm(t, h.termBuffer, signalWire)
				stopWritten = true
			}
		}
		h.advanceClock()
		return h.demuxPoll(targets)
	}

	err := h.controlOps(pollFn).stopRecordingAwaitSignal(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.NoError(t, err)
	require.True(t, okWritten)
	require.True(t, stopWritten)
	require.True(t, tracker.Stopped())
	require.Equal(t, int64(3003), tracker.StoppedPosition())
	require.False(t, h.poller.IsPollComplete())
}

// TestStopRecordingAwaitSignal_trailingOKFilteredByNextAwait proves a stop OK that
// lands after the STOP proof returned does not block the next control await: ResetAwait
// arms the new correlation before offer and demux, so the old-corr OK never latches.
func TestStopRecordingAwaitSignal_trailingOKFilteredByNextAwait(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewStopSignalTracker(controlResponseTestControlSessionID, lifecycleRecordingID)
	targets := h.signalTargets(tracker)
	var stopCorrelationID int64
	stopWritten := false
	pollFn := func() int {
		if offered := h.pub.lastOffer(); !stopWritten && offered != nil {
			stopCorrelationID = offeredArchiveCorrelationID(t, offered)
			signalWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
				ControlSessionID: controlResponseTestControlSessionID,
				CorrelationID:    909,
				RecordingID:      lifecycleRecordingID,
				SubscriptionID:   2002,
				Position:         3003,
				Signal:           archivewire.RecordingSignal_STOP,
			})
			writeUnfragmentedWireToTerm(t, h.termBuffer, signalWire)
			stopWritten = true
		}
		h.advanceClock()
		return h.demuxPoll(targets)
	}

	err := h.controlOps(pollFn).stopRecordingAwaitSignal(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.NoError(t, err)
	require.True(t, tracker.Stopped())
	require.False(t, h.poller.IsPollComplete())

	trailingWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    stopCorrelationID,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	})
	appendUnfragmentedWireToTerm(t, h.termBuffer, trailingWire)

	// First demux poll sees the trailing OK under the new await correlation filter; it
	// must not latch ready. Matching OK for the new correlation is written only after
	// that poll so the real response can still capture.
	responseTargets := h.responseTargets()
	staleDispatched := false
	okWritten := false
	nextPollFn := func() int {
		if offered := h.pub.lastOffer(); staleDispatched && !okWritten && offered != nil {
			responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
				ControlSessionID: controlResponseTestControlSessionID,
				CorrelationID:    offeredArchiveCorrelationID(t, offered),
				Code:             archivewire.ControlResponseCode_OK,
				Version:          520,
			})
			appendUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
			okWritten = true
		}
		h.advanceClock()
		work := h.demuxPoll(responseTargets)
		staleDispatched = true
		return work
	}

	err = h.controlOps(nextPollFn).stopRecording(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
	)
	require.NoError(t, err)
	require.True(t, okWritten)
}

func TestStopRecordingAwaitSignal_errorResponseReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	tracker := NewStopSignalTracker(controlResponseTestControlSessionID, -1)
	targets := h.signalTargets(tracker)
	written := false
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		archivewire.ControlResponseCode_ERROR,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).stopRecordingAwaitSignal(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.Error(t, err)
	require.False(t, tracker.Stopped())
	require.True(t, written)
	var archiveErr *Exception
	require.ErrorAs(t, err, &archiveErr)
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
		Signal:           archivewire.RecordingSignal_STOP,
	})
	require.True(t, tracker.Stopped())

	targets := h.signalTargets(tracker)
	stopWritten := false
	pollFn := func() int {
		if !stopWritten && h.pub.lastOffer() != nil {
			signalWire := encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
				ControlSessionID: controlResponseTestControlSessionID,
				CorrelationID:    910,
				RecordingID:      lifecycleRecordingID,
				SubscriptionID:   2002,
				Position:         4004,
				Signal:           archivewire.RecordingSignal_STOP,
			})
			writeUnfragmentedWireToTerm(t, h.termBuffer, signalWire)
			stopWritten = true
		}
		h.advanceClock()
		return h.demuxPoll(targets)
	}

	err := h.controlOps(pollFn).stopRecordingAwaitSignal(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.NoError(t, err)
	require.True(t, stopWritten)
	require.True(t, tracker.Stopped())
	require.Equal(t, int64(4004), tracker.StoppedPosition())
}

func TestStopRecordingAwaitSignal_nilTrackerReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	targets := h.responseTargets()

	err := h.controlOps(func() int { return h.demuxPoll(targets) }).stopRecordingAwaitSignal(
		t.Context(),
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
		h.nowNs.Add(int64(h.cfg.ControlResponseTimeout))
		return h.demuxPoll(targets)
	}

	err := h.controlOps(pollFn).stopRecordingAwaitSignal(
		t.Context(),
		archiveProxyTestRecordingStreamID,
		[]byte(lifecycleRecordingChannel),
		targets,
		tracker,
	)
	require.Error(t, err)
	var timeoutErr *ControlResponseTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.Equal(t, StopRecordingAwaitOperation, timeoutErr.Operation)
}
