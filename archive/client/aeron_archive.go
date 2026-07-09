package client

import (
	"context"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

// AeronArchive is a connected archive control session over one control-response
// subscription.
//
// Poll demuxes control responses and recording descriptors on the shared subscription
// through PollArchiveControlEvents; it never polls the underlying pollers directly and
// never calls DoWork. Recording signals are dropped until ArchiveContext.RecordingSignalHandler
// or SetRecordingSignalHandler configures a handler; configured handlers also run during
// blocking StartRecording, StopRecording, StartReplay, StopReplay, and ListRecording awaits.
// ListRecording uses a descriptor-mode poll so its OK response routes to the descriptor
// poller rather than the session control-response poller.
//
// One FragmentAssembler is session-owned and reused across polls.
//
// Close releases the control publication and subscription, and the Aeron client when
// Connect created it; it is idempotent.
//
// Not safe for concurrent use.
type AeronArchive struct {
	controlSessionID int64
	archiveID        int64

	proxy            *ArchiveProxy
	controlPoller    *ControlResponsePoller
	descriptorPoller *RecordingDescriptorPoller
	signalHandler    RecordingSignalHandler

	controlSubscription *aeron.Subscription
	controlAssembler    *FragmentAssembler
	aeronClient         *aeron.Client
	ownsAeronClient     bool
	ctx                 ArchiveContext
	closed              bool
}

// Connect performs the blocking archive control connect handshake and returns a session
// wrapper.
//
// It drives DoWork on the transport client until the handshake completes,
// ctx is canceled or its deadline is exceeded, or ConnectTimeout elapses. The await loop
// paces via archiveCtx.IdleStrategy.Idle(0) and calls IdleStrategy.Reset on exit so a
// shared backoff instance is not left in PARKING before the post-connect duty cycle.
//
// NewAsyncConnect normalizes and validates archiveCtx; pre-normalizing is harmless.
//
// On handshake failure or ctx cancellation, partial resources are released via
// AsyncConnect.Close.
//
// Returns ctx.Err() when the context is done before handshake success
// (context.Canceled or context.DeadlineExceeded). Connect timeout errors apply only
// while the context remains active. A completed handshake returns a session even if ctx
// is canceled concurrently on that iteration.
//
// Not safe for concurrent use during connect.
func Connect(ctx context.Context, archiveCtx ArchiveContext) (*AeronArchive, error) {
	connect, err := NewAsyncConnect(archiveCtx)
	if err != nil {
		return nil, err
	}

	notifyConnectAsyncConnectObserver(connect)

	result, err := awaitArchiveConnect(ctx, connect)
	if err != nil {
		_ = connect.Close()
		return nil, err
	}
	return newAeronArchive(result), nil
}

func awaitArchiveConnect(ctx context.Context, connect *AsyncConnect) (*ConnectResult, error) {
	idle := connect.ctx.IdleStrategy
	defer idle.Reset()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		notifyAwaitArchiveConnectStepObserver(connect)
		if connect.client != nil {
			connect.client.DoWork()
		}
		result, err := connect.Poll()
		if err != nil {
			return nil, aeron.PreferContextError(ctx, err)
		}
		if result != nil {
			return result, nil
		}
		idle.Idle(0)
	}
}

func newAeronArchive(result *ConnectResult) *AeronArchive {
	archive := &AeronArchive{
		controlSessionID:    result.ControlSessionID,
		archiveID:           result.ArchiveID,
		proxy:               result.Proxy,
		controlPoller:       result.Poller,
		descriptorPoller:    NewRecordingDescriptorPoller(),
		signalHandler:       result.Context.RecordingSignalHandler,
		controlSubscription: result.ControlSubscription,
		controlAssembler:    NewFragmentAssembler(nil),
		aeronClient:         result.Aeron,
		ownsAeronClient:     result.OwnsAeronClient,
		ctx:                 result.Context,
	}
	if archive.controlPoller != nil {
		archive.controlPoller.ResetCapture()
	}
	return archive
}

// ControlSessionID returns the archive control session id assigned at connect.
func (archive *AeronArchive) ControlSessionID() int64 {
	return archive.controlSessionID
}

// ArchiveID returns the archive id reported at connect.
func (archive *AeronArchive) ArchiveID() int64 {
	return archive.archiveID
}

// Close sends a best-effort CloseSessionRequest, then releases the control publication
// and subscription. When Connect created the underlying Aeron client, Close closes it
// too; a user-supplied client is left open.
//
// Close is idempotent: after the first call it releases nothing further and returns nil.
func (archive *AeronArchive) Close() error {
	if archive.closed {
		return nil
	}
	archive.closed = true
	if archive.proxy != nil {
		_, _ = archive.proxy.TryCloseSession(archive.controlSessionID)
		_ = archive.proxy.Close()
	}
	if archive.controlSubscription != nil {
		_ = archive.controlSubscription.Close()
		archive.controlSubscription = nil
	}
	if archive.ownsAeronClient && archive.aeronClient != nil {
		_ = archive.aeronClient.Close()
		archive.aeronClient = nil
	}
	return nil
}

// IsClosed reports whether Close has been invoked.
func (archive *AeronArchive) IsClosed() bool {
	if archive == nil {
		return true
	}
	return archive.closed
}

// Poll performs one demux poll of the control-response subscription, dispatching control
// responses and recording descriptors to the session pollers. Recording signals are
// dropped until a signal handler is configured. It never calls DoWork.
func (archive *AeronArchive) Poll() int {
	return PollArchiveControlEvents(
		archive.controlSubscription,
		DefaultControlResponseFragmentLimit,
		archive.controlAssembler,
		archive.sessionPollTargets(archive.signalHandler),
	)
}

func (archive *AeronArchive) listRecordingPollTargets() ArchiveControlPollTargets {
	return ArchiveControlPollTargets{
		SignalHandler:    archive.signalHandler,
		ControlSessionID: archive.controlSessionID,
		DescriptorPoller: archive.descriptorPoller,
	}
}

func (archive *AeronArchive) pollListRecording() int {
	return PollArchiveControlEvents(
		archive.controlSubscription,
		DefaultControlResponseFragmentLimit,
		archive.controlAssembler,
		archive.listRecordingPollTargets(),
	)
}

func (archive *AeronArchive) pollStopRecordingAwaitSignal(
	tracker *StopSignalTracker,
) func() int {
	targets := archive.sessionPollTargets(tracker)
	return func() int {
		return PollArchiveControlEvents(
			archive.controlSubscription,
			DefaultControlResponseFragmentLimit,
			archive.controlAssembler,
			targets,
		)
	}
}

// SetRecordingSignalHandler overrides ArchiveContext.RecordingSignalHandler for recording
// signals on Poll and during blocking control awaits that use this handler
// (StartRecording, StopRecording, StartReplay, StopReplay, ListRecording).
func (archive *AeronArchive) SetRecordingSignalHandler(handler RecordingSignalHandler) {
	archive.signalHandler = handler
}

// StartRecording offers a StartRecordingRequest and blocks until the archive replies
// with a matching ControlResponse. The returned RelevantID is the subscription
// registration id on the wire, not the catalog recording id; await RecordingSignal_START
// for the assigned recording id.
//
// Returns ctx.Err() when the context is done before a response is received.
func (archive *AeronArchive) StartRecording(
	ctx context.Context,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	channel []byte,
) (int64, error) {
	return archive.sessionControlOps(archive.Poll).startRecording(
		ctx,
		streamID,
		sourceLocation,
		channel,
	)
}

// StopRecording offers a StopRecordingRequest and blocks until the archive replies with
// a matching OK ControlResponse.
//
// Returns ctx.Err() when the context is done before a response is received.
func (archive *AeronArchive) StopRecording(
	ctx context.Context,
	streamID int32,
	channel []byte,
) error {
	return archive.sessionControlOps(archive.Poll).stopRecording(ctx, streamID, channel)
}

// StopRecordingAwaitSignal offers a StopRecordingRequest and blocks until the archive
// proves the stop with either a matching OK ControlResponse or a STOP signal recorded
// by tracker.
//
// tracker must be non-nil: it carries the STOP signal proof and is dereferenced on
// every proof check.
//
// Returns ctx.Err() when the context is done before proof is received.
func (archive *AeronArchive) StopRecordingAwaitSignal(
	ctx context.Context,
	streamID int32,
	channel []byte,
	tracker *StopSignalTracker,
) error {
	return archive.sessionControlOps(
		archive.pollStopRecordingAwaitSignal(tracker),
	).stopRecordingAwaitSignal(
		ctx,
		streamID,
		channel,
		archive.sessionPollTargets(tracker),
		tracker,
	)
}

// StartReplay offers a ReplayRequest and blocks until the archive replies with a
// matching ControlResponse, returning the assigned replay session id.
//
// Returns ctx.Err() when the context is done before a response is received.
func (archive *AeronArchive) StartReplay(
	ctx context.Context,
	recordingID int64,
	position int64,
	length int64,
	replayStreamID int32,
	replayChannel []byte,
) (int64, error) {
	return archive.sessionControlOps(archive.Poll).startReplay(
		ctx,
		recordingID,
		position,
		length,
		replayStreamID,
		replayChannel,
	)
}

// StopReplay offers a StopReplayRequest and blocks until the archive replies with a
// matching OK ControlResponse.
//
// Returns ctx.Err() when the context is done before a response is received.
func (archive *AeronArchive) StopReplay(ctx context.Context, replaySessionID int64) error {
	return archive.sessionControlOps(archive.Poll).stopReplay(ctx, replaySessionID)
}

// ListRecording lists a single recording by id, returning its descriptor extent fields
// with var-ascii bytes cloned at latch time for retention.
//
// Returns ctx.Err() when the context is done before a response is received.
func (archive *AeronArchive) ListRecording(
	ctx context.Context,
	recordingID int64,
) (codecs.RecordingDescriptorFields, error) {
	return archive.sessionControlOps(archive.pollListRecording).listRecording(ctx, recordingID)
}

func (archive *AeronArchive) sessionPollTargets(
	handler RecordingSignalHandler,
) ArchiveControlPollTargets {
	return ArchiveControlPollTargets{
		SignalHandler:    handler,
		ControlSessionID: archive.controlSessionID,
		ResponsePoller:   archive.controlPoller,
		DescriptorPoller: archive.descriptorPoller,
	}
}
