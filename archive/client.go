package archive

import (
	"context"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

// Client is a connected archive control session over one control-response
// subscription.
//
// Poll demuxes control responses and recording descriptors on the shared subscription
// through PollControlEvents; it never polls the underlying pollers directly and
// never calls DoWork. Recording signals are dropped until Context.RecordingSignalHandler
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
type Client struct {
	controlSessionID int64
	archiveID        int64

	proxy            *Proxy
	controlPoller    *ControlResponsePoller
	descriptorPoller *RecordingDescriptorPoller
	signalHandler    RecordingSignalHandler

	controlSubscription *aeron.Subscription
	controlAssembler    *FragmentAssembler
	aeronClient         *aeron.Client
	ownsAeronClient     bool
	archiveCtx          Context
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
// When archiveCtx.Aeron is nil, ctx also bounds owned transport attach (CNC ready and
// driver heartbeat) via NewAsyncConnect. Shared Aeron skips that wait.
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
func Connect(ctx context.Context, archiveCtx Context) (*Client, error) {
	connect, err := NewAsyncConnect(ctx, archiveCtx)
	if err != nil {
		return nil, err
	}

	notifyConnectAsyncConnectObserver(connect)

	result, err := awaitConnect(ctx, connect)
	if err != nil {
		_ = connect.Close()
		return nil, err
	}
	return newClient(result), nil
}

func awaitConnect(ctx context.Context, connect *AsyncConnect) (*ConnectResult, error) {
	idle := connect.archiveCtx.IdleStrategy
	defer idle.Reset()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		notifyAwaitConnectStepObserver(connect)
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

func newClient(result *ConnectResult) *Client {
	client := &Client{
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
		archiveCtx:          result.Context,
	}
	if client.controlPoller != nil {
		client.controlPoller.ResetCapture()
	}
	return client
}

// ControlSessionID returns the archive control session id assigned at connect.
func (client *Client) ControlSessionID() int64 {
	return client.controlSessionID
}

// ArchiveID returns the archive id reported at connect.
func (client *Client) ArchiveID() int64 {
	return client.archiveID
}

// Context returns the normalized archive context stored at connect.
// Nil receiver returns a zero Context.
func (client *Client) Context() Context {
	if client == nil {
		return Context{}
	}
	return client.archiveCtx
}

// Close sends a best-effort CloseSessionRequest, then releases the control publication
// and subscription. When Connect created the underlying Aeron client, Close closes it
// too; a user-supplied client is left open.
//
// Close is idempotent: after the first call it releases nothing further and returns nil.
func (client *Client) Close() error {
	if client.closed {
		return nil
	}
	client.closed = true
	if client.proxy != nil {
		_, _ = client.proxy.TryCloseSession(client.controlSessionID)
		_ = client.proxy.Close()
	}
	if client.controlSubscription != nil {
		_ = client.controlSubscription.Close()
		client.controlSubscription = nil
	}
	if client.ownsAeronClient && client.aeronClient != nil {
		_ = client.aeronClient.Close()
		client.aeronClient = nil
	}
	return nil
}

// IsClosed reports whether Close has been invoked.
func (client *Client) IsClosed() bool {
	if client == nil {
		return true
	}
	return client.closed
}

// Poll performs one demux poll of the control-response subscription, dispatching control
// responses and recording descriptors to the session pollers. Recording signals are
// dropped until a signal handler is configured. It never calls DoWork.
func (client *Client) Poll() int {
	return PollControlEvents(
		client.controlSubscription,
		DefaultControlResponseFragmentLimit,
		client.controlAssembler,
		client.sessionPollTargets(client.signalHandler),
	)
}

func (client *Client) listRecordingPollTargets() ControlPollTargets {
	return ControlPollTargets{
		SignalHandler:    client.signalHandler,
		ControlSessionID: client.controlSessionID,
		DescriptorPoller: client.descriptorPoller,
	}
}

func (client *Client) pollListRecording() int {
	return PollControlEvents(
		client.controlSubscription,
		DefaultControlResponseFragmentLimit,
		client.controlAssembler,
		client.listRecordingPollTargets(),
	)
}

func (client *Client) pollStopRecordingAwaitSignal(
	tracker *StopSignalTracker,
) func() int {
	targets := client.sessionPollTargets(tracker)
	return func() int {
		return PollControlEvents(
			client.controlSubscription,
			DefaultControlResponseFragmentLimit,
			client.controlAssembler,
			targets,
		)
	}
}

// SetRecordingSignalHandler overrides Context.RecordingSignalHandler for recording
// signals on Poll and during blocking control awaits that use this handler
// (StartRecording, StopRecording, StartReplay, StopReplay, ListRecording).
func (client *Client) SetRecordingSignalHandler(handler RecordingSignalHandler) {
	client.signalHandler = handler
}

// StartRecording offers a StartRecordingRequest and blocks until the archive replies
// with a matching ControlResponse. The returned RelevantID is the subscription
// registration id on the wire, not the catalog recording id; await RecordingSignal_START
// for the assigned recording id.
//
// Returns ctx.Err() when the context is done before a response is received.
func (client *Client) StartRecording(
	ctx context.Context,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	channel []byte,
) (int64, error) {
	return client.sessionControlOps(client.Poll).startRecording(
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
func (client *Client) StopRecording(
	ctx context.Context,
	streamID int32,
	channel []byte,
) error {
	return client.sessionControlOps(client.Poll).stopRecording(ctx, streamID, channel)
}

// StopRecordingAwaitSignal offers a StopRecordingRequest and blocks until the archive
// records a matching STOP signal on tracker. A latched STOP wins over a matching
// ControlResponse in the same window: ERROR fails only when STOP is not yet latched. A
// matching OK alone does not complete: control OK can race ahead of the STOP signal, and
// callers need the latched stop position.
//
// ControlResponseTimeout bounds the whole wait after a successful offer. Timeout may
// fire after a matching OK was consumed if STOP is not latched in time; treat that as
// inconclusive for stop success. Use StopRecording when only an OK ControlResponse proof
// is required.
//
// tracker must be non-nil: it carries the STOP signal proof and is dereferenced on
// every proof check. Matching follows NewStopSignalTracker: a negative recordingID
// latches the first STOP for the session, including unrelated recordings.
//
// Returns ctx.Err() when the context is done before proof is received.
func (client *Client) StopRecordingAwaitSignal(
	ctx context.Context,
	streamID int32,
	channel []byte,
	tracker *StopSignalTracker,
) error {
	return client.sessionControlOps(
		client.pollStopRecordingAwaitSignal(tracker),
	).stopRecordingAwaitSignal(
		ctx,
		streamID,
		channel,
		client.sessionPollTargets(tracker),
		tracker,
	)
}

// StartReplay offers a ReplayRequest and blocks until the archive replies with a
// matching ControlResponse, returning the assigned replay session id.
//
// Returns ctx.Err() when the context is done before a response is received.
func (client *Client) StartReplay(
	ctx context.Context,
	recordingID int64,
	position int64,
	length int64,
	replayStreamID int32,
	replayChannel []byte,
) (int64, error) {
	return client.sessionControlOps(client.Poll).startReplay(
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
func (client *Client) StopReplay(ctx context.Context, replaySessionID int64) error {
	return client.sessionControlOps(client.Poll).stopReplay(ctx, replaySessionID)
}

// ListRecording lists a single recording by id, returning its descriptor extent fields
// with var-ascii bytes cloned at latch time for retention.
//
// Returns ctx.Err() when the context is done before a response is received.
func (client *Client) ListRecording(
	ctx context.Context,
	recordingID int64,
) (codecs.RecordingDescriptorFields, error) {
	return client.sessionControlOps(client.pollListRecording).listRecording(ctx, recordingID)
}

func (client *Client) sessionPollTargets(
	handler RecordingSignalHandler,
) ControlPollTargets {
	return ControlPollTargets{
		SignalHandler:    handler,
		ControlSessionID: client.controlSessionID,
		ResponsePoller:   client.controlPoller,
		DescriptorPoller: client.descriptorPoller,
	}
}
