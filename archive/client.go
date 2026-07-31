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
// through PollControlEvents.
//
// Poll never polls the underlying pollers directly and never calls DoWork.
//
// Recording signals are dropped until Config.RecordingSignalHandler or
// SetRecordingSignalHandler configures a handler.
//
// Configured handlers also run during blocking control awaits that use this
// handler:
//   - StartRecording
//   - StopRecording
//   - StartReplay
//   - StopReplay
//   - ListRecording
//
// StartRecordingAwaitSignal, StopRecordingAwaitSignal, and
// ExtendRecordingAwaitSignal poll internally.
//
// Those methods route START, STOP, and EXTEND proofs to the per-call tracker,
// not the configured handler.
//
// ListRecording uses a descriptor-mode poll so its OK response routes to the
// descriptor poller rather than the session control-response poller.
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
	cfg                 Config
	closed              bool
}

// Connect performs the blocking archive control connect handshake and returns a session
// wrapper.
//
// It drives DoWork on the transport client until the handshake completes,
// ctx is canceled or its deadline is exceeded, or ConnectTimeout elapses. The await loop
// paces via cfg.IdleStrategy.Idle(0) and calls IdleStrategy.Reset on exit so a
// shared backoff instance is not left in PARKING before the post-connect duty cycle.
//
// When cfg.Aeron is nil, ctx also bounds owned transport attach (CNC ready and
// driver heartbeat) via NewAsyncConnect. Shared Aeron skips that wait.
//
// NewAsyncConnect normalizes and validates cfg; pre-normalizing is harmless.
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
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	connect, err := NewAsyncConnect(ctx, cfg)
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
	idle := connect.cfg.IdleStrategy
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
		signalHandler:       result.Config.RecordingSignalHandler,
		controlSubscription: result.ControlSubscription,
		controlAssembler:    NewFragmentAssembler(nil),
		aeronClient:         result.Aeron,
		ownsAeronClient:     result.OwnsAeronClient,
		cfg:                 result.Config,
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

// Config returns the normalized archive config stored at connect.
//
// Nil receiver returns a zero Config.
func (client *Client) Config() Config {
	if client == nil {
		return Config{}
	}
	return client.cfg
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

// pollRecordingSignalAwait routes control-event polls through targets for the whole
// await (offer retry and response loop).
func (client *Client) pollRecordingSignalAwait(targets ControlPollTargets) func() int {
	return func() int {
		return PollControlEvents(
			client.controlSubscription,
			DefaultControlResponseFragmentLimit,
			client.controlAssembler,
			targets,
		)
	}
}

// SetRecordingSignalHandler overrides Config.RecordingSignalHandler for recording
// signals on Poll and during blocking control awaits that use this handler:
//   - StartRecording
//   - StopRecording
//   - StartReplay
//   - StopReplay
//   - ListRecording
//
// StartRecordingAwaitSignal, StopRecordingAwaitSignal, and
// ExtendRecordingAwaitSignal route proofs to the per-call tracker instead of
// this handler.
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

// StartRecordingAwaitSignal offers a StartRecordingRequest and blocks until tracker
// latches the correlated START RecordingSignalEvent.
//
// Returns the catalog recording id from that signal.
//
// Behaviour:
//   - the proof check reads tracker before the control response
//   - a latched START wins when both are present
//   - ERROR fails the wait only when START is not yet latched
//   - a matching OK alone does not complete the wait
//   - the control response carries the subscription registration id, not the
//     catalog recording id
//   - the archive sends the START signal only after an image appears on the
//     recording subscription
//
// ControlResponseTimeout bounds the whole wait after a successful offer.
//
// On timeout:
//   - the recording may still be started
//   - the caller owns the cleanup
//
// Use StartRecording when only an OK ControlResponse proof is required.
//
// tracker must be non-nil.
//
// The tracker carries the START signal proof and is dereferenced on every proof check.
//
// Returns ctx.Err() when the context is done before proof is received.
func (client *Client) StartRecordingAwaitSignal(
	ctx context.Context,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	channel []byte,
	tracker *StartSignalTracker,
) (recordingID int64, err error) {
	targets := client.sessionPollTargets(tracker)
	err = client.sessionControlOps(
		client.pollRecordingSignalAwait(targets),
	).startRecordingAwaitSignal(
		ctx,
		streamID,
		sourceLocation,
		channel,
		targets,
		tracker,
	)
	if err != nil {
		return 0, err
	}
	return tracker.StartedRecordingID(), nil
}

// ExtendRecordingAwaitSignal offers an ExtendRecordingRequest2 and blocks until
// tracker latches the correlated EXTEND RecordingSignalEvent.
//
// Returns the recording id the caller asked to extend. No new recording id is
// assigned.
//
// Behaviour:
//   - the proof check reads tracker before the control response
//   - a latched EXTEND wins when both are present
//   - ERROR fails the wait only when EXTEND is not yet latched
//   - a matching OK alone does not complete the wait
//   - the archive replies OK once the recording subscription is added
//   - OK arrives before any image exists and before catalog image validation
//   - the archive sends EXTEND only after that validation passes
//   - latched EXTEND is the proof the extend took effect
//
// tracker must be non-nil and built with the same recordingID passed to this
// method:
//   - NewExtendSignalTracker filters EXTEND by that id
//   - a mismatch ignores a real EXTEND and the wait times out
//   - the tracker carries the EXTEND signal proof and is dereferenced on every
//     proof check
//
// autoStop true:
//   - releases the recording subscription when the recording session ends
//   - makes the archive release the subscription when validation refuses the
//     extend after OK
//
// autoStop false:
//   - the caller must release the subscription by stopping the recording on
//     the same channel and stream
//
// Pass true unless the caller owns a release path it drives itself.
//
// ControlResponseTimeout bounds the whole wait after a successful offer.
//
// On timeout:
//   - the extend may still have taken effect
//   - the caller owns the cleanup
//
// Concurrency: invoker-only.
//
// Blocks the invoker. Drives the transport client while it waits.
//
// Returns an error when:
//   - the extend request cannot be offered within the offer bound
//   - the archive refuses the extend, including ErrorUnknownRecording,
//     ErrorActiveRecording, ErrorActiveSubscription, and ErrorInvalidExtension
//   - no correlated EXTEND signal arrives within the response bound
//   - ctx is done before proof is received
func (client *Client) ExtendRecordingAwaitSignal(
	ctx context.Context,
	recordingID int64,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	autoStop bool,
	channel []byte,
	tracker *ExtendSignalTracker,
) (
	extendedRecordingID int64,
	err error,
) {
	wireAutoStop := archivewire.BooleanType_FALSE
	if autoStop {
		wireAutoStop = archivewire.BooleanType_TRUE
	}
	targets := client.sessionPollTargets(tracker)
	err = client.sessionControlOps(
		client.pollRecordingSignalAwait(targets),
	).extendRecordingAwaitSignal(
		ctx,
		recordingID,
		streamID,
		sourceLocation,
		wireAutoStop,
		channel,
		targets,
		tracker,
	)
	if err != nil {
		return 0, err
	}
	return tracker.ExtendedRecordingID(), nil
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

// StopRecordingAwaitSignal offers a StopRecordingRequest and blocks until tracker
// latches a matching STOP RecordingSignalEvent.
//
// The proof check reads tracker before the control response, so a latched STOP
// wins when both are captured: ERROR fails the wait only when STOP is not yet
// latched. A matching OK alone does not complete: control OK can race ahead of
// the STOP signal, and callers need the latched stop position.
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
	targets := client.sessionPollTargets(tracker)
	return client.sessionControlOps(
		client.pollRecordingSignalAwait(targets),
	).stopRecordingAwaitSignal(
		ctx,
		streamID,
		channel,
		targets,
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
