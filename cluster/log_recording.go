package cluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// recordingIDUnknown is the catalog id sentinel while a start or extend is in
// flight, or after a dirty attempt that never latched START or EXTEND.
const recordingIDUnknown int64 = -1

// logRecordingAutoStop is the autoStop value on every start and extend
// request. The archive releases the recording subscription when the recorded
// session ends, or when it refuses an extend after replying OK.
const logRecordingAutoStop = true

// TermRecordingLookup reports the recording id every TERM entry in the local
// recording log shares.
//
// Behaviour:
//   - implementations return NullRecordingID when no TERM entry exists
//   - Null selects a fresh start recording
//   - any other id selects extend of the established recording
//
// Concurrency: invoker-only.
//
// Read on the same goroutine as [consensus.Agent.DoWork].
//
// Lifetime: the module owns the recording log and closes it after it clears the
// recording control. No read happens after that close.
type TermRecordingLookup interface {
	TermRecordingID() int64
}

var _ TermRecordingLookup = (*recordinglog.Log)(nil)

// logArchiveControl is the archive surface logRecording uses.
//
// *archive.Client implements it. Tests may substitute a double that records
// which stop path ran.
type logArchiveControl interface {
	ControlSessionID() int64
	StartRecordingAwaitSignal(
		ctx context.Context,
		streamID int32,
		sourceLocation archivewire.SourceLocation,
		autoStop bool,
		channel []byte,
		tracker *archive.StartSignalTracker,
	) (recordingID int64, err error)
	ExtendRecordingAwaitSignal(
		ctx context.Context,
		recordingID int64,
		streamID int32,
		sourceLocation archivewire.SourceLocation,
		autoStop bool,
		channel []byte,
		tracker *archive.ExtendSignalTracker,
	) (extendedRecordingID int64, err error)
	StopRecording(ctx context.Context, streamID int32, channel []byte) error
	StopRecordingAwaitSignal(
		ctx context.Context,
		streamID int32,
		channel []byte,
		tracker *archive.StopSignalTracker,
	) error
	ListRecording(
		ctx context.Context,
		recordingID int64,
	) (codecs.RecordingDescriptorFields, error)
	TruncateRecording(
		ctx context.Context,
		recordingID int64,
		position int64,
	) (deletedSegmentCount int64, err error)
}

var _ logArchiveControl = (*archive.Client)(nil)

// logOfferRequest is the offer step every in-flight archive request shares.
type logOfferRequest interface {
	TryOffer() (offered bool, err error)
}

// logStopRequest is one in-flight non-blocking stop request.
//
// *archive.AsyncStopRecording and *archive.AsyncStopRecordingByIdentity
// implement it. Tests substitute a double.
type logStopRequest interface {
	logOfferRequest
	PollResponse() (responded bool, err error)
	Close()
}

var (
	_ logStopRequest = (*archive.AsyncStopRecording)(nil)
	_ logStopRequest = (*archive.AsyncStopRecordingByIdentity)(nil)
)

// logListRequest is one in-flight non-blocking list recording request.
//
// *archive.AsyncListRecording implements it. Tests substitute a double.
type logListRequest interface {
	logOfferRequest
	PollDescriptor() (
		done bool,
		descriptor codecs.RecordingDescriptorFields,
		err error,
	)
	Close()
}

var _ logListRequest = (*archive.AsyncListRecording)(nil)

// pendingStop tracks one async stop attempt across duty ticks.
type pendingStop struct {
	request logStopRequest
	// list is the catalog read. enterCatalogRead opens it after
	// ErrorUnknownSubscription on a known-id stop, or after OK on an identity
	// stop.
	list logListRequest
	// identityRecordingID is the recording an identity attempt stops.
	// NullRecordingID means a stop by channel.
	identityRecordingID int64
	// offered is true once the archive accepts the current step's TryOffer.
	// Later polls skip TryOffer and poll the reply.
	offered bool
	// deadlineNs bounds offer retries, then the reply, in each step. Expiry
	// fail-closes.
	deadlineNs int64
}

func (p *pendingStop) isIdentity() bool {
	return p.identityRecordingID != recordinglog.NullRecordingID
}

var (
	errStopOfferTimeout      = errors.New("offer timed out")
	errStopResponseTimeout   = errors.New("response timed out")
	errNoCatalogStopPosition = errors.New("catalog reports no stop position")
)

// logRecording starts, stops, and truncates this member's cluster log
// recording through the module's archive control client.
//
// Holds needs-stop state in startChannel. A dirty start keeps the channel
// until a later stop drains the channel. A re-start drains any prior
// unstopped channel first.
//
// PollStopLogRecording also stops a recovered term recording by identity. The
// identity stop runs only when no start channel needs a stop and the control
// stores no stop for that id.
//
// Start, stop, and truncate block the invoker until the archive replies or
// ControlResponseTimeout expires. PollStopLogRecording never blocks.
// handleRecordingSignal does not block.
//
// Concurrency: invoker-only.
//
// Lifetime:
//   - outlives the agent method calls that use the control
//   - module clears the control before closing the archive client and before
//     closing the recovery that owns the recording log
//   - no TermRecordingID read happens after the recording-log close
//   - retained context is Open's context with cancel detached
//     (context.WithoutCancel)
//   - Open cancel does not fail later start, stop, or truncate
//
// Ownership: does not own the archive client, the topology, or the term
// recording lookup.
type logRecording struct {
	ctx          context.Context
	client       logArchiveControl
	terms        TermRecordingLookup
	logChannel   string
	logStreamID  int32
	startChannel []byte
	// recordingID is the live catalog id for the armed start, or
	// recordingIDUnknown while the start is unproven or after a dirty arm.
	recordingID int64

	// latchedRecordingID is the catalog id after the last clean stop, successful
	// truncate, identity catalog stop, or plan seed. StopPositionFor matches this
	// id when nothing is pending. The id survives a failed re-start that sets
	// recordingID to unknown then drains dirty.
	latchedRecordingID int64
	stopPosition       int64

	// stopSignalled marks a STOP signal for the live recording.
	// ConsumeStopSignal and StartLogRecording clear the mark.
	stopSignalled bool
	// stopSignalPosition is the position of the marked STOP signal.
	stopSignalPosition int64

	offerRetryTimeoutNs int64
	responseTimeoutNs   int64
	// newStopRequest builds one in-flight stop request. Closed over the
	// archive client at newLogRecording. Tests substitute.
	newStopRequest func(streamID int32, channel []byte) logStopRequest
	// newListRequest builds one in-flight list recording request. Closed over
	// the archive client at newLogRecording. Tests substitute.
	newListRequest func(recordingID int64) logListRequest
	// newIdentityStopRequest builds one in-flight stop by identity request.
	// Closed over the archive client at newLogRecording. Tests substitute.
	newIdentityStopRequest func(recordingID int64) logStopRequest
	pendingStop            *pendingStop
}

var (
	_ consensus.LogRecordingControl = (*logRecording)(nil)
	_ consensus.LogTruncateControl  = (*logRecording)(nil)
)

// moduleControlContext returns the context module archive controls use for
// control calls.
//
// The returned context keeps Open values and detaches cancel with
// context.WithoutCancel so Open cancel does not fail later control calls. A
// nil input returns context.Background().
func moduleControlContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

func newLogRecording(
	ctx context.Context,
	client *archive.Client,
	logChannel string,
	logStreamID int32,
	terms TermRecordingLookup,
) *logRecording {
	ctx = moduleControlContext(ctx)
	cfg := client.Config()
	r := &logRecording{
		ctx:                 ctx,
		client:              client,
		terms:               terms,
		logChannel:          logChannel,
		logStreamID:         logStreamID,
		recordingID:         recordingIDUnknown,
		latchedRecordingID:  recordingIDUnknown,
		offerRetryTimeoutNs: cfg.ControlOfferRetryTimeout.Nanoseconds(),
		responseTimeoutNs:   cfg.ControlResponseTimeout.Nanoseconds(),
	}
	if client != nil {
		r.newStopRequest = func(streamID int32, channel []byte) logStopRequest {
			return client.NewAsyncStopRecording(streamID, channel)
		}
		r.newListRequest = func(recordingID int64) logListRequest {
			return client.NewAsyncListRecording(recordingID)
		}
		r.newIdentityStopRequest = func(recordingID int64) logStopRequest {
			return client.NewAsyncStopRecordingByIdentity(recordingID)
		}
	}
	return r
}

// sessionScopedLogChannel appends session-id=<logSessionID> to logChannel.
func sessionScopedLogChannel(logChannel string, logSessionID int32) string {
	return aeron.AppendChannelParam(
		logChannel,
		aeron.ChannelSessionIDParam,
		strconv.FormatInt(int64(logSessionID), 10),
	)
}

// needsStop reports whether a prior start left a channel that must still be
// stopped.
func (r *logRecording) needsStop() bool {
	return len(r.startChannel) > 0
}

// IsActive reports whether a successful start or extend has not yet cleanly
// stopped.
//
//   - true only when a start channel is kept and the catalog id is known from
//     START or EXTEND
//   - dirty starts keep the channel with an unknown catalog id and are not
//     active
//   - an adopted unsolicited STOP is a clean stop
func (r *logRecording) IsActive() bool {
	return r.needsStop() && r.recordingID >= 0
}

// handleRecordingSignal applies one archive recording signal from
// archive.Client.Poll on the duty cycle.
//
// Behaviour:
//   - STOP for the known catalog id: marks a clean stop, stores the signal
//     position, sets the STOP mark
//   - any other signal: no-op
//   - a foreign recording id: no-op
//   - an inactive control: no-op
//
// Concurrency: invoker-only. Called from archive.Client.Poll.
func (r *logRecording) handleRecordingSignal(event codecs.RecordingSignalEventFields) {
	if event.Signal != archivewire.RecordingSignal_STOP {
		return
	}
	if !r.IsActive() {
		return
	}
	if event.RecordingID != r.recordingID {
		return
	}
	r.markStopped(event.Position)
	r.stopSignalled = true
	r.stopSignalPosition = event.Position
}

// ConsumeStopSignal takes the STOP mark. The archive STOP signal for the
// live recording sets the mark. A blocking stop sets no mark.
//
// Never blocks. Never calls archive Poll.
//
// Concurrency: invoker-only.
//
// Returns:
//   - stopPosition, true: a STOP arrived since the last call
//   - recordinglog.NullPosition, false: no STOP arrived
func (r *logRecording) ConsumeStopSignal() (stopPosition int64, stopped bool) {
	if !r.stopSignalled {
		return recordinglog.NullPosition, false
	}
	r.stopSignalled = false
	return r.stopSignalPosition, true
}

// isUnknownSubscription reports whether err is archive ErrorUnknownSubscription.
func isUnknownSubscription(err error) bool {
	code, ok := archive.ExceptionCode(err)
	return ok && code == archive.ErrorUnknownSubscription
}

// establishedTermRecordingID returns the shared TERM recording id, or
// NullRecordingID when no lookup is wired or the log holds no TERM entry.
func (r *logRecording) establishedTermRecordingID() int64 {
	if r.terms == nil {
		return recordinglog.NullRecordingID
	}
	return r.terms.TermRecordingID()
}

// StartLogRecording starts or extends recording this member's cluster log and
// returns the catalog recording id.
//
// Abandons a pending async stop first, then drains any prior unstopped
// channel. No TERM recording id starts a fresh recording. An established
// TERM id extends that recording.
//
// Does not refuse session ids, including -1. Structural refusal is
// [consensus.Agent.JoinLogAsLeader] only.
//
// Returns an error when:
//   - a prior unstopped recording cannot be drained
//   - the start or extend request cannot be offered within the offer bound
//   - the archive refuses the start or extend
//   - no correlated START or EXTEND signal arrives within the response bound
//
// On error after the offer, StopLogRecording drains. ErrorUnknownSubscription
// is a clean rollback and clears startChannel. A dirty rollback keeps
// startChannel.
func (r *logRecording) StartLogRecording(
	sourceLocation archivewire.SourceLocation,
	logSessionID int32,
	recordingChannel string,
) (
	recordingID int64,
	err error,
) {
	if drainErr := r.StopLogRecording(); drainErr != nil {
		return 0, fmt.Errorf(
			"cluster: drain prior log recording before start: %w",
			drainErr,
		)
	}

	if recordingChannel == "" {
		recordingChannel = r.logChannel
	}
	channelBytes := []byte(sessionScopedLogChannel(recordingChannel, logSessionID))
	// Arm channel state for this attempt.
	//
	// Catalog id stays unknown until START or EXTEND proves so a dirty rollback
	// cannot stop or list a stale or zero id. A STOP mark names the prior
	// recording, so the new attempt clears it.
	r.startChannel = channelBytes
	r.recordingID = recordingIDUnknown
	r.stopSignalled = false

	established := r.establishedTermRecordingID()
	var operation string
	if established == recordinglog.NullRecordingID {
		operation = "start log recording"
		tracker := archive.NewStartSignalTracker(r.client.ControlSessionID())
		recordingID, err = r.client.StartRecordingAwaitSignal(
			r.ctx,
			r.logStreamID,
			sourceLocation,
			logRecordingAutoStop,
			channelBytes,
			tracker,
		)
	} else {
		operation = fmt.Sprintf("extend log recording %d", established)
		tracker := archive.NewExtendSignalTracker(
			r.client.ControlSessionID(),
			established,
		)
		recordingID, err = r.client.ExtendRecordingAwaitSignal(
			r.ctx,
			established,
			r.logStreamID,
			sourceLocation,
			logRecordingAutoStop,
			channelBytes,
			tracker,
		)
	}
	if err != nil {
		// Armed state is the dirty stop path. Start and extend rollback share
		// StopLogRecording with the prepare/Close drain. autoStop releases
		// nothing when OK arrived without START, so this stop must run.
		if stopErr := r.StopLogRecording(); stopErr != nil {
			err = errors.Join(err, stopErr)
		}
		return 0, fmt.Errorf(
			"cluster: %s for stream %d on %s: %w",
			operation,
			r.logStreamID,
			string(channelBytes),
			err,
		)
	}

	r.recordingID = recordingID
	return recordingID, nil
}

// StopLogRecording stops the recording StartLogRecording started. A known
// catalog id stores the stop position for [logRecording.StopPositionFor].
//
// Abandons a pending async stop first. Known catalog id awaits STOP and
// stores the position. On ErrorUnknownSubscription, one ListRecording
// resolves the stop. Dirty uses an OK-bound StopRecording.
// ErrorUnknownSubscription is clean. Dirty must not use a match-any STOP
// tracker.
//
// Idempotent. Second call with nothing started offers nothing.
//
// Concurrency: invoker-only.
//
// Returns an error when:
//   - the stop request cannot be offered within the offer bound
//   - known catalog id and no STOP signal within the response bound
//   - known catalog id, ErrorUnknownSubscription, and ListRecording cannot
//     resolve a stop position
//
// On error the recording may still be running. startChannel stays so a later
// stop can retry.
func (r *logRecording) StopLogRecording() error {
	r.clearPendingStop()
	if !r.needsStop() {
		return nil
	}

	// Dirty: OK-bound stop. No stop position to adopt.
	if r.recordingID < 0 {
		err := r.client.StopRecording(r.ctx, r.logStreamID, r.startChannel)
		if err != nil && !isUnknownSubscription(err) {
			return err
		}
		r.markStopped(r.stopPosition)
		return nil
	}

	tracker := archive.NewStopSignalTracker(
		r.client.ControlSessionID(),
		r.recordingID,
	)
	err := r.client.StopRecordingAwaitSignal(
		r.ctx,
		r.logStreamID,
		r.startChannel,
		tracker,
	)
	if err != nil {
		if isUnknownSubscription(err) {
			return r.resolveStopPositionFromCatalog()
		}
		return err
	}

	r.markStopped(tracker.StoppedPosition())
	return nil
}

// PollStopLogRecording advances the pending stop one step. Never blocks.
// Never calls Agent.DoWork. Never calls archive.Client.Poll.
//
// Known catalog id waits for STOP. Dirty finishes on OK.
// ErrorUnknownSubscription is clean when dirty. For a known id, the method
// closes the stop request and lists the recording. The catalog stop position
// marks the stop. A null catalog stop position fails closed.
//
// Identity attempt: no start channel needs a stop and the control stores no
// stop for the term recording id. The method stops that recording by identity.
// An OK reply, stopped or not, closes the stop request and lists the
// recording. The method stores the catalog stop for that id. Every error reply
// and a null catalog stop fail closed.
//
// Concurrency: invoker-only.
//
// Returns:
//   - true, nil: stop confirmed, identity catalog stop stored, or nothing
//     needed stop
//   - false, nil: poll again
//   - false, err: fail closed. Pending state is cleared. startChannel stays
func (r *logRecording) PollStopLogRecording(nowNs int64) (bool, error) {
	if !r.needsStop() {
		recordingID := r.identityStopTarget()
		if recordingID == recordinglog.NullRecordingID {
			// A truncate can store the target's stop while an identity
			// attempt runs. The attempt then has nothing left to stop.
			r.clearPendingStop()
			return true, nil
		}
		return r.pollIdentityStop(recordingID, nowNs)
	}
	p := r.pendingStop
	if p == nil {
		p = r.armPendingStop(nowNs)
	}
	if p.list != nil {
		return r.pollCatalogStop(p, nowNs)
	}
	responded, err := r.pollStopReply(p, nowNs)
	dirty := r.recordingID < 0
	unknownSubscription := isUnknownSubscription(err)
	switch {
	case dirty && unknownSubscription:
		r.markStopped(r.stopPosition)
		return true, nil
	case unknownSubscription:
		r.enterCatalogRead(p, nowNs)
		return r.pollCatalogStop(p, nowNs)
	case err != nil:
		return false, r.failPendingStop(err)
	case dirty && responded:
		r.markStopped(r.stopPosition)
		return true, nil
	}
	return false, nil
}

// identityStopTarget returns the term recording id when the control stores
// no stop for that id. Returns NullRecordingID otherwise. The caller checks
// that no start channel needs a stop.
func (r *logRecording) identityStopTarget() int64 {
	id := r.establishedTermRecordingID()
	if id == recordinglog.NullRecordingID || id == r.latchedRecordingID {
		return recordinglog.NullRecordingID
	}
	return id
}

// pollIdentityStop advances the identity stop for recordingID one step. An OK
// reply, stopped or not, enters the catalog read. Every failure fail-closes.
func (r *logRecording) pollIdentityStop(recordingID, nowNs int64) (bool, error) {
	p := r.pendingStop
	if p == nil {
		p = &pendingStop{
			request:             r.newIdentityStopRequest(recordingID),
			identityRecordingID: recordingID,
			deadlineNs:          nowNs + r.offerRetryTimeoutNs,
		}
		r.pendingStop = p
	}
	if p.list != nil {
		return r.pollCatalogStop(p, nowNs)
	}
	responded, err := r.pollStopReply(p, nowNs)
	switch {
	case err != nil:
		return false, r.failPendingStop(err)
	case responded:
		r.enterCatalogRead(p, nowNs)
		return r.pollCatalogStop(p, nowNs)
	}
	return false, nil
}

// pollStopReply offers the stop request of p until the archive accepts, then
// polls the reply once per call. The caller maps the reply.
//
// Returns:
//   - true, nil: OK reply
//   - true, err: the archive replied with err
//   - false, nil: poll again
//   - false, err: the offer failed, or a bound expired
func (r *logRecording) pollStopReply(p *pendingStop, nowNs int64) (bool, error) {
	offered, err := r.offerPendingStep(p, p.request, nowNs)
	if err != nil {
		return false, err
	}
	if !offered {
		return false, nil
	}
	responded, err := p.request.PollResponse()
	if responded || err != nil {
		return responded, err
	}
	if nowNs >= p.deadlineNs {
		return false, errStopResponseTimeout
	}
	return false, nil
}

// offerPendingStep offers request once per poll until the archive accepts. On
// accept it starts the reply deadline.
//
// Returns:
//   - true, nil: the offer is accepted, on this call or an earlier one
//   - false, nil: offer again on a later poll
//   - false, err: the offer failed, or errStopOfferTimeout past the deadline
func (r *logRecording) offerPendingStep(
	p *pendingStop,
	request logOfferRequest,
	nowNs int64,
) (
	offered bool,
	err error,
) {
	if p.offered {
		return true, nil
	}
	offered, err = request.TryOffer()
	if err != nil {
		return false, err
	}
	if !offered {
		if nowNs >= p.deadlineNs {
			return false, errStopOfferTimeout
		}
		return false, nil
	}
	p.offered = true
	p.deadlineNs = nowNs + r.responseTimeoutNs
	return true, nil
}

// enterCatalogRead swaps the stop request for a list request. The stop
// request closes first, so the client carries one outstanding request.
func (r *logRecording) enterCatalogRead(p *pendingStop, nowNs int64) {
	p.request.Close()
	p.list = r.newListRequest(r.catalogRecordingID(p))
	p.offered = false
	p.deadlineNs = nowNs + r.offerRetryTimeoutNs
}

// catalogRecordingID returns the recording id the catalog read of p lists:
// the identity target, or the live catalog id for a stop by channel.
func (r *logRecording) catalogRecordingID(p *pendingStop) int64 {
	if p.isIdentity() {
		return p.identityRecordingID
	}
	return r.recordingID
}

// pollCatalogStop advances the list request one step. A descriptor with a
// stop position stores that stop. Every failure fail-closes.
func (r *logRecording) pollCatalogStop(p *pendingStop, nowNs int64) (bool, error) {
	offered, err := r.offerPendingStep(p, p.list, nowNs)
	if err != nil {
		return false, r.failCatalogStop(p, err)
	}
	if !offered {
		return false, nil
	}
	done, descriptor, err := p.list.PollDescriptor()
	switch {
	case err != nil:
		return false, r.failCatalogStop(p, err)
	case done && descriptor.StopPosition == recordinglog.NullPosition:
		return false, r.failCatalogStop(p, errNoCatalogStopPosition)
	case done:
		r.storeCatalogStop(p, descriptor.StopPosition)
		return true, nil
	}
	if nowNs >= p.deadlineNs {
		return false, r.failCatalogStop(p, errStopResponseTimeout)
	}
	return false, nil
}

// storeCatalogStop stores the catalog stop. An identity attempt stores the
// stop for its target id, then clears the attempt. A stop by channel marks a
// clean stop through markStopped.
func (r *logRecording) storeCatalogStop(p *pendingStop, stopPosition int64) {
	if p.isIdentity() {
		r.storeStopPosition(p.identityRecordingID, stopPosition)
		r.clearPendingStop()
		return
	}
	r.markStopped(stopPosition)
}

// failCatalogStop fail-closes with the listed recording id in the error.
func (r *logRecording) failCatalogStop(p *pendingStop, cause error) error {
	return r.failPendingStop(
		fmt.Errorf("list recording %d: %w", r.catalogRecordingID(p), cause),
	)
}

// armPendingStop builds one async stop request and stores it as the in-flight
// attempt.
func (r *logRecording) armPendingStop(nowNs int64) *pendingStop {
	p := &pendingStop{
		request:             r.newStopRequest(r.logStreamID, r.startChannel),
		identityRecordingID: recordinglog.NullRecordingID,
		deadlineNs:          nowNs + r.offerRetryTimeoutNs,
	}
	r.pendingStop = p
	return p
}

// failPendingStop closes the open requests, clears pendingStop, and keeps
// startChannel. The wrapped error names the stop operation.
func (r *logRecording) failPendingStop(cause error) error {
	p := r.pendingStop
	r.clearPendingStop()
	if p != nil && p.isIdentity() {
		return fmt.Errorf(
			"cluster: stop log recording %d by identity: %w",
			p.identityRecordingID,
			cause,
		)
	}
	return fmt.Errorf(
		"cluster: stop log recording for stream %d on %s: %w",
		r.logStreamID,
		string(r.startChannel),
		cause,
	)
}

// clearPendingStop closes the open requests when an attempt is armed.
func (r *logRecording) clearPendingStop() {
	p := r.pendingStop
	if p == nil {
		return
	}
	p.request.Close()
	if p.list != nil {
		p.list.Close()
	}
	r.pendingStop = nil
}

// markStopped marks a clean stop. The method stores stopPosition, clears the
// channel, and closes any in-flight async stop or list request.
//
// When the live catalog id is known, markStopped also stores that id for
// StopPositionFor. Dirty drains leave the prior stored id unchanged.
func (r *logRecording) markStopped(stopPosition int64) {
	if r.recordingID >= 0 {
		r.latchedRecordingID = r.recordingID
	}
	r.stopPosition = stopPosition
	r.startChannel = nil
	r.clearPendingStop()
}

// catalogStopPosition reads the archive catalog stop position for recordingID.
//
// Behaviour:
//   - performs one bounded ListRecording
//   - does not latch
//
// The caller decides whether the value belongs to the recording this control
// stopped. A read for a foreign id cannot overwrite the retained position.
//
// Returns an error when:
//   - the catalog read fails
//   - the descriptor carries no stop position
func (r *logRecording) catalogStopPosition(recordingID int64) (int64, error) {
	desc, err := r.client.ListRecording(r.ctx, recordingID)
	if err != nil {
		return 0, fmt.Errorf(
			"cluster: resolve log recording stop position for recording %d: %w",
			recordingID,
			err,
		)
	}
	if desc.StopPosition == recordinglog.NullPosition {
		return 0, fmt.Errorf(
			"cluster: log recording %d reports no stop position",
			recordingID,
		)
	}
	return desc.StopPosition, nil
}

// resolveStopPositionFromCatalog reads the catalog stop position for the
// retained recording id after ErrorUnknownSubscription on stop. The method
// latches the position through markStopped.
func (r *logRecording) resolveStopPositionFromCatalog() error {
	stopPosition, err := r.catalogStopPosition(r.recordingID)
	if err != nil {
		return err
	}
	r.markStopped(stopPosition)
	return nil
}

// StopPositionFor returns the archive stop position recorded for recordingID.
//
// Behaviour:
//   - same id as the stored catalog id (last clean stop, truncate, identity
//     catalog stop, or plan seed) and nothing pending: returns the stored stop
//     position. No catalog read. A later clean stop overwrites a truncate
//     store.
//   - any other case: reads the catalog once. Does not store that read.
//   - same id while stop still pending: takes the catalog path. Errors only
//     when the catalog read fails or reports no stop position
//
// StopPositionFor has no caller-supplied position to fall back to.
//
// Concurrency: invoker-only.
//
// Returns an error when:
//   - recordingID is negative
//   - the catalog read fails
//   - the catalog reports no stop position for recordingID
//
// On error the caller must not substitute another position.
func (r *logRecording) StopPositionFor(recordingID int64) (int64, error) {
	if recordingID < 0 {
		return recordinglog.NullPosition, fmt.Errorf(
			"cluster: resolve log recording stop position for recording %d: negative recording id",
			recordingID,
		)
	}
	if recordingID == r.latchedRecordingID && r.latchedRecordingID >= 0 && !r.needsStop() {
		return r.stopPosition, nil
	}
	stopPosition, err := r.catalogStopPosition(recordingID)
	if err != nil {
		return recordinglog.NullPosition, err
	}
	return stopPosition, nil
}

// seedStopPosition stores recordingID and stopPosition for StopPositionFor.
// No-op when either is Null. StopPositionFor for the seeded id takes the
// stored path when nothing needs stop.
func (r *logRecording) seedStopPosition(recordingID, stopPosition int64) {
	if recordingID == recordinglog.NullRecordingID ||
		stopPosition == recordinglog.NullPosition {
		return
	}
	r.storeStopPosition(recordingID, stopPosition)
}

// seedLogRecordingStopFromPlan copies a concrete recovery-extent stop into the
// recording control.
//
// No-op when the control is nil, the plan log is nil, the term recording id is
// NullRecordingID, or the extent stop is NullPosition.
func seedLogRecordingStopFromPlan(
	r *logRecording,
	plan recordinglog.RecoveryPlan,
) {
	if r == nil || plan.Log == nil {
		return
	}
	r.seedStopPosition(plan.TermRecordingID(), plan.Log.Extent.StopPosition)
}

// TruncateLogRecording removes every recorded byte at or above position. The
// caller must not call truncate while the recording is active.
//
// Behaviour: offers one bounded archive truncate for recordingID. On success
// the truncate position is stored so [logRecording.StopPositionFor] reports
// that position until a later clean stop overwrites it. On error nothing is
// stored.
//
// Concurrency: invoker-only. Blocks the invoker until the archive replies or
// ControlResponseTimeout expires.
//
// Returns deletedSegmentCount from the archive reply with a nil error on
// success. The error wraps the archive cause. deletedSegmentCount is
// meaningless on error.
func (r *logRecording) TruncateLogRecording(
	recordingID int64,
	position int64,
) (
	deletedSegmentCount int64,
	err error,
) {
	count, err := r.client.TruncateRecording(r.ctx, recordingID, position)
	if err != nil {
		return 0, fmt.Errorf(
			"cluster: truncate log recording %d to %d: %w",
			recordingID, position, err,
		)
	}
	r.storeStopPosition(recordingID, position)
	return count, nil
}

// storeStopPosition stores recordingID and position for StopPositionFor.
//
// startChannel is kept. A truncate may run while a dirty drain is pending. That
// drain still needs the channel. While the channel is pending, StopPositionFor
// uses the catalog path and reports the same lowered stop.
func (r *logRecording) storeStopPosition(recordingID, position int64) {
	r.latchedRecordingID = recordingID
	r.stopPosition = position
}
