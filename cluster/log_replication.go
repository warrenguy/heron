package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/consensus"
)

const (
	channelInterfaceParam = "interface="
	channelControlParam   = "control="
)

var (
	errLogReplicationNoProgress       = errors.New("cluster: log replication has not progressed")
	errLogReplicationStopMissed       = errors.New("cluster: log replication did not stop")
	errLogReplicationRecordingDeleted = errors.New("cluster: log replication recording deleted")
	errLogReplicationEndedEarly       = errors.New("cluster: log replication ended before extend")
	errLogReplicationStoppedShort     = errors.New(
		"cluster: log replication stopped below the stop position",
	)
)

// replicateRequest is the archive.AsyncReplicate surface logReplication drives.
type replicateRequest interface {
	TryOffer() (bool, error)
	PollResponse() (bool, error)
	RecordingSignal(event codecs.RecordingSignalEventFields)
	ReplicationID() int64
	RecordingID() int64
	Position() int64
	HasExtended() bool
	HasStopped() bool
	HasReplicationEnded() bool
	IsRecordingDeleted() bool
	Close()
}

var (
	_ replicateRequest = (*archive.AsyncReplicate)(nil)
	_ logStopRequest   = (*archive.AsyncStopReplication)(nil)
)

// replicationControl is the archive control surface logReplication needs.
//
// Concurrency: the request builders never block. StopReplication blocks the
// caller until the archive replies.
type replicationControl interface {
	NewAsyncReplicate(
		sourceRecordingID int64,
		sourceControlStreamID int32,
		sourceControlChannel []byte,
		params archive.ReplicationParams,
	) replicateRequest
	NewAsyncStopReplication(replicationID int64) logStopRequest
	StopReplication(ctx context.Context, replicationID int64) (bool, error)
}

// archiveReplicationControl is the replicationControl over an archive client.
type archiveReplicationControl struct {
	client *archive.Client
}

var _ replicationControl = archiveReplicationControl{}

func (c archiveReplicationControl) NewAsyncReplicate(
	sourceRecordingID int64,
	sourceControlStreamID int32,
	sourceControlChannel []byte,
	params archive.ReplicationParams,
) replicateRequest {
	return c.client.NewAsyncReplicate(
		sourceRecordingID,
		sourceControlStreamID,
		sourceControlChannel,
		params,
	)
}

func (c archiveReplicationControl) NewAsyncStopReplication(
	replicationID int64,
) logStopRequest {
	return c.client.NewAsyncStopReplication(replicationID)
}

func (c archiveReplicationControl) StopReplication(
	ctx context.Context,
	replicationID int64,
) (bool, error) {
	return c.client.StopReplication(ctx, replicationID)
}

// recordingPositionSource reads the RecordingPos counter of one recording.
type recordingPositionSource interface {
	// RecordingPosition returns the counter value and true. It returns 0 and
	// false when the archive does not hold a counter for recordingID.
	RecordingPosition(recordingID int64) (int64, bool)
}

var _ recordingPositionSource = (*cnc.RecordingPositionCache)(nil)

// archiveDestinationChannel returns the control request channel of the
// archive at endpoint. The channel keeps the interface of template and drops
// every other template parameter.
//
// Returns an error when checkChannelEndpoint rejects template or endpoint.
func archiveDestinationChannel(template, endpoint string) (string, error) {
	if err := checkChannelEndpoint(template, endpoint); err != nil {
		return "", err
	}
	channel := aeron.AppendChannelParam(udpChannelPrefix, aeron.ChannelEndpointParam, endpoint)
	return copyChannelParamIfPresent(channel, template, channelInterfaceParam), nil
}

// responseReplicationChannel returns channel without endpoint=. The result
// sets control=responseEndpoint and control-mode=response. Each set replaces
// an existing value.
//
// Returns an error when checkChannelEndpoint rejects channel or
// responseEndpoint.
func responseReplicationChannel(channel, responseEndpoint string) (string, error) {
	if err := checkChannelEndpoint(channel, responseEndpoint); err != nil {
		return "", err
	}
	channel = removeChannelParam(channel, aeron.ChannelEndpointParam)
	channel = aeron.SetChannelParam(channel, channelControlParam, responseEndpoint)
	return aeron.SetChannelParam(
		channel,
		aeron.ChannelControlModeParam,
		aeron.ChannelControlModeResponse,
	), nil
}

// removeChannelParam returns channel without param and its value. A query
// left empty loses its '?'.
func removeChannelParam(channel, param string) string {
	idx := aeron.ChannelParamIndex(channel, param, true)
	if idx < 0 {
		return channel
	}
	end := idx + len(param)
	if pipe := strings.IndexByte(channel[end:], '|'); pipe >= 0 {
		return channel[:idx] + channel[end+pipe+1:]
	}
	// The param is last. Drop it and the '|' or '?' before it.
	return channel[:idx-1]
}

// logReplicationDeps holds the collaborators of logReplication. Every field is
// required except Ctx and ReplicationChannel.
type logReplicationDeps struct {
	// Ctx carries values into the blocking StopReplication call at teardown.
	// newLogReplication detaches cancel. Nil uses context.Background().
	Ctx context.Context

	// Control builds the replicate and stop requests.
	Control replicationControl

	// Positions reads the RecordingPos counter of the destination recording.
	Positions recordingPositionSource

	// NextCorrelationID supplies the replication session id.
	NextCorrelationID func() int64

	// Agent supplies the local log recording id.
	Agent *consensus.Agent

	// Recording holds the stop position of the destination recording.
	Recording *logRecording

	// ReplicationChannel is the channel the destination archive receives the
	// replay on. Empty uses the archive's own replication channel.
	ReplicationChannel string

	// LeaderArchiveControlChannel is the template of the leader archive
	// control request channel.
	LeaderArchiveControlChannel string

	// ControlRequestStreamID is the control request stream of the leader
	// archive.
	ControlRequestStreamID int32

	// ProgressCheckIntervalNs spaces the RecordingPos counter reads.
	// ProgressTimeoutNs caps the time a running replication may go without a
	// position rise.
	ProgressCheckIntervalNs int64
	ProgressTimeoutNs       int64

	// OfferTimeoutNs caps each request offer. ResponseTimeoutNs caps each
	// response wait.
	OfferTimeoutNs    int64
	ResponseTimeoutNs int64
}

// logReplicationPhase is one step of a log replication.
type logReplicationPhase uint8

const (
	logReplicationOffering logReplicationPhase = iota
	logReplicationAwaitingResponse
	logReplicationRunning
	logReplicationStopping
	logReplicationClosed
)

// logReplication copies the leader's log recording into this member's log
// recording through the local archive.
//
// Behaviour:
//   - offering: DoWork offers the replicate request. DoWork fails
//     OfferTimeoutNs after newLogReplication. An accepted offer drops the
//     stored stop of the destination recording
//   - awaiting response: DoWork polls the response. DoWork fails
//     ResponseTimeoutNs after the accepted offer. The OK starts both progress
//     clocks
//   - running: DoWork fails on DELETE or on REPLICATE_END without EXTEND.
//     DoWork reads the RecordingPos counter every ProgressCheckIntervalNs. A
//     rise moves the progress deadline to now plus ProgressTimeoutNs. DoWork
//     fails at the progress deadline
//   - stopping: DoWork offers the stop request, then polls the response.
//     Each timeout runs from the DoWork call that starts its step. An OK or an
//     unknown replication closes the helper
//   - closed: DoWork returns 0. OnSignal drops the signal
//
// A failure closes the helper before DoWork returns the error. A running
// helper that has not ended moves to stopping instead.
//
// Concurrency: invoker-only.
//
// Lifetime: the request handle aliases the channel and params slices until
// it closes.
//
// Ownership: owns the replicate and stop request handles. Does not own the
// archive client, the agent, or the log recording control.
type logReplication struct {
	deps        logReplicationDeps
	params      archive.ReplicationParams
	request     replicateRequest
	stopRequest logStopRequest
	phase       logReplicationPhase

	// buildErr is the build failure the first DoWork returns.
	buildErr error

	// closeRequested marks a Close while the response is outstanding. The OK
	// then moves the helper to stopping.
	closeRequested bool

	offerDeadlineNs     int64
	responseDeadlineNs  int64
	nextProgressCheckNs int64
	progressDeadlineNs  int64

	// counterPosition is the last RecordingPos counter rise, or NullPosition.
	counterPosition int64

	stopOffered            bool
	stopAnchored           bool
	stopOfferDeadlineNs    int64
	stopResponseDeadlineNs int64

	// stopResolved is true once the stop request reported OK or an unknown
	// replication.
	stopResolved bool

	// stopStored is true once the replication stop reached the log recording
	// control.
	stopStored bool

	// tornDown is true once stopReplication ran.
	tornDown bool
}

var _ consensus.LogReplication = (*logReplication)(nil)

// newLogReplication builds the replicate request for leaderRecordingID up to
// stopPosition. nowNs anchors the offer deadline.
//
// Returns a non-nil helper. After a build failure, the first DoWork returns
// the error.
func newLogReplication(
	deps logReplicationDeps,
	leaderArchiveEndpoint string,
	responseArchiveEndpoint string,
	leaderRecordingID int64,
	stopPosition int64,
	nowNs int64,
) *logReplication {
	deps.Ctx = moduleControlContext(deps.Ctx)
	r := &logReplication{
		deps:            deps,
		phase:           logReplicationOffering,
		offerDeadlineNs: nowNs + deps.OfferTimeoutNs,
		counterPosition: archive.NullPosition,
	}

	sourceControlChannel, err := archiveDestinationChannel(
		deps.LeaderArchiveControlChannel,
		leaderArchiveEndpoint,
	)
	if err != nil {
		r.buildErr = fmt.Errorf("cluster: log replication request: %w", err)
		return r
	}

	params := archive.NewReplicationParams()
	params.DestinationRecordingID = deps.Agent.LocalLogRecordingID()
	params.StopPosition = stopPosition
	//nolint:gosec // G115: upstream truncates the correlation id to an int
	params.ReplicationSessionID = int32(deps.NextCorrelationID())
	if responseArchiveEndpoint == "" {
		params.ReplicationChannel = []byte(deps.ReplicationChannel)
	} else {
		channel, err := responseReplicationChannel(deps.ReplicationChannel, responseArchiveEndpoint)
		if err != nil {
			r.buildErr = fmt.Errorf("cluster: log replication request: %w", err)
			return r
		}
		params.ReplicationChannel = []byte(channel)
		params.SourceResponseChannel = params.ReplicationChannel
	}
	r.params = params
	r.request = deps.Control.NewAsyncReplicate(
		leaderRecordingID,
		deps.ControlRequestStreamID,
		[]byte(sourceControlChannel),
		params,
	)
	return r
}

// DoWork drives one step of the current phase.
func (r *logReplication) DoWork(nowNs int64) (int, error) {
	if err := r.buildErr; err != nil {
		r.buildErr = nil
		r.closeNow()
		return 0, err
	}
	switch r.phase {
	case logReplicationOffering:
		return r.offer(nowNs)
	case logReplicationAwaitingResponse:
		return r.awaitResponse(nowNs)
	case logReplicationRunning:
		return r.run(nowNs)
	case logReplicationStopping:
		return r.driveStop(nowNs)
	case logReplicationClosed:
	}
	return 0, nil
}

func (r *logReplication) offer(nowNs int64) (int, error) {
	accepted, err := exchange(
		nowNs,
		r.offerDeadlineNs,
		r.deps.OfferTimeoutNs,
		"request",
		"offer",
		r.request.TryOffer,
	)
	if err != nil {
		return r.fail(err)
	}
	if !accepted {
		return 0, nil
	}
	r.deps.Recording.forgetStopPosition(r.params.DestinationRecordingID)
	r.phase = logReplicationAwaitingResponse
	r.responseDeadlineNs = nowNs + r.deps.ResponseTimeoutNs
	return 1, nil
}

func (r *logReplication) awaitResponse(nowNs int64) (int, error) {
	responded, err := exchange(
		nowNs,
		r.responseDeadlineNs,
		r.deps.ResponseTimeoutNs,
		"request",
		"response",
		r.request.PollResponse,
	)
	if err != nil {
		return r.fail(err)
	}
	if !responded {
		return 0, nil
	}
	if r.closeRequested {
		r.enterStopping()
		return 1, nil
	}
	r.phase = logReplicationRunning
	r.nextProgressCheckNs = nowNs + r.deps.ProgressCheckIntervalNs
	r.progressDeadlineNs = nowNs + r.deps.ProgressTimeoutNs
	// The handle may have folded the end before the OK. Run the running checks
	// on the same tick, so a short copy fails before the election reads IsDone.
	if _, err := r.run(nowNs); err != nil {
		return 0, err
	}
	return 1, nil
}

func (r *logReplication) run(nowNs int64) (int, error) {
	request := r.request
	if request.IsRecordingDeleted() {
		return r.fail(errLogReplicationRecordingDeleted)
	}
	if r.IsDone() {
		if r.Position() < r.params.StopPosition {
			return r.fail(errLogReplicationStoppedShort)
		}
		return 0, nil
	}
	extended := request.HasExtended()
	if request.HasReplicationEnded() && !extended {
		return r.fail(errLogReplicationEndedEarly)
	}

	workCount := 0
	if nowNs >= r.nextProgressCheckNs {
		r.nextProgressCheckNs = nowNs + r.deps.ProgressCheckIntervalNs
		if extended {
			position, ok := r.deps.Positions.RecordingPosition(request.RecordingID())
			if ok && position > r.Position() {
				r.counterPosition = position
				r.progressDeadlineNs = nowNs + r.deps.ProgressTimeoutNs
				workCount++
			}
		}
	}
	if nowNs >= r.progressDeadlineNs {
		if r.Position() < r.params.StopPosition {
			return r.fail(errLogReplicationNoProgress)
		}
		return r.fail(errLogReplicationStopMissed)
	}
	return workCount, nil
}

func (r *logReplication) driveStop(nowNs int64) (int, error) {
	if !r.stopAnchored {
		r.stopAnchored = true
		r.stopOfferDeadlineNs = nowNs + r.deps.OfferTimeoutNs
	}
	if !r.stopOffered {
		accepted, err := exchange(
			nowNs,
			r.stopOfferDeadlineNs,
			r.deps.OfferTimeoutNs,
			"stop",
			"offer",
			r.stopRequest.TryOffer,
		)
		if err != nil {
			return r.fail(err)
		}
		if !accepted {
			return 0, nil
		}
		r.stopOffered = true
		r.stopResponseDeadlineNs = nowNs + r.deps.ResponseTimeoutNs
		return 1, nil
	}
	responded, err := exchange(
		nowNs,
		r.stopResponseDeadlineNs,
		r.deps.ResponseTimeoutNs,
		"stop",
		"response",
		r.stopRequest.PollResponse,
	)
	if err != nil {
		return r.fail(err)
	}
	if !responded {
		return 0, nil
	}
	r.stopResolved = true
	r.closeNow()
	return 1, nil
}

// exchange runs call, then applies deadlineNs to a false result. A result that
// lands on the deadline tick counts. request names the request in errors. wait
// names the step timeoutNs caps.
//
// Returns the call result. Returns an error when call fails, or when a false
// result reaches the deadline.
func exchange(
	nowNs, deadlineNs, timeoutNs int64,
	request, wait string,
	call func() (bool, error),
) (bool, error) {
	done, err := call()
	if err != nil {
		return false, fmt.Errorf("cluster: log replication %s: %w", request, err)
	}
	if !done && nowNs >= deadlineNs {
		return false, fmt.Errorf(
			"cluster: log replication %s: no %s within %s",
			request,
			wait,
			time.Duration(timeoutNs),
		)
	}
	return done, nil
}

// fail closes the helper and returns err.
func (r *logReplication) fail(err error) (int, error) {
	if r.phase == logReplicationRunning {
		r.Close()
	} else {
		r.closeNow()
	}
	return 0, err
}

// enterStopping builds the stop request for the replication id.
func (r *logReplication) enterStopping() {
	r.phase = logReplicationStopping
	r.stopRequest = r.deps.Control.NewAsyncStopReplication(r.request.ReplicationID())
}

// closeNow closes both request handles and enters closed.
func (r *logReplication) closeNow() {
	if r.stopRequest != nil {
		r.stopRequest.Close()
	}
	if r.request != nil {
		r.request.Close()
	}
	r.phase = logReplicationClosed
}

// RecordingID returns the destination recording id, or -1 before EXTEND.
func (r *logReplication) RecordingID() int64 {
	if r.request == nil || !r.request.HasExtended() {
		return cnc.NullValue
	}
	return r.request.RecordingID()
}

// Position returns the larger of the signal position and the last counter
// rise, or -1 before EXTEND.
func (r *logReplication) Position() int64 {
	if r.request == nil || !r.request.HasExtended() {
		return archive.NullPosition
	}
	return max(r.request.Position(), r.counterPosition)
}

// IsDone reports REPLICATE_END and STOP of this replication.
func (r *logReplication) IsDone() bool {
	request := r.request
	return request != nil && request.HasReplicationEnded() && request.HasStopped()
}

// OnSignal passes one archive recording signal to the replicate handle. The
// first STOP with a known id and position stores the stop on the log
// recording control. A closed helper drops the signal.
func (r *logReplication) OnSignal(
	correlationID, recordingID, position int64,
	signal archivewire.RecordingSignal,
) {
	if r.phase == logReplicationClosed || r.request == nil {
		return
	}
	r.request.RecordingSignal(codecs.RecordingSignalEventFields{
		CorrelationID: correlationID,
		RecordingID:   recordingID,
		Position:      position,
		Signal:        signal,
	})
	if r.stopStored || !r.request.HasStopped() {
		return
	}
	id, stopPosition := r.RecordingID(), r.Position()
	if id == cnc.NullValue || stopPosition == archive.NullPosition {
		return
	}
	r.stopStored = true
	r.deps.Recording.storeStopPosition(id, stopPosition)
}

// Close starts the stop. Close never blocks. Close is idempotent.
func (r *logReplication) Close() {
	switch r.phase {
	case logReplicationOffering:
		r.closeNow()
	case logReplicationAwaitingResponse:
		r.closeRequested = true
	case logReplicationRunning:
		if r.request.HasReplicationEnded() {
			r.closeNow()
			return
		}
		r.enterStopping()
	case logReplicationStopping, logReplicationClosed:
	}
}

// IsClosed reports that the helper has no stop left to run.
func (r *logReplication) IsClosed() bool {
	return r.phase == logReplicationClosed
}

// hasUnresolvedStop reports a replication the archive may still run: the
// replication id is known, the replication has not ended, and no stop request
// resolved. A torn-down helper reports false.
func (r *logReplication) hasUnresolvedStop() bool {
	request := r.request
	if r.tornDown || request == nil || r.stopResolved || request.HasReplicationEnded() {
		return false
	}
	return request.ReplicationID() != cnc.NullValue
}

// stopReplication tears the helper down at module close. A second call
// returns nil.
//
// Behaviour:
//  1. closes an open stop request
//  2. closes the replicate request, which keeps the replication id from a
//     latched OK
//  3. calls the blocking StopReplication when hasUnresolvedStop reports true
//  4. marks the helper torn down
//
// Concurrency: invoker-only. Blocks the invoker during StopReplication.
//
// Returns the StopReplication error. An unknown replication returns nil.
func (r *logReplication) stopReplication() error {
	if r.tornDown {
		return nil
	}
	r.closeNow()
	stopNeeded := r.hasUnresolvedStop()
	r.tornDown = true
	if !stopNeeded {
		return nil
	}
	replicationID := r.request.ReplicationID()
	if _, err := r.deps.Control.StopReplication(r.deps.Ctx, replicationID); err != nil {
		return fmt.Errorf("cluster: log replication stop: %w", err)
	}
	return nil
}
