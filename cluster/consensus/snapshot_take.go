package consensus

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// Causes of a failed or skipped module snapshot take.
var (
	errSnapshotServiceFailed  = errors.New("consensus: service snapshot failed")
	errSnapshotNotConfigured  = errors.New("consensus: snapshot recording not configured")
	errSnapshotPublicationAdd = errors.New("consensus: add snapshot publication")
	errSnapshotRecordingStart = errors.New("consensus: start snapshot recording")
	errSnapshotWrite          = errors.New("consensus: write snapshot")
	errSnapshotRecordingStop  = errors.New("consensus: stop snapshot recording")
	errSnapshotRecordingLog   = errors.New("consensus: write snapshot rows to recording log")
)

// SnapshotTakeResult describes one resolved module snapshot take.
type SnapshotTakeResult struct {
	// LogPosition is the snapshot action position.
	LogPosition int64

	// LeadershipTermID is the leadership term of the snapshot action.
	LeadershipTermID int64

	// Timestamp is the snapshot action wire timestamp in cluster time.
	Timestamp int64

	// RecordingID is the catalog id of the module snapshot recording, or -1
	// when Err is not nil.
	RecordingID int64

	// ServiceRecordingIDs holds each service's ack RelevantID, indexed by
	// service id.
	ServiceRecordingIDs []int64

	// Err is nil when the module recorded its snapshot and wrote its
	// recording log rows. Otherwise Err names why the take skipped or failed.
	Err error

	// ForceErr is the recording log Force error after the module row is in
	// the log. The rows stay in the log. Err stays nil.
	ForceErr error
}

// SnapshotStartRequest is one non-blocking start of the module snapshot
// recording. *archive.AsyncStartRecording satisfies it.
type SnapshotStartRequest interface {
	TryOffer() (bool, error)
	PollSignal() (bool, error)
	RecordingID() int64
	Close()
}

// SnapshotRecordingControl starts the module snapshot recording. It watches
// for the STOP signal.
//
// Concurrency: invoker-only.
type SnapshotRecordingControl interface {
	// NewStartRecording builds one LOCAL start with autoStop true.
	NewStartRecording(streamID int32, channel string) SnapshotStartRequest

	// ControlTimeoutsNs returns the archive offer retry and response
	// timeouts in nanoseconds.
	ControlTimeoutsNs() (offerRetryTimeoutNs, responseTimeoutNs int64)

	// WatchStop watches for the STOP signal of recordingID. It replaces an
	// earlier watch.
	WatchStop(recordingID int64)

	// StopPosition returns the STOP position of the watched recording.
	StopPosition() (stopPosition int64, stopped bool)

	// ClearStop ends the watch. It is idempotent.
	ClearStop()
}

// SnapshotPublication is the module snapshot publication.
// *aeron.ExclusivePublication satisfies it.
type SnapshotPublication interface {
	Offer(buffer []byte) int64
	Position() int64
	SessionID() int32
	Close() error
}

// SnapshotPublicationAdder adds the module snapshot publication without
// blocking.
//
// Concurrency: invoker-only.
type SnapshotPublicationAdder interface {
	OfferAddExclusivePublication(streamID int32, channel string) (int64, error)
	PollAddExclusivePublication(correlationID int64) (SnapshotPublication, bool, error)
	CancelPending(correlationID int64)
}

// SnapshotRecordingLog is the recording log the module writes its snapshot
// rows into. *recordinglog.Log satisfies it.
//
// Concurrency: invoker-only.
type SnapshotRecordingLog interface {
	FindTermEntry(leadershipTermID int64) *recordinglog.Entry
	AppendSnapshot(
		recordingID int64,
		leadershipTermID int64,
		termBaseLogPosition int64,
		logPosition int64,
		timestamp int64,
		serviceID int32,
	) error
	Force(level recordinglog.FileSyncLevel) error
}

var _ SnapshotRecordingLog = (*recordinglog.Log)(nil)

// snapshotTakeState names the step a module take runs or waits in.
type snapshotTakeState uint8

const (
	// takeChecking checks the service acks, the seams and commit.
	takeChecking snapshotTakeState = iota

	// takeAddingPublication waits for the snapshot publication add.
	takeAddingPublication

	// takeOfferingStart offers the start recording request.
	takeOfferingStart

	// takeAwaitingStart waits for the START signal.
	takeAwaitingStart

	// takeWriting offers the rows, then closes the publication.
	takeWriting

	// takeAwaitingStop waits for the autoStop STOP signal, then writes the
	// recording log rows and forces the log.
	takeAwaitingStop
)

// snapshotTake is the one module snapshot take in flight.
//
// Ownership: the take owns the pending add, the start request and the
// publication. cleanupSnapshotTake cancels or closes each one.
type snapshotTake struct {
	state snapshotTakeState

	// logPosition is the snapshot action position. The module state holds
	// there until the take resolves.
	logPosition      int64
	leadershipTermID int64
	timestamp        int64

	// serviceRecordingIDs holds each service's ack RelevantID.
	serviceRecordingIDs []int64

	// nextSessionID, sessionRows and timerRows hold the module state copied
	// at the pop.
	nextSessionID int64
	sessionRows   []codecs.ClusterSessionFields
	timerRows     []codecs.TimerFields

	// takeChecking copies the agent seams into control, adder, recordingLog
	// and recordingLogSyncLevel.
	control               SnapshotRecordingControl
	adder                 SnapshotPublicationAdder
	recordingLog          SnapshotRecordingLog
	recordingLogSyncLevel recordinglog.FileSyncLevel

	offerRetryTimeoutNs int64
	responseTimeoutNs   int64
	deadlineNs          int64

	addCorrelationID int64
	addPending       bool

	publication SnapshotPublication
	start       SnapshotStartRequest
	recordingID int64

	// nextRow indexes the next row to offer. See encodeSnapshotRow.
	nextRow       int
	closePosition int64

	// scratch holds the encoded row of the current offer.
	scratch []byte
}

// SetSnapshotRecordingControl sets or clears the module snapshot recording
// control. A take that starts while the control is nil fails. A take past
// takeChecking keeps the control it copied.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) SetSnapshotRecordingControl(control SnapshotRecordingControl) {
	a.snapshotControl = control
}

// SetSnapshotPublicationAdder sets or clears the module snapshot publication
// adder. A take that starts while the adder is nil fails. A take past
// takeChecking keeps the adder it copied.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) SetSnapshotPublicationAdder(adder SnapshotPublicationAdder) {
	a.snapshotAdder = adder
}

// SetSnapshotRecordingLog sets or clears the recording log and the Force
// level of the snapshot rows. A take that starts while the log is nil fails.
// A take past takeChecking keeps the log it copied.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) SetSnapshotRecordingLog(
	log SnapshotRecordingLog,
	syncLevel recordinglog.FileSyncLevel,
) {
	a.snapshotRecordingLog = log
	a.snapshotRecordingLogSyncLevel = syncLevel
}

// SetSnapshotStream sets the channel and stream id of the module snapshot
// publication.
//
// Concurrency: invoker-only. Do not call while [Agent.DoWork] runs.
func (a *Agent) SetSnapshotStream(channel string, streamID int32) {
	a.snapshotChannel = channel
	a.snapshotStreamID = streamID
}

// LastSnapshotTake returns the latest resolved module snapshot take.
//
// Returns false before any take resolves. A cancelled take does not store a
// result.
//
// Concurrency: invoker-only.
func (a *Agent) LastSnapshotTake() (SnapshotTakeResult, bool) {
	return a.lastSnapshotTake, a.hasLastSnapshotTake
}

// CloseSnapshotTake cancels a take in flight. The cancel does not store a
// result. Idempotent.
//
// Concurrency: invoker-only. The election replay close calls it from inside
// [Agent.DoWork] before its blocking archive stop. Module Close calls it
// once DoWork has stopped.
func (a *Agent) CloseSnapshotTake() {
	a.cancelSnapshotTake()
}

// snapshotHoldPosition returns the position that holds the module state. A
// pending snapshot wait comes first, then the take in flight. Returns 0 when
// neither holds.
func (a *Agent) snapshotHoldPosition() int64 {
	if a.snapshotWaitPosition != 0 {
		return a.snapshotWaitPosition
	}
	if a.snapshotTake != nil {
		return a.snapshotTake.logPosition
	}
	return 0
}

// isLeaderSnapshotPending reports whether this member leads with a module
// take unresolved, or with a snapshot action wait that can still pop.
func (a *Agent) isLeaderSnapshotPending() bool {
	if a.role != RoleLeader {
		return false
	}
	if a.snapshotTake != nil {
		return true
	}
	waitCanPop := a.serviceAckFailure == nil
	return a.snapshotWaitPosition != 0 && waitCanPop
}

// buildSnapshotTake stores a new take at the popped snapshot wait. The take
// copies the service ack RelevantIDs and the module state.
//
// Call only after hasReachedServiceAcks returned true, and before
// removeServiceAckHead.
func (a *Agent) buildSnapshotTake(logPosition int64) {
	a.cancelSnapshotTake()
	a.snapshotTake = &snapshotTake{
		state:               takeChecking,
		logPosition:         logPosition,
		leadershipTermID:    a.snapshotWaitLeadershipTermID,
		timestamp:           a.snapshotWaitTimestamp,
		serviceRecordingIDs: a.serviceAcks.firstRelevantIDs(),
		nextSessionID:       a.sessions.nextSessionID,
		sessionRows:         a.sessions.snapshotSessionRows(),
		timerRows:           a.timers.snapshotTimerRows(),
	}
}

// snapshotTakeWork advances the module take until a state waits, or the take
// resolves or fails. A completed state runs the next state in the same call.
//
// Returns one per completed state, plus one for a failure.
//
// Concurrency: invoker-only.
func (a *Agent) snapshotTakeWork(nowNs int64) int {
	work := 0
	for a.snapshotTake != nil {
		completed, err := a.stepSnapshotTake(a.snapshotTake, nowNs)
		if err != nil {
			a.failSnapshotTake(err)
			return work + 1
		}
		if !completed {
			return work
		}
		work++
	}
	return work
}

// stepSnapshotTake runs the current state of take once.
//
// Returns:
//   - true, nil: the state completed
//   - false, nil: the state waits
//   - false, err: the take fails with err
func (a *Agent) stepSnapshotTake(take *snapshotTake, nowNs int64) (bool, error) {
	switch take.state {
	case takeChecking:
		return a.checkSnapshotTake(take)
	case takeAddingPublication:
		return a.addSnapshotPublication(take, nowNs)
	case takeOfferingStart:
		return offerSnapshotStart(take, nowNs)
	case takeAwaitingStart:
		return awaitSnapshotStart(take, nowNs)
	case takeWriting:
		return a.writeSnapshot(take, nowNs)
	case takeAwaitingStop:
		return a.awaitSnapshotStop(take, nowNs)
	}
	return false, fmt.Errorf("%w: unknown take state %d", errSnapshotWrite, take.state)
}

// checkSnapshotTake fails the take when a service take failed or a seam is
// nil. Otherwise the take waits for commit at its position.
func (a *Agent) checkSnapshotTake(take *snapshotTake) (bool, error) {
	for serviceID, recordingID := range take.serviceRecordingIDs {
		if recordingID == nullValue {
			return false, fmt.Errorf("%w: service %d", errSnapshotServiceFailed, serviceID)
		}
	}
	if a.snapshotControl == nil || a.snapshotAdder == nil || a.snapshotRecordingLog == nil {
		return false, errSnapshotNotConfigured
	}
	if a.CommitPosition() < take.logPosition {
		return false, nil
	}
	take.control = a.snapshotControl
	take.adder = a.snapshotAdder
	take.recordingLog = a.snapshotRecordingLog
	take.recordingLogSyncLevel = a.snapshotRecordingLogSyncLevel
	take.offerRetryTimeoutNs, take.responseTimeoutNs = take.control.ControlTimeoutsNs()
	take.state = takeAddingPublication
	return true, nil
}

// addSnapshotPublication offers the add once, then polls it once per call.
// The conductor DriverTimeout bounds the add. A done add builds the start
// request on the publication session.
func (a *Agent) addSnapshotPublication(take *snapshotTake, nowNs int64) (bool, error) {
	if !take.addPending {
		id, err := take.adder.OfferAddExclusivePublication(a.snapshotStreamID, a.snapshotChannel)
		if err != nil {
			return false, fmt.Errorf("%w: %w", errSnapshotPublicationAdd, err)
		}
		take.addCorrelationID = id
		take.addPending = true
	}
	publication, done, err := take.adder.PollAddExclusivePublication(take.addCorrelationID)
	if done {
		take.addPending = false
	}
	switch {
	case err != nil:
		return false, fmt.Errorf("%w: %w", errSnapshotPublicationAdd, err)
	case !done:
		return false, nil
	case publication == nil:
		return false, fmt.Errorf("%w: adder returned no publication", errSnapshotPublicationAdd)
	}

	take.publication = publication
	channel := aeron.AppendChannelParam(
		a.snapshotChannel,
		aeron.ChannelSessionIDParam,
		strconv.FormatInt(int64(publication.SessionID()), 10),
	)
	take.start = take.control.NewStartRecording(a.snapshotStreamID, channel)
	take.deadlineNs = nowNs + take.offerRetryTimeoutNs
	take.state = takeOfferingStart
	return true, nil
}

// offerSnapshotStart offers the start request once per call.
func offerSnapshotStart(take *snapshotTake, nowNs int64) (bool, error) {
	offered, err := take.start.TryOffer()
	switch {
	case err != nil:
		return false, fmt.Errorf("%w: %w", errSnapshotRecordingStart, err)
	case offered:
		take.deadlineNs = nowNs + take.responseTimeoutNs
		take.state = takeAwaitingStart
		return true, nil
	case nowNs >= take.deadlineNs:
		return false, fmt.Errorf("%w: offer timed out", errSnapshotRecordingStart)
	}
	return false, nil
}

// awaitSnapshotStart polls the START signal. On START the take reads the
// catalog id and watches for STOP. The OK RelevantID never reaches the take.
func awaitSnapshotStart(take *snapshotTake, nowNs int64) (bool, error) {
	signalled, err := take.start.PollSignal()
	switch {
	case err != nil:
		return false, fmt.Errorf("%w: %w", errSnapshotRecordingStart, err)
	case !signalled && nowNs >= take.deadlineNs:
		return false, fmt.Errorf("%w: response timed out", errSnapshotRecordingStart)
	case !signalled:
		return false, nil
	}

	take.recordingID = take.start.RecordingID()
	take.control.WatchStop(take.recordingID)
	take.start.Close()
	take.start = nil
	take.deadlineNs = nowNs + take.responseTimeoutNs
	take.state = takeWriting
	return true, nil
}

// writeSnapshot offers the rows in order, then closes the publication.
//
// A retriable offer code waits at the same row. Each accepted row restarts
// the row deadline. A STOP in this state fails the take: the archive stopped
// the recording early.
func (a *Agent) writeSnapshot(take *snapshotTake, nowNs int64) (bool, error) {
	if _, stopped := take.control.StopPosition(); stopped {
		return false, fmt.Errorf("%w: recording stopped early", errSnapshotWrite)
	}
	rowCount := take.rowCount()
	for take.nextRow < rowCount {
		row, err := take.encodeSnapshotRow(a.appVersion, take.nextRow)
		if err != nil {
			return false, err
		}
		result := take.publication.Offer(row)
		switch {
		case result > 0:
			take.nextRow++
			take.deadlineNs = nowNs + take.responseTimeoutNs
			continue
		case !aeron.IsOfferRetriable(result):
			return false, fmt.Errorf("%w: row offer code %d", errSnapshotWrite, result)
		case nowNs >= take.deadlineNs:
			return false, fmt.Errorf("%w: row offer timed out", errSnapshotWrite)
		}
		return false, nil
	}

	take.closePosition = take.publication.Position()
	publication := take.publication
	take.publication = nil
	if err := publication.Close(); err != nil {
		return false, fmt.Errorf("%w: close publication: %w", errSnapshotWrite, err)
	}
	take.deadlineNs = nowNs + take.responseTimeoutNs
	take.state = takeAwaitingStop
	return true, nil
}

// awaitSnapshotStop waits for STOP. A STOP at or above the close position
// writes the recording log rows, forces the log, then resolves the take
// with the recording id. A Force error does not fail the take.
func (a *Agent) awaitSnapshotStop(take *snapshotTake, nowNs int64) (bool, error) {
	stopPosition, stopped := take.control.StopPosition()
	switch {
	case !stopped && nowNs >= take.deadlineNs:
		return false, fmt.Errorf("%w: stop signal timed out", errSnapshotRecordingStop)
	case !stopped:
		return false, nil
	case stopPosition < take.closePosition:
		return false, fmt.Errorf(
			"%w: stop position %d below close position %d",
			errSnapshotRecordingStop,
			stopPosition,
			take.closePosition,
		)
	}

	take.control.ClearStop()
	if err := take.appendRecordingLogRows(); err != nil {
		return false, err
	}
	result := take.result(take.recordingID, nil)
	if err := take.recordingLog.Force(take.recordingLogSyncLevel); err != nil {
		result.ForceErr = fmt.Errorf("%w: force: %w", errSnapshotRecordingLog, err)
	}
	a.resolveSnapshotTake(result)
	return true, nil
}

// appendRecordingLogRows appends the snapshot group to the recording log:
//  1. one row per service, from the highest service id down to 0
//  2. the module row with [recordinglog.ConsensusModuleServiceID]
//
// The module row closes the group. PlanRecovery keys a group on the
// module row.
//
// Returns an error when:
//   - the TERM row is missing
//   - an append fails
//
// The write stops at the first error.
func (t *snapshotTake) appendRecordingLogRows() error {
	term := t.recordingLog.FindTermEntry(t.leadershipTermID)
	if term == nil {
		return fmt.Errorf(
			"%w: no term entry for leadership term %d",
			errSnapshotRecordingLog,
			t.leadershipTermID,
		)
	}

	// Copy the base first. An append can move the cached entry.
	termBaseLogPosition := term.TermBaseLogPosition
	appendRow := func(recordingID int64, serviceID int32) error {
		return t.recordingLog.AppendSnapshot(
			recordingID,
			t.leadershipTermID,
			termBaseLogPosition,
			t.logPosition,
			t.timestamp,
			serviceID,
		)
	}
	for serviceID, recordingID := range slices.Backward(t.serviceRecordingIDs) {
		//nolint:gosec // G115: acked service ids are int32
		wireServiceID := int32(serviceID)
		if err := appendRow(recordingID, wireServiceID); err != nil {
			return fmt.Errorf("%w: service %d: %w", errSnapshotRecordingLog, serviceID, err)
		}
	}
	if err := appendRow(t.recordingID, recordinglog.ConsensusModuleServiceID); err != nil {
		return fmt.Errorf("%w: module row: %w", errSnapshotRecordingLog, err)
	}
	return nil
}

// failSnapshotTake runs the cleanup, then resolves the take with cause.
func (a *Agent) failSnapshotTake(cause error) {
	take := a.snapshotTake
	cleanupSnapshotTake(take)
	a.resolveSnapshotTake(take.result(nullValue, cause))
}

// resolveSnapshotTake stores result and drops the take.
func (a *Agent) resolveSnapshotTake(result SnapshotTakeResult) {
	a.snapshotTake = nil
	a.lastSnapshotTake = result
	a.hasLastSnapshotTake = true
}

// cancelSnapshotTake drops a take in flight and runs its cleanup. It does not
// store a result. The take position joins abandonedSnapshotWaits, so a
// redelivered action there does not queue a wait. The pop consumed every ack
// at the position, so the retired entry does not owe an ack.
func (a *Agent) cancelSnapshotTake() {
	take := a.snapshotTake
	if take == nil {
		return
	}
	a.snapshotTake = nil
	cleanupSnapshotTake(take)
	a.abandonSnapshotPosition(take.logPosition)
}

// cleanupSnapshotTake cancels or closes everything take owns, in order:
//  1. the pending publication add
//  2. the start request
//  3. the STOP watch
//  4. the publication. autoStop then stops a started recording.
//
// Cleanup does not send a stop by channel.
//
// Cleanup polls a pending add once before a cancel. A ready poll closes the
// publication instead. A conductor tick outside the take can read the ready
// reply, and CancelPending alone then leaks that publication.
func cleanupSnapshotTake(take *snapshotTake) {
	if take.addPending {
		publication, done, err := take.adder.PollAddExclusivePublication(take.addCorrelationID)
		ready := err == nil && done && publication != nil
		if ready {
			_ = publication.Close()
		} else {
			take.adder.CancelPending(take.addCorrelationID)
		}
		take.addPending = false
	}
	if take.start != nil {
		take.start.Close()
		take.start = nil
	}
	if take.control != nil {
		take.control.ClearStop()
	}
	if take.publication != nil {
		_ = take.publication.Close()
		take.publication = nil
	}
}

// result returns the take result with recordingID and err.
func (t *snapshotTake) result(recordingID int64, err error) SnapshotTakeResult {
	return SnapshotTakeResult{
		LogPosition:         t.logPosition,
		LeadershipTermID:    t.leadershipTermID,
		Timestamp:           t.timestamp,
		RecordingID:         recordingID,
		ServiceRecordingIDs: t.serviceRecordingIDs,
		Err:                 err,
	}
}

// rowCount counts BEGIN, the ConsensusModule row, the session and timer rows,
// one tracker row per service, and END.
func (t *snapshotTake) rowCount() int {
	return 3 + len(t.sessionRows) + len(t.timerRows) + len(t.serviceRecordingIDs)
}

// encodeSnapshotRow encodes row index into the take scratch buffer. Rows
// run in this order:
//  1. SnapshotMarker BEGIN
//  2. ConsensusModule
//  3. one ClusterSession row per copied session
//  4. one Timer row per copied timer
//  5. one PendingMessageTracker row per service
//  6. SnapshotMarker END
//
// Every row encodes at the cluster schema version.
func (t *snapshotTake) encodeSnapshotRow(appVersion int32, index int) ([]byte, error) {
	const version = clusterwire.SnapshotMarkerSbeSchemaVersion
	firstSession := 2
	firstTimer := firstSession + len(t.sessionRows)
	firstTracker := firstTimer + len(t.timerRows)
	end := firstTracker + len(t.serviceRecordingIDs)

	var (
		wireEnd int
		ok      bool
		label   string
	)
	switch {
	case index == 0 || index == end:
		mark := clusterwire.SnapshotMark_BEGIN
		if index == end {
			mark = clusterwire.SnapshotMark_END
		}
		label = "snapshot marker"
		buf := t.scratchOf(codecs.SnapshotMarkerEncodedLen())
		wireEnd, ok = codecs.EncodeSnapshotMarker(buf, 0, version, codecs.SnapshotMarkerFields{
			TypeID:           codecs.ConsensusModuleSnapshotTypeID,
			LogPosition:      t.logPosition,
			LeadershipTermID: t.leadershipTermID,
			Index:            0,
			Mark:             mark,
			TimeUnit:         clusterwire.ClusterTimeUnit_NANOS,
			AppVersion:       appVersion,
		})
	case index == 1:
		label = "consensus module"
		buf := t.scratchOf(codecs.ConsensusModuleEncodedLen())
		wireEnd, ok = codecs.EncodeConsensusModule(buf, 0, version, t.consensusModuleRow())
	case index < firstTimer:
		session := t.sessionRows[index-firstSession]
		length, fits := codecs.ClusterWireMessageLen(
			clusterwire.ClusterSessionSbeBlockLength,
			len(session.ResponseChannel),
		)
		if !fits {
			return nil, fmt.Errorf(
				"%w: cluster session %d too long",
				errSnapshotWrite,
				session.ClusterSessionID,
			)
		}
		label = "cluster session"
		wireEnd, ok = codecs.EncodeClusterSession(t.scratchOf(length), 0, version, session)
	case index < firstTracker:
		label = "timer"
		buf := t.scratchOf(codecs.TimerEncodedLen())
		wireEnd, ok = codecs.EncodeTimer(buf, 0, version, t.timerRows[index-firstTimer])
	default:
		label = "pending message tracker"
		buf := t.scratchOf(codecs.PendingMessageTrackerEncodedLen())
		wireEnd, ok = codecs.EncodePendingMessageTracker(
			buf,
			0,
			version,
			emptyPendingMessageTracker(index-firstTracker),
		)
	}
	if !ok {
		return nil, fmt.Errorf("%w: encode %s", errSnapshotWrite, label)
	}
	return t.scratch[:wireEnd], nil
}

// scratchOf resizes the scratch buffer to length bytes.
func (t *snapshotTake) scratchOf(length int) []byte {
	t.scratch = slices.Grow(t.scratch[:0], length)[:length]
	return t.scratch
}

// consensusModuleRow returns the ConsensusModule row. The tracker fields
// carry service 0's empty tracker, or 0 with no service.
func (t *snapshotTake) consensusModuleRow() codecs.ConsensusModuleFields {
	row := codecs.ConsensusModuleFields{NextSessionID: t.nextSessionID}
	if len(t.serviceRecordingIDs) > 0 {
		tracker := emptyPendingMessageTracker(0)
		row.NextServiceSessionID = tracker.NextServiceSessionID
		row.LogServiceSessionID = tracker.LogServiceSessionID
	}
	return row
}

// emptyPendingMessageTracker returns the tracker row of serviceID with no
// pending message. The values equal a Java tracker that never held one.
func emptyPendingMessageTracker(serviceID int) codecs.PendingMessageTrackerFields {
	logServiceSessionID := int64(serviceID)<<56 | math.MinInt64
	//nolint:gosec // G115: acked service ids are int32
	wireServiceID := int32(serviceID)
	return codecs.PendingMessageTrackerFields{
		NextServiceSessionID:   logServiceSessionID + 1,
		LogServiceSessionID:    logServiceSessionID,
		PendingMessageCapacity: 0,
		ServiceID:              wireServiceID,
	}
}
