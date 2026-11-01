package service

import (
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// ErrSnapshotNoArchive marks a snapshot take with no archive client. The error
// is fatal.
var ErrSnapshotNoArchive = errors.New("service: snapshot archive not configured")

// ErrSnapshotTimeUnitUnknown marks a snapshot take with no known time unit. The
// error is fatal.
var ErrSnapshotTimeUnitUnknown = errors.New("service: snapshot time unit unknown")

// ErrSnapshotPublicationAdd wraps a snapshot publication add error. The error
// is fatal.
var ErrSnapshotPublicationAdd = errors.New("service: add snapshot publication")

// ErrSnapshotRecordingStart wraps a snapshot recording start error. The error
// is fatal.
var ErrSnapshotRecordingStart = errors.New("service: start snapshot recording")

// ErrSnapshotWrite wraps a snapshot write error. The error is fatal.
var ErrSnapshotWrite = errors.New("service: write snapshot")

// ErrSnapshotRecordingStop wraps a snapshot recording stop error. The error is
// fatal.
var ErrSnapshotRecordingStop = errors.New("service: stop snapshot recording")

// snapshotArchiveControl is the archive client surface the agent holds.
// DoWork calls Poll once per tick. The take sets its STOP tracker as the
// session handler.
//
// *archive.Client implements it. Tests substitute a double.
type snapshotArchiveControl interface {
	Poll() int
	ControlSessionID() int64
	Config() archive.Config
	SetRecordingSignalHandler(handler archive.RecordingSignalHandler)
}

var _ snapshotArchiveControl = (*archive.Client)(nil)

// snapshotStartRequest is one in-flight non-blocking start recording request.
//
// *archive.AsyncStartRecording implements it. Tests substitute a double.
type snapshotStartRequest interface {
	TryOffer() (offered bool, err error)
	PollSignal() (signalled bool, err error)
	RecordingID() int64
	Close()
}

var _ snapshotStartRequest = (*archive.AsyncStartRecording)(nil)

// snapshotTakeState names the step a take runs or waits in.
type snapshotTakeState uint8

const (
	// takeAddingPublication waits for the snapshot publication add.
	takeAddingPublication snapshotTakeState = iota

	// takeOfferingStart offers the start recording request.
	takeOfferingStart

	// takeAwaitingStart waits for the START signal.
	takeAwaitingStart

	// takeWriting offers the rows, runs the handler, and closes the
	// publication.
	takeWriting

	// takeAwaitingStop waits for the autoStop STOP signal.
	takeAwaitingStop
)

// snapshotTake is the one snapshot take in flight.
//
// Ownership: the take owns the pending add, the publication, and the start
// request. closeSnapshotTake cancels or closes each one.
type snapshotTake struct {
	state snapshotTakeState

	// logPosition is the action frame end. The log poll holds there.
	logPosition      int64
	leadershipTermID int64
	timeUnit         clusterwire.ClusterTimeUnit
	appVersion       int32

	// sessionIDs holds the open session ids at the action, ascending.
	sessionIDs []int64

	offerRetryTimeoutNs int64
	responseTimeoutNs   int64
	deadlineNs          int64

	addCorrelationID int64
	addPending       bool

	publication *aeron.ExclusivePublication
	start       snapshotStartRequest
	stop        *archive.StopSignalTracker
	recordingID int64

	// nextRow indexes the next row: 0 is BEGIN, 1 to n the sessions, n+1 END.
	nextRow       int
	closePosition int64
}

// startSnapshotTake runs on a SNAPSHOT action with flags 0.
//
// Order:
//  1. append one unready snapshot entry at logPosition
//  2. no archive client or no publication adder: fail the entry with
//     ErrSnapshotNoArchive
//  3. no known time unit: fail the entry with ErrSnapshotTimeUnitUnknown
//  4. otherwise store a new take
func (a *Agent) startSnapshotTake(logPosition, leadershipTermID int64) {
	a.coordinationAcks = append(a.coordinationAcks, coordinationAck{
		logPosition: logPosition,
		kind:        ackKindSnapshot,
		relevantID:  nullValue,
	})
	if a.archive == nil || a.newSnapshotStart == nil || a.snapshotPublications == nil {
		a.failAckEntry(ackKindSnapshot, ErrSnapshotNoArchive)
		return
	}
	if a.timeUnit == clusterwire.ClusterTimeUnit_NULL_VALUE {
		a.failAckEntry(ackKindSnapshot, ErrSnapshotTimeUnitUnknown)
		return
	}

	sessionIDs := make([]int64, len(a.sessionList))
	for i, session := range a.sessionList {
		sessionIDs[i] = session.id
	}
	cfg := a.archive.Config()
	a.take = &snapshotTake{
		state:               takeAddingPublication,
		logPosition:         logPosition,
		leadershipTermID:    leadershipTermID,
		timeUnit:            a.timeUnit,
		appVersion:          a.appVersion,
		sessionIDs:          sessionIDs,
		offerRetryTimeoutNs: cfg.ControlOfferRetryTimeout.Nanoseconds(),
		responseTimeoutNs:   cfg.ControlResponseTimeout.Nanoseconds(),
		recordingID:         nullValue,
	}
}

// snapshotTakeWork advances the take until a state waits, or the take
// resolves or fails. A completed state runs the next state in the same call.
//
// Returns one per completed state.
//
// Concurrency: invoker-only.
func (a *Agent) snapshotTakeWork(nowNs int64) int {
	work := 0
	for a.take != nil {
		completed, err := a.stepSnapshotTake(a.take, nowNs)
		if err != nil {
			a.failSnapshotTake(err)
			return work
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
	case takeAddingPublication:
		return a.addSnapshotPublication(take, nowNs)
	case takeOfferingStart:
		return offerSnapshotStart(take, nowNs)
	case takeAwaitingStart:
		return a.awaitSnapshotStart(take, nowNs)
	case takeWriting:
		return a.writeSnapshot(take, nowNs)
	case takeAwaitingStop:
		return a.awaitSnapshotStop(take, nowNs)
	}
	return false, fmt.Errorf("%w: unknown take state %d", ErrSnapshotWrite, take.state)
}

// addSnapshotPublication offers the add once, then polls it once per call.
// The conductor DriverTimeout bounds the add.
func (a *Agent) addSnapshotPublication(take *snapshotTake, nowNs int64) (bool, error) {
	if !take.addPending {
		id, err := a.snapshotPublications.OfferAddExclusivePublication(
			a.snapshotStreamID,
			a.snapshotChannel,
		)
		if err != nil {
			return false, fmt.Errorf("%w: %w", ErrSnapshotPublicationAdd, err)
		}
		take.addCorrelationID = id
		take.addPending = true
	}
	publication, done, err := a.snapshotPublications.PollAddExclusivePublication(
		take.addCorrelationID,
	)
	if done {
		take.addPending = false
	}
	switch {
	case err != nil:
		return false, fmt.Errorf("%w: %w", ErrSnapshotPublicationAdd, err)
	case !done:
		return false, nil
	case publication == nil:
		return false, fmt.Errorf("%w: adder returned no publication", ErrSnapshotPublicationAdd)
	}

	take.publication = publication
	channel := sessionScopedChannel(a.snapshotChannel, publication.SessionID())
	take.start = a.newSnapshotStart(a.snapshotStreamID, []byte(channel))
	take.deadlineNs = nowNs + take.offerRetryTimeoutNs
	take.state = takeOfferingStart
	return true, nil
}

// offerSnapshotStart offers the start request once per call.
func offerSnapshotStart(take *snapshotTake, nowNs int64) (bool, error) {
	offered, err := take.start.TryOffer()
	switch {
	case err != nil:
		return false, fmt.Errorf("%w: %w", ErrSnapshotRecordingStart, err)
	case offered:
		take.deadlineNs = nowNs + take.responseTimeoutNs
		take.state = takeAwaitingStart
		return true, nil
	case nowNs >= take.deadlineNs:
		return false, fmt.Errorf("%w: offer timed out", ErrSnapshotRecordingStart)
	}
	return false, nil
}

// awaitSnapshotStart polls the START signal. On START the take sets its STOP
// tracker as the session handler. The OK RelevantID never reaches the take.
func (a *Agent) awaitSnapshotStart(take *snapshotTake, nowNs int64) (bool, error) {
	signalled, err := take.start.PollSignal()
	switch {
	case err != nil:
		return false, fmt.Errorf("%w: %w", ErrSnapshotRecordingStart, err)
	case !signalled && nowNs >= take.deadlineNs:
		return false, fmt.Errorf("%w: response timed out", ErrSnapshotRecordingStart)
	case !signalled:
		return false, nil
	}

	take.recordingID = take.start.RecordingID()
	take.stop = archive.NewStopSignalTracker(a.archive.ControlSessionID(), take.recordingID)
	a.archive.SetRecordingSignalHandler(take.stop)
	take.start.Close()
	take.start = nil
	take.deadlineNs = nowNs + take.responseTimeoutNs
	take.state = takeWriting
	return true, nil
}

// writeSnapshot offers the rows in order, then runs the handler once, then
// closes the publication.
//
// A retriable offer code waits at the same row. Each accepted row restarts
// the row deadline. A STOP in this state fails the take: the archive stopped
// the recording early.
func (a *Agent) writeSnapshot(take *snapshotTake, nowNs int64) (bool, error) {
	if take.stop.Stopped() {
		return false, fmt.Errorf("%w: recording stopped early", ErrSnapshotWrite)
	}
	rowCount := len(take.sessionIDs) + 2
	for take.nextRow < rowCount {
		row, err := a.encodeSnapshotRow(take, take.nextRow)
		if err != nil {
			return false, err
		}
		result := a.offerSnapshotRow(take.publication, row)
		switch {
		case result > 0:
			take.nextRow++
			take.deadlineNs = nowNs + take.responseTimeoutNs
			continue
		case !aeron.IsOfferRetriable(result):
			return false, fmt.Errorf("%w: row offer code %d", ErrSnapshotWrite, result)
		case nowNs >= take.deadlineNs:
			return false, fmt.Errorf("%w: row offer timed out", ErrSnapshotWrite)
		}
		return false, nil
	}

	if a.snapshotHandler != nil {
		if err := a.snapshotHandler.TakeSnapshot(take.publication); err != nil {
			return false, fmt.Errorf("%w: %w", ErrTakeSnapshot, err)
		}
	}

	// Position returns Closed when the handler closed the publication.
	take.closePosition = take.publication.Position()
	if take.closePosition < 0 {
		return false, fmt.Errorf(
			"%w: publication closed, position code %d",
			ErrSnapshotWrite,
			take.closePosition,
		)
	}
	if err := take.publication.Close(); err != nil {
		return false, fmt.Errorf("%w: close publication: %w", ErrSnapshotWrite, err)
	}
	take.deadlineNs = nowNs + take.responseTimeoutNs
	take.state = takeAwaitingStop
	return true, nil
}

// awaitSnapshotStop waits for STOP. A STOP at or above the close position
// marks the entry ready with the recording id. The take then ends.
func (a *Agent) awaitSnapshotStop(take *snapshotTake, nowNs int64) (bool, error) {
	if !take.stop.Stopped() {
		if nowNs >= take.deadlineNs {
			return false, fmt.Errorf("%w: stop signal timed out", ErrSnapshotRecordingStop)
		}
		return false, nil
	}
	if stopped := take.stop.StoppedPosition(); stopped < take.closePosition {
		return false, fmt.Errorf(
			"%w: stop position %d below close position %d",
			ErrSnapshotRecordingStop,
			stopped,
			take.closePosition,
		)
	}

	if e := a.markAckReady(ackKindSnapshot); e != nil {
		e.relevantID = take.recordingID
	}
	a.archive.SetRecordingSignalHandler(nil)
	a.take = nil
	return true, nil
}

// offerSnapshotRow offers one row on publication through the test seam when
// set.
func (a *Agent) offerSnapshotRow(publication *aeron.ExclusivePublication, row []byte) int64 {
	if a.snapshotRowOffer != nil {
		return a.snapshotRowOffer(publication, row)
	}
	return publication.Offer(row)
}

// encodeSnapshotRow encodes row index into the agent scratch buffer.
//
// Returns:
//   - the encoded row
//   - nil, err when the encode fails or the registry no longer holds the
//     row's session
func (a *Agent) encodeSnapshotRow(take *snapshotTake, index int) ([]byte, error) {
	last := len(take.sessionIDs) + 1
	if index == 0 || index == last {
		mark := clusterwire.SnapshotMark_BEGIN
		if index == last {
			mark = clusterwire.SnapshotMark_END
		}
		length := codecs.SnapshotMarkerEncodedLen()
		a.snapshotScratch = slices.Grow(a.snapshotScratch[:0], length)[:length]
		buf := a.snapshotScratch
		end, ok := codecs.EncodeSnapshotMarker(
			buf,
			0,
			clusterwire.SnapshotMarkerSbeSchemaVersion,
			codecs.SnapshotMarkerFields{
				TypeID:           codecs.ServiceSnapshotTypeID,
				LogPosition:      take.logPosition,
				LeadershipTermID: take.leadershipTermID,
				Index:            0,
				Mark:             mark,
				TimeUnit:         take.timeUnit,
				AppVersion:       take.appVersion,
			},
		)
		if !ok {
			return nil, fmt.Errorf("%w: encode snapshot marker", ErrSnapshotWrite)
		}
		return buf[:end], nil
	}

	sessionID := take.sessionIDs[index-1]
	session, found := a.sessions[sessionID]
	if !found {
		return nil, fmt.Errorf("%w: client session %d missing", ErrSnapshotWrite, sessionID)
	}
	length, ok := codecs.ClusterWireMessageLen(
		clusterwire.ClientSessionSbeBlockLength,
		len(session.responseChannel),
		len(session.encodedPrincipal),
	)
	if !ok {
		return nil, fmt.Errorf("%w: client session %d too long", ErrSnapshotWrite, sessionID)
	}
	a.snapshotScratch = slices.Grow(a.snapshotScratch[:0], length)[:length]
	buf := a.snapshotScratch
	end, ok := codecs.EncodeClientSession(
		buf,
		0,
		clusterwire.ClientSessionSbeSchemaVersion,
		codecs.ClientSessionFields{
			ClusterSessionID: sessionID,
			ResponseStreamID: session.responseStreamID,
			ResponseChannel:  []byte(session.responseChannel),
			EncodedPrincipal: session.encodedPrincipal,
		},
	)
	if !ok {
		return nil, fmt.Errorf("%w: encode client session %d", ErrSnapshotWrite, sessionID)
	}
	return buf[:end], nil
}

// failSnapshotTake runs closeSnapshotTake, then fails the snapshot entry
// with cause.
func (a *Agent) failSnapshotTake(cause error) {
	a.closeSnapshotTake()
	a.failAckEntry(ackKindSnapshot, cause)
}

// closeSnapshotTake cancels or closes everything the take owns. The archive
// then stops a started recording through autoStop. Does not mark an entry.
//
// closeSnapshotTake polls a pending add once before a cancel. A ready poll
// closes the publication instead. A conductor tick outside the take can read
// the ready reply, and CancelPending alone then leaks that publication.
//
// A poll error does not reach the error handler. On a failed take, the
// poll error repeats the take cause. During Close, the poll error is a
// teardown error.
func (a *Agent) closeSnapshotTake() {
	take := a.take
	if take == nil {
		return
	}
	a.take = nil
	if take.addPending {
		publication, done, err := a.snapshotPublications.PollAddExclusivePublication(
			take.addCorrelationID,
		)
		ready := err == nil && done && publication != nil
		if ready {
			_ = publication.Close()
		} else {
			a.snapshotPublications.CancelPending(take.addCorrelationID)
		}
	}
	if take.start != nil {
		take.start.Close()
	}
	a.archive.SetRecordingSignalHandler(nil)
	if take.publication != nil {
		_ = take.publication.Close()
	}
}

// sessionScopedChannel appends session-id=sessionID to channel.
func sessionScopedChannel(channel string, sessionID int32) string {
	return aeron.AppendChannelParam(
		channel,
		aeron.ChannelSessionIDParam,
		strconv.FormatInt(int64(sessionID), 10),
	)
}
