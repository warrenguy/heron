//go:build rapid && testhooks

package consensus

import (
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

var errSnapshotTakeRapidBoom = errors.New("boom")

// snapshotActionStatus is the model status of the one snapshot action.
type snapshotActionStatus int

const (
	actionStatusWaiting snapshotActionStatus = iota
	actionStatusTake
	actionStatusResolved
	actionStatusCancelled
	actionStatusRetired
)

// snapshotTakeScript is the drawn fake archive script of one take.
type snapshotTakeScript struct {
	relevantIDs     []int64
	addOutcome      int
	addPendingPolls int
	startOutcome    int
	startID         int64
	offerRetries    int
	signalDelay     int
	rowCodes        []int64
	closeFails      bool
	stopOutcome     int
	stopAboveBy     int64
	stopDelay       int
	logOutcome      int

	// logFailRow is the 0-based append call that fails under
	// scriptLogAppendError.
	logFailRow int
}

const (
	scriptAddOK = iota
	scriptAddOfferError
	scriptAddPollError
	scriptAddNoPublication
)

const (
	scriptStartOK = iota
	scriptStartOfferError
	scriptStartOfferNever
	scriptStartSignalError
	scriptStartSignalNever
)

const (
	scriptStopOK = iota
	scriptStopAbove
	scriptStopEarly
	scriptStopShort
	scriptStopNever
)

const (
	scriptLogOK = iota
	scriptLogMissingTerm
	scriptLogAppendError
	scriptLogForceError
)

// outcome returns the deadline causes that may fire first under the drawn
// clock, and the first cause the script forces, nil for a success.
func (s snapshotTakeScript) outcome() (possible []error, certain error) {
	if slices.Contains(s.relevantIDs, nullValue) {
		return nil, errSnapshotServiceFailed
	}
	if s.addOutcome != scriptAddOK {
		return nil, errSnapshotPublicationAdd
	}
	if s.offerRetries > 0 {
		possible = append(possible, errSnapshotRecordingStart)
	}
	switch s.startOutcome {
	case scriptStartOfferError, scriptStartOfferNever, scriptStartSignalError,
		scriptStartSignalNever:
		return possible, errSnapshotRecordingStart
	}
	if s.signalDelay > 0 {
		possible = append(possible, errSnapshotRecordingStart)
	}
	if s.stopOutcome == scriptStopEarly {
		return possible, errSnapshotWrite
	}
	for _, code := range s.rowCodes {
		if code == fakeOfferAccept {
			continue
		}
		if !aeron.IsOfferRetriable(code) {
			return possible, errSnapshotWrite
		}
		possible = append(possible, errSnapshotWrite)
	}
	if s.closeFails {
		return possible, errSnapshotWrite
	}
	switch s.stopOutcome {
	case scriptStopShort, scriptStopNever:
		return possible, errSnapshotRecordingStop
	}
	if s.stopDelay > 0 {
		possible = append(possible, errSnapshotRecordingStop)
	}
	switch s.logOutcome {
	case scriptLogMissingTerm, scriptLogAppendError:
		return possible, errSnapshotRecordingLog
	}
	return possible, nil
}

// apply installs the script on archive and recordingLog.
func (s snapshotTakeScript) apply(
	archive *fakeSnapshotArchive,
	recordingLog *fakeSnapshotRecordingLog,
	errBoom error,
) {
	switch s.logOutcome {
	case scriptLogMissingTerm:
		recordingLog.missingTerm = true
	case scriptLogAppendError:
		recordingLog.appendErr = errBoom
		recordingLog.appendErrAt = s.logFailRow
	case scriptLogForceError:
		recordingLog.forceErr = errBoom
	}
	switch s.addOutcome {
	case scriptAddOfferError:
		archive.offerAddErr = errBoom
	case scriptAddPollError:
		archive.pollAddErr = errBoom
	case scriptAddNoPublication:
		archive.nilPublication = true
	}
	archive.addPendingPolls = s.addPendingPolls
	archive.startScript = func(start *fakeSnapshotStart) {
		start.recordingID = s.startID
		start.offerRetries = s.offerRetries
		start.signalDelay = s.signalDelay
		switch s.startOutcome {
		case scriptStartOfferError:
			start.offerErr = errBoom
		case scriptStartOfferNever:
			start.offerNever = true
		case scriptStartSignalError:
			start.signalErr = errBoom
		case scriptStartSignalNever:
			start.signalNever = true
		}
	}
	archive.pubScript = func(pub *fakeSnapshotPublication) {
		pub.offerCodes = slices.Clone(s.rowCodes)
		if s.closeFails {
			pub.closeErr = errBoom
		}
	}
	archive.stopDelay = s.stopDelay
	switch s.stopOutcome {
	case scriptStopAbove:
		archive.stopShortBy = -s.stopAboveBy
	case scriptStopEarly:
		archive.earlyStop = true
	case scriptStopShort:
		archive.stopShortBy = 1
	case scriptStopNever:
		archive.stopNever = true
	}
}

func drawSnapshotTakeScript(rt *rapid.T, serviceCount int) snapshotTakeScript {
	// A clean script lets every service and archive step succeed. Without it,
	// few drawn takes reach the recording log write. The clean draw comes
	// first, so a clean script skips the step draws it would fix.
	clean := rapid.Bool().Draw(rt, "clean")
	s := snapshotTakeScript{
		addPendingPolls: rapid.IntRange(0, 2).Draw(rt, "addPendingPolls"),
		// The START id never equals fakeSnapshotPreProofID.
		startID: fakeSnapshotFirstRecordingID +
			rapid.Int64Range(0, 1000).Draw(rt, "startIDOffset"),
		stopAboveBy: rapid.Int64Range(1, 64).Draw(rt, "stopAboveBy"),
		stopDelay:   rapid.IntRange(0, 2).Draw(rt, "stopDelay"),
		logOutcome:  rapid.IntRange(scriptLogOK, scriptLogForceError).Draw(rt, "logOutcome"),
		logFailRow:  rapid.IntRange(0, serviceCount).Draw(rt, "logFailRow"),
	}
	if clean {
		s.stopOutcome = rapid.SampledFrom([]int{scriptStopOK, scriptStopAbove}).Draw(rt, "stopOutcome")
	} else {
		s.addOutcome = rapid.IntRange(scriptAddOK, scriptAddNoPublication).Draw(rt, "addOutcome")
		s.startOutcome = rapid.IntRange(scriptStartOK, scriptStartSignalNever).Draw(rt, "startOutcome")
		s.offerRetries = rapid.IntRange(0, 2).Draw(rt, "offerRetries")
		s.signalDelay = rapid.IntRange(0, 2).Draw(rt, "signalDelay")
		s.rowCodes = rapid.SliceOfN(rapid.SampledFrom([]int64{
			fakeOfferAccept,
			aeron.BackPressured,
			aeron.AdminAction,
			aeron.NotConnected,
			aeron.Closed,
		}), 0, 4).Draw(rt, "rowCodes")
		s.closeFails = rapid.Bool().Draw(rt, "closeFails")
		s.stopOutcome = rapid.IntRange(scriptStopOK, scriptStopNever).Draw(rt, "stopOutcome")
	}
	for i := range serviceCount {
		id := int64(100 + i)
		if !clean && rapid.Bool().Draw(rt, "serviceFailed") {
			id = nullValue
		}
		s.relevantIDs = append(s.relevantIDs, id)
	}
	return s
}

// snapshotTakeAckModel mirrors the ServiceAck tracker for the one action.
type snapshotTakeAckModel struct {
	queues        [][]int64
	frames        []snapshotTakeAckFrame
	expectedAckID int64
	marked        bool
}

type snapshotTakeAckFrame struct {
	serviceID int
	ackID     int64
}

// poll offers every queued frame. Each offer runs the sequence check.
func (m *snapshotTakeAckModel) poll() {
	for _, frame := range m.frames {
		m.queues[frame.serviceID] = append(m.queues[frame.serviceID], frame.ackID)
		if m.marked {
			continue
		}
		for _, q := range m.queues {
			if len(q) == 0 {
				break
			}
			if q[0] != m.expectedAckID {
				m.marked = true
				break
			}
		}
	}
	m.frames = nil
}

// pop reports whether the wait pops. A first entry mismatch marks the
// failure.
func (m *snapshotTakeAckModel) pop() bool {
	if m.marked {
		return false
	}
	for _, q := range m.queues {
		if len(q) == 0 {
			return false
		}
		if q[0] != m.expectedAckID {
			m.marked = true
			return false
		}
	}
	for i, q := range m.queues {
		m.queues[i] = q[1:]
	}
	m.expectedAckID++
	return true
}

func TestSnapshotTake_model_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		serviceCount := rapid.IntRange(0, 3).Draw(rt, "serviceCount")
		isLeader := rapid.Bool().Draw(rt, "isLeader")
		script := drawSnapshotTakeScript(rt, serviceCount)
		timeoutNs := rapid.Int64Range(20, 400).Draw(rt, "timeoutNs")
		sessionTimeoutNs := rapid.Int64Range(50, 800).Draw(rt, "sessionTimeoutNs")
		commitLag := rapid.Int64Range(0, 2).Draw(rt, "commitLag")

		role := RoleFollower
		if isLeader {
			role = RoleLeader
		}
		commit := new(atomic.Int64)
		a := NewAgent(AgentConfig{
			Role:             role,
			ServiceCount:     serviceCount,
			Commit:           commit,
			LeadershipTermID: 4,
			SessionTimeoutNs: sessionTimeoutNs,
		})
		stateCounter := &recordingModuleStateCounter{}
		a.SetModuleStateCounter(stateCounter)
		q := attachQueueServiceIngress(a)
		// Outer t: rapid.T is not testing.TB (missing ArtifactDir).
		lp, _ := boundLogPublisher(t)
		a.SetLogPublisher(lp)
		archive := newFakeSnapshotArchive()
		archive.offerRetryTimeoutNs = timeoutNs
		archive.responseTimeoutNs = timeoutNs
		recordingLog := &fakeSnapshotRecordingLog{}
		script.apply(archive, recordingLog, errSnapshotTakeRapidBoom)
		a.SetSnapshotRecordingControl(archive)
		a.SetSnapshotPublicationAdder(archive)
		a.SetSnapshotRecordingLog(recordingLog, recordinglog.FileSyncData)
		a.SetSnapshotStream(testSnapshotChannel, testSnapshotStreamID)

		nowNs := int64(1000)
		a.sessions.learnSessionOpen(codecs.SessionOpenEventFields{
			ClusterSessionID: 1,
			ResponseChannel:  []byte("aeron:ipc"),
		}, 32, nowNs)
		scheduleTimer(a, 7, 1<<60)
		// A leader also holds an expired timer, a pending session and a queued
		// connect frame. The hold must leave all three alone. The pre-open
		// reclaim still removes the pending session at its timeout.
		var ingress *QueueClientIngress
		acceptNs := nowNs
		timerRows := 1
		if isLeader {
			wireImmediateEgress(t, a)
			ingress = &QueueClientIngress{}
			a.SetClientIngress(ingress)
			ingress.Enqueue(encodeSessionConnectWire(t, validConnectFields(61)))
			a.acceptSessionConnect(validConnectFields(60), nowNs)
			require.Equal(rt, 1, a.PendingSessionCount())
			scheduleTimer(a, 11, 1)
			timerRows = 2
		}
		requireHeldUntouched := func(step string) {
			rt.Helper()
			require.Equal(rt, 1, ingress.Len(), step)
			wantPending := 1
			if nowNs-acceptNs >= sessionTimeoutNs {
				wantPending = 0
			}
			require.Equal(rt, wantPending, a.PendingSessionCount(), step)
			require.Equal(rt, 1, a.OpenSessionCount(), step)
			require.Equal(rt, int64(timerRows), a.ScheduledTimerCount(), step)
		}

		var position int64
		if isLeader {
			require.NoError(rt, a.RequestSnapshot(nowNs))
			position = a.ExpectedAckPosition()
		} else {
			position = 4096
			a.queueSnapshotWait(position, 4, nowNs)
		}
		commit.Store(position - commitLag)

		status := actionStatusWaiting
		// wantLog is the fake log before any write, then at resolve. Later
		// steps must not change it.
		wantLog := *recordingLog
		acks := &snapshotTakeAckModel{queues: make([][]int64, serviceCount)}
		acksSent := false
		sendAcks := func() {
			if acksSent {
				return
			}
			acksSent = true
			order := make([]int, serviceCount)
			for i := range order {
				order[i] = i
			}
			for _, serviceID := range rapid.Permutation(order).Draw(rt, "ackOrder") {
				q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
					LogPosition: position,
					AckID:       0,
					RelevantID:  script.relevantIDs[serviceID],
					ServiceID:   int32(serviceID),
				}))
				acks.frames = append(acks.frames, snapshotTakeAckFrame{serviceID: serviceID})
			}
		}

		requireModel := func(step string) {
			rt.Helper()
			active := status == actionStatusWaiting || status == actionStatusTake
			require.Equal(rt, active, a.HasPendingSnapshotAction(), step)
			require.Equal(rt, status == actionStatusTake, a.snapshotTake != nil, step)
			if a.snapshotTake != nil {
				require.Equal(rt, position, a.snapshotTake.logPosition, step)
			}
			marked := a.ServiceAckFailure() != nil
			require.Equal(rt, acks.marked, marked, step)
			waitCanPop := status == actionStatusWaiting && !marked
			wantSnapshot := status == actionStatusTake || waitCanPop
			wantState := ModuleStateActive
			if wantSnapshot {
				wantState = ModuleStateSnapshot
			}
			require.Equal(rt, wantState, a.ModuleState(), step)
			require.Equal(rt, wantState, stateCounter.last(), step)
			wantHold := a.Role() == RoleLeader && wantSnapshot
			require.Equal(rt, wantHold, a.isLeaderHeld(), step)

			result, ok := a.LastSnapshotTake()
			require.Equal(rt, status == actionStatusResolved, ok, step)
			require.Equal(rt, wantLog, *recordingLog, step)
			if ok {
				require.NotEqual(rt, fakeSnapshotPreProofID, result.RecordingID, step)
			}
			if status == actionStatusResolved || status == actionStatusCancelled {
				require.Empty(rt, archive.pending, step)
				for _, start := range archive.starts {
					require.Equal(rt, 1, start.closed, step)
				}
				require.False(rt, archive.watching, step)
				for _, pub := range archive.pubs {
					require.Equal(rt, 1, pub.closed, step)
				}
			}
		}

		tick := func(stepNs int64) {
			rt.Helper()
			nowNs += stepNs
			held := a.isLeaderHeld()
			a.DoWork(nowNs)

			if status == actionStatusWaiting || status == actionStatusTake {
				acks.poll()
			}
			if status == actionStatusWaiting && acks.pop() {
				status = actionStatusTake
			}
			if _, ok := a.LastSnapshotTake(); ok && status == actionStatusTake {
				status = actionStatusResolved
				requireSnapshotTakeResult(
					rt, a, archive, recordingLog, script, position, serviceCount, timerRows,
				)
				wantLog = *recordingLog
			}
			if held && a.isLeaderHeld() {
				requireHeldUntouched("held tick")
			}

			lifted := held && !a.isLeaderHeld()
			if lifted {
				session, ok := a.sessions.sessionByID(1)
				require.True(rt, ok, "lift tick")
				require.Equal(rt, sessionOpen, session.state, "lift tick")
				require.Equal(rt, nowNs, session.lastActivityNs, "lift tick")
			}
		}

		opNames := []string{
			"tick", "acks", "commit", "wrong ack", "prepare", "join", "close",
		}
		steps := rapid.IntRange(1, 30).Draw(rt, "steps")
		for range steps {
			op := rapid.IntRange(0, len(opNames)-1).Draw(rt, "op")
			active := status == actionStatusWaiting || status == actionStatusTake
			switch op {
			case 0:
				tick(rapid.Int64Range(0, 2*timeoutNs).Draw(rt, "stepNs"))
			case 1:
				if active {
					sendAcks()
				}
			case 2:
				commit.Store(position)
			case 3:
				if active && serviceCount > 0 {
					q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
						LogPosition: position,
						AckID:       99,
						RelevantID:  1,
					}))
					acks.frames = append(acks.frames, snapshotTakeAckFrame{ackID: 99})
				}
			case 4:
				_, prepared, err := a.PrepareForNewLeadership(0, nowNs)
				require.NoError(rt, err)
				require.True(rt, prepared)
				switch status {
				case actionStatusWaiting:
					status = actionStatusRetired
				case actionStatusTake:
					status = actionStatusCancelled
				}
			default:
				if op == 5 {
					joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{}, nowNs)
					require.NoError(rt, err)
					require.False(rt, joined)
				} else {
					a.CloseSnapshotTake()
				}
				if status == actionStatusTake {
					status = actionStatusCancelled
				}
			}
			requireModel(opNames[op])
			if status == actionStatusCancelled {
				require.NotNil(rt, a.abandonedSnapshotWaitAt(position))
			}
		}

		// Final drain: deliver the acks, raise commit, and step past every
		// deadline until the action ends.
		if status == actionStatusWaiting {
			sendAcks()
		}
		commit.Store(position)
		for range 32 {
			if status != actionStatusWaiting && status != actionStatusTake {
				break
			}
			if status == actionStatusWaiting && acks.marked {
				break
			}
			tick(2 * timeoutNs)
			requireModel("final drain")
		}
		if !acks.marked {
			require.NotEqual(rt, actionStatusWaiting, status)
			require.NotEqual(rt, actionStatusTake, status)
		}

		a.MarkClosed()
		nowNs++
		a.DoWork(nowNs)
		require.Equal(rt, ModuleStateClosed, a.ModuleState())
		require.Equal(rt, ModuleStateClosed, stateCounter.last())
	})
}

// requireSnapshotTakeResult checks a resolved take against the script.
func requireSnapshotTakeResult(
	rt *rapid.T,
	a *Agent,
	archive *fakeSnapshotArchive,
	recordingLog *fakeSnapshotRecordingLog,
	script snapshotTakeScript,
	position int64,
	serviceCount int,
	timerRows int,
) {
	rt.Helper()
	result, ok := a.LastSnapshotTake()
	require.True(rt, ok)
	require.Equal(rt, position, result.LogPosition)
	require.Equal(rt, int64(4), result.LeadershipTermID)
	require.Equal(rt, script.relevantIDs, result.ServiceRecordingIDs)

	possible, certain := script.outcome()
	if result.Err != nil {
		require.NoError(rt, result.ForceErr)
		require.Equal(rt, int64(nullValue), result.RecordingID)
		matched := certain != nil && errors.Is(result.Err, certain)
		for _, cause := range possible {
			matched = matched || errors.Is(result.Err, cause)
		}
		require.True(rt, matched, "cause %v, certain %v, possible %v", result.Err, certain, possible)
		if errors.Is(result.Err, errSnapshotServiceFailed) {
			require.Empty(rt, archive.addOffers)
		}
		requireFailedTakeRows(rt, recordingLog, script, result)
		return
	}

	require.NoError(rt, certain)
	if script.logOutcome == scriptLogForceError {
		require.ErrorIs(rt, result.ForceErr, errSnapshotRecordingLogForce)
		require.ErrorIs(rt, result.ForceErr, errSnapshotTakeRapidBoom)
	} else {
		require.NoError(rt, result.ForceErr)
	}
	group := fakeSnapshotGroup(result, script.startID)
	require.Equal(rt, group, recordingLog.rows)
	require.Equal(rt, []fakeSnapshotForce{
		{level: recordinglog.FileSyncData, rows: len(group)},
	}, recordingLog.forces)
	require.Len(rt, archive.starts, 1)
	start := archive.starts[0]
	require.True(rt, start.proven)
	require.Equal(rt, script.startID, result.RecordingID)
	require.Equal(rt, script.startID, archive.watchedID)
	require.Len(rt, archive.pubs, 1)
	pub := archive.pubs[0]
	require.Equal(rt, 1, pub.closed)
	require.True(rt, archive.reportedStopped)
	require.GreaterOrEqual(rt, archive.reportedStop, pub.closePosition)
	if script.stopOutcome == scriptStopAbove {
		require.Equal(rt, pub.closePosition+script.stopAboveBy, archive.reportedStop)
	}

	rows := decodeSnapshotRows(rt, pub.rows)
	wantTemplates := []uint16{
		clusterwire.SnapshotMarkerSbeTemplateID,
		clusterwire.ConsensusModuleSbeTemplateID,
		clusterwire.ClusterSessionSbeTemplateID,
	}
	for range timerRows {
		wantTemplates = append(wantTemplates, clusterwire.TimerSbeTemplateID)
	}
	for range serviceCount {
		wantTemplates = append(wantTemplates, clusterwire.PendingMessageTrackerSbeTemplateID)
	}
	wantTemplates = append(wantTemplates, clusterwire.SnapshotMarkerSbeTemplateID)
	require.Len(rt, pub.rows, len(wantTemplates))
	for i, row := range pub.rows {
		_, templateID, _, _, ok := codecs.DecodeMessageHeader(row, 0)
		require.True(rt, ok)
		require.Equal(rt, wantTemplates[i], templateID, "row %d", i)
	}
	begin, ok := rows[0].(codecs.SnapshotMarkerFields)
	require.True(rt, ok)
	require.Equal(rt, clusterwire.SnapshotMark_BEGIN, begin.Mark)
	require.Equal(rt, position, begin.LogPosition)
	end, ok := rows[len(rows)-1].(codecs.SnapshotMarkerFields)
	require.True(rt, ok)
	require.Equal(rt, clusterwire.SnapshotMark_END, end.Mark)
	for i := range serviceCount {
		tracker, ok := rows[3+timerRows+i].(codecs.PendingMessageTrackerFields)
		require.True(rt, ok)
		require.Equal(rt, emptyPendingMessageTracker(i), tracker)
	}
}

// requireFailedTakeRows asserts the recording log after a failed take:
//   - an append error keeps the rows before the failed call
//   - any other cause does not append
//
// The failed take does not call Force.
func requireFailedTakeRows(
	rt *rapid.T,
	recordingLog *fakeSnapshotRecordingLog,
	script snapshotTakeScript,
	result SnapshotTakeResult,
) {
	rt.Helper()
	group := fakeSnapshotGroup(result, script.startID)
	var wantRows int
	appendFailed := errors.Is(result.Err, errSnapshotRecordingLog) &&
		script.logOutcome == scriptLogAppendError
	if appendFailed {
		require.ErrorIs(rt, result.Err, errSnapshotTakeRapidBoom)
		wantRows = script.logFailRow
	}
	require.Len(rt, recordingLog.rows, wantRows)
	for i, row := range recordingLog.rows {
		require.Equal(rt, group[i], row, "row %d", i)
	}
	require.Empty(rt, recordingLog.forces)
}
