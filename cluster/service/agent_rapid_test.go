//go:build rapid && testhooks

package service

import (
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/logbuffer"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
	"github.com/warrenguy/luas/internal/testutil"
)

// modelMessage is one append-order log message with stream positions.
type modelMessage struct {
	kind      string // open, close, message, timer, padding
	sessionID int64
	startPos  int64 // start of last frame (delivery bound uses this)
	endPos    int64
}

// TestAgent_boundedConsume_rapid asserts bounded consumption against a model:
//   - recorded callbacks match the model delivered prefix
//   - LogPosition is zero before the first delivery. After it, LogPosition
//     equals the end of the last delivered non-padding message. LogPosition
//     may exceed the raw commit under frame-granular delivery.
//   - image position tracks the stream cursor, including padding
//   - the session registry matches the delivered open and close set
//   - positions are monotonic
//   - a callback never delivers twice
//
// End of stream: a zero-fragment tick at the end-of-stream word unbinds.
// LogPosition then covers trailing padding.
//
// Ops: append a committed frame or padding, advance commit, DoWork, and write
// the end-of-stream word once at the stream end. Appends stop after the word.
// The draws leave out a session-open clash. Table tests assert the clash path.
func TestAgent_boundedConsume_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		t := testutil.IterationTB(t, rt)

		rt.Helper()

		svc := &recordingService{}
		sub, image := newLogImage(t, testLogSessionID, testLogStreamID, 0)

		committed := int64(0)
		const fragmentLimit = 50
		agent := NewAgent(AgentConfig{
			Service:           svc,
			CommittedPosition: func() int64 { return committed },
			Subscriptions:     &stubAdder{sub: sub},
			FragmentLimit:     fragmentLimit,
		})
		// Set the image directly. The property does not depend on the join
		// lifecycle.
		agent.adapter.SetImage(image)
		agent.adapter.SetMaxLogPosition(1 << 40)
		agent.started = true
		agent.role = codecs.RoleLeader

		appendSessions := make(map[int64]struct{})
		var messages []modelMessage
		logEnd := int64(0)
		nextMsg := 0
		var lastDeliveredEnd int64
		modelSessions := make(map[int64]struct{})
		var wantOrder []string
		var wantOpens []int64
		var wantCloses []int64

		eosWritten := false
		logBound := true

		record := func(m modelMessage, span int) {
			m.startPos = logEnd
			m.endPos = logEnd + int64(span)
			messages = append(messages, m)
			logEnd = m.endPos
		}

		modelImagePosition := func() int64 {
			if nextMsg == 0 {
				return 0
			}
			return messages[nextMsg-1].endPos
		}

		// modelDoWork mirrors one poll. Messages whose last frame starts below
		// committed deliver in order, at most fragmentLimit non-padding
		// fragments per call.
		modelDoWork := func() {
			rt.Helper()
			if !logBound {
				return
			}
			frags := 0
			for nextMsg < len(messages) && frags < fragmentLimit {
				m := &messages[nextMsg]
				if m.startPos >= committed {
					break
				}
				if m.kind == "padding" {
					// Padding moves the image position only.
					nextMsg++
					continue
				}
				lastDeliveredEnd = m.endPos
				wantOrder = append(wantOrder, m.kind)
				switch m.kind {
				case "open":
					modelSessions[m.sessionID] = struct{}{}
					wantOpens = append(wantOpens, m.sessionID)
				case "close":
					delete(modelSessions, m.sessionID)
					wantCloses = append(wantCloses, m.sessionID)
				}
				nextMsg++
				// The model counts a fragmented train as one fragment. Table
				// tests pin the real two-fragment poll.
				frags++
			}
			atEndOfStream := eosWritten && modelImagePosition() == logEnd
			if frags == 0 && atEndOfStream {
				logBound = false
			}
		}

		assertInvariants := func() {
			rt.Helper()
			require.Equal(rt, wantOrder, svc.order)
			require.Len(rt, svc.opens, len(wantOpens))
			for i, id := range wantOpens {
				require.Equal(rt, id, svc.opens[i].session.ID())
			}
			require.Len(rt, svc.closes, len(wantCloses))
			for i, id := range wantCloses {
				require.Equal(rt, id, svc.closes[i].session.ID())
			}

			// Frame-granular delivery: a frame that starts before the bound
			// delivers whole, so LogPosition may pass the raw commit value.
			wantLogPosition := lastDeliveredEnd
			wantRole := codecs.RoleLeader
			if !logBound {
				wantLogPosition = modelImagePosition()
				wantRole = codecs.RoleFollower
			}
			require.Equal(rt, wantLogPosition, agent.LogPosition())
			require.Equal(rt, logBound, agent.IsLogBound())
			require.Equal(rt, wantRole, agent.Role())

			// Image position counts padding. LogPosition does not.
			require.Equal(rt, modelImagePosition(), image.Position())

			require.Equal(rt, len(modelSessions), agent.SessionCount())
			for id := range modelSessions {
				require.True(rt, agent.HasSession(id))
			}
			require.NoError(rt, agent.ServiceFailure())
		}

		prevPos := agent.LogPosition()
		runDoWork := func() {
			rt.Helper()
			modelDoWork()
			agent.DoWork(0)
			require.GreaterOrEqual(rt, agent.LogPosition(), prevPos)
			prevPos = agent.LogPosition()
		}

		stepCount := rapid.IntRange(1, 64).Draw(rt, "stepCount")
		for step := range stepCount {
			op := rapid.IntRange(0, 3).Draw(rt, "op")
			switch op {
			case 0: // append frame
				if logEnd > 1<<16 || eosWritten {
					break
				}

				record(appendRandomFrame(t, rt, image, int(logEnd), step, appendSessions))

			case 1: // advance commit
				if committed < logEnd {
					committed = rapid.Int64Range(committed+1, logEnd).Draw(rt, "commitPos")
				}

			case 2: // DoWork
				runDoWork()

			case 3: // end of stream, once
				if eosWritten {
					break
				}
				eosWritten = true
				aeron.ImageSetEndOfStreamPositionForHooks(image, logEnd)
			}

			assertInvariants()
		}

		// Final drain.
		committed = logEnd
		for range 64 {
			if nextMsg >= len(messages) {
				break
			}
			runDoWork()
			assertInvariants()
		}
		require.Equal(rt, len(messages), nextMsg)

		if eosWritten {
			runDoWork()
			assertInvariants()
			require.False(rt, agent.IsLogBound())
		}
	})
}

// appendRandomFrame writes one unfragmented frame or padding at writeAt. It
// returns the model entry and the frame span.
func appendRandomFrame(
	t testing.TB,
	rt *rapid.T,
	image *aeron.Image,
	writeAt int,
	step int,
	openSessions map[int64]struct{},
) (
	modelMessage,
	int,
) {
	rt.Helper()

	sessionPool := []int64{1, 2, 3, 4, 5}
	ts := int64(step + 1)

	var free []int64

	for _, id := range sessionPool {
		if _, open := openSessions[id]; !open {
			free = append(free, id)
		}
	}

	// Sorted keys keep the draw seed-stable.
	openIDs := slices.Sorted(maps.Keys(openSessions))

	kindRoll := rapid.IntRange(0, 4).Draw(rt, "kind")
	noFreeSession := kindRoll == 0 && len(free) == 0
	noOpenSession := kindRoll == 1 && len(openIDs) == 0
	if noFreeSession || noOpenSession {
		// No session available for the drawn kind: append a timer.
		kindRoll = 3
	}

	var m modelMessage
	var payload []byte

	switch kindRoll {
	case 0: // open
		id := free[rapid.IntRange(0, len(free)-1).Draw(rt, "openID")]
		payload = encodeSessionOpen(t, codecs.SessionOpenEventFields{
			LeadershipTermID: 1,
			ClusterSessionID: id,
			Timestamp:        ts,
			ResponseChannel:  []byte("c"),
		})
		m = modelMessage{kind: "open", sessionID: id}
		openSessions[id] = struct{}{}

	case 1: // close
		id := openIDs[rapid.IntRange(0, len(openIDs)-1).Draw(rt, "closeID")]
		payload = encodeSessionClose(t, codecs.SessionCloseEventFields{
			LeadershipTermID: 1,
			ClusterSessionID: id,
			Timestamp:        ts,
			CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
		})
		m = modelMessage{kind: "close", sessionID: id}
		delete(openSessions, id)

	case 2: // message
		id := sessionPool[rapid.IntRange(0, len(sessionPool)-1).Draw(rt, "msgID")]
		bodyLen := rapid.IntRange(0, 16).Draw(rt, "bodyLen")
		body := make([]byte, bodyLen)

		for i := range body {
			body[i] = byte(i)
		}

		payload = encodeSessionMessage(t, codecs.SessionMessageHeaderFields{
			LeadershipTermID: 1,
			ClusterSessionID: id,
			Timestamp:        ts,
		}, body)
		m = modelMessage{kind: "message", sessionID: id}

	case 3: // timer
		payload = encodeTimerEvent(t, codecs.TimerEventFields{
			LeadershipTermID: 1,
			CorrelationID:    int64(step),
			Timestamp:        ts,
		})
		m = modelMessage{kind: "timer"}

	case 4: // padding; IntRange already meets DataHeaderLength
		padLen := int32(logbuffer.AlignFrameLength(
			rapid.IntRange(32, 128).Draw(rt, "padLen"),
		))
		writeAgentPadding(t, image, writeAt, padLen)

		return modelMessage{kind: "padding"}, int(padLen)
	}

	return m, writeAgentFrame(t, image, writeAt, payload)
}

type inflightAdd struct {
	sessionID     int64
	correlationID int64
}

const (
	errorModelOKStream    = testResponseStreamID + 1
	errorModelFatalStream = testResponseStreamID + 2
)

var errorModelPollBoom = errors.New("poll boom")

// errorModelDeadActions name the actions that still run on a failed or closed
// agent.
var errorModelDeadActions = []string{
	"CloseAgent",
	"Tick",
	"TimerPublishFailure",
	"UnknownCloseRequest",
}

// errorHandlerModel tracks the errors, sessions, and acks that an agent exposes
// under random fault actions. A data-precondition skip comes before the first
// draw. The actions here leave the agent running. errorHandlerTerminalModel
// adds the ones that fail or close it. liveOnlyActions stops the rest once the
// agent fails or closes.
type errorHandlerModel struct {
	agent     *Agent
	adder     *fakeResponseAdder
	proxy     *ConsensusModuleProxy
	openPub   *aeron.Publication
	closedPub *aeron.Publication
	reporter  *terminatingReporter
	badSchema []byte
	shortAck  []byte

	got            []error
	want           []error
	failedSentinel error
	openIDs        []int64
	inFlight       []inflightAdd
	queued         []int64
	sent           []codecs.ServiceAckFields
	logPos         int64
	nextAckID      int64
	nextSession    int64
	nextUnknown    int64
	closed         bool
}

func newErrorHandlerModel(t testing.TB) *errorHandlerModel {
	t.Helper()

	adder := newFakeResponseAdder()
	adder.offerErrs = map[int32]error{
		testResponseStreamID:  errors.New("offer boom"),
		errorModelFatalStream: aeron.ErrClientClosed,
	}
	proxy, openPub := boundModuleProxy(t)
	closedPub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	require.NoError(t, closedPub.Close())

	shortAck := encodeRequestServiceAckControl(t, 1)
	m := &errorHandlerModel{
		adder:     adder,
		proxy:     proxy,
		openPub:   openPub,
		closedPub: closedPub,
		reporter:  &terminatingReporter{},
		badSchema: encodeMessageHeader(
			t, 8, clusterwire.JoinLogSbeTemplateID, 999, 16,
		),
		shortAck:    shortAck[:len(shortAck)-4],
		nextSession: 1,
		nextUnknown: -1,
	}

	m.agent, _, _ = bindImage(t, AgentConfig{
		Service:              &recordingService{},
		ResponsePublications: adder,
		ModuleProxy:          proxy,
		ErrorHandler:         collectErrors(&m.got),
		Transport:            m.reporter,
	}, 0)
	m.agent.role = codecs.RoleLeader
	m.agent.started = true

	return m
}

func (m *errorHandlerModel) isLive() bool {
	return !m.closed && m.failedSentinel == nil
}

// liveOnlyActions returns the actions of sm. Each action returns at once on a
// failed or closed agent, except errorModelDeadActions. A return instead of
// rt.Skip keeps rapid under its limit of 100 skipped draws per step.
func (m *errorHandlerModel) liveOnlyActions(sm rapid.StateMachine) map[string]func(*rapid.T) {
	actions := rapid.StateMachineActions(sm)

	for name, action := range actions {
		if name == "" || slices.Contains(errorModelDeadActions, name) {
			continue
		}

		actions[name] = func(rt *rapid.T) {
			if m.isLive() {
				action(rt)
			}
		}
	}

	return actions
}

func (m *errorHandlerModel) skipUnlessAddInFlight(rt *rapid.T) {
	rt.Helper()

	if len(m.inFlight) == 0 {
		rt.Skip("no response publication add in flight")
	}
}

func (m *errorHandlerModel) skipUnlessSessionOpen(rt *rapid.T) {
	rt.Helper()

	if len(m.openIDs) == 0 {
		rt.Skip("no open session")
	}
}

func (m *errorHandlerModel) advance() int64 {
	m.logPos += 64

	return m.logPos
}

func (m *errorHandlerModel) note(sentinel error, fatal bool) {
	if !m.isLive() {
		return
	}

	m.want = append(m.want, sentinel)

	if fatal {
		m.failedSentinel = sentinel
	}
}

func (m *errorHandlerModel) sendQueued() {
	if len(m.queued) == 0 {
		return
	}

	logPosition := m.queued[0]
	m.queued = m.queued[1:]

	m.sent = append(m.sent, codecs.ServiceAckFields{
		LogPosition: logPosition,
		Timestamp:   m.logPos,
		AckID:       m.nextAckID,
		RelevantID:  nullValue,
	})
	m.nextAckID++
}

func (m *errorHandlerModel) nextSessionID() int64 {
	id := m.nextSession
	m.nextSession++

	return id
}

func (m *errorHandlerModel) unknownSession() int64 {
	id := m.nextUnknown
	m.nextUnknown--

	return id
}

func (m *errorHandlerModel) sendSessionOpen(id int64, stream int32) aeron.ControlledAction {
	pos := m.advance()

	return m.agent.handleSessionOpen(pos, codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: id,
		Timestamp:        pos,
		ResponseStreamID: stream,
		ResponseChannel:  []byte(testResponseChannel),
	})
}

func (m *errorHandlerModel) openSession(rt *rapid.T, stream int32) {
	rt.Helper()

	id := m.nextSessionID()
	correlationID := m.adder.nextID
	require.Equal(rt, aeron.ControlledContinue, m.sendSessionOpen(id, stream))

	m.openIDs = append(m.openIDs, id)

	if stream == errorModelOKStream {
		m.inFlight = append(m.inFlight, inflightAdd{
			sessionID:     id,
			correlationID: correlationID,
		})
	}
}

func (m *errorHandlerModel) closeSession(id int64) aeron.ControlledAction {
	pos := m.advance()

	return m.agent.handleSessionClose(pos, codecs.SessionCloseEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: id,
		Timestamp:        pos,
		CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
	})
}

func (m *errorHandlerModel) dropAdd(i int) {
	m.inFlight = slices.Delete(m.inFlight, i, i+1)
}

func (m *errorHandlerModel) dropSession(id int64) {
	m.openIDs = slices.DeleteFunc(m.openIDs, func(open int64) bool { return open == id })
}

func (m *errorHandlerModel) failAddPoll(rt *rapid.T, err error, fatal bool) {
	rt.Helper()

	i := rapid.IntRange(0, len(m.inFlight)-1).Draw(rt, "addPoll")
	m.adder.next(m.inFlight[i].correlationID, responsePoll{err: err})
	m.note(ErrResponsePublicationAdd, fatal)
	m.agent.responseAddWork()
	m.dropAdd(i)
}

func (m *errorHandlerModel) failAddTeardown(
	rt *rapid.T,
	err error,
	fatal bool,
) aeron.ControlledAction {
	rt.Helper()

	i := rapid.IntRange(0, len(m.inFlight)-1).Draw(rt, "teardownAdd")
	add := m.inFlight[i]
	m.adder.next(add.correlationID, responsePoll{err: err})
	m.note(ErrResponsePublicationAdd, fatal)
	action := m.closeSession(add.sessionID)
	m.dropSession(add.sessionID)
	m.dropAdd(i)

	return action
}

func (m *errorHandlerModel) requestClusterAction(
	pos, logPosition int64,
	action clusterwire.ClusterAction,
) aeron.ControlledAction {
	return m.agent.handleClusterAction(pos, codecs.ClusterActionRequestFields{
		LeadershipTermID: 1,
		LogPosition:      logPosition,
		Timestamp:        pos,
		Action:           action,
	})
}

func (m *errorHandlerModel) Check(rt *rapid.T) {
	rt.Helper()

	// Probe first, so the compare below sees any error this call reports.
	if !m.isLive() {
		require.Zero(rt, m.agent.DoWork(0))
	}

	require.Len(rt, m.got, len(m.want))

	failedReports := 0

	for i, sentinel := range m.want {
		require.ErrorIs(rt, m.got[i], sentinel)

		if errors.Is(m.got[i], ErrFailed) {
			failedReports++
			require.ErrorIs(rt, m.got[i], m.failedSentinel)
			require.Same(rt, m.agent.ServiceFailure(), m.got[i])
		}
	}

	if m.failedSentinel != nil {
		require.Equal(rt, 1, failedReports)
		require.ErrorIs(rt, m.agent.ServiceFailure(), m.failedSentinel)
	} else {
		require.Zero(rt, failedReports)
		require.NoError(rt, m.agent.ServiceFailure())
	}

	require.Equal(rt, len(m.openIDs), m.agent.SessionCount())
	require.Equal(rt, len(m.inFlight), m.agent.PendingResponsePublicationAdds())
	require.Equal(rt, m.logPos, m.agent.LogPosition())
	require.Equal(rt, m.nextAckID, m.agent.NextAckID())
	require.Equal(rt, len(m.queued), m.agent.PendingCoordinationAcks())
	require.Equal(rt, m.sent, decodeServiceAcksFromPub(rt, m.openPub))
}

func (m *errorHandlerModel) OpenSession(rt *rapid.T) {
	m.openSession(rt, errorModelOKStream)
}

func (m *errorHandlerModel) OfferFailure(rt *rapid.T) {
	m.note(ErrResponsePublicationAdd, false)
	m.openSession(rt, testResponseStreamID)
}

func (m *errorHandlerModel) PollFailure(rt *rapid.T) {
	m.skipUnlessAddInFlight(rt)
	m.failAddPoll(rt, errorModelPollBoom, false)
}

func (m *errorHandlerModel) UnknownLogClose(rt *rapid.T) {
	id := m.unknownSession()
	m.note(ErrUnknownSessionClose, false)
	require.Equal(rt, aeron.ControlledContinue, m.closeSession(id))
}

func (m *errorHandlerModel) ControlFault(rt *rapid.T) {
	switch rapid.IntRange(0, 2).Draw(rt, "controlFault") {
	case 0:
		m.note(ErrControlMessageSchema, false)
		m.agent.handleControlFragment(m.badSchema, 0)

	case 1:
		m.note(ErrControlMessageDecode, false)
		m.agent.handleControlFragment([]byte{1, 2, 3, 4}, 0)

	default:
		m.note(ErrControlMessageDecode, false)
		m.agent.handleControlFragment(m.shortAck, 0)
	}
}

func (m *errorHandlerModel) UnknownCloseRequest(rt *rapid.T) {
	id := m.unknownSession()
	m.note(ErrCloseUnknownSession, false)
	require.False(rt, m.agent.CloseClientSession(id))
}

func (m *errorHandlerModel) TeardownAddFailure(rt *rapid.T) {
	m.skipUnlessAddInFlight(rt)

	action := m.failAddTeardown(rt, errorModelPollBoom, false)
	require.Equal(rt, aeron.ControlledContinue, action)
}

func (m *errorHandlerModel) UnknownTimeUnit(rt *rapid.T) {
	pos := m.advance()
	m.note(ErrUnknownTimeUnit, false)
	m.agent.handleNewLeadershipTermEvent(pos, codecs.NewLeadershipTermEventFields{
		Timestamp: pos,
		TimeUnit:  clusterwire.ClusterTimeUnit(99),
	})
}

func (m *errorHandlerModel) NullClusterAction(rt *rapid.T) {
	pos := m.advance()
	m.note(ErrUnknownClusterAction, false)
	action := m.requestClusterAction(pos, pos, clusterwire.ClusterAction_NULL_VALUE)
	require.Equal(rt, aeron.ControlledContinue, action)
}

func (m *errorHandlerModel) SessionClash(rt *rapid.T) {
	m.skipUnlessSessionOpen(rt)

	id := rapid.SampledFrom(m.openIDs).Draw(rt, "clashID")
	m.note(ErrSessionClash, false)
	require.Equal(rt, aeron.ControlledBreak, m.sendSessionOpen(id, errorModelOKStream))
}

func (m *errorHandlerModel) TimerPublishFailure(rt *rapid.T) {
	m.proxy.SetPublication(m.closedPub)
	m.note(ErrTimerPublish, false)
	require.False(rt, m.agent.ScheduleTimer(1, 2))
	m.proxy.SetPublication(m.openPub)
}

func (m *errorHandlerModel) AckRequestPassed(rt *rapid.T) {
	// Return, never skip. This action sorts first, and shrinking stalls on a
	// first action that skips.
	if m.logPos == 0 {
		return
	}

	m.note(ErrAckRequestPassed, false)
	m.agent.handleRequestServiceAck(m.logPos - 1)
	m.agent.sweepRequestAckReadiness()
	m.queued = append(m.queued, m.logPos)
}

func (m *errorHandlerModel) CloseSessionPublishFailure(rt *rapid.T) {
	m.skipUnlessSessionOpen(rt)

	id := rapid.SampledFrom(m.openIDs).Draw(rt, "closeID")
	m.proxy.SetPublication(m.closedPub)
	m.note(ErrCloseSessionPublish, false)
	require.False(rt, m.agent.CloseClientSession(id))
	m.proxy.SetPublication(m.openPub)
}

func (m *errorHandlerModel) SnapshotNoArchive(rt *rapid.T) {
	pos := m.advance()
	m.note(ErrSnapshotNoArchive, false)
	action := m.requestClusterAction(pos, pos, clusterwire.ClusterAction_SNAPSHOT)
	require.Equal(rt, aeron.ControlledBreak, action)

	m.queued = append(m.queued, pos)
}

func (m *errorHandlerModel) Tick(*rapid.T) {
	m.agent.DoWork(0)

	if m.isLive() {
		m.sendQueued()
	}
}

// errorHandlerTerminalModel adds the actions that fail or close the agent.
// They run only after a live-only pass. Otherwise they end most sequences
// within a few steps.
type errorHandlerTerminalModel struct {
	*errorHandlerModel
}

func (m *errorHandlerTerminalModel) DispatchFailure(rt *rapid.T) {
	sentinel := rapid.SampledFrom([]error{
		ErrLogDispatchHeader,
		ErrLogDispatchSchema,
		ErrLogDispatchDecode,
	}).Draw(rt, "dispatch")
	m.note(sentinel, true)
	action := m.agent.handleDispatchFailure(sentinel)
	require.Equal(rt, aeron.ControlledAbort, action)
}

func (m *errorHandlerTerminalModel) PositionMismatch(rt *rapid.T) {
	pos := m.advance()
	m.note(ErrActionPositionMismatch, true)
	action := m.requestClusterAction(pos, pos-1, clusterwire.ClusterAction_SNAPSHOT)
	require.Equal(rt, aeron.ControlledAbort, action)
}

func (m *errorHandlerTerminalModel) FatalOffer(rt *rapid.T) {
	m.note(ErrResponsePublicationAdd, true)

	action := m.sendSessionOpen(m.nextSessionID(), errorModelFatalStream)
	require.Equal(rt, aeron.ControlledBreak, action)
}

func (m *errorHandlerTerminalModel) FatalPoll(rt *rapid.T) {
	m.skipUnlessAddInFlight(rt)
	m.failAddPoll(rt, aeron.ErrClientClosed, true)
}

func (m *errorHandlerTerminalModel) FatalTeardown(rt *rapid.T) {
	m.skipUnlessAddInFlight(rt)

	action := m.failAddTeardown(rt, aeron.ErrClientClosed, true)
	require.Equal(rt, aeron.ControlledBreak, action)
}

func (m *errorHandlerTerminalModel) TerminalAckFailure(rt *rapid.T) {
	if len(m.queued) == 0 {
		rt.Skip("no queued ack")
	}

	m.proxy.SetPublication(m.closedPub)
	m.note(ErrAckPublish, true)
	m.agent.DoWork(0)
	m.proxy.SetPublication(m.openPub)
}

func (m *errorHandlerTerminalModel) TransportTerminating(rt *rapid.T) {
	m.reporter.terminating = true
	m.note(ErrTransportTerminating, true)
	require.Zero(rt, m.agent.DoWork(0))
}

func (m *errorHandlerTerminalModel) CloseAgent(rt *rapid.T) {
	if m.closed {
		rt.Skip("agent closed")
	}

	for _, add := range m.inFlight {
		m.adder.next(add.correlationID, responsePoll{err: errorModelPollBoom})
	}

	m.agent.Close()
	m.closed = true
	m.inFlight = nil
}

// TestAgent_errorHandler_rapid drives an agent through random fault actions:
// a live-only pass, then every action. After each action:
//   - the agent reports the expected sentinels in order
//   - a failed agent reports one ErrFailed error, and ServiceFailure returns it
//   - a failed or closed agent stops reporting errors
//   - DoWork returns zero on a failed or closed agent
//   - session, add, and ack counts match the model
//   - LogPosition, NextAckID, and the published acks match the model
func TestAgent_errorHandler_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		t := testutil.IterationTB(t, rt)

		live := newErrorHandlerModel(t)
		rt.Repeat(live.liveOnlyActions(live))
		rt.Repeat(live.liveOnlyActions(&errorHandlerTerminalModel{errorHandlerModel: live}))
	})
}
