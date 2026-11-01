//go:build rapid && testhooks

package service

import (
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// modelMessage is one append-order log message with stream positions.
type modelMessage struct {
	kind      string // open, close, message, timer, padding
	sessionID int64
	startPos  int64 // start of last frame (delivery bound uses this)
	endPos    int64
}

// Asserts bounded consumption. Recorded callbacks match the model delivered
// prefix. LogPosition equals the end of the last delivered non-padding
// message; LogPosition may exceed the raw commit under frame-granular
// delivery, and is zero when nothing is delivered. Image position tracks the
// stream cursor including padding. The session registry matches the delivered
// open/close set. Positions are monotonic. No callback delivers twice.
//
// Ops: append a committed frame (or padding), advance commit, DoWork.
// Session-open clash is constrained out; unit rows assert the clash path.
func TestAgent_boundedConsume_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
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
		// Inject image: consume property is independent of join lifecycle.
		agent.adapter.SetImage(image)
		agent.adapter.SetMaxLogPosition(1 << 40)
		agent.started = true

		sessionPool := []int64{1, 2, 3, 4, 5}
		appendSessions := make(map[int64]struct{})
		var messages []modelMessage
		// logEnd is the next write offset and the stream end position.
		logEnd := int64(0)

		// nextMsg is the next messages index not yet consumed by the model.
		nextMsg := 0
		var lastDeliveredEnd int64
		modelSessions := make(map[int64]struct{})
		var wantOrder []string
		var wantOpens []int64
		var wantCloses []int64

		// record appends one model entry and advances logEnd by span.
		record := func(m modelMessage, span int) {
			m.startPos = logEnd
			m.endPos = logEnd + int64(span)
			messages = append(messages, m)
			logEnd = m.endPos
		}

		// modelDoWork advances the reference model one Poll-shaped step:
		// messages with last-frame start below committed deliver in order,
		// at most fragmentLimit non-padding fragments (padding free).
		modelDoWork := func() {
			rt.Helper()
			frags := 0
			for nextMsg < len(messages) && frags < fragmentLimit {
				m := &messages[nextMsg]
				if m.startPos >= committed {
					break
				}
				if m.kind == "padding" {
					// Padding advances image position only. No callback. No fragment count.
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
				// Unfragmented: one fragment. Fragmented train: two fragments
				// in the real poll; model treats one logical message as one
				// fragment-budget unit. Unit rows pin multi-fragment trains.
				frags++
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
			// LogPosition must equal the end of the last delivered message
			// (bound rounded up to that message end). Zero when nothing has
			// been delivered yet.
			require.Equal(rt, lastDeliveredEnd, agent.LogPosition())

			// Image position tracks the stream cursor, including padding.
			// LogPosition skips padding. nextMsg advances over padding in the
			// model.
			wantImagePos := int64(0)
			if nextMsg > 0 {
				wantImagePos = messages[nextMsg-1].endPos
			}
			require.Equal(rt, wantImagePos, image.Position())

			// Model session set must match agent registry size and membership.
			require.Equal(rt, len(modelSessions), agent.SessionCount())
			for id := range modelSessions {
				require.True(rt, agent.HasSession(id))
			}
			require.NoError(rt, agent.ServiceFailure())
		}

		// Monotonic position across DoWork.
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
			op := rapid.IntRange(0, 2).Draw(rt, "op")
			switch op {
			case 0: // append frame (unfragmented + padding; fragmented pinned by unit rows)
				if logEnd > 1<<16 {
					break
				}
				var m modelMessage
				var payload []byte
				ts := int64(step + 1)
				writeAt := int(logEnd)

				var free []int64
				for _, id := range sessionPool {
					if _, open := appendSessions[id]; !open {
						free = append(free, id)
					}
				}
				// Sort keys so the rapid draw is seed-stable. Map range order is
				// not deterministic.
				openIDs := slices.Sorted(maps.Keys(appendSessions))

				kindRoll := rapid.IntRange(0, 4).Draw(rt, "kind")
				noFreeSession := kindRoll == 0 && len(free) == 0
				noOpenSession := kindRoll == 1 && len(openIDs) == 0
				if noFreeSession || noOpenSession {
					// No session available for the drawn kind: append a timer.
					kindRoll = 3
				}

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
					appendSessions[id] = struct{}{}

				case 1: // close
					id := openIDs[rapid.IntRange(0, len(openIDs)-1).Draw(rt, "closeID")]
					payload = encodeSessionClose(t, codecs.SessionCloseEventFields{
						LeadershipTermID: 1,
						ClusterSessionID: id,
						Timestamp:        ts,
						CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
					})
					m = modelMessage{kind: "close", sessionID: id}
					delete(appendSessions, id)

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
					record(modelMessage{kind: "padding"}, int(padLen))
				}

				if payload != nil {
					span := writeAgentFrame(t, image, writeAt, payload)
					record(m, span)
				}

			case 1: // advance commit (strictly increasing, capped at the log end)
				if committed < logEnd {
					committed = rapid.Int64Range(committed+1, logEnd).Draw(rt, "commitPos")
				}

			case 2: // DoWork
				runDoWork()
			}

			assertInvariants()
		}

		// Final drain: commit to log end and DoWork until model catches up.
		committed = logEnd
		for range 64 {
			if nextMsg >= len(messages) {
				break
			}
			runDoWork()
			assertInvariants()
		}
		require.Equal(rt, len(messages), nextMsg)
	})
}

type inflightAdd struct {
	sessionID     int64
	correlationID int64
}

func TestAgent_errorHandler_rapid(t *testing.T) {
	t.Parallel()
	const okStream = testResponseStreamID + 1
	const (
		opPlain = iota
		opOfferFail
		opPollFail
		opUnknownLogClose
		opControlFault
		opUnknownCloseRequest
		opTeardownAdd
		opUnknownTime
		opDispatch
		opMismatch
		opNullAction
		opClash
		opTimer
		opAckPassed
		opTick
		opCloseAgent
	)
	offerBoom := errors.New("offer boom")
	pollBoom := errors.New("poll boom")
	responseChannel := []byte(testResponseChannel)
	badSchema := encodeMessageHeader(
		t, 8, clusterwire.JoinLogSbeTemplateID, 999, 16,
	)
	shortAck := encodeRequestServiceAckControl(t, 1)
	shortAck = shortAck[:len(shortAck)-4]
	dispatchSentinels := []error{
		ErrLogDispatchHeader,
		ErrLogDispatchSchema,
		ErrLogDispatchDecode,
	}
	closedOps := []int{opTimer, opUnknownCloseRequest, opTick}
	failedOps := []int{opTimer, opUnknownCloseRequest, opTick, opCloseAgent}

	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		adder := newFakeResponseAdder()
		adder.offerErrs = map[int32]error{testResponseStreamID: offerBoom}
		proxy, _ := boundModuleProxy(t)
		closedPub := aeron.NewTestPublicationForHooksWithConnected(t, true)
		require.NoError(t, closedPub.Close())
		var got []error
		agent, _, _ := bindImage(t, AgentConfig{
			Service:              &recordingService{},
			ResponsePublications: adder,
			ModuleProxy:          proxy,
			ErrorHandler:         collectErrors(&got),
		}, 0)
		agent.role = codecs.RoleLeader
		agent.started = true

		var openIDs []int64
		var inFlight []inflightAdd
		var want []error
		var failedSentinel error
		logPos := int64(0)
		nextSession := int64(1)
		nextUnknown := int64(-1)
		closed := false

		advance := func() int64 {
			logPos += 64
			return logPos
		}
		note := func(sentinel error, fatal bool) {
			if failedSentinel != nil || closed {
				return
			}
			want = append(want, sentinel)
			if fatal {
				failedSentinel = sentinel
			}
		}
		unknownSession := func() int64 {
			id := nextUnknown
			nextUnknown--
			return id
		}
		closeSession := func(id int64) {
			pos := advance()
			agent.handleSessionClose(pos, codecs.SessionCloseEventFields{
				LeadershipTermID: 1,
				ClusterSessionID: id,
				Timestamp:        pos,
				CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
			})
		}
		dropAdd := func(i int) {
			inFlight = slices.Delete(inFlight, i, i+1)
		}
		dropSession := func(id int64) {
			i := slices.Index(openIDs, id)
			if i < 0 {
				return
			}
			openIDs = slices.Delete(openIDs, i, i+1)
		}
		openSession := func(stream int32) {
			rt.Helper()
			id := nextSession
			nextSession++
			pos := advance()
			correlationID := adder.nextID
			action := agent.handleSessionOpen(pos, codecs.SessionOpenEventFields{
				LeadershipTermID: 1,
				ClusterSessionID: id,
				Timestamp:        pos,
				ResponseStreamID: stream,
				ResponseChannel:  responseChannel,
			})
			require.Equal(rt, aeron.ControlledContinue, action)
			openIDs = append(openIDs, id)
			if stream == okStream {
				inFlight = append(inFlight, inflightAdd{
					sessionID:     id,
					correlationID: correlationID,
				})
			}
		}
		check := func() {
			rt.Helper()
			require.Len(rt, got, len(want))
			failedReports := 0
			for i, sentinel := range want {
				require.ErrorIs(rt, got[i], sentinel)
				if errors.Is(got[i], ErrFailed) {
					failedReports++
					require.ErrorIs(rt, got[i], failedSentinel)
					require.Same(rt, agent.ServiceFailure(), got[i])
				}
			}
			if failedSentinel != nil {
				require.Equal(rt, 1, failedReports)
				require.ErrorIs(rt, agent.ServiceFailure(), failedSentinel)
			} else {
				require.Zero(rt, failedReports)
				require.NoError(rt, agent.ServiceFailure())
			}
			require.Equal(rt, len(openIDs), agent.SessionCount())
			require.Equal(rt, len(inFlight), agent.PendingResponsePublicationAdds())
			require.Equal(rt, logPos, agent.LogPosition())
		}

		stepCount := rapid.IntRange(1, 40).Draw(rt, "steps")
		for range stepCount {
			var op int
			switch {
			case closed:
				op = closedOps[rapid.IntRange(0, len(closedOps)-1).Draw(rt, "closedOp")]
			case failedSentinel != nil:
				op = failedOps[rapid.IntRange(0, len(failedOps)-1).Draw(rt, "failedOp")]
			default:
				op = rapid.IntRange(opPlain, opCloseAgent).Draw(rt, "op")
			}
			switch op {
			case opPlain:
				openSession(okStream)
			case opOfferFail:
				note(ErrResponsePublicationAdd, false)
				openSession(testResponseStreamID)
			case opPollFail:
				if len(inFlight) == 0 {
					break
				}
				i := rapid.IntRange(0, len(inFlight)-1).Draw(rt, "addPoll")
				adder.next(inFlight[i].correlationID, responsePoll{err: pollBoom})
				note(ErrResponsePublicationAdd, false)
				agent.responseAddWork()
				dropAdd(i)
			case opUnknownLogClose:
				id := unknownSession()
				note(ErrUnknownSessionClose, false)
				closeSession(id)
			case opControlFault:
				switch rapid.IntRange(0, 2).Draw(rt, "controlFault") {
				case 0:
					note(ErrControlMessageSchema, false)
					agent.handleControlFragment(badSchema, 0)
				case 1:
					note(ErrControlMessageDecode, false)
					agent.handleControlFragment([]byte{1, 2, 3, 4}, 0)
				default:
					note(ErrControlMessageDecode, false)
					agent.handleControlFragment(shortAck, 0)
				}
			case opUnknownCloseRequest:
				id := unknownSession()
				note(ErrCloseUnknownSession, false)
				require.False(rt, agent.CloseClientSession(id))
			case opTeardownAdd:
				if len(inFlight) == 0 {
					break
				}
				i := rapid.IntRange(0, len(inFlight)-1).Draw(rt, "teardownAdd")
				add := inFlight[i]
				adder.next(add.correlationID, responsePoll{err: pollBoom})
				note(ErrResponsePublicationAdd, false)
				closeSession(add.sessionID)
				dropSession(add.sessionID)
				dropAdd(i)
			case opUnknownTime:
				pos := advance()
				note(ErrUnknownTimeUnit, false)
				agent.handleNewLeadershipTermEvent(pos, codecs.NewLeadershipTermEventFields{
					Timestamp: pos,
					TimeUnit:  clusterwire.ClusterTimeUnit(99),
				})
			case opDispatch:
				sentinel := rapid.SampledFrom(dispatchSentinels).Draw(rt, "dispatch")
				note(sentinel, true)
				action := agent.handleDispatchFailure(sentinel)
				require.Equal(rt, aeron.ControlledAbort, action)
			case opMismatch:
				pos := advance()
				note(ErrActionPositionMismatch, true)
				action := agent.handleClusterAction(pos, codecs.ClusterActionRequestFields{
					LogPosition: pos - 1,
					Timestamp:   pos,
					Action:      clusterwire.ClusterAction_SNAPSHOT,
				})
				require.Equal(rt, aeron.ControlledAbort, action)
			case opNullAction:
				pos := advance()
				note(ErrUnknownClusterAction, true)
				action := agent.handleClusterAction(pos, codecs.ClusterActionRequestFields{
					LogPosition: pos,
					Timestamp:   pos,
					Action:      clusterwire.ClusterAction_NULL_VALUE,
				})
				require.Equal(rt, aeron.ControlledAbort, action)
			case opClash:
				if len(openIDs) == 0 {
					break
				}
				id := rapid.SampledFrom(openIDs).Draw(rt, "clashID")
				note(ErrSessionClash, true)
				action := agent.handleSessionOpen(logPos, codecs.SessionOpenEventFields{
					LeadershipTermID: 1,
					ClusterSessionID: id,
					Timestamp:        logPos,
					ResponseStreamID: okStream,
					ResponseChannel:  responseChannel,
				})
				require.Equal(rt, aeron.ControlledAbort, action)
			case opTimer:
				proxy.SetPublication(closedPub)
				note(ErrTimerPublish, true)
				require.False(rt, agent.ScheduleTimer(1, 2))
			case opAckPassed:
				if logPos == 0 {
					break
				}
				note(ErrAckRequestPassed, true)
				agent.handleRequestServiceAck(logPos - 1)
				agent.sweepRequestAckReadiness()
			case opTick:
				if failedSentinel != nil || closed {
					require.Zero(rt, agent.DoWork(0))
					break
				}
				agent.DoWork(0)
			case opCloseAgent:
				for _, add := range inFlight {
					adder.next(add.correlationID, responsePoll{err: pollBoom})
				}
				agent.Close()
				closed = true
				inFlight = nil
			}
			check()
		}
	})
}
