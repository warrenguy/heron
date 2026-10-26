//go:build rapid && testhooks

package service

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// modelPollKind is one drawn PollAddPublication outcome.
type modelPollKind int

const (
	pollReady modelPollKind = iota
	pollTerminal
	pollNilPublication
	pollVanished
	pollPending
)

// modelClientSession is the reference state of one pool id.
type modelClientSession struct {
	open bool

	// addCorrelationID is the in-flight add, or zero.
	addCorrelationID int64

	publication *aeron.Publication

	// closing mirrors IsClosing.
	closing bool
}

// modelPublication is one publication the adder handed out.
type modelPublication struct {
	publication *aeron.Publication
	sessionID   int64
}

// pollScripter draws PollAddPublication outcomes and scripts them on adder.
type pollScripter struct {
	tb    testing.TB
	adder *fakeResponseAdder
	err   error

	// handedOut lists every publication a ready poll handed out.
	handedOut []modelPublication
}

// drawPoll draws one poll outcome for correlationID and scripts it.
// Returns the outcome and the handed-out publication, or nil.
func (s *pollScripter) drawPoll(
	rt *rapid.T,
	label string,
	correlationID, sessionID int64,
) (modelPollKind, *aeron.Publication) {
	rt.Helper()
	kind := modelPollKind(rapid.IntRange(0, int(pollPending)).Draw(rt, label))
	var result responsePoll
	var publication *aeron.Publication
	switch kind {
	case pollReady:
		publication = aeron.NewTestPublicationForHooksWithConnected(s.tb, true)
		s.handedOut = append(s.handedOut, modelPublication{
			publication: publication,
			sessionID:   sessionID,
		})
		result = responsePoll{publication: publication, done: true}
	case pollTerminal:
		result = responsePoll{done: true, err: s.err}
	case pollNilPublication:
		result = responsePoll{done: true}
	case pollVanished:
		result = responsePoll{err: s.err}
	case pollPending:
	}
	s.adder.next(correlationID, result)
	return kind, publication
}

// Asserts the client session registry and its response publications against
// a reference model:
//   - ClientSessions walks the open ids ascending. ClientSession agrees per
//     pool id.
//   - an add starts only on an open while leader. At most one add per
//     session runs. The adder in-flight set matches the model.
//   - a close cancels its in-flight add unless the close poll returns a
//     publication.
//   - a handed-out publication stays open only while its session is open
//     and Close has not run.
//   - Offer returns the model code. A positive offer frames the model header
//     and payload.
//   - SessionMessage gets a nil session exactly when the id is not open.
//     Otherwise session.ID() is the message id.
//   - IsClosing matches the model flag of every open session.
//   - the connected proxy publication holds one CloseSession frame per
//     model send, in order. A closing session does not add a frame.
//   - a close request returns true only on a send or on a closing session.
//     A close of an id that is not open returns false.
//   - no step marks a failure.
//
// Ops: open a free id, close any pool id, message any pool id, set the role,
// script one in-flight poll then DoWork, offer to an open id, request a
// close of any pool id. Close is an optional last op. Session-open clash is
// constrained out. Unit rows assert the clash path and terminal proxy codes.
func TestAgent_clientSessions_rapid(t *testing.T) {
	t.Parallel()
	pollErr := errors.New("poll add boom")

	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		svc := &recordingService{}
		adder := newFakeResponseAdder()
		proxy := NewConsensusModuleProxy()
		agent := NewAgent(AgentConfig{
			Service:              svc,
			ResponsePublications: adder,
			ModuleProxy:          proxy,
		})
		agent.started = true

		connectedProxy := aeron.NewTestPublicationForHooksWithConnected(t, true)
		proxyPublications := []*aeron.Publication{
			connectedProxy,
			aeron.NewTestPublicationForHooksWithLimit(t, 0, true),
			aeron.NewTestPublicationForHooksWithLimit(t, 0, false),
		}
		var wantCloseFrames []int64

		sessionPool := []int64{1, 2, 3, 4, 5, 6}
		model := make(map[int64]*modelClientSession, len(sessionPool))
		for _, id := range sessionPool {
			model[id] = &modelClientSession{}
		}
		scripter := &pollScripter{tb: t, adder: adder, err: pollErr}
		var wantCancels []int64
		leader := false
		agentClosed := false
		nextCorrelationID := int64(1)
		position := int64(0)

		// nextPosition advances the log position and cluster time.
		nextPosition := func() int64 {
			position += 64
			return position
		}

		// disconnect applies one close-path poll outcome to the model.
		disconnect := func(label string, id int64) {
			rt.Helper()
			state := model[id]
			if correlationID := state.addCorrelationID; correlationID != 0 {
				kind, _ := scripter.drawPoll(rt, label, correlationID, id)
				if kind != pollReady {
					wantCancels = append(wantCancels, correlationID)
				}
				state.addCorrelationID = 0
			}
			state.publication = nil
		}

		openIDs := func() []int64 {
			var ids []int64
			for _, id := range sessionPool {
				if model[id].open {
					ids = append(ids, id)
				}
			}
			return ids
		}

		inFlightIDs := func() []int64 {
			var ids []int64
			for _, id := range sessionPool {
				if model[id].addCorrelationID != 0 {
					ids = append(ids, id)
				}
			}
			return ids
		}

		assertInvariants := func() {
			rt.Helper()
			wantOpen := openIDs()
			var walked []int64
			for session := range agent.ClientSessions() {
				walked = append(walked, session.ID())
			}
			require.Equal(rt, wantOpen, walked)
			for _, id := range sessionPool {
				session, ok := agent.ClientSession(id)
				require.Equal(rt, model[id].open, ok)
				if ok {
					require.Equal(rt, id, session.ID())
					require.Equal(rt, model[id].closing, session.IsClosing())
				}
			}

			wantInFlight := make(map[int64]bool)
			for _, id := range inFlightIDs() {
				wantInFlight[model[id].addCorrelationID] = true
			}
			require.Equal(rt, wantInFlight, adder.inFlight)
			require.Equal(rt, len(wantInFlight), agent.PendingResponsePublicationAdds())
			require.Len(rt, adder.offers, int(nextCorrelationID-1))
			require.Equal(rt, wantCancels, adder.cancels)

			for _, handed := range scripter.handedOut {
				state := model[handed.sessionID]
				wantOpen := !agentClosed && state.open && state.publication == handed.publication
				require.Equal(rt, !wantOpen, handed.publication.IsClosed())
			}
			require.Equal(rt, wantCloseFrames, closeFrames(rt, connectedProxy))
			require.NoError(rt, agent.ServiceFailure())
		}

		stepCount := rapid.IntRange(1, 40).Draw(rt, "stepCount")
		for range stepCount {
			switch rapid.IntRange(0, 6).Draw(rt, "op") {
			case 0:
				var free []int64
				for _, id := range sessionPool {
					if !model[id].open {
						free = append(free, id)
					}
				}
				if len(free) == 0 {
					continue
				}
				id := rapid.SampledFrom(free).Draw(rt, "openID")
				at := nextPosition()
				agent.handleSessionOpen(at, codecs.SessionOpenEventFields{
					LeadershipTermID: 1,
					ClusterSessionID: id,
					Timestamp:        at,
					ResponseStreamID: testResponseStreamID,
					ResponseChannel:  []byte(testResponseChannel),
				})
				model[id].open = true
				model[id].closing = false
				if leader {
					model[id].addCorrelationID = nextCorrelationID
					nextCorrelationID++
				}

			case 1:
				id := rapid.SampledFrom(sessionPool).Draw(rt, "closeID")
				if model[id].open {
					disconnect("closePoll", id)
					model[id].open = false
				}
				at := nextPosition()
				agent.handleSessionClose(at, codecs.SessionCloseEventFields{
					LeadershipTermID: 1,
					ClusterSessionID: id,
					Timestamp:        at,
					CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
				})

			case 2:
				id := rapid.SampledFrom(sessionPool).Draw(rt, "messageID")
				at := nextPosition()
				before := len(svc.messages)
				agent.handleSessionMessage(at, id, at, []byte("m"))
				require.Len(rt, svc.messages, before+1)
				session := svc.messages[before].session
				require.Equal(rt, !model[id].open, session == nil)
				if session != nil {
					require.Equal(rt, id, session.ID())
					open, ok := agent.ClientSession(id)
					require.True(rt, ok)
					require.Same(rt, open, session)
				}

			case 3:
				leader = rapid.Bool().Draw(rt, "leader")
				agent.role = codecs.RoleFollower
				if leader {
					agent.role = codecs.RoleLeader
				}

			case 4:
				adding := inFlightIDs()
				if len(adding) == 0 {
					continue
				}
				id := rapid.SampledFrom(adding).Draw(rt, "pollID")
				state := model[id]
				kind, publication := scripter.drawPoll(rt, "tickPoll", state.addCorrelationID, id)
				wantWork := len(adding) - 1
				switch kind {
				case pollReady:
					state.publication = publication
					state.addCorrelationID = 0
					wantWork++
				case pollPending:
					wantWork++
				default:
					state.addCorrelationID = 0
				}
				require.Equal(rt, wantWork, agent.DoWork(0))

			case 5:
				open := openIDs()
				if len(open) == 0 {
					continue
				}
				id := rapid.SampledFrom(open).Draw(rt, "offerID")
				state := model[id]
				headerLength := codecs.SessionMessageHeaderEncodedLen()
				oversize := state.publication != nil && rapid.Bool().Draw(rt, "oversize")
				payloadLength := rapid.IntRange(0, 256).Draw(rt, "payloadLength")
				if oversize {
					payloadLength += state.publication.MaxMessageLength() - headerLength + 1
				}
				payload := bytes.Repeat([]byte("egress!"), payloadLength/7+1)[:payloadLength]

				var start int64
				if state.publication != nil {
					start = state.publication.Position()
				}
				session, ok := agent.ClientSession(id)
				require.True(rt, ok)
				result := session.Offer(payload)
				switch {
				case !leader:
					require.Equal(rt, MockedOffer, result)
				case state.publication == nil:
					require.Equal(rt, aeron.NotConnected, result)
				case oversize:
					require.Equal(rt, aeron.UnsupportedMessageLength, result)
				default:
					require.Positive(rt, result)
					requireEgressFrame(rt, state.publication, start, codecs.SessionMessageHeaderFields{
						LeadershipTermID: nullValue,
						ClusterSessionID: id,
						Timestamp:        position,
					}, payload)
				}

			case 6:
				id := rapid.SampledFrom(sessionPool).Draw(rt, "closeRequestID")
				index := rapid.IntRange(0, len(proxyPublications)-1).Draw(rt, "proxyPublication")
				proxy.SetPublication(proxyPublications[index])
				state := model[id]
				var sent bool
				if state.open && rapid.Bool().Draw(rt, "sessionClose") {
					session, ok := agent.ClientSession(id)
					require.True(rt, ok)
					sent = session.Close()
				} else {
					sent = agent.CloseClientSession(id)
				}
				switch {
				case !state.open:
					require.False(rt, sent)
				case state.closing:
					require.True(rt, sent)
				case proxyPublications[index] == connectedProxy:
					require.True(rt, sent)
					state.closing = true
					wantCloseFrames = append(wantCloseFrames, id)
				default:
					require.False(rt, sent)
				}
				proxy.SetPublication(connectedProxy)
			}
			assertInvariants()
		}

		if rapid.Bool().Draw(rt, "close") {
			for _, id := range inFlightIDs() {
				disconnect("closeAgentPoll", id)
			}
			agent.Close()
			agentClosed = true
			assertInvariants()
		}
	})
}

// closeFrames returns the CloseSession session ids on publication in stream
// order.
func closeFrames(rt *rapid.T, publication *aeron.Publication) []int64 {
	rt.Helper()
	var ids []int64
	for _, fields := range decodeFramesFromPub(rt, publication, codecs.TryDecodeCloseSessionAt) {
		ids = append(ids, fields.ClusterSessionID)
	}
	return ids
}

// modelJoin is one delivered JoinLog the model has not bound.
type modelJoin struct {
	role      codecs.Role
	isStartup bool
	position  int64
}

// Asserts response publications across real joins and unbinds against a
// reference model:
//   - while unbound or not leader, every handed-out publication is closed.
//     The adder holds zero in-flight adds.
//   - an add starts only on a leader SessionOpen or on the bind of a leader
//     join with isStartup false. The adder offer count matches the model.
//   - PendingResponsePublicationAdds and ResponsePublicationCount match the
//     model.
//   - Offer returns the model code. A positive offer frames the model term,
//     the session id and the payload.
//   - PendingCoordinationAcks equals the delivered join count. Role matches
//     the model. ServiceFailure stays nil at every step.
//
// Ops: deliver a join with a drawn role and isStartup, script each in-flight
// poll then DoWork, close the bound image, open a free id, close or message
// any pool id, dispatch a NewLeadershipTermEvent, offer to an open id. The
// log handlers run only while bound with no pending join. Close is an
// optional last op.
func TestAgent_egressRejoin_rapid(t *testing.T) {
	t.Parallel()
	pollErr := errors.New("poll add boom")
	roles := []codecs.Role{codecs.RoleLeader, codecs.RoleFollower, codecs.RoleCandidate}

	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		adder := newFakeResponseAdder()
		agent := NewAgent(AgentConfig{
			CommittedPosition:    func() int64 { return math.MaxInt64 },
			ResponsePublications: adder,
		})

		sessionPool := []int64{1, 2, 3, 4, 5, 6}
		model := make(map[int64]*modelClientSession, len(sessionPool))
		for _, id := range sessionPool {
			model[id] = &modelClientSession{}
		}
		scripter := &pollScripter{tb: t, adder: adder, err: pollErr}
		var wantCancels []int64
		var pending *modelJoin
		role := codecs.RoleFollower
		bound := false
		imageClosed := false
		imageJoinPosition := int64(0)
		maxLogPosition := int64(0)
		joins := 0
		streamID := int32(200)
		term := nullValue
		nextCorrelationID := int64(1)
		position := int64(0)
		agentClosed := false

		nextPosition := func() int64 {
			position += 64
			return position
		}

		openIDs := func() []int64 {
			var ids []int64
			for _, id := range sessionPool {
				if model[id].open {
					ids = append(ids, id)
				}
			}
			return ids
		}

		inFlightIDs := func() []int64 {
			var ids []int64
			for _, id := range sessionPool {
				if model[id].addCorrelationID != 0 {
					ids = append(ids, id)
				}
			}
			return ids
		}

		// connect starts the model add of id unless it holds one.
		connect := func(id int64) {
			state := model[id]
			if state.publication != nil || state.addCorrelationID != 0 {
				return
			}
			state.addCorrelationID = nextCorrelationID
			nextCorrelationID++
		}

		// disconnect ends the model add of id with an unscripted poll, then
		// drops its publication.
		disconnect := func(id int64) {
			state := model[id]
			if state.addCorrelationID != 0 {
				wantCancels = append(wantCancels, state.addCorrelationID)
				state.addCorrelationID = 0
			}
			state.publication = nil
		}

		// disconnectAll runs disconnect on every open id in id order.
		disconnectAll := func() {
			for _, id := range openIDs() {
				disconnect(id)
			}
		}

		assertInvariants := func() {
			rt.Helper()
			wantInFlight := make(map[int64]bool)
			wantPublications := 0
			for _, id := range openIDs() {
				state := model[id]
				if state.addCorrelationID != 0 {
					wantInFlight[state.addCorrelationID] = true
				}
				if state.publication != nil {
					wantPublications++
				}
			}
			require.Equal(rt, wantInFlight, adder.inFlight)
			require.Equal(rt, len(wantInFlight), agent.PendingResponsePublicationAdds())
			require.Equal(rt, wantPublications, agent.ResponsePublicationCount())
			require.Len(rt, adder.offers, int(nextCorrelationID-1))
			require.Equal(rt, wantCancels, adder.cancels)

			egressAllowed := agent.IsLogBound() && agent.Role() == codecs.RoleLeader
			for _, handed := range scripter.handedOut {
				state := model[handed.sessionID]
				wantOpen := !agentClosed && state.open && state.publication == handed.publication
				require.Equal(rt, !wantOpen, handed.publication.IsClosed())
				if !egressAllowed {
					require.True(rt, handed.publication.IsClosed())
				}
			}
			if !egressAllowed {
				require.Empty(rt, adder.inFlight)
				require.Zero(rt, agent.PendingResponsePublicationAdds())
			}

			require.Equal(rt, !agentClosed && bound, agent.IsLogBound())
			require.Equal(rt, pending != nil, agent.HasPendingJoin())
			require.Equal(rt, role, agent.Role())
			require.Equal(rt, joins, agent.PendingCoordinationAcks())
			require.Equal(rt, position, agent.LogPosition())
			require.Equal(rt, len(openIDs()), agent.SessionCount())
			require.NoError(rt, agent.ServiceFailure())
		}

		handlersAllowed := func() bool {
			return bound && pending == nil
		}

		stepCount := rapid.IntRange(1, 60).Draw(rt, "stepCount")
		for range stepCount {
			switch rapid.IntRange(0, 7).Draw(rt, "op") {
			case 0:
				join := &modelJoin{
					role:      rapid.SampledFrom(roles).Draw(rt, "joinRole"),
					isStartup: rapid.Bool().Draw(rt, "isStartup"),
					position:  position,
				}
				streamID++
				replaceLogSubscription(t, agent, testLogSessionID, streamID, join.position)
				event := defaultJoinEvent(join.position)
				event.logStreamID = streamID
				event.role = join.role
				event.isStartup = join.isStartup
				agent.handleJoinLog(event)
				joins++
				if bound {
					maxLogPosition = join.position
				}
				pending = join

			case 1:
				outcomes := make(map[int64]modelPollKind)
				publications := make(map[int64]*aeron.Publication)
				for _, id := range inFlightIDs() {
					kind, publication := scripter.drawPoll(rt, "tickPoll", model[id].addCorrelationID, id)
					outcomes[id] = kind
					publications[id] = publication
				}
				agent.DoWork(0)

				for id, kind := range outcomes {
					state := model[id]
					switch kind {
					case pollReady:
						state.publication = publications[id]
						state.addCorrelationID = 0
					case pollPending:
					default:
						state.addCorrelationID = 0
					}
				}

				if pending != nil && !bound {
					bound = true
					role = pending.role
					imageJoinPosition = pending.position
					maxLogPosition = math.MaxInt64
					imageClosed = false
					switch {
					case role != codecs.RoleLeader:
						disconnectAll()
					case !pending.isStartup:
						for _, id := range openIDs() {
							connect(id)
						}
					}
					pending = nil
				}
				if bound && (imageClosed || imageJoinPosition >= maxLogPosition) {
					disconnectAll()
					bound = false
					role = codecs.RoleFollower
				}
			case 2:
				if !bound {
					continue
				}
				boundImage := agent.adapter.Image()
				require.NotNil(rt, boundImage)
				aeron.ImageInternalCloseForHooks(boundImage)
				imageClosed = true

			case 3:
				if !handlersAllowed() {
					continue
				}
				var free []int64
				for _, id := range sessionPool {
					if !model[id].open {
						free = append(free, id)
					}
				}
				if len(free) == 0 {
					continue
				}
				id := rapid.SampledFrom(free).Draw(rt, "openID")
				at := nextPosition()
				agent.handleSessionOpen(at, codecs.SessionOpenEventFields{
					LeadershipTermID: 1,
					ClusterSessionID: id,
					Timestamp:        at,
					ResponseStreamID: testResponseStreamID,
					ResponseChannel:  []byte(testResponseChannel),
				})
				model[id].open = true
				if role == codecs.RoleLeader {
					connect(id)
				}

			case 4:
				if !handlersAllowed() {
					continue
				}
				id := rapid.SampledFrom(sessionPool).Draw(rt, "closeID")
				state := model[id]
				if state.open {
					if correlationID := state.addCorrelationID; correlationID != 0 {
						kind, _ := scripter.drawPoll(rt, "closePoll", correlationID, id)
						if kind != pollReady {
							wantCancels = append(wantCancels, correlationID)
						}
						state.addCorrelationID = 0
					}
					state.publication = nil
					state.open = false
				}
				at := nextPosition()
				agent.handleSessionClose(at, codecs.SessionCloseEventFields{
					LeadershipTermID: 1,
					ClusterSessionID: id,
					Timestamp:        at,
					CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
				})

			case 5:
				if !handlersAllowed() {
					continue
				}
				id := rapid.SampledFrom(sessionPool).Draw(rt, "messageID")
				at := nextPosition()
				agent.handleSessionMessage(at, id, at, []byte("m"))

			case 6:
				if !handlersAllowed() {
					continue
				}
				term = rapid.Int64Range(0, 20).Draw(rt, "term")
				at := nextPosition()
				agent.handleNewLeadershipTermEvent(at, codecs.NewLeadershipTermEventFields{
					LeadershipTermID: term,
					LogPosition:      at,
					Timestamp:        at,
				})

			case 7:
				open := openIDs()
				if len(open) == 0 {
					continue
				}
				id := rapid.SampledFrom(open).Draw(rt, "offerID")
				state := model[id]
				payloadLength := rapid.IntRange(0, 64).Draw(rt, "payloadLength")
				payload := bytes.Repeat([]byte("rejoin!"), payloadLength/7+1)[:payloadLength]
				var start int64
				if state.publication != nil {
					start = state.publication.Position()
				}
				session, ok := agent.ClientSession(id)
				require.True(rt, ok)
				result := session.Offer(payload)
				switch {
				case role != codecs.RoleLeader:
					require.Equal(rt, MockedOffer, result)
				case state.publication == nil:
					require.Equal(rt, aeron.NotConnected, result)
				default:
					require.Positive(rt, result)
					requireEgressFrame(rt, state.publication, start, codecs.SessionMessageHeaderFields{
						LeadershipTermID: term,
						ClusterSessionID: id,
						Timestamp:        position,
					}, payload)
				}
			}
			assertInvariants()
		}

		if rapid.Bool().Draw(rt, "close") {
			disconnectAll()
			agent.Close()
			agentClosed = true
			assertInvariants()
		}
	})
}
