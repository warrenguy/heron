//go:build testhooks

package service

import (
	"errors"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestAgent_SessionRegistry(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	openFields := codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		CorrelationID:    2,
		ClusterSessionID: 100,
		Timestamp:        50,
		ResponseStreamID: 102,
		ResponseChannel:  []byte("aeron:ipc"),
		EncodedPrincipal: []byte{0x01},
	}
	openPayload := encodeSessionOpen(t, openFields)
	openSpan := writeAgentFrame(t, image, 0, openPayload)
	*committed = int64(openSpan)

	require.Equal(t, 1, agent.DoWork(0))
	require.Equal(t, 1, agent.SessionCount())
	require.True(t, agent.HasSession(100))
	require.Len(t, svc.opens, 1)
	require.Equal(t, int64(100), svc.opens[0].session.ID())
	require.Equal(t, int64(50), agent.ClusterTime())
	require.Equal(t, int64(openSpan), agent.LogPosition())

	session, ok := agent.ClientSession(100)
	require.True(t, ok)
	require.Same(t, session, svc.opens[0].session)
	require.Equal(t, int64(100), session.ID())
	require.Equal(t, int32(102), session.ResponseStreamID())
	require.Equal(t, "aeron:ipc", session.ResponseChannel())
	require.Equal(t, []byte{0x01}, session.EncodedPrincipal())

	// Opens for 9, 2 and 5 arrive after 100. The walk sorts by id. Upstream
	// walks in open order.
	offset := openSpan
	for _, id := range []int64{9, 2, 5} {
		fields := openFields
		fields.ClusterSessionID = id
		offset += writeAgentFrame(t, image, offset, encodeSessionOpen(t, fields))
	}
	*committed = int64(offset)
	require.Equal(t, 3, agent.DoWork(0))
	require.NoError(t, agent.ServiceFailure())

	var walked []int64
	for session := range agent.ClientSessions() {
		walked = append(walked, session.ID())
	}
	require.Equal(t, []int64{2, 5, 9, 100}, walked)

	// An early break stops the walk.
	walked = walked[:0]
	for session := range agent.ClientSessions() {
		walked = append(walked, session.ID())
		if len(walked) == 2 {
			break
		}
	}
	require.Equal(t, []int64{2, 5}, walked)

	session, ok = agent.ClientSession(404)
	require.False(t, ok)
	require.Nil(t, session)

	// Clash: adopt the frame, report, and break. The callback does not run again.
	var got []error
	agent.errorHandler = collectErrors(&got)
	clashFields := openFields
	clashFields.Timestamp = 51
	clashPayload := encodeSessionOpen(t, clashFields)
	clashSpan := writeAgentFrame(t, image, offset, clashPayload)
	*committed = int64(offset + clashSpan)

	require.Equal(t, 1, agent.DoWork(0))
	require.NoError(t, agent.ServiceFailure())
	requireOneNonFatalReport(t, got, ErrSessionClash)
	require.Len(t, svc.opens, 4)
	require.Equal(t, 4, agent.SessionCount())
	require.Equal(t, int64(offset+clashSpan), agent.LogPosition())
	require.Equal(t, int64(51), agent.ClusterTime())
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
}

func TestAgent_SessionClose_knownAndUnknown(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	openPayload := encodeSessionOpen(t, codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 7,
		Timestamp:        1,
		ResponseChannel:  []byte("c"),
	})
	openSpan := writeAgentFrame(t, image, 0, openPayload)

	closeKnown := encodeSessionClose(t, codecs.SessionCloseEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 7,
		Timestamp:        2,
		CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
	})
	closeSpan := writeAgentFrame(t, image, openSpan, closeKnown)

	closeUnknown := encodeSessionClose(t, codecs.SessionCloseEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 999,
		Timestamp:        3,
		CloseReason:      clusterwire.CloseReason_TIMEOUT,
	})
	unkSpan := writeAgentFrame(t, image, openSpan+closeSpan, closeUnknown)

	*committed = int64(openSpan + closeSpan + unkSpan)
	require.Equal(t, 3, agent.DoWork(0))
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, 0, agent.SessionCount())
	require.False(t, agent.HasSession(7))
	require.Len(t, svc.closes, 1)
	require.Equal(t, int64(7), svc.closes[0].session.ID())
	require.Equal(t, clusterwire.CloseReason_CLIENT_ACTION, svc.closes[0].reason)
	require.Equal(t, int64(3), agent.ClusterTime())

	egressTests := []struct {
		name string

		// setup scripts the add of session 7 (correlation id 1) after the
		// open. Returns the publication the close must close, or nil. Nil
		// leaves the add in flight.
		setup func(t *testing.T, log *sessionLog, adder *fakeResponseAdder) *aeron.Publication

		wantPolls   []int64
		wantCancels []int64
	}{
		{
			name: "held publication",
			setup: func(t *testing.T, log *sessionLog, adder *fakeResponseAdder) *aeron.Publication {
				pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
				adder.next(1, responsePoll{publication: pub, done: true})
				log.agent.DoWork(0)
				require.Equal(t, 0, log.agent.PendingResponsePublicationAdds())
				return pub
			},
			wantPolls: []int64{1},
		},
		{
			// The tick poll of the close DoWork stays pending. The close
			// polls once more, then cancels.
			name:        "add in flight stays pending",
			wantPolls:   []int64{1, 1},
			wantCancels: []int64{1},
		},
		{
			// The tick poll stays pending. The close poll returns the ready
			// publication, as after a conductor ready mark.
			name: "add in flight ready at close",
			setup: func(t *testing.T, _ *sessionLog, adder *fakeResponseAdder) *aeron.Publication {
				pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
				adder.next(1, responsePoll{})
				adder.next(1, responsePoll{publication: pub, done: true})
				return pub
			},
			wantPolls: []int64{1, 1},
		},
	}
	for _, tc := range egressTests {
		t.Run(tc.name, func(t *testing.T) {
			adder := newFakeResponseAdder()
			log, svc := bindSessionAgent(t, adder, codecs.RoleLeader)
			log.open(7, 1)
			var pub *aeron.Publication
			if tc.setup != nil {
				pub = tc.setup(t, log, adder)
			}
			session, ok := log.agent.ClientSession(7)
			require.True(t, ok)

			log.close(7, 2)
			require.Len(t, svc.closes, 1)
			require.Same(t, session, svc.closes[0].session)
			_, ok = log.agent.ClientSession(7)
			require.False(t, ok)
			require.Equal(t, aeron.NotConnected, session.Offer([]byte("late")))
			require.Equal(t, 0, log.agent.PendingResponsePublicationAdds())
			require.Empty(t, adder.inFlight)
			require.Equal(t, tc.wantPolls, adder.polls)
			require.Equal(t, tc.wantCancels, adder.cancels)
			if pub != nil {
				require.True(t, pub.IsClosed())
			}
			require.NoError(t, log.agent.ServiceFailure())
		})
	}
}

func TestAgent_SessionMessage_unknownSession(t *testing.T) {
	svc := &recordingService{}
	agent, image, committed := bindAgent(t, svc, 0)

	payload := encodeSessionMessage(
		t,
		codecs.SessionMessageHeaderFields{
			LeadershipTermID: 1,
			ClusterSessionID: 404,
			Timestamp:        9,
		},
		[]byte("body"),
	)
	span := writeAgentFrame(t, image, 0, payload)
	*committed = int64(span)

	require.Equal(t, 1, agent.DoWork(0))
	require.Len(t, svc.messages, 1)
	require.Equal(t, []byte("body"), svc.messages[0].payload)
	// require.Nil accepts a typed nil. Equal does not.
	require.Equal(t, ClientSession(nil), svc.messages[0].session)
	require.Equal(t, 0, agent.SessionCount())
}

func TestAgent_ClientSession_openAsLeader(t *testing.T) {
	adder := newFakeResponseAdder()
	log, svc := bindSessionAgent(t, adder, codecs.RoleLeader)
	agent := log.agent

	// Phase 1: the open starts one add. The session holds no publication.
	log.open(7, 50)
	require.Len(t, svc.opens, 1)
	require.Equal(t, []transportAdd{{
		streamID: testResponseStreamID,
		channel:  testResponseChannel,
	}}, adder.offers)
	require.Equal(t, 1, agent.PendingResponsePublicationAdds())
	session, ok := agent.ClientSession(7)
	require.True(t, ok)
	require.Same(t, session, svc.opens[0].session)
	require.Equal(t, int64(7), session.ID())
	require.Equal(t, testResponseStreamID, session.ResponseStreamID())
	require.Equal(t, testResponseChannel, session.ResponseChannel())
	require.Empty(t, session.EncodedPrincipal())
	require.Equal(t, aeron.NotConnected, session.Offer([]byte("early")))

	// Phase 2: one DoWork resolves the add.
	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	adder.next(1, responsePoll{publication: pub, done: true})
	require.Equal(t, 1, agent.DoWork(0))
	require.Equal(t, 0, agent.PendingResponsePublicationAdds())

	// Phase 3: the offer frames the payload behind one SessionMessageHeader.
	payload := []byte("echo")
	require.Positive(t, session.Offer(payload))
	requireEgressFrame(t, pub, 0, codecs.SessionMessageHeaderFields{
		LeadershipTermID: nullValue,
		ClusterSessionID: 7,
		Timestamp:        agent.ClusterTime(),
	}, payload)
	require.Equal(t, int64(50), agent.ClusterTime())
	require.NoError(t, agent.ServiceFailure())
}

func TestAgent_ClientSession_openAsFollower(t *testing.T) {
	adder := newFakeResponseAdder()
	log, svc := bindSessionAgent(t, adder, codecs.RoleFollower)

	log.open(7, 50)
	require.Len(t, svc.opens, 1)
	require.Empty(t, adder.offers)
	require.Equal(t, 0, log.agent.PendingResponsePublicationAdds())
	session, ok := log.agent.ClientSession(7)
	require.True(t, ok)
	require.Equal(t, MockedOffer, session.Offer([]byte("m")))
	require.NoError(t, log.agent.ServiceFailure())
}

// TestAgent_ClientSession_offerCodes asserts each Offer result. No code
// marks a failure or closes the session.
func TestAgent_ClientSession_offerCodes(t *testing.T) {
	addErr := errors.New("add publication boom")

	tests := []struct {
		name string
		role codecs.Role

		// configure returns the adder the agent gets. Nil passes the fake.
		configure func(adder *fakeResponseAdder) ResponsePublicationAdder

		// poll scripts the next poll of add 1, then runs one DoWork. Nil
		// leaves the add in flight.
		poll func(t *testing.T) responsePoll

		closePublication bool
		oversize         bool
		wantPending      int
		want             int64
	}{
		{name: "follower", role: codecs.RoleFollower, want: MockedOffer},
		{
			name:      "leader with no adder",
			role:      codecs.RoleLeader,
			configure: func(*fakeResponseAdder) ResponsePublicationAdder { return nil },
			want:      aeron.NotConnected,
		},
		{
			name: "leader with a typed-nil adder",
			role: codecs.RoleLeader,
			configure: func(*fakeResponseAdder) ResponsePublicationAdder {
				return (*fakeResponseAdder)(nil)
			},
			want: aeron.NotConnected,
		},
		{
			name: "add offer error",
			role: codecs.RoleLeader,
			configure: func(adder *fakeResponseAdder) ResponsePublicationAdder {
				adder.offerErrs = map[int32]error{testResponseStreamID: addErr}
				return adder
			},
			want: aeron.NotConnected,
		},
		{name: "add in flight", role: codecs.RoleLeader, wantPending: 1, want: aeron.NotConnected},
		{
			name: "terminal add",
			role: codecs.RoleLeader,
			poll: func(*testing.T) responsePoll {
				return responsePoll{done: true, err: addErr}
			},
			want: aeron.NotConnected,
		},
		{
			name: "vanished add",
			role: codecs.RoleLeader,
			poll: func(*testing.T) responsePoll { return responsePoll{err: addErr} },
			want: aeron.NotConnected,
		},
		{
			name: "nil publication from a done poll",
			role: codecs.RoleLeader,
			poll: func(*testing.T) responsePoll { return responsePoll{done: true} },
			want: aeron.NotConnected,
		},
		{
			name: "publication not connected",
			role: codecs.RoleLeader,
			poll: func(t *testing.T) responsePoll {
				pub := aeron.NewTestPublicationForHooksWithLimit(t, 0, false)
				return responsePoll{publication: pub, done: true}
			},
			want: aeron.NotConnected,
		},
		{
			name: "back-pressured publication",
			role: codecs.RoleLeader,
			poll: func(t *testing.T) responsePoll {
				pub := aeron.NewTestPublicationForHooksWithLimit(t, 0, true)
				return responsePoll{publication: pub, done: true}
			},
			want: aeron.BackPressured,
		},
		{
			name: "oversize",
			role: codecs.RoleLeader,
			poll: func(t *testing.T) responsePoll {
				pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
				return responsePoll{publication: pub, done: true}
			},
			oversize: true,
			want:     aeron.UnsupportedMessageLength,
		},
		{
			name: "closed publication",
			role: codecs.RoleLeader,
			poll: func(t *testing.T) responsePoll {
				pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
				return responsePoll{publication: pub, done: true}
			},
			closePublication: true,
			want:             aeron.Closed,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adder := newFakeResponseAdder()
			var configured ResponsePublicationAdder = adder
			if tc.configure != nil {
				configured = tc.configure(adder)
			}
			log, _ := bindSessionAgent(t, configured, tc.role)
			log.open(7, 1)
			if tc.poll != nil {
				adder.next(1, tc.poll(t))
				log.agent.DoWork(0)
			}
			state := log.agent.sessions[7]
			if tc.closePublication {
				require.NoError(t, state.publication.Close())
			}
			payload := []byte("m")
			if tc.oversize {
				headerLength := codecs.SessionMessageHeaderEncodedLen()
				payload = make([]byte, state.publication.MaxMessageLength()-headerLength+1)
			}

			session, ok := log.agent.ClientSession(7)
			require.True(t, ok)
			require.Equal(t, tc.want, session.Offer(payload))
			require.Equal(t, tc.wantPending, log.agent.PendingResponsePublicationAdds())
			require.Len(t, adder.inFlight, tc.wantPending)
			require.NoError(t, log.agent.ServiceFailure())
			require.True(t, log.agent.HasSession(7))
		})
	}
}

func TestAgent_ClientSession_connectIdempotent(t *testing.T) {
	adder := newFakeResponseAdder()
	log, _ := bindSessionAgent(t, adder, codecs.RoleLeader)
	agent := log.agent
	log.open(7, 1)
	session := agent.sessions[7]
	require.Len(t, adder.offers, 1)

	// An add in flight starts no second add.
	agent.connectClientSession(session)
	require.Len(t, adder.offers, 1)
	require.Equal(t, 1, agent.PendingResponsePublicationAdds())

	// A held publication starts no add.
	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	adder.next(1, responsePoll{publication: pub, done: true})
	require.Equal(t, 1, agent.DoWork(0))
	agent.connectClientSession(session)
	require.Len(t, adder.offers, 1)
	require.Equal(t, 0, agent.PendingResponsePublicationAdds())
	require.Same(t, pub, session.publication)
}

func TestAgent_ClientSessions_zeroAllocs(t *testing.T) {
	log, _ := bindSessionAgent(t, nil, codecs.RoleFollower)
	for _, id := range []int64{3, 1, 2} {
		log.open(id, 1)
	}
	agent := log.agent

	var sequence iter.Seq[ClientSession]
	testutil.AssertZeroAllocs(t, "ClientSessions", 100, func() {
		sequence = agent.ClientSessions()
	})

	walked := 0
	yield := func(ClientSession) bool {
		walked++
		return true
	}
	testutil.AssertZeroAllocs(t, "ClientSessions stored yield walk", 100, func() {
		sequence(yield)
	})
	require.Positive(t, walked)
}

func TestAgent_ClientSession_offerZeroAllocs(t *testing.T) {
	adder := newFakeResponseAdder()
	log, _ := bindSessionAgent(t, adder, codecs.RoleLeader)
	log.open(7, 1)
	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	adder.next(1, responsePoll{publication: pub, done: true})
	log.agent.DoWork(0)
	session, ok := log.agent.ClientSession(7)
	require.True(t, ok)

	payload := []byte("zero-alloc")
	require.Positive(t, session.Offer(payload))
	testutil.AssertZeroAllocs(t, "ClientSession.Offer", 100, func() {
		if session.Offer(payload) <= 0 {
			t.Fatal("offer rejected")
		}
	})
}

func TestAgent_ClientSession_closeAfterSessionClose(t *testing.T) {
	log, svc, pub := bindCloseAgent(t, nil, codecs.RoleLeader)
	agent := log.agent
	log.open(7, 1)
	require.True(t, agent.CloseClientSession(7))
	sent := pub.Position()

	log.closeWithReason(7, 5, clusterwire.CloseReason_SERVICE_ACTION)
	require.Len(t, svc.closes, 1)
	require.Equal(t, clusterwire.CloseReason_SERVICE_ACTION, svc.closes[0].reason)
	require.Equal(t, int64(5), svc.closes[0].timestamp)

	session := svc.closes[0].session
	require.True(t, session.IsClosing())
	require.False(t, session.Close())
	require.Equal(t, sent, pub.Position())
	require.NoError(t, agent.ServiceFailure())
}

func TestAgent_ClientSession_offerWhileClosing(t *testing.T) {
	adder := newFakeResponseAdder()
	log, _, _ := bindCloseAgent(t, adder, codecs.RoleLeader)
	agent := log.agent
	log.open(7, 1)
	response := aeron.NewTestPublicationForHooksWithConnected(t, true)
	adder.next(1, responsePoll{publication: response, done: true})
	require.Equal(t, 1, agent.DoWork(0))

	session, ok := agent.ClientSession(7)
	require.True(t, ok)
	require.True(t, session.Close())
	require.True(t, session.IsClosing())

	payload := []byte("closing")
	require.Positive(t, session.Offer(payload))
	requireEgressFrame(t, response, 0, codecs.SessionMessageHeaderFields{
		LeadershipTermID: nullValue,
		ClusterSessionID: 7,
		Timestamp:        agent.ClusterTime(),
	}, payload)
	require.NoError(t, agent.ServiceFailure())
}

func TestAgent_CloseClientSession(t *testing.T) {
	for _, role := range []codecs.Role{codecs.RoleLeader, codecs.RoleFollower} {
		t.Run(role.String(), func(t *testing.T) {
			log, _, pub := bindCloseAgent(t, nil, role)
			agent := log.agent
			fields := sampleCloseSessionFields()
			log.open(fields.ClusterSessionID, 1)
			session, ok := agent.ClientSession(fields.ClusterSessionID)
			require.True(t, ok)
			require.False(t, session.IsClosing())
			require.False(t, agent.CloseClientSession(99))

			// Phase 1: the first call sends one CloseSession frame. The
			// unknown id above sent none.
			require.True(t, agent.CloseClientSession(fields.ClusterSessionID))
			requireDecodedFrame(
				t,
				pub,
				codecs.CloseSessionEncodedLen(),
				codecs.TryDecodeCloseSessionAt,
				fields,
			)
			require.True(t, session.IsClosing())
			sent := pub.Position()

			// Phase 2: a second call and Close do not send a frame.
			require.True(t, agent.CloseClientSession(fields.ClusterSessionID))
			require.True(t, session.Close())
			require.Equal(t, sent, pub.Position())
			require.True(t, session.IsClosing())
			require.True(t, agent.HasSession(fields.ClusterSessionID))
			require.NoError(t, agent.ServiceFailure())
		})
	}
}

func TestAgent_CloseClientSession_failurePaths(t *testing.T) {
	const openID int64 = 7
	for _, tc := range moduleProxyFailureCases(ErrCloseSessionPublish) {
		t.Run(tc.name, func(t *testing.T) {
			log, _ := bindSessionAgent(t, nil, codecs.RoleLeader)
			agent := log.agent
			log.open(openID, 1)
			agent.moduleProxy = tc.setup(t)

			var got []error
			agent.errorHandler = collectErrors(&got)
			require.False(t, agent.CloseClientSession(openID))
			session, ok := agent.ClientSession(openID)
			require.True(t, ok)
			require.False(t, session.IsClosing())
			require.NoError(t, agent.ServiceFailure())
			if tc.reported == nil {
				require.Empty(t, got)
				return
			}
			requireOneNonFatalReport(t, got, tc.reported)
			require.False(t, session.Close())
			require.Len(t, got, 2)
			require.NoError(t, agent.ServiceFailure())
		})
	}
}

func TestAgent_SessionOpen_fatalResponseAddStopsPoll(t *testing.T) {
	adder := newFakeResponseAdder()
	adder.offerErrs = map[int32]error{testResponseStreamID: aeron.ErrClientClosed}
	log, svc := bindSessionAgent(t, adder, codecs.RoleLeader)
	var got []error
	log.agent.errorHandler = collectErrors(&got)
	fields := codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 1,
		Timestamp:        50,
		ResponseStreamID: testResponseStreamID,
		ResponseChannel:  []byte(testResponseChannel),
	}
	first := log.write(t, encodeSessionOpen(t, fields))
	fields.ClusterSessionID, fields.Timestamp = 2, 80
	log.write(t, encodeSessionOpen(t, fields))

	log.agent.DoWork(0)
	require.Equal(t, first, log.agent.LogPosition())
	require.Equal(t, int64(50), log.agent.ClusterTime())
	require.Equal(t, first, log.image.Position())
	require.Equal(t, 0, log.agent.SessionCount())
	require.Empty(t, svc.opens)
	require.Len(t, adder.offers, 1)
	requireFailureReported(t, log.agent, got, ErrResponsePublicationAdd, aeron.ErrClientClosed)
}

func TestAgent_SessionClose_fatalAddPollStopsPoll(t *testing.T) {
	adder := newFakeResponseAdder()
	log, svc := bindSessionAgent(t, adder, codecs.RoleLeader)
	var got []error
	log.agent.errorHandler = collectErrors(&got)
	log.open(1, 1)
	queuePollError(adder, aeron.ErrClientClosed)
	polls := len(adder.polls)
	closeEnd := log.write(t, encodeSessionClose(t, codecs.SessionCloseEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: 1,
		Timestamp:        90,
		CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
	}))
	log.write(t, encodeSessionMessage(t, codecs.SessionMessageHeaderFields{
		ClusterSessionID: 1,
		Timestamp:        91,
	}, []byte("late")))

	log.agent.DoWork(0)
	require.Equal(t, []int64{1, 1}, adder.polls[polls:])
	require.Equal(t, []int64{1}, adder.cancels)
	requireFailureReported(t, log.agent, got, ErrResponsePublicationAdd, aeron.ErrClientClosed)
	require.False(t, log.agent.HasSession(1))
	require.Empty(t, svc.closes)
	require.Empty(t, svc.messages)
	require.Equal(t, closeEnd, log.agent.LogPosition())
	require.Equal(t, closeEnd, log.image.Position())
}

func TestAgent_ResponseAdd_fatalPollStopsTick(t *testing.T) {
	adder := newFakeResponseAdder()
	log, svc := bindSessionAgent(t, adder, codecs.RoleLeader)
	var got []error
	log.agent.errorHandler = collectErrors(&got)
	log.open(1, 1)
	log.open(2, 2)
	require.Equal(t, 2, log.agent.PendingResponsePublicationAdds())
	adder.next(1, responsePoll{err: aeron.ErrClientClosed})
	pos := log.agent.LogPosition()
	log.write(t, encodeSessionMessage(t, codecs.SessionMessageHeaderFields{
		ClusterSessionID: 2,
		Timestamp:        70,
	}, []byte("late")))

	log.agent.DoWork(0)
	requireFailureReported(t, log.agent, got, ErrResponsePublicationAdd, aeron.ErrClientClosed)
	require.Equal(t, 1, log.agent.PendingResponsePublicationAdds())
	require.Equal(t, pos, log.agent.LogPosition())
	require.Empty(t, svc.messages)

	log.agent.Close()
	require.Equal(t, 0, log.agent.PendingResponsePublicationAdds())
	require.Equal(t, []int64{2}, adder.cancels)
}
