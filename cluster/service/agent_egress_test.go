//go:build testhooks

package service

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/aeron"
	"luas.fast/cluster/codecs"
	"luas.fast/cluster/codecs/clusterwire"
)

// joinLog is a sessionLog that binds through JoinLog events. Each join names
// a new log stream with a fresh image at the consumed position.
type joinLog struct {
	*sessionLog
	streamID int32

	// joins counts the delivered JoinLog events.
	joins int
}

// bindJoinAgent binds svc and adder through one JoinLog with role at
// position 0. A nil adder leaves ResponsePublications unset.
func bindJoinAgent(
	tb testing.TB,
	svc ClusteredService,
	adder ResponsePublicationAdder,
	role codecs.Role,
) *joinLog {
	tb.Helper()
	sub, image := newLogImage(tb, testLogSessionID, testLogStreamID, 0)
	committed := int64(0)
	agent := NewAgent(AgentConfig{
		Service:              svc,
		CommittedPosition:    func() int64 { return committed },
		Subscriptions:        &stubAdder{sub: sub},
		ResponsePublications: adder,
	})
	log := &joinLog{
		sessionLog: &sessionLog{
			takeLog: &takeLog{image: image, committed: &committed},
			tb:      tb,
			agent:   agent,
		},
		streamID: testLogStreamID,
	}
	event := defaultJoinEvent(0)
	event.role = role
	agent.handleJoinLog(event)
	log.joins++
	agent.DoWork(0)
	require.NoError(tb, agent.ServiceFailure())
	require.True(tb, agent.IsLogBound())
	require.Equal(tb, role, agent.Role())
	return log
}

// join delivers one JoinLog with role and isStartup at the consumed
// position. Later writes go to the image of that join.
func (l *joinLog) join(role codecs.Role, isStartup bool) {
	l.tb.Helper()
	position := l.agent.LogPosition()
	l.streamID++
	l.image = replaceLogSubscription(l.tb, l.agent, testLogSessionID, l.streamID, position)
	l.offset = int(position)
	event := defaultJoinEvent(position)
	event.logStreamID = l.streamID
	event.role = role
	event.isStartup = isStartup
	l.agent.handleJoinLog(event)
	l.joins++
}

// rebind runs DoWork until the pending join binds. A bound image takes one
// tick to unbind and one to bind.
func (l *joinLog) rebind() {
	l.tb.Helper()
	for range 2 {
		if l.agent.IsLogBound() && !l.agent.HasPendingJoin() {
			break
		}
		l.agent.DoWork(0)
	}
	require.NoError(l.tb, l.agent.ServiceFailure())
	require.True(l.tb, l.agent.IsLogBound())
	require.False(l.tb, l.agent.HasPendingJoin())
}

// responseAddOf is the response add the open of id requests. Each id gets
// its own stream and channel.
func responseAddOf(id int64) transportAdd {
	return transportAdd{
		streamID: testResponseStreamID + int32(id),
		channel:  fmt.Sprintf("aeron:udp?endpoint=localhost:%d", 20120+id),
	}
}

// egressOpenPayload encodes the SessionOpen of id with responseAddOf(id).
func egressOpenPayload(tb testing.TB, id int64) []byte {
	tb.Helper()
	add := responseAddOf(id)
	return encodeSessionOpen(tb, codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: id,
		Timestamp:        id,
		ResponseStreamID: add.streamID,
		ResponseChannel:  []byte(add.channel),
	})
}

// openEgress writes the opens of ids and dispatches them in one DoWork.
func (l *sessionLog) openEgress(ids ...int64) {
	l.tb.Helper()
	for _, id := range ids {
		l.write(l.tb, egressOpenPayload(l.tb, id))
	}
	l.agent.DoWork(0)
	require.NoError(l.tb, l.agent.ServiceFailure())
	for _, id := range ids {
		require.True(l.tb, l.agent.HasSession(id))
	}
}

// termOfferService offers payload to session offerID inside each
// NewLeadershipTermEvent callback. It finds the session through the Cluster
// that Start passes.
type termOfferService struct {
	recordingService
	cluster Cluster
	offerID int64
	payload []byte
	results []int64
}

func (s *termOfferService) Start(cluster Cluster) error {
	s.cluster = cluster
	return s.recordingService.Start(cluster)
}

func (s *termOfferService) NewLeadershipTermEvent(
	int64,
	int64,
	int64,
	int64,
	int32,
	int32,
	clusterwire.ClusterTimeUnit,
	int32,
) {
	session, ok := s.cluster.ClientSession(s.offerID)
	if !ok {
		s.results = append(s.results, aeron.NotConnected)
		return
	}
	s.results = append(s.results, session.Offer(s.payload))
}

// TestAgent_JoinEgress_leaderChange asserts egress across a promote and a
// demote. A follower open does not start an add. The leader join adds one
// response publication per open session. The follower join closes every one.
func TestAgent_JoinEgress_leaderChange(t *testing.T) {
	adder := newFakeResponseAdder()
	svc := &termOfferService{offerID: 1, payload: []byte("term")}
	log := bindJoinAgent(t, svc, adder, codecs.RoleFollower)
	agent := log.agent
	ids := []int64{1, 2, 3}

	// Phase "follower opens": one DoWork dispatches three opens. The agent
	// does not start an add.
	log.openEgress(3, 1, 2)
	require.Equal(t, 3, agent.SessionCount())
	require.Empty(t, adder.offers)
	require.Equal(t, 0, agent.PendingResponsePublicationAdds())
	requireOffers(t, agent, MockedOffer, ids...)

	// Phase "promote": the leader join caps the image. One DoWork unbinds.
	// The next binds and starts one add per session in id order.
	log.join(codecs.RoleLeader, false)
	agent.DoWork(0)
	require.False(t, agent.IsLogBound())
	log.rebind()
	require.Equal(t, codecs.RoleLeader, agent.Role())
	require.Equal(t, []transportAdd{
		responseAddOf(1),
		responseAddOf(2),
		responseAddOf(3),
	}, adder.offers)
	require.Equal(t, 3, agent.PendingResponsePublicationAdds())
	requireOffers(t, agent, aeron.NotConnected, ids...)

	// Phase "resolve": one DoWork resolves the three adds.
	publications := make([]*aeron.Publication, len(ids))
	for i := range ids {
		publications[i] = aeron.NewTestPublicationForHooksWithConnected(t, true)
		adder.next(int64(i+1), responsePoll{publication: publications[i], done: true})
	}
	require.Equal(t, 3, agent.DoWork(0))
	require.Equal(t, 0, agent.PendingResponsePublicationAdds())
	require.Equal(t, 3, agent.ResponsePublicationCount())

	// Phase "term": the callback offer and a later offer carry term 6.
	log.write(t, encodeNLT(t, codecs.NewLeadershipTermEventFields{
		LeadershipTermID: 6,
		Timestamp:        70,
	}))
	agent.DoWork(0)
	require.Len(t, svc.results, 1)
	require.Positive(t, svc.results[0])
	requireEgressFrame(t, publications[0], 0, codecs.SessionMessageHeaderFields{
		LeadershipTermID: 6,
		ClusterSessionID: 1,
		Timestamp:        70,
	}, svc.payload)
	later, ok := agent.ClientSession(2)
	require.True(t, ok)
	payload := []byte("later")
	require.Positive(t, later.Offer(payload))
	requireEgressFrame(t, publications[1], 0, codecs.SessionMessageHeaderFields{
		LeadershipTermID: 6,
		ClusterSessionID: 2,
		Timestamp:        70,
	}, payload)

	// Phase "demote": the follower join caps the image. One DoWork unbinds
	// and closes every publication. The next bind does not change the adder
	// offers or cancels.
	log.join(codecs.RoleFollower, false)
	agent.DoWork(0)
	require.False(t, agent.IsLogBound())
	for _, publication := range publications {
		require.True(t, publication.IsClosed())
	}
	require.Equal(t, 0, agent.ResponsePublicationCount())
	log.rebind()
	require.Equal(t, codecs.RoleFollower, agent.Role())
	require.Len(t, adder.offers, 3)
	require.Empty(t, adder.cancels)
	requireOffers(t, agent, MockedOffer, ids...)
	require.Equal(t, 3, agent.SessionCount())
	require.Equal(t, log.joins, agent.PendingCoordinationAcks())
	require.NoError(t, agent.ServiceFailure())
}

// egressJoin is one JoinLog of a TestAgent_JoinEgress_rules row.
type egressJoin struct {
	role      codecs.Role
	isStartup bool
}

// TestAgent_JoinEgress_rules asserts the join egress rule per setup and
// join. Each row binds with role, opens sessions 1, 2 and 3, then delivers
// its JoinLog events and runs DoWork until bind. Only the join that binds
// applies the rule. ServiceFailure stays nil in every row. Every delivered
// join keeps its ack queued.
func TestAgent_JoinEgress_rules(t *testing.T) {
	offerErr := errors.New("offer add boom")
	ids := []int64{1, 2, 3}
	leaderJoin := egressJoin{role: codecs.RoleLeader}
	startupLeaderJoin := egressJoin{role: codecs.RoleLeader, isStartup: true}
	followerJoin := egressJoin{role: codecs.RoleFollower}

	tests := []struct {
		name string

		// role is the role of the setup join.
		role codecs.Role

		// resolve is how many leader setup adds resolve before the joins.
		// The image then clears, so the unbind disconnect does not run.
		resolve int

		// empty skips the three opens.
		empty   bool
		noAdder bool

		// failID fails the add offer of this session. Zero disables the failure.
		failID int64

		// closing marks every session closing before the joins.
		closing bool

		joins []egressJoin

		// wantAdds lists the sessions whose add offer the join makes.
		wantAdds         []int64
		wantPending      int
		wantPublications int
		wantPolls        []int64
		wantCancels      []int64

		// wantOffer is the Offer result of each session. Zero asserts a
		// positive result.
		wantOffer   int64
		wantClosing bool
	}{
		{
			name:        "follower setup, leader join",
			joins:       []egressJoin{leaderJoin},
			wantAdds:    ids,
			wantPending: 3,
			wantOffer:   aeron.NotConnected,
		},
		{
			name:      "follower setup, startup leader join",
			joins:     []egressJoin{startupLeaderJoin},
			wantOffer: aeron.NotConnected,
		},
		{
			name:             "leader setup, leader join",
			role:             codecs.RoleLeader,
			resolve:          3,
			joins:            []egressJoin{leaderJoin},
			wantPublications: 3,
		},
		{
			name:      "leader setup, follower join",
			role:      codecs.RoleLeader,
			resolve:   3,
			joins:     []egressJoin{followerJoin},
			wantOffer: MockedOffer,
		},
		{
			name:      "leader setup, candidate join",
			role:      codecs.RoleLeader,
			resolve:   3,
			joins:     []egressJoin{{role: codecs.RoleCandidate}},
			wantOffer: MockedOffer,
		},
		{
			name:        "leader setup with an add in flight, follower join",
			role:        codecs.RoleLeader,
			resolve:     2,
			joins:       []egressJoin{followerJoin},
			wantPolls:   []int64{3, 3},
			wantCancels: []int64{3},
			wantOffer:   MockedOffer,
		},
		{
			name:      "follower setup with no adder, leader join",
			noAdder:   true,
			joins:     []egressJoin{leaderJoin},
			wantOffer: aeron.NotConnected,
		},
		{
			name:  "empty registry, leader join",
			empty: true,
			joins: []egressJoin{leaderJoin},
		},
		{
			name:        "follower setup, leader join with one failed add offer",
			failID:      2,
			joins:       []egressJoin{leaderJoin},
			wantAdds:    ids,
			wantPending: 2,
			wantOffer:   aeron.NotConnected,
		},
		{
			name:      "follower setup, leader join superseded by a follower join",
			joins:     []egressJoin{leaderJoin, followerJoin},
			wantOffer: MockedOffer,
		},
		{
			name:        "follower setup, follower join superseded by a leader join",
			joins:       []egressJoin{followerJoin, leaderJoin},
			wantAdds:    ids,
			wantPending: 3,
			wantOffer:   aeron.NotConnected,
		},
		{
			name:        "closing sessions, leader join",
			closing:     true,
			joins:       []egressJoin{leaderJoin},
			wantAdds:    ids,
			wantPending: 3,
			wantOffer:   aeron.NotConnected,
		},
		{
			name:      "closing sessions, startup leader join",
			closing:   true,
			joins:     []egressJoin{startupLeaderJoin},
			wantOffer: aeron.NotConnected,
		},
		{
			name:        "closing sessions, follower join",
			closing:     true,
			joins:       []egressJoin{followerJoin},
			wantOffer:   MockedOffer,
			wantClosing: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adder := newFakeResponseAdder()
			var configured ResponsePublicationAdder = adder
			if tc.noAdder {
				configured = nil
			}
			log := bindJoinAgent(t, nil, configured, tc.role)
			agent := log.agent
			var open []int64
			if !tc.empty {
				open = ids
				log.openEgress(open...)
			}

			// Phase "setup": resolve the leader adds, then clear the image.
			var publications []*aeron.Publication
			if tc.resolve > 0 {
				for i := range tc.resolve {
					publication := aeron.NewTestPublicationForHooksWithConnected(t, true)
					publications = append(publications, publication)
					adder.next(int64(i+1), responsePoll{publication: publication, done: true})
				}
				agent.DoWork(0)
				require.Equal(t, tc.resolve, agent.ResponsePublicationCount())
				agent.adapter.SetImage(nil)
			}
			if tc.failID != 0 {
				adder.offerErrs = map[int32]error{responseAddOf(tc.failID).streamID: offerErr}
			}
			if tc.closing {
				for _, session := range agent.sessionList {
					session.closing = true
				}
			}
			offers := len(adder.offers)
			polls := len(adder.polls)

			// Phase "join": deliver the joins, then bind the last one.
			for _, join := range tc.joins {
				log.join(join.role, join.isStartup)
			}
			log.rebind()

			wantAdds := []transportAdd{}
			for _, id := range tc.wantAdds {
				wantAdds = append(wantAdds, responseAddOf(id))
			}
			require.Equal(t, wantAdds, append([]transportAdd{}, adder.offers[offers:]...))
			require.Equal(t, tc.wantPending, agent.PendingResponsePublicationAdds())
			require.Len(t, adder.inFlight, tc.wantPending)
			require.Equal(t, tc.wantPublications, agent.ResponsePublicationCount())
			require.Equal(t, append([]int64{}, tc.wantPolls...), append([]int64{}, adder.polls[polls:]...))
			require.Equal(t, tc.wantCancels, adder.cancels)
			for _, publication := range publications {
				require.Equal(t, tc.wantPublications == 0, publication.IsClosed())
			}
			if tc.failID != 0 {
				require.Equal(t, nullValue, agent.sessions[tc.failID].addCorrelationID)
			}
			for _, id := range open {
				session, ok := agent.ClientSession(id)
				require.True(t, ok)
				require.Equal(t, tc.wantClosing, session.IsClosing(), "session %d", id)
			}
			requireOffers(t, agent, tc.wantOffer, open...)
			require.Equal(t, len(open), agent.SessionCount())
			require.NoError(t, agent.ServiceFailure())
			require.Equal(t, log.joins, agent.PendingCoordinationAcks())
			require.Equal(t, tc.joins[len(tc.joins)-1].role, agent.Role())
		})
	}
}
