//go:build testhooks

package consensus

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func sampleLearnOpenFields(id int64) codecs.SessionOpenEventFields {
	return codecs.SessionOpenEventFields{
		LeadershipTermID: 7,
		CorrelationID:    id * 10,
		ClusterSessionID: id,
		Timestamp:        100 + id,
		ResponseStreamID: int32(id + 100),
		ResponseChannel:  []byte("aeron:udp?endpoint=127.0.0.1:40123"),
		EncodedPrincipal: []byte("must-not-store"),
	}
}

// writeSessionOpenFrame writes one unfragmented SessionOpenEvent. Returns the
// adapter and the aligned span. Does not poll.
func writeSessionOpenFrame(
	tb testing.TB,
	fields codecs.SessionOpenEventFields,
) (*LogAdapter, int64) {
	tb.Helper()
	adapter, image := boundLogAdapter(tb)
	payload := encodeLogAdapterSessionOpenPayload(tb, fields)
	span := int64(logbuffer.AlignFragmentChunkLength(len(payload)))
	writeLogAdapterUnfragmentedFrame(tb, image, 0, payload)
	return adapter, span
}

// requireLearnedOpen asserts the registry holds one open session matching
// fields, nowNs, and logPos.
func requireLearnedOpen(
	t *testing.T,
	a *Agent,
	fields codecs.SessionOpenEventFields,
	nowNs, logPos int64,
) *clientSession {
	t.Helper()
	require.Equal(t, 1, a.OpenSessionCount())
	s, ok := a.sessions.sessionByID(fields.ClusterSessionID)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s.state)
	require.Equal(t, fields.CorrelationID, s.correlationID)
	require.Equal(t, fields.ResponseChannel, s.responseChannel)
	require.Equal(t, fields.ResponseStreamID, s.responseStreamID)
	require.Equal(t, nowNs, s.lastActivityNs)
	require.Equal(t, logPos, s.openedLogPosition)
	require.Nil(t, s.encodedPrincipal)
	require.Nil(t, s.egressPublication)
	require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
	return s
}

func seedLearnedOpen(
	t *testing.T,
	a *Agent,
	id, nowNs, logPos int64,
) *clientSession {
	t.Helper()
	a.logPollNowNs = nowNs
	a.handleSessionOpenEvent(sampleLearnOpenFields(id), logPos)
	s, ok := a.sessions.sessionByID(id)
	require.True(t, ok)
	return s
}

func seedNewLeaderPending(t *testing.T, a *Agent, id int64) *clientSession {
	t.Helper()
	s := seedLearnedOpen(t, a, id, 1, 8)
	a.sessions.markNewLeaderPending(s)
	return s
}

func attachEgressPub(t *testing.T, s *clientSession, connected bool) *aeron.Publication {
	t.Helper()
	pub := aeron.NewTestPublicationForHooksWithConnected(t, connected)
	s.egressPublication = pub
	return pub
}

func requireNewLeaderEvent(
	t *testing.T,
	pub *aeron.Publication,
	termID, sessionID int64,
	memberID int32,
	ingress string,
) {
	t.Helper()
	got, _ := decodeClusterFieldsAtTermOffset[codecs.NewLeaderEventFields](
		t,
		pub,
		0,
		clusterwire.NewLeaderEventSbeTemplateID,
	)
	require.Equal(t, codecs.NewLeaderEventFields{
		LeadershipTermID: termID,
		ClusterSessionID: sessionID,
		LeaderMemberID:   memberID,
		IngressEndpoints: []byte(ingress),
	}, got)
}

func TestLearnSessionOpen_insert(t *testing.T) {
	a := NewAgent(AgentConfig{})
	fields := sampleLearnOpenFields(9)
	const (
		logPos int64 = 256
		nowNs  int64 = 1_000
	)
	a.logPollNowNs = nowNs
	a.handleSessionOpenEvent(fields, logPos)

	requireLearnedOpen(t, a, fields, nowNs, logPos)
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 0, a.ClosingSessionCount())
	require.Equal(t, int64(10), a.sessions.nextSessionID)
	require.Empty(t, a.sessions.okPending)
	require.Empty(t, a.egressEvents)
}

func TestLearnSessionOpen_updateRedelivery(t *testing.T) {
	a := NewAgent(AgentConfig{})
	s := seedLearnedOpen(t, a, 4, 10, 100)
	pub := attachEgressPub(t, s, true)
	s.egressAddCorrelationID = 77

	second := sampleLearnOpenFields(4)
	second.CorrelationID = 99
	second.ResponseStreamID = 8
	second.ResponseChannel = []byte("aeron:udp?endpoint=127.0.0.1:49999")
	second.EncodedPrincipal = []byte("still-ignored")
	a.logPollNowNs = 20
	a.handleSessionOpenEvent(second, 200)

	require.Equal(t, 1, a.OpenSessionCount())
	require.Same(t, s, a.sessions.byID[4])
	require.Equal(t, sessionOpen, s.state)
	require.Equal(t, second.CorrelationID, s.correlationID)
	require.Equal(t, second.ResponseStreamID, s.responseStreamID)
	require.Equal(t, second.ResponseChannel, s.responseChannel)
	require.Equal(t, int64(200), s.openedLogPosition)
	require.Equal(t, int64(20), s.lastActivityNs)
	require.Nil(t, s.encodedPrincipal)
	require.Same(t, pub, s.egressPublication)
	require.Equal(t, int64(77), s.egressAddCorrelationID)
	require.False(t, pub.IsClosed())
	require.Empty(t, a.sessions.okPending)
}

func TestLearnSessionOpen_reseedOutOfOrder(t *testing.T) {
	m := newSessionManager()
	steps := []struct {
		id       int64
		wantNext int64
		wantOpen int
	}{
		{10, 11, 1},
		{3, 11, 2},
		{20, 21, 3},
	}
	for i, step := range steps {
		m.learnSessionOpen(sampleLearnOpenFields(step.id), int64(i+1), int64(i+1))
		require.Equal(t, step.wantNext, m.nextSessionID)
		require.Equal(t, step.wantOpen, m.openCount())
	}

	require.Equal(t, int64(21), m.allocateSessionID())
	require.Equal(t, int64(22), m.nextSessionID)
}

func TestLearnSessionOpen_knownPendingBecomesOpen(t *testing.T) {
	m := newSessionManager()
	s := addPending(m, validConnectFields(5), 1)
	require.Equal(t, 1, m.pendingCount())
	require.Equal(t, 0, m.openCount())

	fields := sampleLearnOpenFields(s.id)
	m.learnSessionOpen(fields, 64, 9)
	require.Equal(t, 0, m.pendingCount())
	require.Equal(t, 1, m.openCount())
	require.Equal(t, sessionOpen, s.state)
	require.Equal(t, fields.ResponseChannel, s.responseChannel)
	require.Empty(t, m.okPending)
}

func TestLearnSessionClose_removesAndReleasesEgress(t *testing.T) {
	a := NewAgent(AgentConfig{})
	s := seedLearnedOpen(t, a, 6, 1, 32)
	pub := attachEgressPub(t, s, true)

	a.handleSessionCloseEvent(codecs.SessionCloseEventFields{
		LeadershipTermID: 7,
		ClusterSessionID: 6,
		Timestamp:        50,
		CloseReason:      clusterwire.CloseReason_TIMEOUT,
	})
	require.True(t, pub.IsClosed())
	_, still := a.sessions.sessionByID(6)
	require.False(t, still)
	require.Equal(t, 0, a.OpenSessionCount())
	require.Equal(t, 0, a.ClosingSessionCount())
}

func TestLearnSessionClose_withoutPublication(t *testing.T) {
	a := NewAgent(AgentConfig{})
	_ = seedLearnedOpen(t, a, 3, 1, 16)
	require.Equal(t, 1, a.OpenSessionCount())

	a.handleSessionCloseEvent(codecs.SessionCloseEventFields{
		ClusterSessionID: 3,
		CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
	})
	_, ok := a.sessions.sessionByID(3)
	require.False(t, ok)
	require.Equal(t, 0, a.OpenSessionCount())
}

func TestLearnSessionClose_unknownIDNoop(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.NotPanics(t, func() {
		a.handleSessionCloseEvent(codecs.SessionCloseEventFields{
			ClusterSessionID: 99,
			CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
		})
	})
	require.Equal(t, 0, a.OpenSessionCount())
	require.Empty(t, a.sessions.byID)
}

func TestLearn_freshAgentZeroCounts(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.Equal(t, 0, a.OpenSessionCount())
	require.Equal(t, 0, a.PendingSessionCount())
	require.Equal(t, 0, a.ClosingSessionCount())
	require.Empty(t, a.sessions.byID)
	require.Equal(t, int64(1), a.sessions.nextSessionID)
}

func TestFollowerConsensusWork_learnsOpenFromLog(t *testing.T) {
	fields := sampleLearnOpenFields(12)
	adapter, span := writeSessionOpenFrame(t, fields)
	a := newFollowerLogPollAgent(span+100, nil)
	a.SetLogAdapter(adapter)
	a.notifiedCommitPosition = span + 32

	const nowNs int64 = 777
	require.Positive(t, a.followerConsensusWork(nowNs))
	requireLearnedOpen(t, a, fields, nowNs, span)
	require.Equal(t, int64(13), a.sessions.nextSessionID)
}

func TestCatchupPoll_learnsOpenFromLog(t *testing.T) {
	fields := sampleLearnOpenFields(8)
	adapter, span := writeSessionOpenFrame(t, fields)
	a := NewAgent(AgentConfig{LogAdapter: adapter})

	const nowNs int64 = 888
	require.Positive(t, catchupPoll(t, a, span+32, nowNs))
	requireLearnedOpen(t, a, fields, nowNs, span)
}

func TestMarkNewLeaderPending_requiresOpen(t *testing.T) {
	m := newSessionManager()
	m.markNewLeaderPending(nil)

	pending := addPending(m, validConnectFields(1), 1)
	m.markNewLeaderPending(pending)
	require.False(t, pending.newLeaderEventPending)

	require.True(t, m.markAuthenticated(pending.id, nil))
	m.markNewLeaderPending(pending)
	require.False(t, pending.newLeaderEventPending)

	require.True(t, m.markOpen(pending.id, 8))
	m.markNewLeaderPending(pending)
	require.True(t, pending.newLeaderEventPending)

	require.True(t, m.markClosing(pending.id, clusterwire.CloseReason_TIMEOUT))
	require.False(t, pending.newLeaderEventPending)
}

func TestElectionComplete_leaderRebindsOpenSessions(t *testing.T) {
	adder := neverReadyEgressAdder()
	a := NewAgent(AgentConfig{
		Role:             RoleLeader,
		LeadershipTermID: 5,
		ThisMemberID:     2,
	})
	a.SetEgressPublicationAdder(adder)
	a.SetIngressEndpoints("0=127.0.0.1:9000")

	const acceptNs int64 = 10
	openSess := seedLearnedOpen(t, a, 1, acceptNs, 64)
	preOpen := addPending(a.sessions, validConnectFields(2), acceptNs)
	closing := seedLearnedOpen(t, a, 3, acceptNs, 96)
	require.True(t, a.sessions.markClosing(closing.id, clusterwire.CloseReason_TIMEOUT))

	const completeNs int64 = 999
	a.ElectionComplete(completeNs)

	require.True(t, openSess.newLeaderEventPending)
	require.Equal(t, completeNs, openSess.lastActivityNs)
	require.NotEqual(t, int64(nullValue), openSess.egressAddCorrelationID)
	require.Nil(t, openSess.egressPublication)
	require.Len(t, adder.offers, 1)
	require.Equal(t, openSess.responseStreamID, adder.offers[0].streamID)
	require.Equal(t, string(openSess.responseChannel), adder.offers[0].channel)

	require.False(t, preOpen.newLeaderEventPending)
	require.Equal(t, sessionPending, preOpen.state)
	require.Equal(t, acceptNs, preOpen.lastActivityNs)
	require.Equal(t, int64(nullValue), preOpen.egressAddCorrelationID)

	require.False(t, closing.newLeaderEventPending)
	require.Equal(t, sessionClosing, closing.state)
	require.Equal(t, acceptNs, closing.lastActivityNs)
	require.Equal(t, int64(nullValue), closing.egressAddCorrelationID)
}

func TestElectionComplete_startupLeaderClosesOpenSessions(t *testing.T) {
	publisher, logPub := boundLogPublisher(t)
	adder := neverReadyEgressAdder()
	a := leaderWithEgress(t, publisher, 0, 2, adder)
	a.SetIngressEndpoints("0=127.0.0.1:9000")

	const acceptNs int64 = 10
	openA := seedLearnedOpen(t, a, 1, acceptNs, 64)
	a.sessions.markNewLeaderPending(openA)
	openB := seedLearnedOpen(t, a, 2, acceptNs, 80)
	preOpen := addPending(a.sessions, validConnectFields(3), acceptNs)
	closing := seedLearnedOpen(t, a, 4, acceptNs, 96)
	require.True(t, a.sessions.markClosing(closing.id, clusterwire.CloseReason_CLIENT_ACTION))
	withAdd := seedLearnedOpen(t, a, 5, acceptNs, 112)
	require.NoError(t, a.startSessionEgress(withAdd))
	corr := withAdd.egressAddCorrelationID
	withPub := seedLearnedOpen(t, a, 6, acceptNs, 128)
	pub := attachEgressPub(t, withPub, true)
	egressBefore := pub.Position()
	require.Len(t, adder.offers, 1)

	require.NoError(t, a.JoinLogAsLeader(1, 0, 1, true))
	logBefore := logPub.Position()

	const completeNs int64 = 999
	a.ElectionComplete(completeNs)

	for _, session := range []*clientSession{openA, openB, withAdd, withPub} {
		require.Equal(t, sessionClosing, session.state)
		require.Equal(t, clusterwire.CloseReason_TIMEOUT, session.closeReason)
		require.False(t, session.newLeaderEventPending)
		require.Equal(t, acceptNs, session.lastActivityNs)
	}
	require.Equal(t, int64(nullValue), openA.egressAddCorrelationID)
	require.Nil(t, openA.egressPublication)
	require.Equal(t, corr, withAdd.egressAddCorrelationID)
	require.Same(t, pub, withPub.egressPublication)

	require.Equal(t, sessionPending, preOpen.state)
	require.False(t, preOpen.newLeaderEventPending)
	require.Equal(t, acceptNs, preOpen.lastActivityNs)
	require.Equal(t, int64(nullValue), preOpen.egressAddCorrelationID)

	require.Equal(t, sessionClosing, closing.state)
	require.Equal(t, clusterwire.CloseReason_CLIENT_ACTION, closing.closeReason)
	require.False(t, closing.newLeaderEventPending)
	require.Equal(t, int64(nullValue), closing.egressAddCorrelationID)

	require.Len(t, adder.offers, 1)
	require.Equal(t, 0, a.newLeaderEventWork())
	require.Equal(t, egressBefore, pub.Position())
	require.Equal(t, logBefore, logPub.Position())
	requireSessionIDs(t, a, openA.id, openB.id, preOpen.id, closing.id, withAdd.id, withPub.id)
	require.Equal(t, completeNs-a.leaderHeartbeatIntervalNs, a.timeOfLastLogUpdateNs)

	// The next sweep appends each close in walk order and removes the session.
	require.Positive(t, a.sessionCloseWork(completeNs))
	requireSessionIDs(t, a, preOpen.id)
	type logClose struct {
		id     int64
		reason clusterwire.CloseReason
	}
	var closes []logClose
	forEachTermWire(t, logPub, 16, func(w []byte) bool {
		if fields, ok := codecs.TryDecodeSessionCloseEventAt(w, 0); ok {
			closes = append(closes, logClose{fields.ClusterSessionID, fields.CloseReason})
		}
		return false
	})
	require.Equal(t, []logClose{
		{openA.id, clusterwire.CloseReason_TIMEOUT},
		{openB.id, clusterwire.CloseReason_TIMEOUT},
		{closing.id, clusterwire.CloseReason_CLIENT_ACTION},
		{withAdd.id, clusterwire.CloseReason_TIMEOUT},
		{withPub.id, clusterwire.CloseReason_TIMEOUT},
	}, closes)
	require.Equal(t, []int64{corr}, adder.cancels)
	require.Equal(t, []string{clusterwire.CloseReason_TIMEOUT.String()}, closedEventDetails(t, pub))
}

func TestElectionComplete_startupLeaderWithoutLogKeepsOpenSessions(t *testing.T) {
	a := leaderWithEgress(t, nil, 0, 1, neverReadyEgressAdder())
	s := seedLearnedOpen(t, a, 1, 10, 8)

	require.NoError(t, a.JoinLogAsLeader(1, 0, 1, true))
	a.ElectionComplete(50)

	require.Equal(t, sessionOpen, s.state)
	require.False(t, s.newLeaderEventPending)
}

func TestElectionComplete_laterJoinRebindsOpenSessions(t *testing.T) {
	a := leaderWithEgress(t, nil, 0, 1, neverReadyEgressAdder())
	s := seedLearnedOpen(t, a, 1, 10, 8)

	require.NoError(t, a.JoinLogAsLeader(1, 0, 1, true))
	require.NoError(t, a.JoinLogAsLeader(2, 8, 1, false))
	a.ElectionComplete(50)

	require.Equal(t, sessionOpen, s.state)
	require.True(t, s.newLeaderEventPending)
}

func TestElectionComplete_leaderKeepsExistingEgress(t *testing.T) {
	adder := neverReadyEgressAdder()
	a := NewAgent(AgentConfig{Role: RoleLeader, ThisMemberID: 1})
	a.SetEgressPublicationAdder(adder)

	withPub := seedLearnedOpen(t, a, 1, 1, 8)
	pub := attachEgressPub(t, withPub, true)

	withAdd := seedLearnedOpen(t, a, 2, 1, 16)
	require.NoError(t, a.startSessionEgress(withAdd))
	corr := withAdd.egressAddCorrelationID
	require.NotEqual(t, int64(nullValue), corr)
	require.Len(t, adder.offers, 1)

	a.ElectionComplete(50)
	require.True(t, withPub.newLeaderEventPending)
	require.Same(t, pub, withPub.egressPublication)
	require.Equal(t, int64(nullValue), withPub.egressAddCorrelationID)
	require.True(t, withAdd.newLeaderEventPending)
	require.Equal(t, corr, withAdd.egressAddCorrelationID)
	require.Len(t, adder.offers, 1)
}

func TestElectionComplete_followerTouchesNoSessionState(t *testing.T) {
	a := NewAgent(AgentConfig{Role: RoleFollower, ThisMemberID: 1})
	a.SetEgressPublicationAdder(neverReadyEgressAdder())
	s := seedLearnedOpen(t, a, 4, 20, 32)
	before := *s
	a.isLeaderStartup = true

	const completeNs int64 = 500
	a.ElectionComplete(completeNs)
	require.Equal(t, completeNs, a.timeOfLastLogUpdateNs)
	require.Equal(t, before.lastActivityNs, s.lastActivityNs)
	require.False(t, s.newLeaderEventPending)
	require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
	require.Nil(t, s.egressPublication)
	require.Equal(t, sessionOpen, s.state)
}

func TestElectionComplete_leaderReadsNoLog(t *testing.T) {
	fields := sampleLearnOpenFields(12)
	adapter, span := writeSessionOpenFrame(t, fields)
	commit := new(atomic.Int64)
	commit.Store(span + 32)
	a := NewAgent(AgentConfig{
		Role:             RoleLeader,
		Commit:           commit,
		ThisMemberID:     1,
		LeadershipTermID: 7,
	})
	a.SetLogAdapter(adapter)
	a.SetEgressPublicationAdder(neverReadyEgressAdder())

	a.ElectionComplete(777)
	require.Equal(t, int64(0), adapter.Position())
	require.Equal(t, 0, a.OpenSessionCount())
}

func TestEnterElection_readsNoLog(t *testing.T) {
	fields := sampleLearnOpenFields(9)
	adapter, span := writeSessionOpenFrame(t, fields)
	appendV := new(atomic.Int64)
	appendV.Store(span + 100)
	fac := &recordingElectionFactory{}
	a := NewAgent(AgentConfig{
		Role:         RoleFollower,
		AppendSource: &AtomicAppendPosition{V: appendV},
	})
	a.SetLogAdapter(adapter)
	a.notifiedCommitPosition = span + 32
	a.SetElectionFactory(fac.factory())

	require.Equal(t, 1, a.enterElection(ElectionTriggerLeaderHeartbeatTimeout, false))
	require.NotNil(t, a.Election())
	require.Equal(t, int64(0), adapter.Position())
	require.Equal(t, 0, a.OpenSessionCount())
	require.Equal(t, int64(0), a.CommitPosition())
}

func TestNewLeaderEventWork_addInFlightHolds(t *testing.T) {
	adder := neverReadyEgressAdder()
	a := leaderWithEgress(t, nil, 4, 2, adder)
	s := seedNewLeaderPending(t, a, 1)
	require.NoError(t, a.startSessionEgress(s))

	require.Equal(t, 1, a.newLeaderEventWork())
	require.True(t, s.newLeaderEventPending)
	require.Equal(t, sessionOpen, s.state)
	require.Nil(t, s.egressPublication)
	require.NotEqual(t, int64(nullValue), s.egressAddCorrelationID)
}

func TestNewLeaderEventWork_noAddHolds(t *testing.T) {
	adder := newScriptedEgressAdder()
	adder.offerErr = errors.New("invalid channel")
	a := leaderWithEgress(t, nil, 4, 2, adder)
	s := seedNewLeaderPending(t, a, 1)

	require.Equal(t, 0, a.newLeaderEventWork())
	require.True(t, s.newLeaderEventPending)
	require.Equal(t, sessionOpen, s.state)
	require.Nil(t, s.egressPublication)
	require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
	require.Empty(t, adder.offers)
	_, ok := a.sessions.sessionByID(s.id)
	require.True(t, ok)
}

func TestNewLeaderEventWork_noAddAfterFailedRebindHolds(t *testing.T) {
	adder := newScriptedEgressAdder()
	adder.offerErr = errors.New("driver timeout")
	a := leaderWithEgress(t, nil, 4, 2, adder)
	s := seedNewLeaderPending(t, a, 1)
	require.Error(t, a.startSessionEgress(s))
	require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
	require.Empty(t, adder.offers)

	adder.offerErr = nil
	adder.defaultPending = 100
	require.Equal(t, 0, a.newLeaderEventWork())
	require.True(t, s.newLeaderEventPending)
	require.Nil(t, s.egressPublication)
	require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
	require.Empty(t, adder.offers)
}

func TestNewLeaderEventWork_interruptedAddFails(t *testing.T) {
	adder := interruptedFirstOfferAdder()
	a := leaderWithEgress(t, nil, 4, 2, adder)
	s := seedNewLeaderPending(t, a, 1)
	require.NoError(t, a.startSessionEgress(s))
	require.Equal(t, int64(1), s.egressAddCorrelationID)
	adder.defaultPending = 100

	require.Equal(t, 1, a.newLeaderEventWork())
	require.False(t, s.newLeaderEventPending)
	require.Equal(t, sessionOpen, s.state)
	require.Nil(t, s.egressPublication)
	require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
	require.Len(t, adder.offers, 1)
	_, ok := a.sessions.sessionByID(s.id)
	require.True(t, ok)
}

func TestNewLeaderEventWork_connectedSend(t *testing.T) {
	const (
		termID   int64 = 11
		memberID int32 = 3
		ingress        = "0=127.0.0.1:40123,1=127.0.0.1:40124"
	)
	a := NewAgent(AgentConfig{
		Role:             RoleLeader,
		LeadershipTermID: termID,
		ThisMemberID:     memberID,
	})
	a.SetIngressEndpoints(ingress)
	s := seedNewLeaderPending(t, a, 8)
	pub := attachEgressPub(t, s, true)

	require.Equal(t, 1, a.newLeaderEventWork())
	require.False(t, s.newLeaderEventPending)
	require.Equal(t, sessionOpen, s.state)
	requireNewLeaderEvent(t, pub, termID, s.id, memberID, ingress)
}

func TestNewLeaderEventWork_retriableHold(t *testing.T) {
	a := NewAgent(AgentConfig{Role: RoleLeader, ThisMemberID: 1})
	pub := aeron.NewTestPublicationForHooksWithLimit(t, 0, true)
	s := seedNewLeaderPending(t, a, 1)
	s.egressPublication = pub

	require.Equal(t, 0, a.newLeaderEventWork())
	require.True(t, s.newLeaderEventPending)
	require.Same(t, pub, s.egressPublication)
	require.Equal(t, sessionOpen, s.state)
}

func TestNewLeaderEventWork_closedPublicationClears(t *testing.T) {
	adder := neverReadyEgressAdder()
	a := leaderWithEgress(t, nil, 0, 1, adder)
	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	require.NoError(t, pub.Close())
	s := seedNewLeaderPending(t, a, 1)
	s.egressPublication = pub

	require.Equal(t, 1, a.newLeaderEventWork())
	require.False(t, s.newLeaderEventPending)
	require.Nil(t, s.egressPublication)
	require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
	require.Equal(t, sessionOpen, s.state)
	require.Empty(t, adder.offers)
	_, ok := a.sessions.sessionByID(s.id)
	require.True(t, ok)
}

func TestNewLeaderEventWork_terminalAdd(t *testing.T) {
	adder := terminalFirstOfferAdder()
	a := leaderWithEgress(t, nil, 4, 2, adder)
	s := seedNewLeaderPending(t, a, 1)
	require.NoError(t, a.startSessionEgress(s))

	require.Equal(t, 1, a.newLeaderEventWork())
	require.False(t, s.newLeaderEventPending)
	require.Equal(t, sessionOpen, s.state)
	require.Nil(t, s.egressPublication)
	require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
	require.Len(t, adder.offers, 1)
	_, ok := a.sessions.sessionByID(s.id)
	require.True(t, ok)
}

func TestNewLeaderEventWork_oversizeEndpointsTerminal(t *testing.T) {
	a := NewAgent(AgentConfig{Role: RoleLeader, ThisMemberID: 1})
	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	a.SetIngressEndpoints(string(make([]byte, pub.MaxPayloadLength()+1)))
	s := seedNewLeaderPending(t, a, 1)
	s.egressPublication = pub

	require.Equal(t, 1, a.newLeaderEventWork())
	require.False(t, s.newLeaderEventPending)
	require.Nil(t, s.egressPublication)
	require.Equal(t, sessionOpen, s.state)
	_, ok := a.sessions.sessionByID(s.id)
	require.True(t, ok)
}

func TestNewLeaderEventWork_notConnectedHolds(t *testing.T) {
	a := NewAgent(AgentConfig{Role: RoleLeader, ThisMemberID: 1})
	s := seedNewLeaderPending(t, a, 1)
	pub := attachEgressPub(t, s, false)

	require.Equal(t, 0, a.newLeaderEventWork())
	require.True(t, s.newLeaderEventPending)
	require.Same(t, pub, s.egressPublication)
}

func TestSessionWork_newLeaderEventAfterEgressEvent_leaderOnly(t *testing.T) {
	const ingress = "0=127.0.0.1:9000"

	setup := func(t *testing.T, a *Agent) (*clientSession, *aeron.Publication) {
		t.Helper()
		a.SetIngressEndpoints(ingress)
		s := seedNewLeaderPending(t, a, 1)
		pub := attachEgressPub(t, s, true)
		a.queueSessionRedirect(validConnectFields(9), 1)
		return s, pub
	}

	t.Run("follower_drains_event_skips_new_leader", func(t *testing.T) {
		adder, _ := scriptedConnectedPub(t, 0)
		a := followerWithEgress(t, adder, 2)
		s, pub := setup(t, a)

		require.Positive(t, a.sessionWork(2))
		require.Empty(t, a.egressEvents)
		require.True(t, s.newLeaderEventPending)
		require.Same(t, pub, s.egressPublication)
	})

	t.Run("leader_drains_event_and_sends_new_leader", func(t *testing.T) {
		adder, _ := scriptedConnectedPub(t, 0)
		a := leaderWithEgress(t, nil, 6, 4, adder)
		s, pub := setup(t, a)

		require.Positive(t, a.sessionWork(2))
		require.Empty(t, a.egressEvents)
		require.False(t, s.newLeaderEventPending)
		requireNewLeaderEvent(t, pub, 6, s.id, 4, ingress)
	})

	t.Run("election_live_skips_both", func(t *testing.T) {
		adder, _ := scriptedConnectedPub(t, 0)
		a := leaderWithEgress(t, nil, 6, 4, adder)
		a.election = &recordingRunner{}
		s, _ := setup(t, a)

		require.Equal(t, 0, a.sessionWork(2))
		require.Len(t, a.egressEvents, 1)
		require.True(t, s.newLeaderEventPending)
	})
}

func TestReleaseSessionsForNewTerm(t *testing.T) {
	adder := neverReadyEgressAdder()
	a := leaderWithEgress(t, nil, 5, 1, adder)
	a.SetIngressEndpoints("0=h:1")

	withPub := seedLearnedOpen(t, a, 1, 10, 8)
	pub := attachEgressPub(t, withPub, true)
	a.sessions.markNewLeaderPending(withPub)
	a.sessions.setOKPending(withPub, true)

	withAdd := seedLearnedOpen(t, a, 2, 10, 16)
	require.NoError(t, a.startSessionEgress(withAdd))
	withAddCorr := withAdd.egressAddCorrelationID
	require.NotEqual(t, int64(nullValue), withAddCorr)

	preOpen := addPending(a.sessions, validConnectFields(3), 10)
	require.NoError(t, a.startSessionEgress(preOpen))
	require.NotEqual(t, int64(nullValue), preOpen.egressAddCorrelationID)

	closing := seedLearnedOpen(t, a, 4, 10, 24)
	closingPub := attachEgressPub(t, closing, true)
	require.True(t, a.sessions.markClosing(closing.id, clusterwire.CloseReason_CLIENT_ACTION))

	a.queueSessionRedirect(validConnectFields(99), 10)
	require.Len(t, a.egressEvents, 1)
	require.Len(t, a.sessions.byID, 4)
	require.Len(t, a.sessions.okPending, 1)

	a.releaseSessionsForNewTerm()

	require.True(t, pub.IsClosed())
	require.True(t, closingPub.IsClosed())
	require.Nil(t, withPub.egressPublication)
	require.Equal(t, int64(nullValue), withPub.egressAddCorrelationID)
	require.False(t, withPub.newLeaderEventPending)
	require.Nil(t, withAdd.egressPublication)
	require.Equal(t, int64(nullValue), withAdd.egressAddCorrelationID)
	require.False(t, withAdd.newLeaderEventPending)
	require.Equal(t, int64(nullValue), preOpen.egressAddCorrelationID)
	require.Nil(t, closing.egressPublication)
	require.Empty(t, a.sessions.okPending)
	require.Len(t, a.sessions.byID, 4)
	require.Equal(t, 2, a.OpenSessionCount())
	require.Equal(t, 1, a.PendingSessionCount())
	require.Equal(t, 1, a.ClosingSessionCount())
	require.Len(t, a.egressEvents, 1)
	require.Contains(t, adder.cancels, withAddCorr)

	// Second call must leave registry membership and the event queue.
	a.releaseSessionsForNewTerm()
	require.Len(t, a.sessions.byID, 4)
	require.Len(t, a.egressEvents, 1)
	require.Empty(t, a.sessions.okPending)
}

func TestPrepareForNewLeadership_releasesSessionsBeforeStop(t *testing.T) {
	adder := neverReadyEgressAdder()
	a := leaderWithEgress(t, nil, 5, 1, adder)
	s := seedNewLeaderPending(t, a, 1)
	pub := attachEgressPub(t, s, true)
	a.sessions.setOKPending(s, true)
	a.queueSessionRedirect(validConnectFields(1), 1)
	above := seedLearnedOpen(t, a, 2, 1, 2000)
	abovePub := attachEgressPub(t, above, true)

	got, prepared, err := a.PrepareForNewLeadership(1234, 99)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, int64(1234), got)
	require.Equal(t, RoleFollower, a.Role())
	require.True(t, pub.IsClosed())
	require.False(t, s.newLeaderEventPending)
	require.Empty(t, a.sessions.okPending)
	_, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Len(t, a.egressEvents, 1)

	// The session opened above the prepare position goes with CLOSED election.
	_, ok = a.sessions.sessionByID(2)
	require.False(t, ok)
	require.Equal(t, []*clientSession{s}, a.sessions.all)
	requireElectionClosed(t, abovePub, above)
}

// requireElectionClosed asserts pub is closed and holds one SessionEvent
// CLOSED with detail election for session, naming member 1 as leader.
func requireElectionClosed(t *testing.T, pub *aeron.Publication, session *clientSession) {
	t.Helper()
	require.True(t, pub.IsClosed())
	got, next := decodeSessionEventAtTermOffset(t, pub, 0)
	require.Zero(t, logbuffer.FrameLengthVolatile(pub.TermBuffer(0), next))
	require.Equal(t, clusterwire.EventCode_CLOSED, got.Code)
	require.Equal(t, []byte("election"), got.Detail)
	require.Equal(t, session.id, got.ClusterSessionID)
	require.Equal(t, session.correlationID, got.CorrelationID)
	require.Equal(t, int32(1), got.LeaderMemberID)
}

// requireSessionIDs asserts the registry holds exactly ids in all, in order,
// and in byID.
func requireSessionIDs(t *testing.T, a *Agent, ids ...int64) {
	t.Helper()
	got := make([]int64, 0, len(a.sessions.all))
	for _, session := range a.sessions.all {
		got = append(got, session.id)
	}
	require.Equal(t, append([]int64{}, ids...), got)
	require.Len(t, a.sessions.byID, len(ids))
	for _, id := range ids {
		_, ok := a.sessions.byID[id]
		require.True(t, ok)
	}
}

func TestClearSessionsAfter(t *testing.T) {
	const prepareAt int64 = 1000

	tests := []struct {
		name string
		run  func(t *testing.T, a *Agent, adder *scriptedEgressAdder)
	}{
		{
			name: "open at the position stays and open above it goes",
			run: func(t *testing.T, a *Agent, _ *scriptedEgressAdder) {
				at := seedLearnedOpen(t, a, 1, 1, prepareAt)
				atPub := attachEgressPub(t, at, true)
				above := seedLearnedOpen(t, a, 2, 1, prepareAt+1)
				abovePub := attachEgressPub(t, above, true)

				a.clearSessionsAfter(prepareAt)
				requireSessionIDs(t, a, 1)
				require.False(t, atPub.IsClosed())
				require.Zero(t, logbuffer.FrameLengthVolatile(atPub.TermBuffer(0), 0))
				requireElectionClosed(t, abovePub, above)
				require.Equal(t, 1, a.OpenSessionCount())
			},
		},
		{
			name: "closing session opened above the position goes",
			run: func(t *testing.T, a *Agent, _ *scriptedEgressAdder) {
				s := seedLearnedOpen(t, a, 1, 1, prepareAt+1)
				pub := attachEgressPub(t, s, true)
				require.True(t, a.sessions.markClosing(s.id, clusterwire.CloseReason_TIMEOUT))

				a.clearSessionsAfter(prepareAt)
				requireSessionIDs(t, a)
				requireElectionClosed(t, pub, s)
				require.Equal(t, 0, a.ClosingSessionCount())
			},
		},
		{
			name: "pre-open sessions go with CLOSED on each egress",
			run: func(t *testing.T, a *Agent, _ *scriptedEgressAdder) {
				pre := make([]*clientSession, 0, 4)
				for id := int64(1); id <= 4; id++ {
					s := a.sessions.addPendingConnect(id, validConnectFields(id), 0)
					require.NotNil(t, s)
					pre = append(pre, s)
				}
				require.True(t, a.sessions.markChallenged(2))
				require.True(t, a.sessions.markAuthenticated(3, nil))
				require.True(t, a.sessions.markRejected(4))
				pubs := make([]*aeron.Publication, 0, len(pre))
				for _, s := range pre {
					pubs = append(pubs, attachEgressPub(t, s, true))
				}

				a.clearSessionsAfter(prepareAt)
				requireSessionIDs(t, a)
				require.Empty(t, a.sessions.pending)
				require.Equal(t, 0, a.PendingSessionCount())
				for i, s := range pre {
					requireElectionClosed(t, pubs[i], s)
				}
			},
		},
		{
			name: "clear cancels an in-flight egress add",
			run: func(t *testing.T, a *Agent, adder *scriptedEgressAdder) {
				s := a.sessions.addPendingConnect(1, validConnectFields(1), 0)
				require.NotNil(t, s)
				require.NoError(t, a.startSessionEgress(s))
				addCorrelationID := s.egressAddCorrelationID
				require.NotEqual(t, int64(nullValue), addCorrelationID)

				a.clearSessionsAfter(prepareAt)
				requireSessionIDs(t, a)
				require.Equal(t, []int64{addCorrelationID}, adder.cancels)
				require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
			},
		},
		{
			name: "egress-event queue keeps its entries",
			run: func(t *testing.T, a *Agent, adder *scriptedEgressAdder) {
				a.queueSessionRedirect(validConnectFields(1), 0)
				require.Len(t, a.egressEvents, 1)

				a.clearSessionsAfter(prepareAt)
				require.Len(t, a.egressEvents, 1)
				require.Empty(t, adder.cancels)
			},
		},
		{
			name: "sessions at or below the position stay",
			run: func(t *testing.T, a *Agent, _ *scriptedEgressAdder) {
				seedLearnedOpen(t, a, 1, 1, 8)
				closing := seedLearnedOpen(t, a, 2, 1, prepareAt)
				require.True(t, a.sessions.markClosing(closing.id, clusterwire.CloseReason_TIMEOUT))
				nextSessionID := a.sessions.nextSessionID

				a.clearSessionsAfter(prepareAt)
				requireSessionIDs(t, a, 1, 2)
				require.Equal(t, 1, a.OpenSessionCount())
				require.Equal(t, 1, a.ClosingSessionCount())
				require.Equal(t, nextSessionID, a.sessions.nextSessionID)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adder := neverReadyEgressAdder()
			a := leaderWithEgress(t, nil, 5, 1, adder)
			tt.run(t, a, adder)
		})
	}
}

func TestStartSessionEgress_offerErrorOnOpenLeavesSession(t *testing.T) {
	adder := newScriptedEgressAdder()
	adder.offerErr = errors.New("invalid channel")
	a := leaderWithEgress(t, nil, 1, 1, adder)
	s := seedNewLeaderPending(t, a, 1)
	require.Error(t, a.startSessionEgress(s))

	require.Equal(t, sessionOpen, s.state)
	require.True(t, s.newLeaderEventPending)
	require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
	_, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
}
