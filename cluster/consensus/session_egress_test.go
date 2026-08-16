//go:build testhooks

package consensus

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// scriptedEgressAdder drives async publication adds without a media driver.
//
// Per-correlation outcomes, in poll order:
//   - N not-done polls
//   - one optional not-done error
//   - ready with a publication, or a terminal error
type scriptedEgressAdder struct {
	nextCorr int64
	// pendingPolls maps correlation id to remaining not-done polls.
	pendingPolls map[int64]int
	// pubs maps correlation id to the publication returned when ready.
	pubs map[int64]*aeron.Publication
	// pollErrs maps correlation id to a terminal poll error.
	pollErrs map[int64]error
	// pollNotDoneErrs maps correlation id to a not-done poll error.
	// Models PollAddPublication (nil, false, err) when the pending is gone.
	pollNotDoneErrs map[int64]error
	// defaultPub is used when no per-corr pub is set. Nil means ready with a
	// nil publication (treated as terminal by pollSessionEgress).
	defaultPub func() *aeron.Publication
	// defaultPending is applied to new offers when pendingPolls has no entry.
	defaultPending int
	offerErr       error
	offers         []egressOfferRecord
	cancels        []int64
}

type egressOfferRecord struct {
	streamID int32
	channel  string
	corr     int64
}

func newScriptedEgressAdder() *scriptedEgressAdder {
	return &scriptedEgressAdder{
		pendingPolls:    make(map[int64]int),
		pubs:            make(map[int64]*aeron.Publication),
		pollErrs:        make(map[int64]error),
		pollNotDoneErrs: make(map[int64]error),
	}
}

// withImmediateConnected returns an adder that resolves to a connected
// unlimited concurrent publication on the first poll.
func withImmediateConnected(t testing.TB) *scriptedEgressAdder {
	t.Helper()
	a := newScriptedEgressAdder()
	a.defaultPub = func() *aeron.Publication {
		return aeron.NewTestPublicationForHooksWithConnected(t, true)
	}
	return a
}

// neverReadyEgressAdder returns an adder that stays not-done for the test
// window (defaultPending high enough that reclaim/deadline paths fire first).
func neverReadyEgressAdder() *scriptedEgressAdder {
	a := newScriptedEgressAdder()
	a.defaultPending = 100
	return a
}

func (s *scriptedEgressAdder) OfferAddPublication(
	streamID int32,
	channel string,
) (int64, error) {
	if s.offerErr != nil {
		return 0, s.offerErr
	}
	s.nextCorr++
	corr := s.nextCorr
	s.offers = append(s.offers, egressOfferRecord{
		streamID: streamID,
		channel:  channel,
		corr:     corr,
	})
	if _, ok := s.pendingPolls[corr]; !ok {
		s.pendingPolls[corr] = s.defaultPending
	}
	return corr, nil
}

func (s *scriptedEgressAdder) PollAddPublication(
	correlationID int64,
) (*aeron.Publication, bool, error) {
	if n, ok := s.pendingPolls[correlationID]; ok && n > 0 {
		s.pendingPolls[correlationID] = n - 1
		return nil, false, nil
	}
	delete(s.pendingPolls, correlationID)
	if err, ok := s.pollNotDoneErrs[correlationID]; ok {
		delete(s.pollNotDoneErrs, correlationID)
		return nil, false, err
	}
	if err, ok := s.pollErrs[correlationID]; ok {
		return nil, true, err
	}
	if pub, ok := s.pubs[correlationID]; ok {
		return pub, true, nil
	}
	if s.defaultPub != nil {
		return s.defaultPub(), true, nil
	}
	return nil, true, nil
}

func (s *scriptedEgressAdder) CancelPending(correlationID int64) {
	s.cancels = append(s.cancels, correlationID)
	delete(s.pendingPolls, correlationID)
	delete(s.pubs, correlationID)
	delete(s.pollErrs, correlationID)
	delete(s.pollNotDoneErrs, correlationID)
}

// setNextPub queues the next offer to resolve to pub after pending polls.
func (s *scriptedEgressAdder) setNextPub(pub *aeron.Publication, pending int) {
	// nextCorr+1 is the next allocated correlation id.
	corr := s.nextCorr + 1
	s.pubs[corr] = pub
	s.pendingPolls[corr] = pending
}

// scriptedConnectedPub returns an adder whose next offer resolves to a
// connected unlimited concurrent publication after pending not-done polls.
func scriptedConnectedPub(
	t testing.TB,
	pending int,
) (*scriptedEgressAdder, *aeron.Publication) {
	t.Helper()
	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	adder := newScriptedEgressAdder()
	adder.setNextPub(pub, pending)
	return adder, pub
}

// terminalFirstOfferAdder returns an adder that fails the first poll of
// correlation id 1.
func terminalFirstOfferAdder() *scriptedEgressAdder {
	a := newScriptedEgressAdder()
	a.pollErrs[1] = errors.New("registration failed")
	return a
}

// interruptedFirstOfferAdder returns an adder whose first poll of
// correlation id 1 is not-done with ErrAwaitInterrupted.
func interruptedFirstOfferAdder() *scriptedEgressAdder {
	a := newScriptedEgressAdder()
	a.pollNotDoneErrs[1] = aeron.ErrAwaitInterrupted
	return a
}

// leaderNeverReady returns a settled leader with a never-ready egress adder.
func leaderNeverReady(t testing.TB) (*Agent, *scriptedEgressAdder) {
	t.Helper()
	p, _ := boundLogPublisher(t)
	adder := neverReadyEgressAdder()
	return leaderWithEgress(t, p, 0, 0, adder), adder
}

// leaderImmediateAccept returns a settled leader with immediate egress and no
// log publisher. Accept and reject paths use this surface.
func leaderImmediateAccept(t testing.TB) *Agent {
	t.Helper()
	return leaderWithEgress(t, nil, 0, 0, withImmediateConnected(t))
}

func fillSessionsToCap(a *Agent, nowNs int64) {
	for range maxConcurrentSessions {
		a.acceptSessionConnect(validConnectFields(1), nowNs)
	}
}

func requireSessionCounts(t *testing.T, a *Agent, pending, open int) {
	t.Helper()
	require.Equal(t, pending, a.PendingSessionCount())
	require.Equal(t, open, a.OpenSessionCount())
}

func requireInvalidVersionDetail(t *testing.T, got []byte, version int32) {
	t.Helper()
	require.Equal(
		t,
		[]byte(invalidVersionDetail(version, codecs.ClusterProtocolSemanticVersion())),
		got,
	)
}

// requireDeadlineReclaim asserts one sessionWork tick just before the deadline
// and one at the deadline. beforeHeld runs after the pre-deadline tick.
// afterReclaim runs after reclaim. When cancelCorr is non-nil, require that
// correlation id in adder.cancels.
func requireDeadlineReclaim(
	t *testing.T,
	a *Agent,
	adder *scriptedEgressAdder,
	startNs int64,
	beforeHeld func(),
	afterReclaim func(),
	cancelCorr *int64,
) {
	t.Helper()
	// Before deadline: not-done poll counts as work; no reclaim.
	require.Equal(t, 1, a.sessionWork(startNs+DefaultSessionTimeoutNs-1))
	beforeHeld()

	// At deadline: reclaim.
	require.Equal(t, 1, a.sessionWork(startNs+DefaultSessionTimeoutNs))
	afterReclaim()
	if cancelCorr != nil {
		require.Contains(t, adder.cancels, *cancelCorr)
	}
}

// requireElectionHoldPastDeadline asserts sessionWork is 0 while an election is
// live past the deadline, then reclaim on the first settled tick.
func requireElectionHoldPastDeadline(
	t *testing.T,
	a *Agent,
	startNs int64,
	held func(),
	afterReclaim func(),
	reclaimWork int,
) {
	t.Helper()
	a.election = &recordingRunner{}
	pastDeadline := startNs + DefaultSessionTimeoutNs + 1
	require.Equal(t, 0, a.sessionWork(pastDeadline))
	held()

	a.election = nil
	require.Equal(t, reclaimWork, a.sessionWork(pastDeadline))
	afterReclaim()
}

// decodeClusterFieldsAtTermOffset decodes one cluster SBE DATA frame at
// termOffset. Asserts the template id. Returns the typed fields and the next
// aligned term offset.
func decodeClusterFieldsAtTermOffset[T any](
	t *testing.T,
	pub *aeron.Publication,
	termOffset int,
	templateID uint16,
) (T, int) {
	t.Helper()
	wire, next := termWireAt(t, pub, termOffset)
	decoded, ok := codecs.TryDecodeByTemplate(wire, 0, codecs.ClusterDecodeMaxVarLen)
	require.True(t, ok)
	require.Equal(t, templateID, decoded.TemplateID)
	fields, ok := decoded.Fields.(T)
	require.True(t, ok)
	return fields, next
}

func decodeSessionEventAtTermOffset(
	t *testing.T,
	pub *aeron.Publication,
	termOffset int,
) (codecs.SessionEventFields, int) {
	t.Helper()
	return decodeClusterFieldsAtTermOffset[codecs.SessionEventFields](
		t,
		pub,
		termOffset,
		clusterwire.SessionEventSbeTemplateID,
	)
}

func requireSessionEventOK(
	t *testing.T,
	got codecs.SessionEventFields,
	sessionID, correlationID, termID int64,
	leaderMemberID int32,
	heartbeatNs int64,
) {
	t.Helper()
	require.Equal(t, sessionID, got.ClusterSessionID)
	require.Equal(t, correlationID, got.CorrelationID)
	require.Equal(t, termID, got.LeadershipTermID)
	require.Equal(t, leaderMemberID, got.LeaderMemberID)
	require.Equal(t, clusterwire.EventCode_OK, got.Code)
	require.Equal(t, codecs.ClusterProtocolSemanticVersion(), got.Version)
	require.Equal(t, heartbeatNs, got.LeaderHeartbeatTimeoutNs)
	require.Empty(t, got.Detail)
}

func leaderWithEgress(
	t testing.TB,
	p *LogPublisher,
	leadershipTermID int64,
	thisMemberID int32,
	adder EgressPublicationAdder,
) *Agent {
	t.Helper()
	a := NewAgent(AgentConfig{
		Role:             RoleLeader,
		LeadershipTermID: leadershipTermID,
		ThisMemberID:     thisMemberID,
		LogPublisher:     p,
	})
	a.SetEgressPublicationAdder(adder)
	return a
}

func followerWithEgress(
	t testing.TB,
	adder EgressPublicationAdder,
	leaderMemberID int32,
) *Agent {
	t.Helper()
	a := NewAgent(AgentConfig{
		Role:           RoleFollower,
		LeaderMemberID: leaderMemberID,
	})
	a.SetEgressPublicationAdder(adder)
	return a
}

func TestSessionEgress_happyPath_OK(t *testing.T) {
	p, logPub := boundLogPublisher(t)
	// Capture the egress pub by scripting one explicit publication.
	adder, egressPub := scriptedConnectedPub(t, 0)

	const thisMember int32 = 3
	a := leaderWithEgress(t, p, 11, thisMember, adder)
	fields := validConnectFields(22)
	const nowNs int64 = 44
	a.acceptSessionConnect(fields, nowNs)

	require.Len(t, adder.offers, 1)
	require.Equal(t, fields.ResponseStreamID, adder.offers[0].streamID)
	require.Equal(t, string(fields.ResponseChannel), adder.offers[0].channel)

	work := a.sessionWork(nowNs)
	require.GreaterOrEqual(t, work, 2) // open + OK
	requireSessionCounts(t, a, 0, 1)

	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionOpen, s.state)
	require.Empty(t, a.sessions.okPending)

	decoded, _ := decodeSessionOpenAtTermOffset(t, logPub, 0)
	requireSessionOpenFields(
		t, decoded, 11, 22, 1, nowNs, fields.ResponseStreamID, fields.ResponseChannel,
	)

	evt, _ := decodeSessionEventAtTermOffset(t, egressPub, 0)
	requireSessionEventOK(t, evt, 1, 22, 11, thisMember, a.leaderHeartbeatTimeoutNs)
}

func TestSessionEgress_notReady_noAppend(t *testing.T) {
	p, _ := boundLogPublisher(t)
	adder := withImmediateConnected(t)
	adder.defaultPending = 5
	a := leaderWithEgress(t, p, 0, 0, adder)
	a.acceptSessionConnect(validConnectFields(1), 1)

	for range 3 {
		// Not-done poll contacts the adder and counts as work.
		require.Equal(t, 1, a.sessionWork(10))
		requireSessionCounts(t, a, 1, 0)
	}
}

func TestSessionEgress_windowGate(t *testing.T) {
	p, _ := boundLogPublisher(t)
	adder := newScriptedEgressAdder()
	// Connected with limit 0: AvailableWindow <= 0, no append.
	zeroWin := aeron.NewTestPublicationForHooksWithLimit(t, 0, true)
	adder.setNextPub(zeroWin, 0)
	a := leaderWithEgress(t, p, 0, 0, adder)
	a.acceptSessionConnect(validConnectFields(1), 1)

	require.Equal(t, 0, a.sessionWork(10))
	requireSessionCounts(t, a, 1, 0)

	// Lift the gate by swapping a ready unlimited publication onto the session.
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	s.egressPublication = aeron.NewTestPublicationForHooksWithConnected(t, true)

	require.Equal(t, 2, a.sessionWork(20)) // open + OK
	requireSessionCounts(t, a, 0, 1)
}

func TestSessionEgress_readinessOrder(t *testing.T) {
	p, logPub := boundLogPublisher(t)
	// Session 1: stall 2 polls. Session 2: ready immediately.
	adder, _ := scriptedConnectedPub(t, 2)
	ready := aeron.NewTestPublicationForHooksWithConnected(t, true)
	a := leaderWithEgress(t, p, 5, 1, adder)

	a.acceptSessionConnect(validConnectFields(1), 1)
	adder.setNextPub(ready, 0)
	a.acceptSessionConnect(validConnectFields(2), 2)

	// First tick: session 1 not-done poll (work 1); session 2 opens + OK (work 2).
	require.Equal(t, 3, a.sessionWork(10))
	requireSessionCounts(t, a, 1, 1)
	require.Equal(t, int64(1), a.sessions.pending[0].id)
	_, ok := a.sessions.sessionByID(2)
	require.True(t, ok)

	// Drain stall polls on session 1.
	require.Equal(t, 1, a.sessionWork(11)) // still one pending poll
	require.Equal(t, 2, a.sessionWork(12)) // open session 1 + OK
	requireSessionCounts(t, a, 0, 2)

	// Log open order is readiness order: session 2 then session 1.
	d1, next := decodeSessionOpenAtTermOffset(t, logPub, 0)
	require.Equal(t, int64(2), d1.ClusterSessionID)
	d2, _ := decodeSessionOpenAtTermOffset(t, logPub, next)
	require.Equal(t, int64(1), d2.ClusterSessionID)
}

func TestSessionEgress_logBackPressureHoldsQueue(t *testing.T) {
	p := boundLogPublisherWithLimit(t, 0, true)
	adder := withImmediateConnected(t)
	a := leaderWithEgress(t, p, 0, 0, adder)
	a.acceptSessionConnect(validConnectFields(1), 1)
	a.acceptSessionConnect(validConnectFields(2), 2)

	require.Equal(t, 0, a.sessionWork(10))
	requireSessionCounts(t, a, 2, 0)
}

func TestSessionEgress_publicationWideTerminalHoldsQueue(t *testing.T) {
	p, pub := boundLogPublisher(t)
	require.NoError(t, pub.Close())
	adder := withImmediateConnected(t)
	a := leaderWithEgress(t, p, 0, 0, adder)
	a.acceptSessionConnect(validConnectFields(1), 1)
	a.acceptSessionConnect(validConnectFields(2), 2)

	require.Equal(t, 0, a.sessionWork(10))
	require.Equal(t, 2, a.PendingSessionCount())
	require.Equal(t, int64(1), a.sessions.pending[0].id)
	require.Equal(t, int64(2), a.sessions.pending[1].id)
}

func TestSessionEgress_perSessionTerminalContinues(t *testing.T) {
	p, logPub := boundLogPublisher(t)
	adder := withImmediateConnected(t)
	a := leaderWithEgress(t, p, 4, 0, adder)

	emptyFixed, ok := codecs.ClusterWireMessageLen(
		clusterwire.SessionOpenEventSbeBlockLength, 0, 0,
	)
	require.True(t, ok)
	oversize := make([]byte, logPub.MaxMessageLength()-emptyFixed+1)
	head := validConnectFields(1)
	head.ResponseChannel = oversize
	tail := validConnectFields(2)
	a.acceptSessionConnect(head, 1)
	a.acceptSessionConnect(tail, 2)

	const nowNs int64 = 60
	// Terminal remove of head (work 1) + open and OK of tail (work 2).
	require.Equal(t, 3, a.sessionWork(nowNs))
	requireSessionCounts(t, a, 0, 1)
	_, headOK := a.sessions.sessionByID(1)
	require.False(t, headOK)
}

func TestSessionEgress_pollAddTerminalRemoves(t *testing.T) {
	p, _ := boundLogPublisher(t)
	// First offer gets corr 1.
	adder := terminalFirstOfferAdder()
	a := leaderWithEgress(t, p, 0, 0, adder)
	a.acceptSessionConnect(validConnectFields(1), 1)
	require.Equal(t, 1, a.PendingSessionCount())

	// Poll terminal: session removed, no leak.
	require.Equal(t, 1, a.sessionWork(10))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Empty(t, a.sessions.byID)
}

func TestSessionEgress_pollAddInterruptedRemoves(t *testing.T) {
	p, _ := boundLogPublisher(t)
	adder := interruptedFirstOfferAdder()
	a := leaderWithEgress(t, p, 0, 0, adder)
	a.acceptSessionConnect(validConnectFields(1), 1)
	require.Equal(t, 1, a.PendingSessionCount())

	// Not-done err clears the correlation id. First-connect is fail-closed.
	require.Equal(t, 1, a.sessionWork(10))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Empty(t, a.sessions.byID)
}

func TestSessionEgress_pendingDeadlineReclaim(t *testing.T) {
	a, adder := leaderNeverReady(t)
	const acceptNs int64 = 100
	a.acceptSessionConnect(validConnectFields(1), acceptNs)
	require.Equal(t, 1, a.PendingSessionCount())
	require.Len(t, adder.offers, 1)
	corr := adder.offers[0].corr

	requireDeadlineReclaim(
		t, a, adder, acceptNs,
		func() { require.Equal(t, 1, a.PendingSessionCount()) },
		func() {
			require.Equal(t, 0, a.PendingSessionCount())
			require.Empty(t, a.sessions.byID)
		},
		&corr,
	)
}

func TestSessionEgress_leadershipLoss_deadlineReclaim(t *testing.T) {
	a, adder := leaderNeverReady(t)
	const acceptNs int64 = 50
	a.acceptSessionConnect(validConnectFields(1), acceptNs)
	a.acceptSessionConnect(validConnectFields(2), acceptNs)
	require.Equal(t, 2, a.PendingSessionCount())

	// Demote to follower. egressEventWork still runs; deadline not reached so
	// no work. Follower does not poll session egress adds.
	a.role = RoleFollower
	require.Equal(t, 0, a.sessionWork(acceptNs+1))
	require.Equal(t, 2, a.PendingSessionCount())

	require.Equal(t, 2, a.sessionWork(acceptNs+DefaultSessionTimeoutNs))
	require.Equal(t, 0, a.PendingSessionCount())
	require.Empty(t, a.sessions.byID)
	require.Len(t, adder.cancels, 2)
}

func TestSessionEgress_electionHold_thenReclaim(t *testing.T) {
	a, _ := leaderNeverReady(t)
	const acceptNs int64 = 10
	a.acceptSessionConnect(validConnectFields(1), acceptNs)

	requireElectionHoldPastDeadline(
		t, a, acceptNs,
		func() { require.Equal(t, 1, a.PendingSessionCount()) },
		func() { require.Equal(t, 0, a.PendingSessionCount()) },
		1,
	)
}

func TestSessionEgress_electionHold_eventQueue_thenReclaim(t *testing.T) {
	adder := neverReadyEgressAdder()
	a := followerWithEgress(t, adder, 1)
	const queueNs int64 = 10
	a.queueSessionRedirect(validConnectFields(1), queueNs)
	require.Len(t, a.egressEvents, 1)
	corr := adder.offers[0].corr

	requireElectionHoldPastDeadline(
		t, a, queueNs,
		func() { require.Len(t, a.egressEvents, 1) },
		func() {
			require.Empty(t, a.egressEvents)
			require.Contains(t, adder.cancels, corr)
		},
		1,
	)
}

func TestSessionEgress_sameTickOK(t *testing.T) {
	p, _ := boundLogPublisher(t)
	adder, egressPub := scriptedConnectedPub(t, 0)
	a := leaderWithEgress(t, p, 9, 2, adder)
	fields := validConnectFields(7)
	a.acceptSessionConnect(fields, 1)

	work := a.sessionWork(100)
	require.Equal(t, 2, work) // open + OK
	_, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Empty(t, a.sessions.okPending)

	evt, _ := decodeSessionEventAtTermOffset(t, egressPub, 0)
	requireSessionEventOK(t, evt, 1, 7, 9, 2, a.leaderHeartbeatTimeoutNs)
}

func TestSessionEgress_OKRetry(t *testing.T) {
	p, _ := boundLogPublisher(t)
	// Connected unlimited egress so the append gate passes. After markOpen,
	// swap a limit-0 publication so the OK send is retriable, then raise for
	// retry.
	adder, _ := scriptedConnectedPub(t, 0)
	a := leaderWithEgress(t, p, 3, 1, adder)
	a.acceptSessionConnect(validConnectFields(9), 1)

	// sessionPendingWork only: open and append to okPending. No OK yet.
	require.Equal(t, 1, a.sessionPendingWork(10))
	requireSessionCounts(t, a, 0, 1)
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Len(t, a.sessions.okPending, 1)
	require.Same(t, s, a.sessions.okPending[0])
	openPos := s.openedLogPosition
	require.Positive(t, openPos)

	// Limit 0 after append: OK send is retriable. The okPending entry is held. The
	// open mark stays.
	blocked := aeron.NewTestPublicationForHooksWithLimit(t, 0, true)
	s.egressPublication = blocked
	require.Equal(t, 0, a.okPendingWork())
	require.Len(t, a.sessions.okPending, 1)
	require.Same(t, s, a.sessions.okPending[0])
	require.Equal(t, sessionOpen, s.state)
	require.Equal(t, openPos, s.openedLogPosition)

	// Raise limit: swap unlimited publication; next tick sends OK.
	okPub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	s.egressPublication = okPub
	require.Equal(t, 1, a.okPendingWork())
	require.Empty(t, a.sessions.okPending)
	require.Equal(t, sessionOpen, s.state)
	require.Equal(t, openPos, s.openedLogPosition)

	evt, _ := decodeSessionEventAtTermOffset(t, okPub, 0)
	requireSessionEventOK(t, evt, 1, 9, 3, 1, a.leaderHeartbeatTimeoutNs)
}

// TestSessionEgress_OKTerminalRelease asserts a terminal offer on okPending
// releases the dead egress handle, clears okPending, and keeps the session
// open (SessionOpenEvent already on the log).
func TestSessionEgress_OKTerminalRelease(t *testing.T) {
	p, _ := boundLogPublisher(t)
	adder, _ := scriptedConnectedPub(t, 0)
	a := leaderWithEgress(t, p, 3, 1, adder)
	a.acceptSessionConnect(validConnectFields(9), 1)

	require.Equal(t, 1, a.sessionPendingWork(10))
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Len(t, a.sessions.okPending, 1)
	openPos := s.openedLogPosition
	require.Positive(t, openPos)

	// Closed publication: Offer returns Closed (terminal under IsOfferRetriable).
	closed := aeron.NewTestPublicationForHooksWithConnected(t, true)
	require.NoError(t, closed.Close())
	s.egressPublication = closed

	require.Equal(t, 1, a.okPendingWork())
	require.Empty(t, a.sessions.okPending)
	require.Equal(t, sessionOpen, s.state)
	require.Equal(t, openPos, s.openedLogPosition)
	require.Nil(t, s.egressPublication)
	require.True(t, closed.IsClosed())
	require.Equal(t, 1, a.OpenSessionCount())
	_, still := a.sessions.sessionByID(1)
	require.True(t, still)
}

func TestSessionEgress_redirect(t *testing.T) {
	adder, egressPub := scriptedConnectedPub(t, 0)

	const leaderID int32 = 7
	a := followerWithEgress(t, adder, leaderID)
	a.thisMemberID = 1
	a.SetIngressEndpoints("0=host:1,1=host:2")

	fields := validConnectFields(42)
	a.queueSessionRedirect(fields, 100)
	require.Len(t, a.egressEvents, 1)
	require.Equal(t, int64(nullValue), a.egressEvents[0].clusterSessionID)
	require.Len(t, adder.offers, 1)
	require.Equal(t, "aeron:udp?endpoint=127.0.0.1:40123|linger=0", adder.offers[0].channel)

	require.Equal(t, 1, a.sessionWork(100))
	require.Empty(t, a.egressEvents)
	require.True(t, egressPub.IsClosed())

	evt, _ := decodeSessionEventAtTermOffset(t, egressPub, 0)
	require.Equal(t, int64(nullValue), evt.ClusterSessionID)
	require.Equal(t, int64(42), evt.CorrelationID)
	require.Equal(t, leaderID, evt.LeaderMemberID)
	require.Equal(t, clusterwire.EventCode_REDIRECT, evt.Code)
	require.Equal(t, []byte("0=host:1,1=host:2"), evt.Detail)
	require.Equal(t, codecs.ClusterProtocolSemanticVersion(), evt.Version)
}

func TestSessionEgress_redirect_leaderHintAtSendTime(t *testing.T) {
	// One pending poll so we can change the hint before send.
	adder, egressPub := scriptedConnectedPub(t, 1)

	a := followerWithEgress(t, adder, 1)
	a.SetIngressEndpoints("0=a:1")
	a.queueSessionRedirect(validConnectFields(1), 10)

	// First tick: not-done poll counts as work.
	require.Equal(t, 1, a.sessionWork(10))
	// Hint changes before send.
	a.leaderMemberID = 9
	require.Equal(t, 1, a.sessionWork(11))

	evt, _ := decodeSessionEventAtTermOffset(t, egressPub, 0)
	require.Equal(t, int32(9), evt.LeaderMemberID)
}

func TestSessionEgress_eventQueueCap(t *testing.T) {
	adder := withImmediateConnected(t)
	a := followerWithEgress(t, adder, 1)

	for range maxPendingEgressEvents {
		a.queueSessionRedirect(validConnectFields(1), 1)
	}
	require.Len(t, a.egressEvents, maxPendingEgressEvents)
	offersAtCap := len(adder.offers)

	// Over-cap: drop without an add.
	a.queueSessionRedirect(validConnectFields(2), 1)
	require.Len(t, a.egressEvents, maxPendingEgressEvents)
	require.Len(t, adder.offers, offersAtCap)
}

func TestSessionEgress_eventDeadline(t *testing.T) {
	adder := neverReadyEgressAdder()
	a := followerWithEgress(t, adder, 1)
	const queueNs int64 = 5
	a.queueSessionRedirect(validConnectFields(1), queueNs)
	require.Len(t, a.egressEvents, 1)
	corr := adder.offers[0].corr

	requireDeadlineReclaim(
		t, a, adder, queueNs,
		func() { require.Len(t, a.egressEvents, 1) },
		func() { require.Empty(t, a.egressEvents) },
		&corr,
	)
}

func TestSessionEgress_eventPollTerminalDrops(t *testing.T) {
	// First offer gets corr 1; terminal on first poll.
	adder := terminalFirstOfferAdder()
	a := followerWithEgress(t, adder, 1)
	a.queueSessionRedirect(validConnectFields(1), 1)
	require.Len(t, a.egressEvents, 1)

	// Terminal poll drops immediately; no wait for deadline.
	require.Equal(t, 1, a.sessionWork(1))
	require.Empty(t, a.egressEvents)
}

func TestSessionEgress_eventPollInterruptedHolds(t *testing.T) {
	adder := interruptedFirstOfferAdder()
	a := followerWithEgress(t, adder, 1)
	const startNs int64 = 5
	a.queueSessionRedirect(validConnectFields(1), startNs)
	require.Len(t, a.egressEvents, 1)

	require.Equal(t, 0, a.sessionWork(startNs+DefaultSessionTimeoutNs-1))
	require.Len(t, a.egressEvents, 1)
	require.Equal(t, int64(nullValue), a.egressEvents[0].addCorrelationID)
	require.Len(t, adder.offers, 1)
	require.Empty(t, adder.cancels)

	require.Equal(t, 1, a.sessionWork(startNs+DefaultSessionTimeoutNs))
	require.Empty(t, a.egressEvents)
	require.Empty(t, adder.cancels)
}

func TestEventEgressPublicationChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "linger absent appends linger=0",
			in:   "aeron:udp?endpoint=127.0.0.1:40123",
			want: "aeron:udp?endpoint=127.0.0.1:40123|linger=0",
		},
		{
			name: "linger present unchanged",
			in:   "aeron:udp?endpoint=127.0.0.1:40123|linger=5",
			want: "aeron:udp?endpoint=127.0.0.1:40123|linger=5",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, eventEgressPublicationChannel([]byte(tc.in)))
		})
	}
}

func TestSessionEgress_eventSendRetry(t *testing.T) {
	adder := newScriptedEgressAdder()
	// Connected with limit 0: cannot send until swapped.
	tiny := aeron.NewTestPublicationForHooksWithLimit(t, 0, true)
	adder.setNextPub(tiny, 0)
	a := followerWithEgress(t, adder, 2)
	a.SetIngressEndpoints("0=x:1")
	a.queueSessionRedirect(validConnectFields(3), 1)

	// Publication ready but the send is retriable. The entry is held.
	require.Equal(t, 0, a.sessionWork(1))
	require.Len(t, a.egressEvents, 1)

	ready := aeron.NewTestPublicationForHooksWithConnected(t, true)
	a.egressEvents[0].publication = ready
	require.Equal(t, 1, a.sessionWork(2))
	require.Empty(t, a.egressEvents)

	evt, _ := decodeSessionEventAtTermOffset(t, ready, 0)
	require.Equal(t, clusterwire.EventCode_REDIRECT, evt.Code)
	require.Equal(t, int32(2), evt.LeaderMemberID)
}

// TestSessionEgress_eventSendTerminalDrops asserts a terminal offer on a
// REDIRECT entry drops the queue slot before the deadline.
func TestSessionEgress_eventSendTerminalDrops(t *testing.T) {
	adder := newScriptedEgressAdder()
	closed := aeron.NewTestPublicationForHooksWithConnected(t, true)
	require.NoError(t, closed.Close())
	adder.setNextPub(closed, 0)
	a := followerWithEgress(t, adder, 2)
	a.SetIngressEndpoints("0=x:1")
	a.queueSessionRedirect(validConnectFields(3), 1)
	require.Len(t, a.egressEvents, 1)

	// Closed publication: terminal offer drops immediately. Does not wait for
	// the deadline.
	require.Equal(t, 1, a.sessionWork(1))
	require.Empty(t, a.egressEvents)
	require.True(t, closed.IsClosed())
}

func TestCloseSessionEgress(t *testing.T) {
	adder := neverReadyEgressAdder() // leave first session add in flight
	p, _ := boundLogPublisher(t)
	a := leaderWithEgress(t, p, 0, 0, adder)

	a.acceptSessionConnect(validConnectFields(1), 1)
	s1, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.NotEqual(t, int64(nullValue), s1.egressAddCorrelationID)
	inFlightCorr := s1.egressAddCorrelationID

	// Second session: resolved publication, still pending in the registry.
	openPub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	adder.defaultPending = 0
	adder.setNextPub(openPub, 0)
	a.acceptSessionConnect(validConnectFields(2), 2)
	s2, ok := a.sessions.sessionByID(2)
	require.True(t, ok)
	ready, removed, pollWork := a.pollSessionEgress(s2)
	require.True(t, ready)
	require.False(t, removed)
	require.Equal(t, 0, pollWork)
	require.Same(t, openPub, s2.egressPublication)

	// Authenticate then markOpen session 2. Leave the session on okPending;
	// skip okPendingWork.
	require.True(t, a.sessions.markAuthenticated(s2.id, nil))
	require.True(t, a.sessions.markOpen(s2.id, 4096))
	require.Len(t, a.sessions.okPending, 1)
	require.Same(t, s2, a.sessions.okPending[0])

	// Queue a redirect event with in-flight add.
	a.role = RoleFollower
	a.leaderMemberID = 1
	adder.defaultPending = 10
	a.queueSessionRedirect(validConnectFields(99), 20)
	require.Len(t, a.egressEvents, 1)
	eventCorr := a.egressEvents[0].addCorrelationID
	require.NotEqual(t, int64(nullValue), eventCorr)

	a.CloseSessionEgress()
	for _, s := range a.sessions.byID {
		require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
		require.Nil(t, s.egressPublication)
	}
	require.Empty(t, a.sessions.okPending)
	require.Empty(t, a.egressEvents)
	// Registry keeps sessions.
	require.Len(t, a.sessions.byID, 2)
	require.True(t, openPub.IsClosed())
	require.Contains(t, adder.cancels, inFlightCorr)
	require.Contains(t, adder.cancels, eventCorr)

	// Idempotent second call.
	require.NotPanics(t, func() { a.CloseSessionEgress() })
	require.Len(t, a.sessions.byID, 2)
	require.Empty(t, a.sessions.okPending)
}

// TestCloseSessionEgress_moduleCloseOrder mirrors ConsensusModule.Close binding
// teardown on a bare agent. Full OpenConsensusModule and module.Close need a
// connected media driver and archive control client; this package has no light
// fixture for that path.
func TestCloseSessionEgress_moduleCloseOrder(t *testing.T) {
	adder := newScriptedEgressAdder()
	adder.defaultPending = 10
	p, _ := boundLogPublisher(t)
	a := leaderWithEgress(t, p, 0, 0, adder)
	a.acceptSessionConnect(validConnectFields(1), 1)
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	corr := s.egressAddCorrelationID
	require.NotEqual(t, int64(nullValue), corr)

	// Close order: ingress unbind, session egress teardown, adder clear.
	a.SetClientIngress(nil)
	a.SetSessionConnectHandler(nil)
	a.CloseSessionEgress()
	a.SetEgressPublicationAdder(nil)

	require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
	require.Nil(t, s.egressPublication)
	require.Contains(t, adder.cancels, corr)
	require.Nil(t, a.egressAdder)
	// Registry retained after close-order unbind.
	require.Len(t, a.sessions.byID, 1)
}

func TestAcceptSessionConnect_versionMismatch(t *testing.T) {
	a := leaderImmediateAccept(t)

	fields := validConnectFields(1)
	fields.Version = cnc.ComposeSemanticVersion(1, 0, 0)
	a.acceptSessionConnect(fields, 10)

	require.Equal(t, 0, a.PendingSessionCount())
	require.Empty(t, a.sessions.byID)
	require.Equal(t, int64(2), a.sessions.nextSessionID) // id 1 consumed
	require.Len(t, a.egressEvents, 1)
	e := a.egressEvents[0]
	require.Equal(t, int64(1), e.clusterSessionID)
	require.Equal(t, clusterwire.EventCode_ERROR, e.code)
	requireInvalidVersionDetail(t, e.detail, fields.Version)
}

func TestAcceptSessionConnect_overCap(t *testing.T) {
	a := leaderImmediateAccept(t)

	fillSessionsToCap(a, 1)
	require.Equal(t, maxConcurrentSessions, a.PendingSessionCount())
	require.Empty(t, a.egressEvents)

	nextID := a.sessions.nextSessionID
	a.acceptSessionConnect(validConnectFields(2), 2)
	require.Equal(t, maxConcurrentSessions, a.PendingSessionCount())
	require.Equal(t, nextID+1, a.sessions.nextSessionID) // id consumed
	require.Len(t, a.egressEvents, 1)
	require.Equal(t, clusterwire.EventCode_ERROR, a.egressEvents[0].code)
	require.Equal(t, []byte(sessionLimitDetail), a.egressEvents[0].detail)
}

func TestAcceptSessionConnect_versionBeforeCap(t *testing.T) {
	a := leaderImmediateAccept(t)
	fillSessionsToCap(a, 1)
	require.Empty(t, a.egressEvents)

	fields := validConnectFields(9)
	fields.Version = cnc.ComposeSemanticVersion(1, 2, 3)
	a.acceptSessionConnect(fields, 2)
	require.Len(t, a.egressEvents, 1)
	requireInvalidVersionDetail(t, a.egressEvents[0].detail, fields.Version)
}

func TestAcceptSessionConnect_idSequenceCoversRejects(t *testing.T) {
	a := leaderImmediateAccept(t)

	a.acceptSessionConnect(validConnectFields(1), 1) // id 1 accept
	bad := validConnectFields(2)
	bad.Version = cnc.ComposeSemanticVersion(1, 0, 0)
	a.acceptSessionConnect(bad, 2)                   // id 2 reject
	a.acceptSessionConnect(validConnectFields(3), 3) // id 3 accept

	require.Equal(t, 2, a.PendingSessionCount())
	_, ok1 := a.sessions.sessionByID(1)
	require.True(t, ok1)
	_, ok2 := a.sessions.sessionByID(2)
	require.False(t, ok2)
	_, ok3 := a.sessions.sessionByID(3)
	require.True(t, ok3)
	require.Equal(t, int64(4), a.sessions.nextSessionID)
}

func TestAcceptSessionConnect_offerAddError(t *testing.T) {
	adder := newScriptedEgressAdder()
	adder.offerErr = errors.New("invalid channel")
	a := leaderWithEgress(t, nil, 0, 0, adder)
	a.acceptSessionConnect(validConnectFields(1), 1)
	require.Equal(t, 0, a.PendingSessionCount())
	require.Empty(t, a.sessions.byID)
	require.Empty(t, a.egressEvents)
}

func TestAcceptSessionConnect_nilAdder_staysPending(t *testing.T) {
	a := NewAgent(AgentConfig{Role: RoleLeader})
	// No adder.
	a.acceptSessionConnect(validConnectFields(1), 1)
	require.Equal(t, 1, a.PendingSessionCount())
	s, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
	require.Nil(t, s.egressPublication)

	// Deadline reclaims.
	require.Equal(t, 1, a.sessionWork(1+DefaultSessionTimeoutNs))
	require.Equal(t, 0, a.PendingSessionCount())
}

func TestAcceptSessionConnect_handlerOverrides_noEvent(t *testing.T) {
	adder := withImmediateConnected(t)
	a, q, handler := leaderWithClientIngress(nil)
	a.SetEgressPublicationAdder(adder)
	fields := validConnectFields(1)
	// Encode with valid protocol version so acceptSessionConnect would not ERROR.
	wire := encodeSessionConnectWire(t, fields)
	q.Enqueue(wire)
	require.Equal(t, 1, a.pollClientIngress(1_000))
	require.Len(t, handler.calls, 1)
	require.Equal(t, 0, a.PendingSessionCount())
	require.Empty(t, a.egressEvents)
	require.Empty(t, adder.offers)
}
