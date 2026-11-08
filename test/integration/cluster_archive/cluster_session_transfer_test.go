//go:build integration && archive_live

package cluster_archive_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"luas.fast/aeron"
	"luas.fast/cluster"
	"luas.fast/cluster/client"
	"luas.fast/cluster/codecs/clusterwire"
	"luas.fast/cluster/service"
	"luas.fast/idlestrategy"
	"luas.fast/test/integration/harness"
)

const (
	transferNewLeaderTimeout = 60 * time.Second
	transferMessageTimeout   = 20 * time.Second
	transferSessionTimeoutNs = int64(30 * time.Second)
)

var (
	sessionTransferPayload     = []byte("session-transfer-message")
	sessionTransferPostPayload = []byte("session-transfer-post-message")
)

// amendTransferMember installs session auth and a 30s session timeout.
func amendTransferMember(cfg cluster.Config) cluster.Config {
	cfg = amendSessionAuth(cfg)
	cfg.SessionTimeoutNs = transferSessionTimeoutNs
	return cfg
}

// amendServiceTransferMember sets session auth, a 30s session timeout, and
// ServiceCount 1.
func amendServiceTransferMember(cfg cluster.Config) cluster.Config {
	cfg = amendTransferMember(cfg)
	cfg.ServiceCount = 1
	return cfg
}

// startTransferSessionConnect opens one AsyncConnect with transfer client
// timeouts and polls the connect against the live member set. Egress binds a
// fixed loopback UDP port. Does not bind endpoint=:0.
func startTransferSessionConnect(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leader *cluster.ConsensusModule,
	pump *transferPump,
	credentials []byte,
) (*client.ConnectResult, int, error) {
	t.Helper()

	egressPort := transferEgressPort()
	return connectTransferSession(t, set, leader, pump, credentials, egressPort)
}

// connectTransferSession is connectTransferSessionWithEgress with no egress
// handlers.
func connectTransferSession(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leader *cluster.ConsensusModule,
	pump *transferPump,
	credentials []byte,
	egressPort int,
	dump ...func() string,
) (*client.ConnectResult, int, error) {
	t.Helper()
	return connectTransferSessionWithEgress(
		t, set, leader, pump, credentials, egressPort, &client.EgressHandlers{}, dump...,
	)
}

// connectTransferSessionWithEgress opens one AsyncConnect on egressPort with
// egress as the client egress handler. It polls the connect against the live
// member set. Does not bind endpoint=:0.
//
// dump, when present, replaces set.State() on a connect-poll timeout.
func connectTransferSessionWithEgress(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leader *cluster.ConsensusModule,
	pump *transferPump,
	credentials []byte,
	egressPort int,
	egress client.EgressHandler,
	dump ...func() string,
) (*client.ConnectResult, int, error) {
	t.Helper()

	egressChannel := "aeron:udp?endpoint=" + harness.LoopbackUDPEndpoint(egressPort)
	connect, err := client.NewAsyncConnect(t.Context(), client.Config{
		IngressChannel:      cluster.DefaultIngressChannel,
		IngressEndpoints:    leader.Topology().IngressEndpoints(),
		EgressChannel:       egressChannel,
		Aeron:               pump.transport,
		MessageTimeout:      transferMessageTimeout,
		NewLeaderTimeout:    transferNewLeaderTimeout,
		IngressRetryTimeout: durableWorkBound,
		CredentialsSupplier: fixedCredentialsSupplier{credentials: credentials},
		EgressHandler:       egress,
	})
	require.NoError(t, err)
	harness.CleanupClose(t, "async connect", connect.Close)

	var result *client.ConnectResult
	var pollErr error
	done := set.TickUntil(durableWorkBound, func() bool {
		result, pollErr = pump.pollConnect(connect)
		return result != nil || pollErr != nil
	})
	if !done {
		require.FailNow(
			t,
			"transfer connect poll did not resolve",
			"%s",
			transferTimeoutDump(set, dump),
		)
	}
	return result, egressPort, pollErr
}

// openTransferSession opens one session to leader on egressPort. It starts the
// client transport and keep-alive pump first. egress is the client egress
// handler. The returned pump holds the session.
func openTransferSession(
	t *testing.T,
	sc serviceCluster,
	leader *cluster.ConsensusModule,
	egressPort int,
	egress client.EgressHandler,
) (*transferPump, *client.AeronCluster) {
	t.Helper()

	transport := startSessionClientTransport(t)
	pump := &transferPump{transport: transport}
	startTransferClientKeepAlive(t, pump)
	result, _, connectErr := connectTransferSessionWithEgress(
		t, sc.set, leader, pump, []byte(sessionSmokeCredentials), egressPort, egress, sc.dump,
	)
	require.NoError(t, connectErr)
	session, err := client.NewAeronCluster(result)
	require.NoError(t, err)
	harness.CleanupClose(t, "session", session.Close)
	pump.session = session
	return pump, session
}

// transferPump serializes transport DoWork and session Poll. A keep-alive
// goroutine must not DoWork while the test goroutine Polls the same
// subscription.
type transferPump struct {
	mu        sync.Mutex
	transport *aeron.Client
	session   *client.AeronCluster
}

func (p *transferPump) doWork() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.transport.DoWork()
}

func (p *transferPump) pollSession() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.transport.DoWork()
	if p.session != nil {
		p.session.PollEgress()
	}
}

func (p *transferPump) pollConnect(connect *client.AsyncConnect) (*client.ConnectResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.transport.DoWork()
	return connect.Poll()
}

func (p *transferPump) sessionPump(pred func() bool) func() bool {
	lastKeepalive := time.Now()
	return func() bool {
		p.mu.Lock()
		p.transport.DoWork()
		if p.session != nil {
			p.session.PollEgress()
			sendKeepAliveIfDue(p.session, &lastKeepalive)
		}
		p.mu.Unlock()
		return pred()
	}
}

// transferReElectOpts holds optional per-tick and timeout-dump hooks for
// awaitTransferReElection.
type transferReElectOpts struct {
	onTick func()
	dump   func() string
}

// awaitTransferReElection waits until survivors elect a new leader after the
// member at leaderIndex stopped.
//
// opts.dump, when present, replaces set.State() on a bound expiry.
func awaitTransferReElection(
	t *testing.T,
	set *harness.ClusterMemberSet,
	pump *transferPump,
	leaderIndex int,
	preTerm int64,
	opts ...transferReElectOpts,
) {
	t.Helper()

	var onTick func()
	dump := func() string { return set.State() }
	if len(opts) > 0 {
		if opts[0].onTick != nil {
			onTick = opts[0].onTick
		}
		if opts[0].dump != nil {
			dump = opts[0].dump
		}
	}

	start := time.Now()
	done := set.TickUntil(durableReElectBound, func() bool {
		pump.pollSession()
		for i, m := range set.Modules() {
			if m == nil || i == leaderIndex {
				continue
			}
			failIfFollowerLogReplication(t, set, m, i)
		}
		if onTick != nil {
			onTick()
		}
		if !reElectionReady(set, leaderIndex, preTerm) {
			return false
		}
		return set.Leaders()[0].ElectionComplete()
	})
	if !done {
		require.FailNow(
			t,
			"re-elect after leader stop did not complete",
			"after %s (bound %s):\n%s",
			time.Since(start),
			durableReElectBound,
			dump(),
		)
	}
	t.Logf("re-elect after leader stop elapsed=%s", time.Since(start))
	requireReElectedLeader(t, set, leaderIndex, preTerm)
}

// offerTransferSessionMessage holds pump.mu while offerSessionMessage offers
// payload on the pump session.
//
// dump, when present, replaces set.State() on offer failure.
func offerTransferSessionMessage(
	t *testing.T,
	set *harness.ClusterMemberSet,
	pump *transferPump,
	payload []byte,
	dump ...func() string,
) int64 {
	t.Helper()

	pump.mu.Lock()
	defer pump.mu.Unlock()
	return offerSessionMessage(t, set, pump.session, payload, func() string {
		return transferTimeoutDump(set, dump)
	})
}

// offerSessionMessage offers payload on session until the offer returns a
// positive position. Each attempt ticks set.
func offerSessionMessage(
	t *testing.T,
	set *harness.ClusterMemberSet,
	session *client.AeronCluster,
	payload []byte,
	dump func() string,
) int64 {
	t.Helper()

	offerCtx, cancelOffer := context.WithTimeout(t.Context(), durableWorkBound)
	defer cancelOffer()
	lastKeepalive := time.Now()
	pos, offerErr := session.IngressOfferUntilPositive(
		offerCtx,
		client.IngressOfferConfig{
			IdleStrategy: idlestrategy.NewSleeping(aeron.DefaultIdleSleepDuration),
		},
		func() int64 {
			set.Tick()
			sendKeepAliveIfDue(session, &lastKeepalive)
			return session.Offer(payload)
		},
	)
	if offerErr != nil {
		require.FailNow(
			t,
			"session offer failed",
			"err=%v payload=%q:\n%s",
			offerErr,
			payload,
			dump(),
		)
	}
	require.Positive(t, pos)
	return pos
}

// sendKeepAliveIfDue sends one keepalive when the last send is at least one
// second old.
func sendKeepAliveIfDue(session *client.AeronCluster, last *time.Time) {
	now := time.Now()
	if now.Sub(*last) >= time.Second {
		_ = session.SendKeepAlive()
		*last = now
	}
}

// requireFollowersOpenSessionCount asserts every live non-leader reports
// OpenSessionCount 1.
func requireFollowersOpenSessionCount(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leaderID int32,
	dump func() string,
) {
	t.Helper()

	for _, i := range nonLeaderIndices(set, leaderID) {
		m := set.Module(i)
		require.Equal(
			t,
			1,
			m.Agent().OpenSessionCount(),
			"follower %d OpenSessionCount:\n%s",
			m.ThisMemberID(),
			dump(),
		)
	}
}

func transferSessionRecovered(
	session *client.AeronCluster,
	sessionID int64,
	preTerm int64,
	newLeader *cluster.ConsensusModule,
) bool {
	return session.SessionState() == client.SessionStateConnected &&
		session.ClusterSessionID() == sessionID &&
		session.LeadershipTermID() > preTerm &&
		session.LeaderMemberID() == newLeader.ThisMemberID() &&
		newLeader.Agent().OpenSessionCount() == 1
}

// failIfCommitBelowPreStop fails the test when commit is below preCommit.
func failIfCommitBelowPreStop(
	t *testing.T,
	commit, preCommit, recoveryCommit int64,
	dump func() string,
) {
	t.Helper()

	if commit < preCommit {
		require.FailNow(
			t,
			"new leader commit receded below pre-stop commit",
			"commit=%d preCommit=%d recoveryCommit=%d:\n%s",
			commit,
			preCommit,
			recoveryCommit,
			dump(),
		)
	}
}

// installSurvivorLogWitness installs a log witness on the first surviving
// follower. The helper returns the witness and that slot index. The helper
// fails the test when no survivor exists.
func installSurvivorLogWitness(
	t *testing.T,
	set *harness.ClusterMemberSet,
	newLeader *cluster.ConsensusModule,
	dump func() string,
) (*logWitness, int) {
	t.Helper()

	survivorIndex := firstNonLeaderIndex(set, newLeader.ThisMemberID())
	require.GreaterOrEqual(
		t,
		survivorIndex,
		0,
		"no surviving follower after re-elect:\n%s",
		dump(),
	)
	return installLogWitness(t, set, set.Module(survivorIndex)), survivorIndex
}

// formatSessionRecoveryState returns client session and new-leader recovery
// fields as one line.
func formatSessionRecoveryState(
	session *client.AeronCluster,
	sessionID int64,
	preTerm int64,
	newLeader *cluster.ConsensusModule,
) string {
	return fmt.Sprintf(
		"clientState=%s sessionID=%d wantID=%d term=%d preTerm=%d "+
			"leaderMember=%d wantLeader=%d open=%d",
		session.SessionState(),
		session.ClusterSessionID(),
		sessionID,
		session.LeadershipTermID(),
		preTerm,
		session.LeaderMemberID(),
		newLeader.ThisMemberID(),
		newLeader.Agent().OpenSessionCount(),
	)
}

// TestClusterSessionTransfer_recoversOpenSessionAcrossLeaderStop asserts an
// open client session continues across a leader stop.
func TestClusterSessionTransfer_recoversOpenSessionAcrossLeaderStop(t *testing.T) {
	t.Parallel()

	set := openThreeNodeCluster(
		t,
		harness.ClusterSessionTransferPortBase,
		harness.ClusterSessionTransferLogEndpoint,
		amendTransferMember,
	)
	leader := requireSingleLeader(t, set, "")
	followers := attachFollowerWitnesses(t, set, leader)

	transport := startSessionClientTransport(t)
	pump := &transferPump{transport: transport}
	startTransferClientKeepAlive(t, pump)
	result, _, connectErr := startTransferSessionConnect(
		t, set, leader, pump, []byte(sessionSmokeCredentials),
	)
	require.NoError(t, connectErr)
	session, err := client.NewAeronCluster(result)
	require.NoError(t, err)
	harness.CleanupClose(t, "session", session.Close)
	pump.session = session

	require.GreaterOrEqual(t, session.ClusterSessionID(), int64(0))
	require.Equal(t, leader.LeadershipTermID(), session.LeadershipTermID())

	requireAwaitTick(t, set, "session open committed", durableWorkBound, pump.sessionPump(
		func() bool {
			return leader.Agent().OpenSessionCount() == 1
		},
	))

	preMsgLogPos := leader.LogPublisher().Position()
	offerTransferSessionMessage(t, set, pump, sessionTransferPayload)

	var postAppendLogPos int64
	requireAwaitTick(t, set, "session message appended", durableWorkBound, pump.sessionPump(
		func() bool {
			postAppendLogPos = leader.LogPublisher().Position()
			return postAppendLogPos > preMsgLogPos
		},
	))

	sessionID := session.ClusterSessionID()
	requireAwaitTick(t, set, "session message committed", durableWorkBound, pump.sessionPump(
		func() bool {
			if !membersReachCommit(set, postAppendLogPos) {
				return false
			}
			return allSawSession(followers, sessionID)
		},
	))

	requireFollowersOpenSessionCount(t, set, leader.ThisMemberID(), set.State)

	preTerm := leader.LeadershipTermID()
	preCommit := leader.CommitPosition()
	leaderIndex := int(leader.ThisMemberID())

	require.NoError(t, set.StopMember(leaderIndex))
	require.Nil(t, set.Module(leaderIndex))

	awaitTransferReElection(t, set, pump, leaderIndex, preTerm)
	newLeader := requireSingleLeader(t, set, "after re-elect")

	recovered := set.TickUntil(durableWorkBound, func() bool {
		pump.pollSession()
		return transferSessionRecovered(session, sessionID, preTerm, newLeader)
	})
	if !recovered {
		require.FailNow(
			t,
			"session recovery did not complete",
			"%s\n%s",
			formatSessionRecoveryState(session, sessionID, preTerm, newLeader),
			set.State(),
		)
	}

	recoveryCommit := newLeader.CommitPosition()
	require.GreaterOrEqual(
		t,
		recoveryCommit,
		preCommit,
		"recovery commit below pre-stop commit:\n%s",
		set.State(),
	)

	survivorWitness, _ := installSurvivorLogWitness(t, set, newLeader, set.State)

	offerTransferSessionMessage(t, set, pump, sessionTransferPostPayload)

	requireAwaitTick(t, set, "post-transfer message committed", durableWorkBound, pump.sessionPump(
		func() bool {
			commit := newLeader.CommitPosition()
			failIfCommitBelowPreStop(t, commit, preCommit, recoveryCommit, set.State)
			return survivorWitness.sawSession(sessionID) && commit > recoveryCommit
		},
	))

	require.NoError(t, session.Close())
	requireAwaitTick(t, set, "session registry drained", sessionCloseBound, func() bool {
		pump.doWork()
		agent := newLeader.Agent()
		return agent.OpenSessionCount() == 0 &&
			agent.PendingSessionCount() == 0 &&
			agent.ClosingSessionCount() == 0
	})
}

func serviceTransferEgressPort() int {
	return harness.ClientEgressPort(harness.ClusterServiceTransferPortBase)
}

// heldClock is a module NanoClock that a test can hold and step. After release
// it runs at real-time speed from the held value.
//
// While held, module timeouts do not advance and the module client does not
// send keepalives. Hold it across short waits only.
//
// Concurrency: safe from any goroutine.
type heldClock struct {
	held   atomic.Int64
	offset atomic.Int64
}

func (c *heldClock) now() int64 {
	if held := c.held.Load(); held != 0 {
		return held
	}
	return time.Now().UnixNano() + c.offset.Load()
}

func (c *heldClock) hold() {
	c.held.Store(c.now())
}

func (c *heldClock) step(delta int64) {
	c.held.Add(delta)
}

func (c *heldClock) release() {
	c.offset.Store(c.held.Load() - time.Now().UnixNano())
	c.held.Store(0)
}

const (
	leaderStopNearTimerID      int64 = 1
	leaderStopCancelledTimerID int64 = 2
	leaderStopFireTimerID      int64 = 3
	leaderStopFarTimerID       int64 = 4

	leaderStopNearDelay      = int64(100 * time.Millisecond)
	leaderStopCancelledDelay = int64(time.Second)
	leaderStopFireDelay      = int64(2 * time.Second)
	leaderStopFarDelay       = int64(time.Hour)

	// leaderStopNearStep passes the near deadline and one timer wheel tick. It
	// stays short of the cancelled deadline.
	leaderStopNearStep = leaderStopNearDelay + int64(20*time.Millisecond)
)

type recordedTimerEvent struct {
	correlationID int64
	timestamp     int64
}

// leaderStopTimers schedules the near, cancelled, fire, and far timers on its
// first session message. TimerEvent for the near timer cancels the cancelled
// timer. TimerEvent for the fire timer cancels the far timer.
//
// Concurrency: the goroutine that ticks the service owns the wrapper. Not safe
// from other goroutines.
type leaderStopTimers struct {
	*sessionRecorder

	cluster         service.Cluster
	scheduled       bool
	scheduledAt     int64
	requestFailures int
	timerEvents     []recordedTimerEvent
}

func (s *leaderStopTimers) Start(c service.Cluster) error {
	s.cluster = c
	return s.sessionRecorder.Start(c)
}

func (s *leaderStopTimers) SessionMessage(
	session service.ClientSession,
	timestamp int64,
	payload []byte,
) {
	s.sessionRecorder.SessionMessage(session, timestamp, payload)
	if s.scheduled {
		return
	}
	s.scheduled = true
	s.scheduledAt = timestamp
	s.countRefusal(s.cluster.ScheduleTimer(leaderStopNearTimerID, timestamp+leaderStopNearDelay))
	s.countRefusal(s.cluster.ScheduleTimer(
		leaderStopCancelledTimerID,
		timestamp+leaderStopCancelledDelay,
	))
	s.countRefusal(s.cluster.ScheduleTimer(leaderStopFireTimerID, timestamp+leaderStopFireDelay))
	s.countRefusal(s.cluster.ScheduleTimer(leaderStopFarTimerID, timestamp+leaderStopFarDelay))
}

func (s *leaderStopTimers) TimerEvent(correlationID, timestamp int64) {
	s.timerEvents = append(s.timerEvents, recordedTimerEvent{
		correlationID: correlationID,
		timestamp:     timestamp,
	})
	switch correlationID {
	case leaderStopNearTimerID:
		s.countRefusal(s.cluster.CancelTimer(leaderStopCancelledTimerID))
	case leaderStopFireTimerID:
		s.countRefusal(s.cluster.CancelTimer(leaderStopFarTimerID))
	}
}

func (s *leaderStopTimers) countRefusal(sent bool) {
	if !sent {
		s.requestFailures++
	}
}

func (s *leaderStopTimers) timerIDs() []int64 {
	ids := make([]int64, len(s.timerEvents))
	for i, event := range s.timerEvents {
		ids[i] = event.correlationID
	}
	return ids
}

func formatLeaderStopTimers(
	sc serviceCluster,
	timers map[*sessionRecorder]*leaderStopTimers,
) string {
	var b strings.Builder
	for i, mem := range sc.members {
		s := timers[mem.rec]
		fmt.Fprintf(
			&b,
			"timers index=%d member=%d events=%+v requestFailures=%d",
			i,
			mem.agent.MemberID(),
			s.timerEvents,
			s.requestFailures,
		)
		if m := sc.set.Module(i); m != nil {
			fmt.Fprintf(
				&b,
				" scheduled=%d expired=%d",
				m.Agent().ScheduledTimerCount(),
				m.Agent().ExpiredTimerCount(),
			)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// failIfTimerEventsUnexpected fails when a live service holds an id list that
// is not a prefix of want. It also fails on any request failure.
func failIfTimerEventsUnexpected(
	t *testing.T,
	sc serviceCluster,
	timers map[*sessionRecorder]*leaderStopTimers,
	want ...int64,
) {
	t.Helper()

	for i, mem := range sc.members {
		if sc.set.Module(i) == nil {
			continue
		}
		s := timers[mem.rec]
		ids := s.timerIDs()
		if len(ids) > len(want) || !slices.Equal(ids, want[:len(ids)]) {
			require.FailNow(
				t,
				"unexpected TimerEvent",
				"member=%d ids=%v want=%v:\n%s\n%s",
				mem.agent.MemberID(),
				ids,
				want,
				formatLeaderStopTimers(sc, timers),
				sc.dump(),
			)
		}
		if s.requestFailures != 0 {
			require.FailNow(
				t,
				"timer request refused",
				"member=%d requestFailures=%d ids=%v:\n%s\n%s",
				mem.agent.MemberID(),
				s.requestFailures,
				ids,
				formatLeaderStopTimers(sc, timers),
				sc.dump(),
			)
		}
	}
}

// hasLeaderStopTimersSettled reports whether every live service holds exactly
// want. Each live module must also report ScheduledTimerCount equal to
// scheduled and ExpiredTimerCount zero.
func hasLeaderStopTimersSettled(
	sc serviceCluster,
	timers map[*sessionRecorder]*leaderStopTimers,
	scheduled int64,
	want ...int64,
) bool {
	for i, mem := range sc.members {
		m := sc.set.Module(i)
		if m == nil {
			continue
		}
		if !slices.Equal(timers[mem.rec].timerIDs(), want) {
			return false
		}
		agent := m.Agent()
		if agent.ScheduledTimerCount() != scheduled || agent.ExpiredTimerCount() != 0 {
			return false
		}
	}
	return true
}

// requireLastTimerEventAgrees asserts every live service stores the same last
// TimerEvent timestamp. That timestamp must sit at or after scheduledAt plus
// delay. Call it after hasLeaderStopTimersSettled reports true, so each live
// service holds a TimerEvent.
func requireLastTimerEventAgrees(
	t *testing.T,
	sc serviceCluster,
	timers map[*sessionRecorder]*leaderStopTimers,
	label string,
	delay int64,
) {
	t.Helper()

	detail := formatLeaderStopTimers(sc, timers)
	var firstTimestamp int64
	for i, mem := range sc.members {
		if sc.set.Module(i) == nil {
			continue
		}
		s := timers[mem.rec]
		memberID := mem.agent.MemberID()
		event := s.timerEvents[len(s.timerEvents)-1]
		require.GreaterOrEqual(
			t,
			event.timestamp,
			s.scheduledAt+delay,
			"%s: TimerEvent %d before its deadline member %d:\n%s",
			label,
			event.correlationID,
			memberID,
			detail,
		)
		if firstTimestamp == 0 {
			firstTimestamp = event.timestamp
		}
		require.Equal(
			t,
			firstTimestamp,
			event.timestamp,
			"%s: TimerEvent %d timestamp member %d:\n%s",
			label,
			event.correlationID,
			memberID,
			detail,
		)
	}
}

// failIfServiceTransferStall fails when a live member other than skipIndex
// reports ServiceFailure, ServiceAckFailure, or a commit-bound violation.
// Skips a nil module.
func failIfServiceTransferStall(t *testing.T, sc serviceCluster, skipIndex int) {
	t.Helper()

	for i, mem := range sc.members {
		if i == skipIndex {
			continue
		}
		m := sc.set.Module(i)
		if m == nil {
			continue
		}
		if mem.checked != nil && mem.checked.violation != nil {
			require.NoError(
				t,
				mem.checked.violation,
				"commit bound member %d:\n%s",
				m.ThisMemberID(),
				sc.dump(),
			)
		}
		requireNoServiceFailure(t, sc, i, "")
	}
}

// snapshotServiceLogPositions copies each attached service LogPosition in
// member-index order.
func snapshotServiceLogPositions(sc serviceCluster) []int64 {
	floors := make([]int64, len(sc.members))
	for i, mem := range sc.members {
		floors[i] = mem.agent.LogPosition()
	}
	return floors
}

// failIfServiceLogRegressed fails when a live member other than skipIndex
// reports a service log position below that member's pre-stop floor. Skips a
// nil module.
func failIfServiceLogRegressed(
	t *testing.T,
	sc serviceCluster,
	skipIndex int,
	floors []int64,
) {
	t.Helper()

	for i, mem := range sc.members {
		if i == skipIndex {
			continue
		}
		m := sc.set.Module(i)
		if m == nil {
			continue
		}
		pos := mem.agent.LogPosition()
		floor := int64(0)
		if i < len(floors) {
			floor = floors[i]
		}
		if pos < floor {
			require.FailNow(
				t,
				"service log position below pre-stop floor",
				"member=%d logPos=%d floor=%d:\n%s",
				m.ThisMemberID(),
				pos,
				floor,
				sc.dump(),
			)
		}
	}
}

// recorderSequenceStatus reports whether rec holds sessionID open then
// payloads in order.
//
//   - complete: exact match
//   - corrupt: a close, extra open, extra message, or prefix mismatch
//   - neither: a correct prefix that is still short
func recorderSequenceStatus(
	rec *sessionRecorder,
	sessionID int64,
	payloads ...[]byte,
) (complete, corrupt bool) {
	return recorderSessionStatus(rec, sessionID, clusterwire.CloseReason_NULL_VALUE, payloads...)
}

// recorderSessionStatus is recorderSequenceStatus that also expects one
// trailing close with closeReason. CloseReason_NULL_VALUE expects no close.
func recorderSessionStatus(
	rec *sessionRecorder,
	sessionID int64,
	closeReason clusterwire.CloseReason,
	payloads ...[]byte,
) (complete, corrupt bool) {
	if rec == nil {
		return false, false
	}
	wantCloses := 0
	if closeReason != clusterwire.CloseReason_NULL_VALUE {
		wantCloses = 1
	}
	if len(rec.Closes) > wantCloses {
		return false, true
	}
	if len(rec.Closes) == 1 && !rec.HasCloseWithReason(sessionID, closeReason) {
		return false, true
	}
	if len(rec.opens) > 1 {
		return false, true
	}
	if len(rec.opens) == 1 && rec.opens[0] != sessionID {
		return false, true
	}
	if len(rec.messages) > len(payloads) {
		return false, true
	}
	if len(rec.opens) == 0 && (len(rec.messages) != 0 || len(rec.Closes) != 0) {
		return false, true
	}
	for i, msg := range rec.messages {
		if msg.sessionID != sessionID || !bytes.Equal(msg.payload, payloads[i]) {
			return false, true
		}
	}
	// A close ends the session. Every payload must precede it.
	if len(rec.Closes) == 1 && len(rec.messages) != len(payloads) {
		return false, true
	}

	want := slices.Repeat([]string{"message"}, len(rec.messages))
	if len(rec.opens) == 1 {
		want = append([]string{"open"}, want...)
	}
	if len(rec.Closes) == 1 {
		want = append(want, "close")
	}
	if !slices.Equal(rec.order, want) {
		return false, true
	}
	done := len(rec.opens) == 1 &&
		len(rec.messages) == len(payloads) &&
		len(rec.Closes) == wantCloses
	return done, false
}

// recorderHoldsOpenThenMessages reports one session open followed by the
// payloads in order. Extra opens, extra messages, closes, and payload mismatch
// fail the check.
func recorderHoldsOpenThenMessages(
	rec *sessionRecorder,
	sessionID int64,
	payloads ...[]byte,
) bool {
	complete, _ := recorderSequenceStatus(rec, sessionID, payloads...)
	return complete
}

// failIfSurvivorStalled fails when a live service other than stoppedIndex
// stalled or regressed below its floor in floors.
func failIfSurvivorStalled(t *testing.T, sc serviceCluster, stoppedIndex int, floors []int64) {
	t.Helper()
	failIfServiceTransferStall(t, sc, stoppedIndex)
	failIfServiceLogRegressed(t, sc, stoppedIndex, floors)
}

func hasServicesAtCommit(sc serviceCluster) bool {
	for i, m := range sc.set.Modules() {
		if m == nil {
			continue
		}
		if sc.members[i].agent.LogPosition() < m.CommitPosition() {
			return false
		}
	}
	return true
}

// requireServicesAtCommit awaits every service log position reaching its
// module commit position. Each tick fails on a service stall.
func requireServicesAtCommit(t *testing.T, sc serviceCluster, pump *transferPump) {
	t.Helper()

	requireServiceAwaitTick(
		t, sc,
		"services at commit",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfServiceTransferStall(t, sc, -1)
			return hasServicesAtCommit(sc)
		}),
	)
}

// requireTransferSessionRecovery awaits re-election after the member at
// stoppedIndex stopped. It then awaits session recovery on the new leader.
// Each tick fails on a survivor stall or log regression below floors. Fails
// when the recovery commit sits below preCommit. Returns the new leader.
func requireTransferSessionRecovery(
	t *testing.T,
	sc serviceCluster,
	pump *transferPump,
	session *client.AeronCluster,
	stoppedIndex int,
	preTerm, preCommit int64,
	floors []int64,
) *cluster.ConsensusModule {
	t.Helper()

	set := sc.set
	awaitTransferReElection(t, set, pump, stoppedIndex, preTerm, transferReElectOpts{
		onTick: func() { failIfSurvivorStalled(t, sc, stoppedIndex, floors) },
		dump:   sc.dump,
	})
	newLeader := requireSingleLeader(t, set, "after re-elect")
	failIfServiceTransferStall(t, sc, stoppedIndex)

	sessionID := session.ClusterSessionID()
	requireServiceAwaitTick(
		t, sc,
		"session recovery",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfSurvivorStalled(t, sc, stoppedIndex, floors)
			return transferSessionRecovered(session, sessionID, preTerm, newLeader)
		}),
		func() string {
			return formatSessionRecoveryState(session, sessionID, preTerm, newLeader)
		},
	)
	require.GreaterOrEqual(t, newLeader.CommitPosition(), preCommit,
		"recovery commit below pre-stop commit:\n%s", sc.dump())
	return newLeader
}

// failIfRecorderSequenceCorrupt fails when a live recorder other than
// skipIndex already has a close, extra open, extra message, or prefix
// mismatch. A short correct prefix does not fail.
func failIfRecorderSequenceCorrupt(
	t *testing.T,
	sc serviceCluster,
	skipIndex int,
	sessionID int64,
	detail func() string,
	payloads ...[]byte,
) {
	t.Helper()

	for i, mem := range sc.members {
		if i == skipIndex {
			continue
		}
		m := sc.set.Module(i)
		if m == nil {
			continue
		}
		_, corrupt := recorderSequenceStatus(mem.rec, sessionID, payloads...)
		if !corrupt {
			continue
		}
		msg := sc.dump()
		if detail != nil {
			msg = detail() + "\n" + msg
		}
		require.FailNow(
			t,
			"survivor service sequence corrupted",
			"member=%d:\n%s",
			m.ThisMemberID(),
			msg,
		)
	}
}

// survivorServicesHoldSequence reports whether every live member other than
// skipIndex recorded sessionID open then the payloads in order.
func survivorServicesHoldSequence(
	sc serviceCluster,
	skipIndex int,
	sessionID int64,
	payloads ...[]byte,
) bool {
	for i, mem := range sc.members {
		if i == skipIndex || sc.set.Module(i) == nil {
			continue
		}
		if !recorderHoldsOpenThenMessages(mem.rec, sessionID, payloads...) {
			return false
		}
	}
	return true
}

// closeServiceTransferContainer closes the container at index when present.
// A missing index or a nil container is a no-op.
func closeServiceTransferContainer(t *testing.T, sc serviceCluster, index int) {
	t.Helper()

	if index < 0 || index >= len(sc.members) {
		return
	}
	mem := sc.members[index]
	if mem.container == nil {
		return
	}
	require.NoError(t, mem.container.Close())
	mem.container = nil
	sc.members[index] = mem
}

// formatServiceTransferContinuation prints survivor recorder order, payloads,
// witness result, and commit floors for a timeout dump.
func formatServiceTransferContinuation(
	sc serviceCluster,
	skipIndex int,
	sessionID int64,
	witnessOK bool,
	commit, preCommit, recoveryCommit int64,
	want ...[]byte,
) string {
	var b strings.Builder
	fmt.Fprintf(
		&b,
		"sessionID=%d witness=%v commit=%d preCommit=%d recoveryCommit=%d\n",
		sessionID,
		witnessOK,
		commit,
		preCommit,
		recoveryCommit,
	)
	b.WriteString("want order=[open")
	for _, payload := range want {
		fmt.Fprintf(&b, " message=%q", payload)
	}
	b.WriteString("]\n")
	for i, mem := range sc.members {
		if i == skipIndex {
			continue
		}
		m := sc.set.Module(i)
		if m == nil {
			continue
		}
		fmt.Fprintf(
			&b,
			"recorder member=%d order=%v",
			m.ThisMemberID(),
			mem.rec.order,
		)
		for j, msg := range mem.rec.messages {
			fmt.Fprintf(
				&b,
				" msg[%d] session=%d payload=%q",
				j,
				msg.sessionID,
				msg.payload,
			)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// TestClusterSessionTransfer_serviceContinuesAcrossLeaderStop asserts attached
// services keep consuming the committed log and get TimerEvent only for
// uncancelled timers across a leader stop.
func TestClusterSessionTransfer_serviceContinuesAcrossLeaderStop(t *testing.T) {
	t.Parallel()

	timers := make(map[*sessionRecorder]*leaderStopTimers)
	wrap := func(rec *sessionRecorder) service.ClusteredService {
		s := &leaderStopTimers{sessionRecorder: rec}
		timers[rec] = s
		return s
	}
	clock := &heldClock{}
	amend := func(cfg cluster.Config) cluster.Config {
		cfg = amendServiceTransferMember(cfg)
		cfg.AeronConfig.NanoClock = clock.now
		return cfg
	}
	sc := startWrappedServiceCluster(
		t,
		3,
		harness.ClusterServiceTransferPortBase,
		harness.ClusterServiceTransferLogEndpoint,
		amend,
		wrap,
	)
	set := sc.set
	leader := requireSingleLeader(t, set, "")

	requireServicesJoined(t, sc)
	requireNoServiceFailures(t, sc, "")

	pump, session := openTransferSession(
		t, sc, leader, serviceTransferEgressPort(), &client.EgressHandlers{},
	)

	require.GreaterOrEqual(t, session.ClusterSessionID(), int64(0))
	require.Equal(t, leader.LeadershipTermID(), session.LeadershipTermID())

	requireServiceAwaitTick(
		t, sc,
		"session open committed",
		durableWorkBound,
		pump.sessionPump(func() bool {
			if leader.Agent().OpenSessionCount() != 1 {
				return false
			}
			for _, mem := range sc.members {
				if !mem.rec.holdsOpen(session.ClusterSessionID()) {
					return false
				}
			}
			return true
		}),
	)

	// Hold the clock until the stop. A host stall then cannot carry the old
	// leader past the cancelled or fire deadline.
	clock.hold()

	preMsgLogPos := leader.LogPublisher().Position()
	offerTransferSessionMessage(t, set, pump, sessionTransferPayload, sc.dump)

	var postAppendLogPos int64
	requireServiceAwaitTick(
		t, sc,
		"session message appended",
		durableWorkBound,
		pump.sessionPump(func() bool {
			postAppendLogPos = leader.LogPublisher().Position()
			return postAppendLogPos > preMsgLogPos
		}),
	)

	sessionID := session.ClusterSessionID()
	requireServiceAwaitTick(
		t, sc,
		"session message committed",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfServiceTransferStall(t, sc, -1)
			if !membersReachCommit(set, postAppendLogPos) {
				return false
			}
			for _, mem := range sc.members {
				if !mem.rec.holdsMessagesAfterOpen(sessionID, sessionTransferPayload) {
					return false
				}
			}
			return true
		}),
	)

	requireFollowersOpenSessionCount(t, set, leader.ThisMemberID(), sc.dump)
	requireNoServiceFailures(t, sc, " after baseline")

	// Phase: timers settle before the stop. Commit at the append position and
	// each service at commit show the old leader did not append a second
	// TimerEvent.
	timersDump := func() string { return formatLeaderStopTimers(sc, timers) }
	clock.step(leaderStopNearStep)
	requireServiceAwaitTick(
		t, sc,
		"timers settled before stop",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfServiceTransferStall(t, sc, -1)
			failIfTimerEventsUnexpected(t, sc, timers, leaderStopNearTimerID)
			return hasLeaderStopTimersSettled(sc, timers, 2, leaderStopNearTimerID) &&
				hasServicesAtCommit(sc) &&
				membersReachCommit(set, leader.LogPublisher().Position())
		}),
		timersDump,
	)
	requireLastTimerEventAgrees(t, sc, timers, "before stop", leaderStopNearDelay)

	// A tick does not run between the release and the stop. The old leader holds
	// the fire and far timers when it stops.
	clock.release()
	preTerm := leader.LeadershipTermID()
	preCommit := leader.CommitPosition()
	leaderIndex := int(leader.ThisMemberID())
	logFloors := snapshotServiceLogPositions(sc)

	require.NoError(t, set.StopMember(leaderIndex))
	require.Nil(t, set.Module(leaderIndex))
	closeServiceTransferContainer(t, sc, leaderIndex)

	newLeader := requireTransferSessionRecovery(
		t, sc, pump, session, leaderIndex, preTerm, preCommit, logFloors,
	)
	recoveryCommit := newLeader.CommitPosition()

	survivorWitness, survivorIndex := installSurvivorLogWitness(t, set, newLeader, sc.dump)

	offerTransferSessionMessage(t, set, pump, sessionTransferPostPayload, sc.dump)

	survivorTimerIDs := []int64{leaderStopNearTimerID, leaderStopFireTimerID}
	continuationDump := func() string {
		return formatServiceTransferContinuation(
			sc,
			leaderIndex,
			sessionID,
			survivorWitness.sawSession(sessionID),
			newLeader.CommitPosition(),
			preCommit,
			recoveryCommit,
			sessionTransferPayload,
			sessionTransferPostPayload,
		)
	}
	requireServiceAwaitTick(
		t, sc,
		"post-transfer message committed",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfSurvivorStalled(t, sc, leaderIndex, logFloors)
			failIfRecorderSequenceCorrupt(
				t, sc, leaderIndex, sessionID,
				continuationDump,
				sessionTransferPayload,
				sessionTransferPostPayload,
			)
			failIfTimerEventsUnexpected(t, sc, timers, survivorTimerIDs...)
			commit := newLeader.CommitPosition()
			failIfCommitBelowPreStop(t, commit, preCommit, recoveryCommit, sc.dump)
			return survivorWitness.sawSession(sessionID) &&
				commit > recoveryCommit &&
				survivorServicesHoldSequence(
					sc,
					leaderIndex,
					sessionID,
					sessionTransferPayload,
					sessionTransferPostPayload,
				)
		}),
		continuationDump,
	)

	// Phase: the new leader fires the fire timer. The old leader did not append
	// its TimerEvent.
	requireServiceAwaitTick(
		t, sc,
		"fire timer on new leader",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfSurvivorStalled(t, sc, leaderIndex, logFloors)
			failIfTimerEventsUnexpected(t, sc, timers, survivorTimerIDs...)
			return hasLeaderStopTimersSettled(sc, timers, 0, survivorTimerIDs...) &&
				hasServicesAtCommit(sc) &&
				liveMembersReachCommit(set, []int{leaderIndex}, newLeader.LogPublisher().Position())
		}),
		timersDump,
	)
	requireLastTimerEventAgrees(t, sc, timers, "after leader change", leaderStopFireDelay)

	requireNoServiceFailure(t, sc, int(newLeader.ThisMemberID()), " after continuation")
	requireNoServiceFailure(t, sc, survivorIndex, " after continuation")
}

// startTransferClientKeepAlive pumps transport.DoWork on a side goroutine.
// The side goroutine calls DoWork only. PollEgress stays on the test goroutine.
//
// TickUntil runs member Tick first. Election INIT can block member Tick in
// StopLogRecording. sessionPump then never runs. The client heartbeat dies.
func startTransferClientKeepAlive(t *testing.T, pump *transferPump) {
	t.Helper()
	ctx := t.Context()
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pump.doWork()
			}
		}
	}()
}

func transferEgressPort() int {
	return harness.ClientEgressPort(harness.ClusterSessionTransferPortBase)
}

func transferTimeoutDump(set *harness.ClusterMemberSet, dump []func() string) string {
	if len(dump) > 0 && dump[0] != nil {
		return dump[0]()
	}
	return set.State()
}
