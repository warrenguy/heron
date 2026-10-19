//go:build integration && archive_live

package cluster_archive_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/idlestrategy"
	"github.com/warrenguy/heron/test/integration/harness"
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

// connectTransferSession opens one AsyncConnect on egressPort and polls the
// connect against the live member set. Does not bind endpoint=:0.
//
// dump, when present, replaces set.State() on a connect-poll timeout.
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
		EgressHandler:       &client.EgressHandlers{},
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

// offerTransferSessionMessage offers payload on session until the offer
// returns a positive position.
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

	offerCtx, cancelOffer := context.WithTimeout(t.Context(), durableWorkBound)
	defer cancelOffer()
	lastKeepalive := time.Now()
	pump.mu.Lock()
	defer pump.mu.Unlock()
	pos, offerErr := pump.session.IngressOfferUntilPositive(
		offerCtx,
		client.IngressOfferConfig{
			IdleStrategy: idlestrategy.NewSleeping(aeron.DefaultIdleSleepDuration),
		},
		func() int64 {
			set.Tick()
			sendKeepAliveIfDue(pump.session, &lastKeepalive)
			return pump.session.Offer(payload)
		},
	)
	if offerErr != nil {
		require.FailNow(
			t,
			"session offer failed",
			"err=%v payload=%q:\n%s",
			offerErr,
			payload,
			transferTimeoutDump(set, dump),
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

// openServiceTransferCluster starts the transfer port block with one session
// recorder container per member. Containers open and attach before the first
// tick. The helper then awaits log images and election.
func openServiceTransferCluster(t *testing.T) serviceCluster {
	t.Helper()
	return startServiceCluster(
		t,
		harness.ClusterServiceTransferPortBase,
		harness.ClusterServiceTransferLogEndpoint,
		amendServiceTransferMember,
	)
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
	if rec == nil {
		return false, false
	}
	if len(rec.closes) != 0 {
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
	if len(rec.opens) == 0 && len(rec.messages) != 0 {
		return false, true
	}
	for i, msg := range rec.messages {
		if msg.sessionID != sessionID || !bytes.Equal(msg.payload, payloads[i]) {
			return false, true
		}
	}

	wantLen := len(rec.opens) + len(rec.messages)
	if len(rec.order) != wantLen {
		return false, true
	}
	if len(rec.opens) == 1 {
		if rec.order[0] != "open" {
			return false, true
		}
		for i := range rec.messages {
			if rec.order[i+1] != "message" {
				return false, true
			}
		}
	}
	if len(rec.opens) == 1 && len(rec.messages) == len(payloads) {
		return true, false
	}
	return false, false
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
// services keep consuming the committed log across a leader stop.
func TestClusterSessionTransfer_serviceContinuesAcrossLeaderStop(t *testing.T) {
	t.Parallel()

	sc := openServiceTransferCluster(t)
	set := sc.set
	leader := requireSingleLeader(t, set, "")

	requireServiceAwaitTick(
		t, sc,
		"join settle",
		durableWorkBound,
		func() bool {
			for i, m := range set.Modules() {
				agent := sc.members[i].agent
				ca := m.Agent()
				if !agent.IsLogBound() || agent.HasPendingJoin() || agent.NextAckID() != 1 {
					return false
				}
				if ca.ExpectedServiceAckID() != 1 || ca.PendingServiceAckWaits() != 0 {
					return false
				}
			}
			return true
		},
	)
	requireNoServiceFailures(t, sc, "")

	transport := startSessionClientTransport(t)
	pump := &transferPump{transport: transport}
	startTransferClientKeepAlive(t, pump)

	result, _, connectErr := connectTransferSession(
		t, set, leader, pump, []byte(sessionSmokeCredentials),
		serviceTransferEgressPort(),
		sc.dump,
	)
	require.NoError(t, connectErr)
	session, err := client.NewAeronCluster(result)
	require.NoError(t, err)
	harness.CleanupClose(t, "session", session.Close)
	pump.session = session

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

	requireServiceAwaitTick(
		t, sc,
		"services at commit",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfServiceTransferStall(t, sc, -1)
			for i, m := range set.Modules() {
				if sc.members[i].agent.LogPosition() < m.CommitPosition() {
					return false
				}
			}
			return true
		}),
	)

	preTerm := leader.LeadershipTermID()
	preCommit := leader.CommitPosition()
	leaderIndex := int(leader.ThisMemberID())
	logFloors := snapshotServiceLogPositions(sc)

	require.NoError(t, set.StopMember(leaderIndex))
	require.Nil(t, set.Module(leaderIndex))
	closeServiceTransferContainer(t, sc, leaderIndex)

	awaitTransferReElection(t, set, pump, leaderIndex, preTerm, transferReElectOpts{
		onTick: func() {
			failIfServiceTransferStall(t, sc, leaderIndex)
			failIfServiceLogRegressed(t, sc, leaderIndex, logFloors)
		},
		dump: sc.dump,
	})
	newLeader := requireSingleLeader(t, set, "after re-elect")
	failIfServiceTransferStall(t, sc, leaderIndex)

	requireServiceAwaitTick(
		t, sc,
		"session recovery",
		durableWorkBound,
		pump.sessionPump(func() bool {
			failIfServiceTransferStall(t, sc, leaderIndex)
			failIfServiceLogRegressed(t, sc, leaderIndex, logFloors)
			return transferSessionRecovered(session, sessionID, preTerm, newLeader)
		}),
		func() string {
			return formatSessionRecoveryState(session, sessionID, preTerm, newLeader)
		},
	)

	recoveryCommit := newLeader.CommitPosition()
	require.GreaterOrEqual(
		t,
		recoveryCommit,
		preCommit,
		"recovery commit below pre-stop commit:\n%s",
		sc.dump(),
	)

	survivorWitness, survivorIndex := installSurvivorLogWitness(t, set, newLeader, sc.dump)

	offerTransferSessionMessage(t, set, pump, sessionTransferPostPayload, sc.dump)

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
			failIfServiceTransferStall(t, sc, leaderIndex)
			failIfServiceLogRegressed(t, sc, leaderIndex, logFloors)
			failIfRecorderSequenceCorrupt(
				t, sc, leaderIndex, sessionID,
				continuationDump,
				sessionTransferPayload,
				sessionTransferPostPayload,
			)
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
