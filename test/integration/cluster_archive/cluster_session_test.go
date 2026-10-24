//go:build integration && archive_live

package cluster_archive_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/idlestrategy"
	"github.com/warrenguy/heron/test/integration/harness"
)

const (
	sessionSmokeCredentials = "heron-session-smoke-credentials"
	sessionSmokePrincipal   = "heron-session-smoke-principal"
)

// amendSessionAuth installs a fresh SimpleAuthenticator on cfg for one member.
func amendSessionAuth(cfg cluster.Config) cluster.Config {
	cfg.Authenticator = consensus.NewSimpleAuthenticator(
		[]consensus.SimplePrincipal{{
			EncodedPrincipal:   []byte(sessionSmokePrincipal),
			EncodedCredentials: []byte(sessionSmokeCredentials),
		}},
	)
	return cfg
}

// fixedCredentialsSupplier returns fixed connect credentials. Does not
// respond to challenges.
type fixedCredentialsSupplier struct {
	credentials []byte
}

func (s fixedCredentialsSupplier) EncodedCredentials() []byte {
	return s.credentials
}

func (s fixedCredentialsSupplier) RespondToChallenge([]byte) []byte {
	return nil
}

// startSessionClientTransport starts the client-side media driver and a
// test-owned transport client. The test owns Close. Session Close does not
// close an injected transport.
//
// Uses harness.Start because the calling Test already enabled t.Parallel.
func startSessionClientTransport(t *testing.T) *aeron.Client {
	t.Helper()
	return harness.ConnectClient(t, harness.Start(t), aeron.Config{})
}

// startSessionConnect opens one AsyncConnect with cleanup and polls the
// connect against the live member set. Ingress endpoints come from the
// leader topology. Egress uses endpoint=:0.
//
// Returns the connect result or the connect error. Fails the test when the
// bound expires and dumps the set state.
func startSessionConnect(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leader *cluster.ConsensusModule,
	transport *aeron.Client,
	credentials []byte,
) (*client.ConnectResult, error) {
	t.Helper()
	return startSessionConnectWithEgress(
		t, set, leader, transport, credentials, &client.EgressHandlers{},
	)
}

// startSessionConnectWithEgress is startSessionConnect with egress as the
// client egress handler.
func startSessionConnectWithEgress(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leader *cluster.ConsensusModule,
	transport *aeron.Client,
	credentials []byte,
	egress client.EgressHandler,
) (*client.ConnectResult, error) {
	t.Helper()

	connect, err := client.NewAsyncConnect(t.Context(), client.Config{
		IngressChannel:      cluster.DefaultIngressChannel,
		IngressEndpoints:    leader.Topology().IngressEndpoints(),
		EgressChannel:       "aeron:udp?endpoint=127.0.0.1:0",
		Aeron:               transport,
		MessageTimeout:      durableWorkBound,
		IngressRetryTimeout: durableWorkBound,
		CredentialsSupplier: fixedCredentialsSupplier{credentials: credentials},
		EgressHandler:       egress,
	})
	require.NoError(t, err)
	harness.CleanupClose(t, "async connect", connect.Close)

	var result *client.ConnectResult
	var pollErr error
	done := set.TickUntil(durableWorkBound, func() bool {
		transport.DoWork()
		result, pollErr = connect.Poll()
		return result != nil || pollErr != nil
	})
	if !done {
		require.FailNow(t, "connect poll did not resolve", "%s", set.State())
	}
	return result, pollErr
}

// sessionPump returns a TickUntil body that pumps the client transport, polls
// session egress, and sends a keepalive at one-second cadence. Keepalive
// results are ignored. The cadence keeps the session inside the 10s server
// timeout under race-detector stalls.
func sessionPump(
	transport *aeron.Client,
	session *client.AeronCluster,
	pred func() bool,
) func() bool {
	lastKeepalive := time.Now()
	return func() bool {
		transport.DoWork()
		session.PollEgress()
		now := time.Now()
		if now.Sub(lastKeepalive) >= time.Second {
			_ = session.SendKeepAlive()
			lastKeepalive = now
		}
		return pred()
	}
}

// logWitness records SessionMessage deliveries under the commit bound.
// Ignores NewLeadershipTermEvent.
type logWitness struct {
	sessionIDs []int64
}

func (w *logWitness) SessionMessage(clusterSessionID, _ int64) {
	w.sessionIDs = append(w.sessionIDs, clusterSessionID)
}

func (w *logWitness) NewLeadershipTermEvent(
	_, _, _, _ int64,
	_ clusterwire.ClusterTimeUnit,
	_ int32,
) {
}

func (w *logWitness) sawSession(sessionID int64) bool {
	return slices.Contains(w.sessionIDs, sessionID)
}

// sessionFollower pairs one follower module with the log-adapter witness.
type sessionFollower struct {
	module  *cluster.ConsensusModule
	witness *logWitness
}

// String formats one follower for failure messages.
func (f sessionFollower) String() string {
	return fmt.Sprintf(
		"member%d{count=%d sessionIDs=%v}",
		f.module.ThisMemberID(),
		len(f.witness.sessionIDs),
		f.witness.sessionIDs,
	)
}

// TestClusterSession_opensSendsAndClosesOverDurableMembers asserts valid
// credentials open a session, one session message reaches commit with
// follower witnesses, and the leader registry drains after Close. Drain may
// complete via SessionCloseRequest or open-session idle TIMEOUT
// (DefaultSessionTimeoutNs). A final session connects with a Connect-owned
// transport client on its own driver. A shutdown of that driver closes the
// session on the next SpinOnce.
func TestClusterSession_opensSendsAndClosesOverDurableMembers(t *testing.T) {
	t.Parallel()

	set := openThreeNodeCluster(
		t,
		harness.ClusterSessionSmokePortBase,
		harness.ClusterSessionSmokeLogEndpoint,
		amendSessionAuth,
	)
	leader := requireSingleLeader(t, set, "")
	preConnectLogPos := leader.LogPublisher().Position()

	followers := make([]sessionFollower, 0, 2)
	for _, m := range set.Modules() {
		if m.ThisMemberID() == leader.ThisMemberID() {
			continue
		}
		w := &logWitness{}
		m.LogAdapter().SetHandler(w)
		followers = append(followers, sessionFollower{module: m, witness: w})
	}
	require.Len(t, followers, 2)

	transport := startSessionClientTransport(t)
	result, connectErr := startSessionConnect(
		t, set, leader, transport, []byte(sessionSmokeCredentials),
	)
	require.NoError(t, connectErr)
	session, err := client.NewAeronCluster(result)
	require.NoError(t, err)
	harness.CleanupClose(t, "session", session.Close)

	require.GreaterOrEqual(t, session.ClusterSessionID(), int64(0))
	require.Equal(t, leader.LeadershipTermID(), session.LeadershipTermID())

	requireAwaitTick(t, set, "session open committed", durableWorkBound, sessionPump(
		transport,
		session,
		func() bool {
			return leader.Agent().OpenSessionCount() == 1 &&
				leader.CommitPosition() > preConnectLogPos
		},
	))

	payload := []byte("session-smoke-message")
	preMsgLogPos := leader.LogPublisher().Position()
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
			now := time.Now()
			if now.Sub(lastKeepalive) >= time.Second {
				_ = session.SendKeepAlive()
				lastKeepalive = now
			}
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
			set.State(),
		)
	}
	require.Positive(t, pos)

	var postAppendLogPos int64
	requireAwaitTick(t, set, "session message appended", durableWorkBound, sessionPump(
		transport,
		session,
		func() bool {
			postAppendLogPos = leader.LogPublisher().Position()
			return postAppendLogPos > preMsgLogPos
		},
	))

	wantSession := session.ClusterSessionID()
	commitStart := time.Now()
	committed := set.TickUntil(durableWorkBound, sessionPump(
		transport,
		session,
		func() bool {
			if !membersReachCommit(set, postAppendLogPos) {
				return false
			}
			for _, f := range followers {
				if !f.witness.sawSession(wantSession) {
					return false
				}
			}
			return true
		},
	))
	if !committed {
		require.FailNow(
			t,
			"session message committed did not complete",
			"wantSession=%d followers=%v bound=%s:\n%s",
			wantSession,
			followers,
			durableWorkBound,
			set.State(),
		)
	}
	t.Logf(
		"session message committed elapsed=%s\n%s",
		time.Since(commitStart),
		set.State(),
	)

	// Body Close sends one best-effort SessionCloseRequest. CleanupClose is
	// idempotent teardown only. A second Close does not re-offer. Registry
	// drain may complete via the close offer or via open-session idle TIMEOUT
	// (DefaultSessionTimeoutNs, 10s). closeBound covers both paths.
	require.NoError(t, session.Close())
	const closeBound = 30 * time.Second
	requireAwaitTick(t, set, "session registry drained", closeBound, func() bool {
		transport.DoWork()
		agent := leader.Agent()
		return agent.OpenSessionCount() == 0 &&
			agent.PendingSessionCount() == 0 &&
			agent.ClosingSessionCount() == 0
	})

	requireOwnedSessionClosesOnDriverShutdown(t, set, leader)
}

// ownedConnectOutcome carries the Connect result from the connect goroutine.
type ownedConnectOutcome struct {
	session *client.AeronCluster
	err     error
}

// requireOwnedSessionClosesOnDriverShutdown connects a session with a
// Connect-owned transport client on a new driver. A shutdown of that driver
// must close the session on SpinOnce within 5s.
func requireOwnedSessionClosesOnDriverShutdown(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leader *cluster.ConsensusModule,
) {
	t.Helper()

	// Phase: Connect pumps its own client on a side goroutine. The test
	// goroutine ticks the members until the outcome arrives.
	clientDriver := harness.Start(t)
	cfg := client.Config{
		IngressChannel:      cluster.DefaultIngressChannel,
		IngressEndpoints:    leader.Topology().IngressEndpoints(),
		EgressChannel:       "aeron:udp?endpoint=127.0.0.1:0",
		AeronDir:            clientDriver.Dir,
		MessageTimeout:      durableWorkBound,
		IngressRetryTimeout: durableWorkBound,
		CredentialsSupplier: fixedCredentialsSupplier{
			credentials: []byte(sessionSmokeCredentials),
		},
		EgressHandler: &client.EgressHandlers{},
		AeronConfig: aeron.Config{
			InterServiceTimeout: harness.ConnectedPollTimeout,
			CloseLingerDuration: 250 * time.Millisecond,
		},
	}
	outcomes := make(chan ownedConnectOutcome, 1)
	received := false
	t.Cleanup(func() {
		if received {
			return
		}
		if outcome := <-outcomes; outcome.session != nil {
			_ = outcome.session.Close()
		}
	})
	go func() {
		session, err := client.Connect(t.Context(), cfg)
		outcomes <- ownedConnectOutcome{session: session, err: err}
	}()
	var outcome ownedConnectOutcome
	received = set.TickUntil(durableWorkBound, func() bool {
		select {
		case outcome = <-outcomes:
			return true
		default:
			return false
		}
	})
	if !received {
		require.FailNow(t, "owned connect did not resolve", "%s", set.State())
	}
	require.NoError(t, outcome.err, "owned connect")
	session := outcome.session
	harness.CleanupClose(t, "owned session", session.Close)
	require.False(t, session.IsClosed())
	require.NotNil(t, session.Aeron())

	// Phase: shut down the client driver. SpinOnce closes the session.
	require.NoError(t, clientDriver.Shutdown())
	requireAwaitTick(t, set, "owned session closed on driver shutdown", 5*time.Second, func() bool {
		session.SpinOnce()
		return session.IsClosed()
	})
	require.Nil(t, session.Aeron())
	fault := session.TransportFault()
	require.ErrorIs(t, fault, client.ErrTransportTerminating)
	_, ok := errors.AsType[*aeron.DriverTimeoutError](fault)
	require.True(t, ok, "fault: %v", fault)
	require.ErrorIs(t, fault, conductor.ErrDriverShutdown)
}

// TestClusterSession_rejectsInvalidCredentials asserts wrong credentials fail
// connect with the authentication-rejected error pair, leave the leader log
// position unchanged, and leave the registry empty.
func TestClusterSession_rejectsInvalidCredentials(t *testing.T) {
	t.Parallel()

	set := openThreeNodeCluster(
		t,
		harness.ClusterSessionRejectPortBase,
		harness.ClusterSessionRejectLogEndpoint,
		amendSessionAuth,
	)
	leader := requireSingleLeader(t, set, "")
	preLogPos := leader.LogPublisher().Position()

	transport := startSessionClientTransport(t)
	result, connectErr := startSessionConnect(
		t, set, leader, transport, []byte("wrong-credentials"),
	)
	require.Nil(t, result)
	require.Error(t, connectErr)
	require.ErrorContains(t, connectErr, "authentication rejected")
	require.ErrorContains(t, connectErr, "session failed authentication")

	// TickUntil runs member agents for the settle bound. A rejected connect
	// that still appends advances the log and ends the wait early.
	const settleBound = 500 * time.Millisecond
	appended := set.TickUntil(settleBound, func() bool {
		return leader.LogPublisher().Position() != preLogPos
	})
	if appended {
		require.FailNow(
			t,
			"rejected connect appended to the leader log",
			"prePos=%d postPos=%d:\n%s",
			preLogPos,
			leader.LogPublisher().Position(),
			set.State(),
		)
	}
	requireAwaitTick(t, set, "reject registry empty", durableWorkBound, func() bool {
		agent := leader.Agent()
		return agent.OpenSessionCount() == 0 && agent.PendingSessionCount() == 0
	})
}
