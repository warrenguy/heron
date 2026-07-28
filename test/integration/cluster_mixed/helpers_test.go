//go:build integration && cluster_mixed && cluster_java

package cluster_mixed_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/idlestrategy"
	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
	"github.com/warrenguy/heron/test/integration/harness"
)

// mixedMemberCloseLinger bounds Client.Close linger at module teardown.
// Nothing polls after the duty cycle stops.
const mixedMemberCloseLinger = 250 * time.Millisecond

// mixedMemberFixture holds one Go consensus member on the external aeronmd.
//
// Concurrency: invoker-only. Every method runs on the test goroutine.
//
// Lifetime: Open registers module Close on t.Cleanup. writeReady runs
// immediately after open, before any assertion.
type mixedMemberFixture struct {
	module *cluster.ConsensusModule
	runner *cluster.Runner
	driver *harness.MediaDriver
}

// requireMixedComposeGate skips when HERON_CLUSTER_MIXED_COMPOSE is unset.
// When that gate env is set, AERON_DIR must equal MixedAeronDirGo or the test fatals.
// JoinExternal polls AERON_DIR for a ready CnC file at open.
func requireMixedComposeGate(t *testing.T) {
	t.Helper()

	if os.Getenv(clusterjava.MixedComposeGateEnvKey) != "1" {
		t.Skip(clusterjava.MixedComposeSkipHint)
	}
	if got := os.Getenv("AERON_DIR"); got != clusterjava.MixedAeronDirGo {
		t.Fatalf(
			"AERON_DIR must equal %s when HERON_CLUSTER_MIXED_COMPOSE is set (got %q)",
			clusterjava.MixedAeronDirGo,
			got,
		)
	}
}

// buildMixedJavaLeaderConfig builds the Go member config for a Java-led term.
// AppointedLeaderID is 0. Members string is the golden.
// LogChannel stays empty so Normalize takes DefaultLogChannel (unicast template).
func buildMixedJavaLeaderConfig() cluster.Config {
	appointed := clusterjava.MixedAppointedLeaderIDJavaLeader
	return cluster.Config{
		MemberID:          clusterjava.MixedGoMemberID,
		ClusterMembers:    clusterjava.MixedClusterMembersJavaLeader,
		ClusterDir:        clusterjava.MixedClusterDirGo,
		AeronDir:          clusterjava.MixedAeronDirGo,
		AppointedLeaderID: &appointed,
		AeronConfig: aeron.Config{
			InterServiceTimeout: harness.ConnectedPollTimeout,
			CloseLingerDuration: mixedMemberCloseLinger,
		},
	}
}

// openMixedGoMember joins the external driver, opens the consensus module, and
// builds a Runner. Does not write the readiness file. Caller writes next.
func openMixedGoMember(t *testing.T, cfg cluster.Config) *mixedMemberFixture {
	t.Helper()

	// JoinExternal registers external-driver Stop on t.Cleanup.
	driver := harness.JoinExternal(t, cfg.AeronDir)

	module, err := cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
		Config: cfg,
	})
	require.NoError(t, err)
	require.NotNil(t, module)
	harness.CleanupClose(t, "mixed go consensus module", module.Close)

	runner, err := cluster.NewRunner(cluster.RunnerDeps{
		Client: module.Client(),
		Agent:  module.Agent(),
		Idle:   module.IdleStrategy(),
		Clock:  module.NanoClock(),
	})
	require.NoError(t, err)

	return &mixedMemberFixture{
		module: module,
		runner: runner,
		driver: driver,
	}
}

// writeReady creates the readiness file the Go service healthcheck tests.
// Call immediately after OpenConsensusModule returns, before any assertion.
func writeReady(t *testing.T) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(clusterjava.MixedReadyPath), 0o755))
	require.NoError(t, os.WriteFile(clusterjava.MixedReadyPath, []byte("ready\n"), 0o644))
}

// tick runs one duty cycle: transport client then agent.
func (f *mixedMemberFixture) tick() {
	f.runner.DoWork()
}

// tickUntil runs the duty cycle until ok reports true or the budget elapses.
//
// Returns:
//   - true when ok reports true before the budget
//   - false on timeout
//
// Sleep is 1ms between duty cycles.
func (f *mixedMemberFixture) tickUntil(budget time.Duration, ok func() bool) bool {
	return harness.PollUntilBool(budget, time.Millisecond, func() bool {
		f.tick()
		return ok()
	})
}

// awaitJavaModuleState waits until each Java member's CnC is readable and its
// module state counter (type 200) is allocated. Opens counters lazily as each
// CnC appears. Ticks the Go member so canvass continues while Java starts.
func awaitJavaModuleState(
	t *testing.T,
	f *mixedMemberFixture,
	budget time.Duration,
) []*harness.DriverCounters {
	t.Helper()

	dirs := []string{clusterjava.MixedAeronDirMember0, clusterjava.MixedAeronDirMember1}
	out := make([]*harness.DriverCounters, len(dirs))
	start := time.Now()

	ok := f.tickUntil(budget, func() bool {
		all := true
		for i, dir := range dirs {
			if out[i] == nil {
				c, err := harness.OpenDriverCounters(dir)
				if err != nil {
					all = false
					continue
				}
				harness.CleanupClose(t, fmt.Sprintf("driver counters %s", dir), c.Close)
				out[i] = c
			}
			if _, ok := out[i].Value(consensus.ClusterConsensusModuleStateCounterTypeID); !ok {
				all = false
			}
		}
		return all
	})

	if !ok {
		// Name each aeronDir: a missing CnC is the common startup failure and
		// dumpMixedState reports counters, not directories.
		var b strings.Builder
		for i, dir := range dirs {
			fmt.Fprintf(&b, "javaMember=%d aeronDir=%s cncOpen=%v\n", i, dir, out[i] != nil)
		}
		t.Fatalf(
			"java module state counter not allocated after %s (budget %s):\n%s%s",
			time.Since(start),
			budget,
			b.String(),
			dumpMixedState(f.module, out),
		)
	}

	t.Logf("java module state allocated elapsed=%s budget=%s", time.Since(start), budget)
	return out
}

// awaitJavaLeaderElection ticks the Go member until election is complete with
// Java member 0 as the sole leader.
func awaitJavaLeaderElection(
	t *testing.T,
	f *mixedMemberFixture,
	java []*harness.DriverCounters,
	budget time.Duration,
) {
	t.Helper()

	start := time.Now()
	ok := f.tickUntil(budget, func() bool {
		if !f.module.ElectionComplete() {
			return false
		}
		if f.module.IsLeader() {
			return false
		}
		if f.module.LeaderMemberID() != clusterjava.MixedAppointedLeaderIDJavaLeader {
			return false
		}
		if f.module.LeadershipTermID() < 0 {
			return false
		}

		leaders := 0
		var leaderID int32 = -1
		for i, c := range java {
			roleVal, roleOK := c.Value(consensus.ClusterNodeRoleCounterTypeID)
			if !roleOK {
				return false
			}
			role, err := consensus.RoleFromCode(roleVal)
			if err != nil {
				return false
			}
			if role == consensus.RoleLeader {
				leaders++
				leaderID = int32(i)
			}

			termVal, termOK := c.Value(consensus.ClusterLeadershipTermIDCounterTypeID)
			if !termOK || termVal != f.module.LeadershipTermID() {
				return false
			}
		}

		// Exactly one of the three reports leader: Go is not leader; one Java is.
		return leaders == 1 && leaderID == clusterjava.MixedAppointedLeaderIDJavaLeader
	})

	elapsed := time.Since(start)
	if !ok {
		t.Fatalf(
			"election did not agree on Java member 0 as leader after %s (budget %s):\n%s",
			elapsed,
			budget,
			dumpMixedState(f.module, java),
		)
	}
	t.Logf("election elapsed=%s budget=%s\n%s", elapsed, budget, dumpMixedState(f.module, java))
}

// requireLiveLogImage asserts the Go follower holds the leader's log image at
// join position 0. Failure names start-order or membership, not commit.
func requireLiveLogImage(t *testing.T, f *mixedMemberFixture) {
	t.Helper()

	// Snapshot state once. No duty cycle runs between the asserts below.
	state := harness.ClusterMemberState(f.module)

	adapter := f.module.LogAdapter()
	require.NotNil(t, adapter, "log adapter missing:\n%s", state)
	image := adapter.Image()
	require.NotNil(
		t,
		image,
		"live log image missing; Go member did not join the leader log "+
			"(term=%d):\n%s",
		f.module.LeadershipTermID(),
		state,
	)

	streams := f.module.LogStreams()
	require.NotNil(t, streams, "log streams missing:\n%s", state)
	pub := streams.Publication()
	require.NotNil(t, pub, "log publication missing:\n%s", state)
	ownSession := pub.SessionID()

	// Leader log session is the adapter image session. The image session must not
	// equal the member's own log publication session (self-image under unicast).
	require.NotEqual(
		t,
		ownSession,
		image.SessionID(),
		"log adapter image is the self-image, not the leader log session "+
			"(term=%d ownSession=%d imageSession=%d):\n%s",
		f.module.LeadershipTermID(),
		ownSession,
		image.SessionID(),
		state,
	)
	require.Equal(
		t,
		int64(0),
		image.JoinPosition(),
		"live log image join position is not 0; Go member may have missed the "+
			"first term start (term=%d joinPosition=%d imageSession=%d):\n%s",
		f.module.LeadershipTermID(),
		image.JoinPosition(),
		image.SessionID(),
		state,
	)
}

// induceSessionMessage opens a cluster client against the two Java ingress
// endpoints, offers one message, and closes the session. Payload is unasserted.
func induceSessionMessage(t *testing.T, f *mixedMemberFixture) {
	t.Helper()

	// Induction client uses its own transport client on the shared driver.
	// The module client is invoker-only on the test goroutine.
	// Payload is unasserted. Empty handlers silent no-op all egress events.
	aeronClient, hadClientError := harness.ConnectIPCClientConfig(t, f.driver, aeron.Config{
		InterServiceTimeout: harness.ConnectedPollTimeout,
		CloseLingerDuration: mixedMemberCloseLinger,
		ErrorHandler: func(err error) {
			t.Errorf("induction client error: %v", err)
		},
	})

	cfg := client.Config{
		IngressChannel:   clusterjava.MixedIngressChannel,
		EgressChannel:    clusterjava.MixedEgressChannel,
		IngressEndpoints: clusterjava.MixedIngressEndpoints,
		AeronDir:         f.driver.Dir,
		Aeron:            aeronClient,
		MessageTimeout:   clusterjava.MixedConnectBudget,
		EgressHandler:    &client.EgressHandlers{},
	}

	ac, err := client.NewAsyncConnect(t.Context(), cfg)
	require.NoError(t, err)
	harness.CleanupClose(t, "close async connect", ac.Close)

	connectStart := time.Now()
	var result *client.ConnectResult
	var pollErr error
	connected := f.tickUntil(clusterjava.MixedConnectBudget, func() bool {
		aeronClient.DoWork()
		result, pollErr = ac.Poll()
		return pollErr != nil || result != nil
	})
	if pollErr != nil {
		t.Fatalf(
			"induction connect failed: %v (state=%v step=%v hadClientError=%v) elapsed=%s",
			pollErr,
			ac.State(),
			ac.Step(),
			hadClientError.Load(),
			time.Since(connectStart),
		)
	}
	if !connected {
		t.Fatalf(
			"induction connect timed out after %s (budget %s): state=%v step=%v hadClientError=%v",
			time.Since(connectStart),
			clusterjava.MixedConnectBudget,
			ac.State(),
			ac.Step(),
			hadClientError.Load(),
		)
	}
	t.Logf(
		"induction connect elapsed=%s clusterSessionId=%d leaderMemberId=%d",
		time.Since(connectStart),
		result.ClusterSessionID,
		result.LeaderMemberID,
	)

	session, err := client.NewAeronCluster(result)
	require.NoError(t, err)

	// Close before fixture cleanup so the session does not outlive the cluster.
	defer func() {
		if closeErr := session.Close(); closeErr != nil {
			t.Errorf("close induction session: %v", closeErr)
		}
	}()
	require.Equal(t, client.SessionStateConnected, session.SessionState())

	payload := []byte("heron-mixed-induce")
	offerStart := time.Now()
	offerCtx, cancel := context.WithTimeout(t.Context(), harness.ConnectedPollTimeout)
	defer cancel()
	pos, offerErr := session.IngressOfferUntilPositive(
		offerCtx,
		client.IngressOfferConfig{
			IdleStrategy: idlestrategy.NewSleeping(aeron.DefaultIdleSleepDuration),
		},
		func() int64 {
			f.tick()
			aeronClient.DoWork()
			require.False(t, hadClientError.Load(), "transport error during induction offer")
			return session.Offer(payload)
		},
	)
	if offerErr != nil {
		t.Fatalf(
			"induction offer failed: %v sessionState=%v hadClientError=%v elapsed=%s",
			offerErr,
			session.SessionState(),
			hadClientError.Load(),
			time.Since(offerStart),
		)
	}
	require.Positive(t, pos, "induction offer position")
	t.Logf("induction offer elapsed=%s position=%d", time.Since(offerStart), pos)
}

// awaitNotifiedCommitAdvance waits for NotifiedCommitPosition to rise above
// recorded.
func awaitNotifiedCommitAdvance(
	t *testing.T,
	f *mixedMemberFixture,
	recorded int64,
	budget time.Duration,
) {
	t.Helper()

	start := time.Now()
	ok := f.tickUntil(budget, func() bool {
		return f.module.NotifiedCommitPosition() > recorded
	})
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf(
			"commit did not advance on the Go follower after %s (budget %s): "+
				"recorded=%d current=%d:\n%s",
			elapsed,
			budget,
			recorded,
			f.module.NotifiedCommitPosition(),
			harness.ClusterMemberState(f.module),
		)
	}
	t.Logf(
		"commit advance elapsed=%s budget=%s recorded=%d current=%d",
		elapsed,
		budget,
		recorded,
		f.module.NotifiedCommitPosition(),
	)
}

// dumpMixedState dumps the Go member and each Java counter set.
func dumpMixedState(
	m *cluster.ConsensusModule,
	java []*harness.DriverCounters,
) string {
	var b strings.Builder
	b.WriteString(harness.ClusterMemberState(m))
	for i, c := range java {
		role, roleOK := c.Value(consensus.ClusterNodeRoleCounterTypeID)
		term, termOK := c.Value(consensus.ClusterLeadershipTermIDCounterTypeID)
		state, stateOK := c.Value(consensus.ClusterConsensusModuleStateCounterTypeID)
		commit, commitOK := c.Value(consensus.ClusterCommitPositionCounterTypeID)
		election, electionOK := c.Value(consensus.ClusterElectionStateCounterTypeID)
		fmt.Fprintf(
			&b,
			"javaMember=%d stateOK=%v state=%d roleOK=%v role=%d termOK=%v term=%d "+
				"commitOK=%v commit=%d electionOK=%v election=%d\n",
			i,
			stateOK,
			state,
			roleOK,
			role,
			termOK,
			term,
			commitOK,
			commit,
			electionOK,
			election,
		)
	}
	return b.String()
}
