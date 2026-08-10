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
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/idlestrategy"
	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
	"github.com/warrenguy/heron/test/integration/harness"
)

// mixedMemberCloseLinger bounds Client.Close linger at module teardown.
// Nothing polls after the duty cycle stops.
const mixedMemberCloseLinger = 250 * time.Millisecond

// mixedIdlePollInterval paces polls that run no Go duty cycle.
// Nothing local advances between reads. A tighter interval only burns wakeups.
const mixedIdlePollInterval = 50 * time.Millisecond

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

// requireMixedCompose skips with skipHint when envKey is unset.
// envKey is the per-overlay gate (java-leader or go-leader compose env).
// skipHint names the make target that runs the caller's smoke.
//
// When the gate is set, AERON_DIR must equal MixedAeronDirGo or the test fatals.
// JoinExternal polls AERON_DIR for a ready CnC file at open.
func requireMixedCompose(t *testing.T, envKey, skipHint string) {
	t.Helper()

	if os.Getenv(envKey) != "1" {
		t.Skip(skipHint)
	}
	if got := os.Getenv("AERON_DIR"); got != clusterjava.MixedAeronDirGo {
		t.Fatalf(
			"AERON_DIR must equal %s when %s is set (got %q)",
			clusterjava.MixedAeronDirGo,
			envKey,
			got,
		)
	}
}

// buildMixedJavaLeaderConfig builds the Go member config for a Java-led term.
// AppointedLeaderID is 0. ClusterMembers uses the compose host list.
// LogChannel stays empty so Normalize takes DefaultLogChannel (unicast template).
func buildMixedJavaLeaderConfig(t *testing.T) cluster.Config {
	t.Helper()

	members, err := clusterjava.MixedComposeClusterMembers("")
	require.NoError(t, err)

	appointed := clusterjava.MixedAppointedLeaderIDJavaLeader
	return cluster.Config{
		MemberID:                      clusterjava.MixedGoMemberID,
		ClusterMembers:                members,
		ClusterDir:                    clusterjava.MixedClusterDirGo,
		AeronDir:                      clusterjava.MixedAeronDirGo,
		AppointedLeaderID:             &appointed,
		ArchiveControlRequestChannel:  archive.DefaultLocalControlChannel,
		ArchiveControlResponseChannel: archive.DefaultLocalControlChannel,
		AeronConfig: aeron.Config{
			InterServiceTimeout: harness.ConnectedPollTimeout,
			CloseLingerDuration: mixedMemberCloseLinger,
		},
	}
}

// buildMixedGoLeaderConfig builds the Go member config for a Go-led term.
// AppointedLeaderID is 2. ClusterMembers uses the shared multicast log endpoint
// on the compose host list. LogChannel is the multicast template
// (interface + ttl only; no endpoint=).
func buildMixedGoLeaderConfig(t *testing.T) cluster.Config {
	t.Helper()

	members, err := clusterjava.MixedComposeClusterMembers(clusterjava.MixedLogEndpoint)
	require.NoError(t, err)

	appointed := clusterjava.MixedAppointedLeaderIDGoLeader
	return cluster.Config{
		MemberID:                      clusterjava.MixedGoMemberID,
		ClusterMembers:                members,
		ClusterDir:                    clusterjava.MixedClusterDirGo,
		AeronDir:                      clusterjava.MixedAeronDirGo,
		LogChannel:                    clusterjava.MixedLogChannelMulticast,
		AppointedLeaderID:             &appointed,
		ArchiveControlRequestChannel:  archive.DefaultLocalControlChannel,
		ArchiveControlResponseChannel: archive.DefaultLocalControlChannel,
		AeronConfig: aeron.Config{
			InterServiceTimeout: harness.ConnectedPollTimeout,
			CloseLingerDuration: mixedMemberCloseLinger,
		},
	}
}

// requireMixedAeronDirsPresent fatals when any of the three mixed Aeron
// directories is missing. go-leader requires Java peers up before Go opens, so
// all three CnC trees exist when the smoke starts.
func requireMixedAeronDirsPresent(t *testing.T) {
	t.Helper()

	dirs := []string{
		clusterjava.MixedAeronDirMember0,
		clusterjava.MixedAeronDirMember1,
		clusterjava.MixedAeronDirGo,
	}
	for _, dir := range dirs {
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("mixed aeron dir missing: %s: %v", dir, err)
		}
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
		Client:  module.Client(),
		Agent:   module.Agent(),
		Archive: module.ArchiveClient(),
		Idle:    module.IdleStrategy(),
		Clock:   module.NanoClock(),
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

// tick runs one Runner.DoWork: transport client, archive Poll, then agent.
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
// module state counter (type 200) is allocated.
// Opens counters lazily as each CnC appears.
//
// Pass nil f when Java peers start before the Go member opens.
// A non-nil f runs the Go duty cycle on every poll.
// Canvass continues while Java starts.
func awaitJavaModuleState(
	t *testing.T,
	f *mixedMemberFixture,
	budget time.Duration,
) []*harness.DriverCounters {
	t.Helper()

	dirs := []string{clusterjava.MixedAeronDirMember0, clusterjava.MixedAeronDirMember1}
	out := make([]*harness.DriverCounters, len(dirs))
	start := time.Now()

	interval := mixedIdlePollInterval
	if f != nil {
		interval = time.Millisecond
	}

	ok := harness.PollUntilBool(budget, interval, func() bool {
		if f != nil {
			f.tick()
		}
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
		var module *cluster.ConsensusModule
		if f != nil {
			module = f.module
		}
		t.Fatalf(
			"java module state counter not allocated after %s (budget %s):\n%s%s",
			time.Since(start),
			budget,
			b.String(),
			dumpMixedState(module, out),
		)
	}

	t.Logf("java module state allocated elapsed=%s budget=%s", time.Since(start), budget)
	return out
}

// javaMemberRole reads one Java member's role counter.
// Reports false when the counter is unallocated or holds an unknown code.
func javaMemberRole(c *harness.DriverCounters) (consensus.Role, bool) {
	val, ok := c.Value(consensus.ClusterNodeRoleCounterTypeID)
	if !ok {
		return 0, false
	}
	role, err := consensus.RoleFromCode(val)
	if err != nil {
		return 0, false
	}
	return role, true
}

// javaMembersAtTerm reports whether every Java member's leadership term counter
// equals term.
func javaMembersAtTerm(java []*harness.DriverCounters, term int64) bool {
	for _, c := range java {
		val, ok := c.Value(consensus.ClusterLeadershipTermIDCounterTypeID)
		if !ok || val != term {
			return false
		}
	}
	return true
}

// javaCommitPositions reads each Java member's commit counter (type 203) in id
// order. Reports false when any counter is unallocated. Values may be zero.
func javaCommitPositions(java []*harness.DriverCounters) ([]int64, bool) {
	out := make([]int64, len(java))
	allocated := true
	for i, c := range java {
		val, ok := c.Value(consensus.ClusterCommitPositionCounterTypeID)
		if !ok {
			allocated = false
			continue
		}
		out[i] = val
	}
	return out, allocated
}

// recordJavaCommitPositions requires each Java member's commit counter (type
// 203) to be allocated and returns the current values. Values may be zero.
// Call after module state is allocated and before the Go member opens as leader.
func recordJavaCommitPositions(
	t *testing.T,
	java []*harness.DriverCounters,
) []int64 {
	t.Helper()

	out, ok := javaCommitPositions(java)
	if !ok {
		t.Fatalf(
			"java commit counter (type 203) not allocated: current=%v\n%s",
			out,
			dumpMixedState(nil, java),
		)
	}
	t.Logf("java commit positions recorded=%v", out)
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
			role, roleOK := javaMemberRole(c)
			if !roleOK {
				return false
			}
			if role == consensus.RoleLeader {
				leaders++
				leaderID = int32(i)
			}
		}
		if !javaMembersAtTerm(java, f.module.LeadershipTermID()) {
			return false
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

// requireGoLogRecording asserts the Go member holds an active recording of the
// cluster log. Returns the recording id and the current recorded position.
//
// wantLogSessionID is the session the recording must name: the leader log
// image session on a follower, or the member's own log publication session on
// a leader.
//
// Behaviour: asserts all of:
//   - non-null recording id
//   - active recording
//   - catalog descriptor stream and session match
//   - TermRecordingID matches LocalLogRecordingID
//   - type-100 counter present for that id
//
// Concurrency: invoker-only. Runs no duty cycle.
func requireGoLogRecording(
	t *testing.T,
	f *mixedMemberFixture,
	wantLogSessionID int32,
) (
	recordingID int64,
	recordedPosition int64,
) {
	t.Helper()

	state := dumpMixedState(f.module, nil)
	agent := f.module.Agent()
	require.NotNil(t, agent, "agent missing:\n%s", state)

	recordingID = agent.LocalLogRecordingID()
	require.NotEqual(
		t,
		recordinglog.NullRecordingID,
		recordingID,
		"LocalLogRecordingID is null:\n%s",
		state,
	)
	require.True(
		t,
		agent.IsRecordingActive(),
		"recording not active (recordingID=%d):\n%s",
		recordingID,
		state,
	)

	topology := f.module.Topology()
	require.NotNil(t, topology, "topology missing:\n%s", state)
	wantStreamID := topology.LogStreamID()

	archiveClient := f.module.ArchiveClient()
	require.NotNil(t, archiveClient, "archive client missing:\n%s", state)
	desc, err := archiveClient.ListRecording(t.Context(), recordingID)
	require.NoError(t, err, "ListRecording(%d):\n%s", recordingID, state)
	require.Equal(
		t,
		wantStreamID,
		desc.StreamID,
		"recording stream id (recordingID=%d):\n%s",
		recordingID,
		state,
	)
	// Session must name the cluster log under test, not another publication on
	// the same channel.
	require.Equal(
		t,
		wantLogSessionID,
		desc.SessionID,
		"recording session id (recordingID=%d wantSession=%d):\n%s",
		recordingID,
		wantLogSessionID,
		state,
	)

	require.Equal(
		t,
		recordingID,
		f.module.RecordingLog().TermRecordingID(),
		"recording-log TermRecordingID (recordingID=%d):\n%s",
		recordingID,
		state,
	)

	recordedPosition = harness.RecordingCounterPosition(f.module, recordingID)
	require.GreaterOrEqual(
		t,
		recordedPosition,
		int64(0),
		"type-100 recording position counter missing (recordingID=%d):\n%s",
		recordingID,
		state,
	)

	return recordingID, recordedPosition
}

// awaitGoRecordingReaches ticks the duty cycle until the recorded position
// reaches wantPosition.
//
// Behaviour: fatals with the mixed state dump when the budget elapses. The
// caller captures wantPosition before the wait, so the wait reads only the
// type-100 counter. current of -1 in the failure output means that counter
// was missing.
//
// Concurrency: invoker-only.
func awaitGoRecordingReaches(
	t *testing.T,
	f *mixedMemberFixture,
	java []*harness.DriverCounters,
	recordingID int64,
	wantPosition int64,
	budget time.Duration,
) {
	t.Helper()

	start := time.Now()
	current := int64(-1)
	ok := f.tickUntil(budget, func() bool {
		current = harness.RecordingCounterPosition(f.module, recordingID)
		return current >= wantPosition
	})
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf(
			"recording position did not reach commit after %s (budget %s): "+
				"recordingID=%d want=%d current=%d:\n%s",
			elapsed,
			budget,
			recordingID,
			wantPosition,
			current,
			dumpMixedState(f.module, java),
		)
	}
	t.Logf(
		"recording position reached elapsed=%s budget=%s recordingID=%d want=%d current=%d",
		elapsed,
		budget,
		recordingID,
		wantPosition,
		current,
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
		CloseLingerDuration: mixedMemberCloseLinger,
		ErrorHandler: func(err error) {
			t.Errorf("induction client error: %v", err)
		},
	})

	egress, err := clusterjava.MixedComposeEgressChannel()
	require.NoError(t, err)
	ingress, err := clusterjava.MixedComposeIngressEndpoints()
	require.NoError(t, err)

	cfg := client.Config{
		IngressChannel:   clusterjava.MixedIngressChannel,
		EgressChannel:    egress,
		IngressEndpoints: ingress,
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

// awaitJavaServiceSawTermStart asserts both Java members write a service-probe
// file for the current Go LeadershipTermID.
//
// Required fields per member:
// - term equals Go LeadershipTermID
// - logPosition >= 0
// - role is FOLLOWER
// - memberId matches the member path
//
// Asserts Java ClusteredService decoded the term-start log event.
// Does not treat the consensus commit counter alone as enough.
//
// Ticks the Go duty cycle while polling.
// Re-reads both probes each poll.
// Both members must report the same term to pass.
//
// On failure: dumps probe paths, file content, and dumpMixedState.
func awaitJavaServiceSawTermStart(
	t *testing.T,
	f *mixedMemberFixture,
	java []*harness.DriverCounters,
	budget time.Duration,
) {
	t.Helper()

	paths := [2]string{
		clusterjava.ServiceProbePath(0),
		clusterjava.ServiceProbePath(1),
	}
	start := time.Now()

	var probes [2]clusterjava.ServiceProbe

	ok := f.tickUntil(budget, func() bool {
		wantTerm := f.module.LeadershipTermID()
		for i := range paths {
			data, err := os.ReadFile(paths[i])
			if err != nil {
				return false
			}
			probe, err := clusterjava.ParseServiceProbe(data)
			if err != nil {
				return false
			}
			if probe.Term != wantTerm ||
				probe.LogPosition < 0 ||
				probe.Role != "FOLLOWER" ||
				probe.MemberID != int32(i) {
				return false
			}
			probes[i] = probe
		}
		return true
	})

	elapsed := time.Since(start)
	if !ok {
		var b strings.Builder
		for i := range paths {
			fmt.Fprintf(&b, "javaMember=%d path=%s", i, paths[i])
			if data, err := os.ReadFile(paths[i]); err != nil {
				fmt.Fprintf(&b, " err=%v\n", err)
			} else {
				fmt.Fprintf(&b, " content=%q\n", string(data))
			}
		}
		t.Fatalf(
			"java service-probe did not report term start after %s (budget %s): "+
				"wantTerm=%d\n%s%s",
			elapsed,
			budget,
			f.module.LeadershipTermID(),
			b.String(),
			dumpMixedState(f.module, java),
		)
	}

	for i := range paths {
		t.Logf(
			"java service-probe member=%d term=%d logPosition=%d role=%s elapsed=%s",
			i,
			probes[i].Term,
			probes[i].LogPosition,
			probes[i].Role,
			elapsed,
		)
	}
}

// awaitGoLeaderElection ticks until the Go member leads and both Java members
// report follower with a matching leadership term.
//
// LeaderMemberID is -1 on the leader (no append target). Asserts -1, never
// self.
//
// Returns the first type-100 recording-position sample seen during the wait.
// LOCAL recording starts on the leader-init tick. The term-start append runs
// on the election-completion tick in this same duty-cycle loop. Sampling
// between those ticks keeps the value below the commit that follows. A
// post-election sample can already equal that commit once the archive
// records the term-start publication. Fatals when no sample appears before
// the election completes.
func awaitGoLeaderElection(
	t *testing.T,
	f *mixedMemberFixture,
	java []*harness.DriverCounters,
	budget time.Duration,
) int64 {
	t.Helper()

	recordingBefore := int64(-1)
	sampleRecording := func() {
		if recordingBefore >= 0 {
			return
		}
		agent := f.module.Agent()
		if agent == nil {
			return
		}
		pos := harness.RecordingCounterPosition(f.module, agent.LocalLogRecordingID())
		if pos >= 0 {
			recordingBefore = pos
		}
	}

	start := time.Now()
	ok := f.tickUntil(budget, func() bool {
		// Sample only while the election is incomplete. tickUntil duty-cycles
		// first, so a sample on the completion poll is already after the
		// term-start append.
		if !f.module.ElectionComplete() {
			sampleRecording()
			return false
		}
		if !f.module.IsLeader() {
			return false
		}
		if f.module.LeaderMemberID() != -1 {
			return false
		}
		if f.module.LeadershipTermID() < 0 {
			return false
		}

		for _, c := range java {
			role, roleOK := javaMemberRole(c)
			if !roleOK || role != consensus.RoleFollower {
				return false
			}
		}
		return javaMembersAtTerm(java, f.module.LeadershipTermID())
	})

	elapsed := time.Since(start)
	if !ok {
		t.Fatalf(
			"election did not agree on Go member as leader after %s (budget %s):\n%s",
			elapsed,
			budget,
			dumpMixedState(f.module, java),
		)
	}
	require.GreaterOrEqual(
		t,
		recordingBefore,
		int64(0),
		"no recording position sample observed during election:\n%s",
		dumpMixedState(f.module, java),
	)
	t.Logf(
		"go leader election elapsed=%s budget=%s\n%s",
		elapsed,
		budget,
		dumpMixedState(f.module, java),
	)
	return recordingBefore
}

// awaitJavaCommitAdvance waits for both Java commit counters to rise strictly
// above recorded and for the Go leader CommitPosition to rise above 0.
// Does not assert NotifiedCommitPosition (leader path does not advance it).
func awaitJavaCommitAdvance(
	t *testing.T,
	f *mixedMemberFixture,
	java []*harness.DriverCounters,
	recorded []int64,
	budget time.Duration,
) {
	t.Helper()

	require.Len(t, recorded, len(java))

	start := time.Now()
	ok := f.tickUntil(budget, func() bool {
		if f.module.CommitPosition() <= 0 {
			return false
		}
		for i, c := range java {
			val, valOK := c.Value(consensus.ClusterCommitPositionCounterTypeID)
			if !valOK || val <= recorded[i] {
				return false
			}
		}
		return true
	})

	elapsed := time.Since(start)
	current, _ := javaCommitPositions(java)
	if !ok {
		t.Fatalf(
			"commit did not advance under Go leader after %s (budget %s): "+
				"recorded=%v current=%v goCommit=%d:\n%s",
			elapsed,
			budget,
			recorded,
			current,
			f.module.CommitPosition(),
			dumpMixedState(f.module, java),
		)
	}
	t.Logf(
		"java commit advance elapsed=%s budget=%s recorded=%v current=%v goCommit=%d",
		elapsed,
		budget,
		recorded,
		current,
		f.module.CommitPosition(),
	)
}

// dumpMixedState dumps the Go member and each Java counter set.
// module may be nil when Go has not opened yet.
func dumpMixedState(
	m *cluster.ConsensusModule,
	java []*harness.DriverCounters,
) string {
	var b strings.Builder
	b.WriteString(harness.ClusterMemberState(m))
	for i, c := range java {
		if c == nil {
			fmt.Fprintf(&b, "javaMember=%d counters=<nil>\n", i)
			continue
		}
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
