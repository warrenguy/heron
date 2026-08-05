//go:build integration && cluster_mixed && cluster_java

package cluster_mixed_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
)

// TestClusterMixedJavaLeader elects a Java appointed leader and advances commit
// across the language boundary. Runs only under
// HERON_CLUSTER_MIXED_JAVA_LEADER_COMPOSE.
//
// Asserts durable log recording after elect: the Go follower records the
// received leader log into its own Archive, and the recorded position reaches
// a commit position the member observed. Makes no claim about Java members'
// recordings.
//
// No t.Parallel: owns fixed ports and one cluster.
func TestClusterMixedJavaLeader(t *testing.T) {
	requireMixedCompose(
		t,
		clusterjava.MixedComposeJavaLeaderEnvKey,
		clusterjava.MixedComposeSkipHintJavaLeader,
	)

	// Open the Go module before Java starts. Compose enforces that order.
	f := openMixedGoMember(t, buildMixedJavaLeaderConfig(t))

	// Readiness releases Java depends_on. Write before any later assertion.
	writeReady(t)

	// Java up: each Java member has allocated a module state counter.
	// Compose starts Java only after the readiness file.
	java := awaitJavaModuleState(t, f, clusterjava.MixedStartupBudget)

	// Elect: Java member 0 leads; Go follows; terms agree.
	awaitJavaLeaderElection(t, f, java, clusterjava.MixedElectionBudget)

	// Join: connected log image on the leader session at join position 0.
	requireLiveLogImage(t, f)

	// Cold start keeps a zero position base for later recording comparisons.
	require.True(t, f.module.IsColdStart())

	// Record: active catalog recording of the leader log session. The returned
	// position is the pre-induction baseline for the commit-wait floor.
	recordingID, recordingBefore := requireGoLogRecording(
		t,
		f,
		f.module.LogAdapter().Image().SessionID(),
	)

	// Store commit high-water before induction.
	recorded := f.module.NotifiedCommitPosition()

	// Induce: one session message to the Java leader.
	induceSessionMessage(t, f)

	// Advance: NotifiedCommitPosition must rise past the pre-induction commit and
	// the pre-induction recorded position. Term-start high-water alone can equal a
	// caught-up recorder sample. Session induction must advance commit past both.
	awaitNotifiedCommitAdvance(
		t,
		f,
		max(recorded, recordingBefore),
		clusterjava.MixedCommitBudget,
	)

	// Durable: recorded position reaches the observed commit.
	commitAfter := f.module.NotifiedCommitPosition()
	awaitGoRecordingReaches(
		t,
		f,
		java,
		recordingID,
		commitAfter,
		clusterjava.MixedCommitBudget,
	)

	// Hold log handles before Close for clean-teardown asserts.
	// Module Close is also registered on Cleanup (idempotent).
	streams := f.module.LogStreams()
	require.NotNil(t, streams)
	pub := streams.Publication()
	sub := streams.Subscription()
	require.NotNil(t, pub)
	require.NotNil(t, sub)

	require.NoError(t, f.module.Close())
	require.True(t, pub.IsClosed(), "log publication must report closed after Close")
	require.True(t, sub.IsClosed(), "log subscription must report closed after Close")

	markMixedSmokeCompleted(t)
}

// TestClusterMixedGoLeader elects the Go appointed leader and advances Java
// commit counters from term-start log frames. Runs only under
// HERON_CLUSTER_MIXED_GO_LEADER_COMPOSE.
//
// Asserts durable log recording after elect: the Go leader records its own
// cluster log publication locally, and the recorded position reaches the
// commit position the Java followers advanced. Makes no claim about Java
// members' recordings.
//
// No client session offer: Go ServiceCount is 0.
//
// No t.Parallel: owns fixed ports and one cluster.
func TestClusterMixedGoLeader(t *testing.T) {
	requireMixedCompose(
		t,
		clusterjava.MixedComposeGoLeaderEnvKey,
		clusterjava.MixedComposeSkipHintGoLeader,
	)

	// Java peers start first under the go-leader overlay. All three Aeron
	// directories must already exist when the smoke starts.
	requireMixedAeronDirsPresent(t)

	// Java up before the Go member opens. Compose starts Java first.
	// Nil fixture: no Go duty cycle until openMixedGoMember below.
	java := awaitJavaModuleState(t, nil, clusterjava.MixedStartupBudget)

	// Record commit counters while allocated and before Go opens as leader.
	recorded := recordJavaCommitPositions(t, java)

	// Open the Go module as appointed leader 2. Multicast log channel.
	f := openMixedGoMember(t, buildMixedGoLeaderConfig(t))
	writeReady(t)

	// Elect: Go leads; both Java members follow; terms agree. Uses the mid-wait
	// recording sample from awaitGoLeaderElection. A post-election read can already
	// equal the term-start commit once the archive records that publication.
	recordingBefore := awaitGoLeaderElection(
		t,
		f,
		java,
		clusterjava.MixedElectionBudget,
	)

	// Cold start keeps a zero position base for later recording comparisons.
	require.True(t, f.module.IsColdStart())

	// Leader must not hold a self-image of the log.
	require.Nil(t, f.module.LogAdapter().Image())

	// Record: active catalog recording of the member's own log publication.
	//
	// Hold log handles here for session id and later clean-teardown asserts.
	streams := f.module.LogStreams()
	require.NotNil(t, streams)
	pub := streams.Publication()
	require.NotNil(t, pub)
	sub := streams.Subscription()
	require.NotNil(t, sub)
	recordingID, _ := requireGoLogRecording(t, f, pub.SessionID())

	// Service: asserts each Java ClusteredService decoded term start. Commit
	// counter alone is not enough for service log consumption.
	awaitJavaServiceSawTermStart(t, f, java, clusterjava.MixedElectionBudget)

	// Advance: both Java commit counters rise; Go CommitPosition > 0.
	// NotifiedCommitPosition is not asserted on the leader.
	awaitJavaCommitAdvance(t, f, java, recorded, clusterjava.MixedCommitBudget)

	// Gap: the mid-election recording sample must sit below the new commit. A
	// re-read here would sample a recorder that may already have reached commit.
	commitAfter := f.module.CommitPosition()
	require.Less(
		t,
		recordingBefore,
		commitAfter,
		"recorded position already at or past commit before advance",
	)

	// Durable: recorded position reaches the observed commit.
	awaitGoRecordingReaches(
		t,
		f,
		java,
		recordingID,
		commitAfter,
		clusterjava.MixedCommitBudget,
	)

	require.NoError(t, f.module.Close())
	require.True(t, pub.IsClosed(), "log publication must report closed after Close")
	require.True(t, sub.IsClosed(), "log subscription must report closed after Close")

	markMixedSmokeCompleted(t)
}
