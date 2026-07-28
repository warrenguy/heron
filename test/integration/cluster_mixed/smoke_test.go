//go:build integration && cluster_mixed && cluster_java

package cluster_mixed_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
)

// TestClusterMixedJavaLeader elects a Java appointed leader and advances commit
// across the language boundary. Runs only under HERON_CLUSTER_MIXED_COMPOSE.
//
// Commit advance asserts mesh liveness after a client offer. Commit advance is
// not a durability claim for the Go member. Java peers record the log. The Go
// member does not.
//
// No t.Parallel: owns fixed ports and one cluster.
func TestClusterMixedJavaLeader(t *testing.T) {
	requireMixedComposeGate(t)

	// Open the Go module before Java starts. Compose enforces that order.
	f := openMixedGoMember(t, buildMixedJavaLeaderConfig())

	// Readiness releases Java depends_on. Write before any later assertion.
	writeReady(t)

	// Java up: each Java member has allocated a module state counter.
	// Compose starts Java only after the readiness file.
	java := awaitJavaModuleState(t, f, clusterjava.MixedStartupBudget)

	// Elect: Java member 0 leads; Go follows; terms agree.
	awaitJavaLeaderElection(t, f, java, clusterjava.MixedElectionBudget)

	// Join: connected log image on the leader session at join position 0.
	requireLiveLogImage(t, f)

	// Store commit high-water before induction.
	recorded := f.module.NotifiedCommitPosition()

	// Induce: one session message to the Java leader.
	induceSessionMessage(t, f)

	// Advance: NotifiedCommitPosition strictly increases.
	awaitNotifiedCommitAdvance(t, f, recorded, clusterjava.MixedCommitBudget)

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
}
