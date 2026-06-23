//go:build integration && cluster_java

package cluster_java_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/harness"
)

// TestClusterHarnessScaffold proves JoinExternal and driver CNC liveness on live compose.
// Does not assert ConsensusModule election or cluster service readiness.
func TestClusterHarnessScaffold(t *testing.T) {
	if os.Getenv("HERON_CLUSTER_JAVA_COMPOSE") != "1" {
		t.Skip("cluster java requires docker compose; run make test-cluster-java")
	}

	dir := os.Getenv("AERON_DIR")
	require.NotEmpty(t, dir)

	md := harness.JoinExternal(t, dir)
	md.AssertAlive(t)
	t.Log("cluster java harness scaffold passed")
}
