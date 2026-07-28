//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/test/integration/harness"
)

const (
	// runnerPortBase reserves a block distinct from every other integration
	// test that binds cluster endpoints.
	runnerPortBase = 24800

	// runnerLogMulticast is this test's own group and port. Two tests sharing a
	// group and port see each other's images.
	runnerLogMulticast = "239.255.12.13:24804"

	// runnerCloseLinger bounds teardown. The default is 3 s per client.
	// Nothing polls once the duty cycle has stopped.
	runnerCloseLinger = 250 * time.Millisecond
)

// TestConsensusModuleRunnerDrivesOneMember elects one member through
// Runner.DoWork. Then stops an active Runner.Run with a cancel.
func TestConsensusModuleRunnerDrivesOneMember(t *testing.T) {
	md := harness.Start(t)

	members, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		StartingMemberID:  0,
		IngressHostnames:  []string{"127.0.0.1"},
		ClusterHostnames:  []string{"127.0.0.1"},
		PortBase:          runnerPortBase,
		SharedLogEndpoint: runnerLogMulticast,
	})
	require.NoError(t, err)

	module, err := cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
		Config: cluster.Config{
			MemberID:       0,
			ClusterMembers: members,
			ClusterDir:     t.TempDir(),
			AeronDir:       md.Dir,
			LogChannel:     harness.ClusterLogChannelTemplate,
			AeronConfig: aeron.Config{
				InterServiceTimeout: harness.ConnectedPollTimeout,
				CloseLingerDuration: runnerCloseLinger,
			},
		},
	})
	require.NoError(t, err)
	// Fail-path teardown if elect or later asserts fail. Close is idempotent.
	harness.CleanupClose(t, "consensus module", module.Close)

	runner, err := cluster.NewRunner(cluster.RunnerDeps{
		Client: module.Client(),
		Agent:  module.Agent(),
		Idle:   module.IdleStrategy(),
		Clock:  module.NanoClock(),
	})
	require.NoError(t, err)

	// DoWork until own log image is present by session id. Then assert
	// ElectionComplete. Each DoWork is client then agent.
	session := module.LogStreams().Publication().SessionID()
	require.True(t, pollUntil(60*time.Second, func() bool {
		runner.DoWork()
		_, ok := harness.ImageBySessionID(module.LogStreams().Subscription(), session)
		return ok
	}), "log image did not arrive")

	require.True(t, pollUntil(60*time.Second, func() bool {
		runner.DoWork()
		return module.ElectionComplete()
	}), "election did not complete: state=%v role=%v", module.ElectionState(), module.Role())
	require.True(t, module.IsLeader())

	// Brief pause so cancel hits an active Run loop more often than the
	// already-done context path.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}

	// Building over a closed module fails. Does not return a stale Runner.
	require.NoError(t, module.Close())
	_, err = cluster.NewRunner(cluster.RunnerDeps{
		Client: module.Client(),
		Agent:  module.Agent(),
		Idle:   module.IdleStrategy(),
		Clock:  module.NanoClock(),
	})
	require.ErrorContains(t, err, "requires a Client")
}

func pollUntil(bound time.Duration, ready func() bool) bool {
	deadline := time.Now().Add(bound)
	for {
		if ready() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}
