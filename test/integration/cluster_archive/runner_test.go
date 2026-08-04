//go:build integration && archive_live

package cluster_archive_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/test/integration/harness"
)

// TestConsensusModuleRunnerDrivesOneMember elects one member through
// Runner.DoWork. Then stops an active Runner.Run with a cancel.
func TestConsensusModuleRunnerDrivesOneMember(t *testing.T) {
	t.Parallel()

	md := harness.Start(t)
	// Archive binds UDP control at PortBase+ArchiveControlPortOffset. The Go
	// client connects over local IPC.
	_ = harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir: md.Dir,
		ControlChannel: harness.ArchiveControlChannel(
			cluster.CalculatePort(0, harness.RunnerPortBase, cluster.ArchiveControlPortOffset),
		),
	})

	members, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		StartingMemberID:  0,
		IngressHostnames:  []string{"127.0.0.1"},
		ClusterHostnames:  []string{"127.0.0.1"},
		PortBase:          harness.RunnerPortBase,
		SharedLogEndpoint: harness.RunnerLogEndpoint,
	})
	require.NoError(t, err)

	// Short harness heartbeats and close linger come with the config builder.
	cfg := harness.MemberClusterConfig(0, members, t.TempDir(), md.Dir)

	module, err := cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
		Config: cfg,
	})
	require.NoError(t, err)

	// Fail-path teardown if elect or later asserts fail. Close is idempotent.
	harness.CleanupClose(t, "consensus module", module.Close)

	require.NotNil(t, module.ArchiveClient())

	runner, err := cluster.NewRunner(cluster.RunnerDeps{
		Client:  module.Client(),
		Agent:   module.Agent(),
		Archive: module.ArchiveClient(),
		Idle:    module.IdleStrategy(),
		Clock:   module.NanoClock(),
	})
	require.NoError(t, err)

	// DoWork until own log image is present by session id. Then assert
	// ElectionComplete. Runner demuxes archive control in the duty cycle.
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

	// Runner built before Close keeps ticking after Close. DoWork reports zero
	// work after Close. Must not panic.
	require.NoError(t, module.Close())
	require.Nil(t, module.ArchiveClient())
	require.Zero(t, runner.DoWork())

	// Building over a closed module fails on Client. NewRunner does not return a
	// stale Runner. NewRunner must not panic.
	_, err = cluster.NewRunner(cluster.RunnerDeps{
		Client:  module.Client(),
		Agent:   module.Agent(),
		Archive: module.ArchiveClient(),
		Idle:    module.IdleStrategy(),
		Clock:   module.NanoClock(),
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
