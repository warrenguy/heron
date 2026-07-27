//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/integration/harness"
)

// File-local consensus ports and multicast log endpoint for this smoke only.
// Distinct from other live cluster-log tests so concurrent suites do not share
// images.
const (
	consensusModulePortBase          = 24700
	consensusModuleSharedLogEndpoint = "239.255.12.3:24704"
	// Short linger so owned-client Close does not wait the 3s default.
	consensusModuleCloseLinger = 250 * time.Millisecond
	consensusModuleElectBound  = 60 * time.Second
)

// TestConsensusModule_electsAndClosesAgainstALiveDriver opens one cold-start
// member, elects it leader, and checks full teardown against a live driver.
func TestConsensusModule_electsAndClosesAgainstALiveDriver(t *testing.T) {
	md := harness.Start(t)

	hosts := []string{"127.0.0.1"}
	members, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		StartingMemberID:  0,
		IngressHostnames:  hosts,
		ClusterHostnames:  hosts,
		PortBase:          consensusModulePortBase,
		SharedLogEndpoint: consensusModuleSharedLogEndpoint,
	})
	require.NoError(t, err)

	cfg := cluster.Config{
		MemberID:       0,
		ClusterMembers: members,
		ClusterDir:     t.TempDir(),
		AeronDir:       md.Dir,
		LogChannel:     harness.ClusterLogChannelTemplate,
		AeronConfig: aeron.Config{
			InterServiceTimeout: harness.ConnectedPollTimeout,
			CloseLingerDuration: consensusModuleCloseLinger,
		},
	}

	module, err := cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
		Config: cfg,
	})
	require.NoError(t, err)
	require.NotNil(t, module)
	// Fail-path teardown if elect or later asserts fail. Close is idempotent.
	harness.CleanupClose(t, "consensus module", module.Close)

	// Open: cold-start single member before any duty tick.
	require.True(t, module.IsColdStart())
	require.Equal(t, int32(0), module.ThisMemberID())
	require.Equal(t, election.ElectionStateInit, module.ElectionState())
	require.False(t, module.ElectionComplete())
	require.Equal(t, int64(0), module.CommitPosition())
	require.Empty(t, module.RecordingLog().Entries())
	require.NotNil(t, module.IdleStrategy())

	// Bind at open: log publication and join/append sources before first tick.
	session := module.Agent().AddLogPublication(0)
	require.NotEqual(t, int32(-1), session)
	require.Equal(t, module.LogPublisher().SessionID(), session)
	require.Equal(t, module.LogStreams().Publication().SessionID(), session)
	require.NotNil(t, module.Agent().AppendSource())
	require.NotNil(t, module.Agent().LogJoinSource())

	// Elect: transport client then agent until the runner clears.
	electStart := time.Now()
	for !module.ElectionComplete() {
		require.Less(
			t, time.Since(electStart), consensusModuleElectBound,
			"election did not complete within %s: state=%v role=%v term=%d",
			consensusModuleElectBound,
			module.ElectionState(),
			module.Role(),
			module.LeadershipTermID(),
		)
		module.Client().DoWork()
		module.Agent().DoWork(module.NanoClock()())
		time.Sleep(time.Millisecond)
	}

	// Leader: single-member cold path, no follower log image.
	require.True(t, module.IsLeader())
	require.Equal(t, int32(-1), module.LeaderMemberID())
	require.Equal(t, int64(0), module.LeadershipTermID())
	require.Nil(t, module.LogAdapter().Image())

	// Recording log stays empty with a null log recording id.
	require.Empty(t, module.RecordingLog().Entries())

	// Capture handles before Close so IsClosed can be checked after teardown.
	pub := module.LogStreams().Publication()
	sub := module.LogStreams().Subscription()
	agent := module.Agent()
	require.NotNil(t, pub)
	require.NotNil(t, sub)

	require.NoError(t, module.Close())
	require.NoError(t, module.Close())

	require.True(t, pub.IsClosed())
	require.True(t, sub.IsClosed())
	require.Nil(t, agent.AppendSource())
	require.Nil(t, agent.LogJoinSource())
	require.Nil(t, agent.Positions())
	require.Nil(t, agent.Egress())
	require.Nil(t, agent.Election())
	require.Equal(t, int32(-1), agent.AddLogPublication(0))
	require.Nil(t, module.LogStreams())
	require.Nil(t, module.RecordingLog())
	require.Nil(t, module.Client())

	// Value getters keep reporting after Close.
	require.Equal(t, consensus.RoleLeader, module.Role())
	require.True(t, module.ElectionComplete())
	require.True(t, module.IsColdStart())
	require.Equal(t, election.ElectionStateClosed, module.ElectionState())
	require.Equal(t, int32(0), module.ThisMemberID())
	require.Equal(t, int64(0), module.LeadershipTermID())
}
