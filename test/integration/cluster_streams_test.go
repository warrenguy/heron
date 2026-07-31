//go:build integration

package integration_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/integration/harness"
)

// twoMemberMembersString builds a 2-member cluster members string at portBase.
func twoMemberMembersString(
	t *testing.T,
	portBase int,
	sharedLogEndpoint string,
) string {
	t.Helper()

	hosts := []string{"127.0.0.1", "127.0.0.1"}
	members, err := cluster.BuildClusterMembersString(cluster.MembersStringConfig{
		IngressHostnames:  hosts,
		ClusterHostnames:  hosts,
		PortBase:          portBase,
		SharedLogEndpoint: sharedLogEndpoint,
	})
	require.NoError(t, err)
	return members
}

// topologyForMember resolves a Topology for memberID over members.
func topologyForMember(
	t *testing.T,
	memberID int32,
	members string,
	aeronDir string,
	logChannel string,
) *cluster.Topology {
	t.Helper()

	cfg := cluster.Config{
		MemberID:                      memberID,
		ClusterMembers:                members,
		ClusterDir:                    t.TempDir(),
		AeronDir:                      aeronDir,
		LogChannel:                    logChannel,
		ArchiveControlRequestChannel:  archive.DefaultLocalControlChannel,
		ArchiveControlResponseChannel: archive.DefaultLocalControlChannel,
	}.Normalize()
	topology, err := cluster.NewTopology(cfg)
	require.NoError(t, err)
	return topology
}

// TestConsensusIngress_commitPositionOverLiveDriver delivers a peer CommitPosition
// through ConsensusSubscription into a real Agent.
func TestConsensusIngress_commitPositionOverLiveDriver(t *testing.T) {
	md0 := harness.StartParallel(t)
	md1 := harness.Start(t)

	const portBase = 24500
	members := twoMemberMembersString(t, portBase, "")

	topology0 := topologyForMember(t, 0, members, md0.Dir, "")
	topology1 := topologyForMember(t, 1, members, md1.Dir, "")

	// Split clients idle while the peer Add* awaits; match two-client IPC tests.
	splitCfg := aeron.Config{
		InterServiceTimeout: harness.ConnectedPollTimeout,
	}
	client0, _ := harness.ConnectIPCClientConfig(t, md0, splitCfg)
	client1, _ := harness.ConnectIPCClientConfig(t, md1, splitCfg)

	agent := consensus.NewAgent(consensus.AgentConfig{
		Role:             consensus.RoleFollower,
		ThisMemberID:     1,
		LeaderMemberID:   0,
		LeadershipTermID: 7,
	})

	cs, err := cluster.OpenConsensusSubscription(cluster.ConsensusSubscriptionDeps{
		Adder:    client1,
		Topology: topology1,
		Agent:    agent,
	})
	require.NoError(t, err)
	harness.CleanupClose(t, "consensus subscription", cs.Close)

	require.NotNil(t, agent.Ingress())
	_, ok := agent.Ingress().(*consensus.SubscriptionConsensusIngress)
	require.True(t, ok, "ingress must be SubscriptionConsensusIngress")

	channel, ok := topology0.ConsensusChannelFor(1)
	require.True(t, ok)

	pub, err := client0.AddExclusivePublication(topology0.ConsensusStreamID(), channel)
	require.NoError(t, err)
	harness.CleanupClose(t, "consensus publication", pub.Close)

	harness.PollSplitClientsUntil(
		t,
		client0,
		client1,
		func() bool { return pub.IsConnected() },
		fmt.Sprintf(
			"consensus publication not connected: channel=%s md0=%s md1=%s",
			channel,
			md0.Dir,
			md1.Dir,
		),
	)

	publisher := election.NewAeronPublisher()
	publisher.SetPublication(1, pub)
	publisher.CommitPosition(
		election.PublicationTarget{MemberID: 1},
		codecs.CommitPositionFields{
			LeadershipTermID: 7,
			LogPosition:      4096,
			LeaderMemberID:   0,
		},
	)

	harness.PollSplitClientsUntil(
		t,
		client0,
		client1,
		func() bool {
			agent.DoWork(time.Now().UnixNano())
			return agent.NotifiedCommitPosition() == 4096
		},
		fmt.Sprintf(
			"notified commit position did not reach 4096: channel=%s md0=%s md1=%s",
			channel,
			md0.Dir,
			md1.Dir,
		),
	)

	require.Equal(t, int64(4096), agent.NotifiedCommitPosition())

	sub := cs.Subscription()
	require.NoError(t, cs.Close())
	require.True(t, sub.IsClosed())
	require.Nil(t, agent.Ingress())
}

// TestClusterStreams_multicastLeadershipTerm delivers a leadership-term frame
// over a shared multicast log between two members.
func TestClusterStreams_multicastLeadershipTerm(t *testing.T) {
	md0 := harness.StartParallel(t)
	md1 := harness.Start(t)

	const portBase = 24500
	sharedLog := harness.ClusterLogMulticastGroup + ":" +
		strconv.Itoa(harness.ClusterLogNLTMulticastPort)
	members := twoMemberMembersString(t, portBase, sharedLog)

	topology0 := topologyForMember(
		t, 0, members, md0.Dir, harness.ClusterLogChannelTemplate,
	)
	topology1 := topologyForMember(
		t, 1, members, md1.Dir, harness.ClusterLogChannelTemplate,
	)

	require.Equal(t, topology0.LogChannel(), topology1.LogChannel())
	require.Equal(t, topology0.LogStreamID(), topology1.LogStreamID())

	// Split clients idle while the peer Add* awaits; match two-client IPC tests.
	splitCfg := aeron.Config{
		InterServiceTimeout: harness.ConnectedPollTimeout,
	}
	client0, _ := harness.ConnectIPCClientConfig(t, md0, splitCfg)
	client1, _ := harness.ConnectIPCClientConfig(t, md1, splitCfg)

	agent0 := consensus.NewAgent(consensus.AgentConfig{
		ThisMemberID: 0,
	})
	agent1 := consensus.NewAgent(consensus.AgentConfig{
		ThisMemberID: 1,
	})

	lp0 := consensus.NewLogPublisher()
	lp1 := consensus.NewLogPublisher()
	adapter0 := consensus.NewLogAdapter(0)
	adapter1 := consensus.NewLogAdapter(0)

	streams1, err := cluster.OpenLogStreams(cluster.LogStreamsDeps{
		Adder:        client1,
		Topology:     topology1,
		Agent:        agent1,
		LogPublisher: lp1,
		LogAdapter:   adapter1,
	})
	require.NoError(t, err)
	harness.CleanupClose(t, "log streams member 1", streams1.Close)

	streams0, err := cluster.OpenLogStreams(cluster.LogStreamsDeps{
		Adder:        client0,
		Topology:     topology0,
		Agent:        agent0,
		LogPublisher: lp0,
		LogAdapter:   adapter0,
	})
	require.NoError(t, err)
	harness.CleanupClose(t, "log streams member 0", streams0.Close)

	require.Equal(
		t,
		streams0.Publication().StreamID(),
		streams1.Subscription().StreamID(),
	)

	logChannel := topology0.LogChannel()
	connectFailMsg := fmt.Sprintf(
		"cluster log multicast not connected: channel=%s md0=%s md1=%s",
		logChannel,
		md0.Dir,
		md1.Dir,
	)
	harness.PollSplitClientsUntil(
		t,
		client0,
		client1,
		func() bool {
			return streams0.Publication().IsConnected() &&
				streams1.Subscription().IsConnected()
		},
		connectFailMsg,
	)

	sessionID := agent0.AddLogPublication(0)
	require.NotEqual(t, int32(-1), sessionID)
	require.Equal(t, streams0.Publication().SessionID(), sessionID)

	// Both members publish on the shared group; each subscription holds one
	// image per publisher. Select the leader by session id, never by index.
	var leaderImage *aeron.Image
	harness.PollSplitClientsUntil(
		t,
		client0,
		client1,
		func() bool {
			if streams1.Subscription().ImageCount() < 2 {
				return false
			}
			img, found := harness.ImageBySessionID(streams1.Subscription(), sessionID)
			if !found {
				return false
			}
			leaderImage = img
			return true
		},
		fmt.Sprintf(
			"leader log image not found: sessionID=%d channel=%s md0=%s md1=%s",
			sessionID,
			logChannel,
			md0.Dir,
			md1.Dir,
		),
	)
	require.NotNil(t, leaderImage)
	require.GreaterOrEqual(t, streams1.Subscription().ImageCount(), 2)

	spy := &recordingLogHandler{}
	adapter1.SetImage(leaderImage)
	adapter1.SetHandler(spy)

	const termID int64 = 3
	nowNs := time.Now().UnixNano()
	require.NoError(t, agent0.JoinLogAsLeader(termID, 0, sessionID, false))
	require.True(t, agent0.AppendNewLeadershipTermEvent(nowNs))

	harness.PollSplitClientsUntil(
		t,
		client0,
		client1,
		func() bool {
			adapter1.Poll(streams0.Publication().Position())
			return len(spy.nlts) >= 1
		},
		fmt.Sprintf(
			"leadership-term event not delivered: channel=%s md0=%s md1=%s",
			logChannel,
			md0.Dir,
			md1.Dir,
		),
	)

	require.Len(t, spy.nlts, 1)
	nlt := spy.nlts[0]
	require.Equal(t, termID, nlt.leadershipTermID)
	require.Equal(t, int64(0), nlt.termBaseLogPosition)
	require.Equal(t, clusterwire.ClusterTimeUnit_NANOS, nlt.timeUnit)
	require.Equal(t, int32(0), nlt.appVersion)

	pub0 := streams0.Publication()
	sub0 := streams0.Subscription()
	pub1 := streams1.Publication()
	sub1 := streams1.Subscription()

	require.NoError(t, streams0.Close())
	require.NoError(t, streams1.Close())

	require.True(t, pub0.IsClosed())
	require.True(t, sub0.IsClosed())
	require.True(t, pub1.IsClosed())
	require.True(t, sub1.IsClosed())

	require.Nil(t, streams0.Publication())
	require.Nil(t, streams0.Subscription())
	require.Nil(t, streams1.Publication())
	require.Nil(t, streams1.Subscription())

	require.Equal(t, int32(-1), agent0.AddLogPublication(0))
}
