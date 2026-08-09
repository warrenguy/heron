package cluster_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/consensus"
)

func TestOpenServiceControlSubscription_nilDeps(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{ServiceCount: 1})
	adder := &fakeLogStreamAdder{}

	t.Run("nil adder", func(t *testing.T) {
		t.Parallel()
		cs, err := cluster.OpenServiceControlSubscription(cluster.ServiceControlSubscriptionDeps{
			Adder:    nil,
			Topology: topology,
			Agent:    agent,
		})
		require.EqualError(t, err, "cluster: subscription adder is required")
		require.Nil(t, cs)
	})

	t.Run("nil topology", func(t *testing.T) {
		t.Parallel()
		cs, err := cluster.OpenServiceControlSubscription(cluster.ServiceControlSubscriptionDeps{
			Adder:    adder,
			Topology: nil,
			Agent:    agent,
		})
		require.EqualError(t, err, "cluster: topology is required")
		require.Nil(t, cs)
	})

	t.Run("nil agent", func(t *testing.T) {
		t.Parallel()
		cs, err := cluster.OpenServiceControlSubscription(cluster.ServiceControlSubscriptionDeps{
			Adder:    adder,
			Topology: topology,
			Agent:    nil,
		})
		require.EqualError(t, err, "cluster: consensus agent is required")
		require.Nil(t, cs)
	})
}

func TestOpenServiceControlSubscription_openAndBind(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{ServiceCount: 1})
	adder := &fakeLogStreamAdder{}

	cs, err := cluster.OpenServiceControlSubscription(cluster.ServiceControlSubscriptionDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.NoError(t, err)
	require.NotNil(t, cs)
	t.Cleanup(func() { _ = cs.Close() })

	require.Len(t, adder.calls, 1)
	require.Equal(t, topology.ConsensusModuleStreamID(), adder.calls[0].streamID)
	require.Equal(t, topology.ServiceControlChannel(), adder.calls[0].channel)

	require.IsType(t, (*consensus.SubscriptionConsensusIngress)(nil), agent.ServiceIngress())
	require.Same(t, adder.lastSub, cs.Subscription())
}

func TestOpenServiceControlSubscription_addFailure(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{ServiceCount: 1})
	cause := errors.New("driver timeout")
	adder := &fakeLogStreamAdder{}
	adder.failAllSubscriptionAdds(cause)

	cs, err := cluster.OpenServiceControlSubscription(cluster.ServiceControlSubscriptionDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.Nil(t, cs)
	channel := topology.ServiceControlChannel()
	want := fmt.Sprintf(
		"cluster: add service control subscription on %s: %s",
		channel,
		cause.Error(),
	)
	require.EqualError(t, err, want)
	require.ErrorIs(t, err, cause)
	require.Nil(t, agent.ServiceIngress())
}

func TestOpenServiceControlSubscription_adderReturnsNilNil(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{ServiceCount: 1})
	adder := &fakeLogStreamAdder{returnNilSub: true}

	cs, err := cluster.OpenServiceControlSubscription(cluster.ServiceControlSubscriptionDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.Nil(t, cs)
	channel := topology.ServiceControlChannel()
	want := fmt.Sprintf(
		"cluster: add service control subscription on %s: adder returned no subscription",
		channel,
	)
	require.EqualError(t, err, want)
	require.Nil(t, agent.ServiceIngress())
}

func TestServiceControlSubscription_close(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{ServiceCount: 1})
	adder := &fakeLogStreamAdder{}

	cs, err := cluster.OpenServiceControlSubscription(cluster.ServiceControlSubscriptionDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.NoError(t, err)

	sub := adder.lastSub
	require.NotNil(t, sub)
	require.False(t, sub.IsClosed())

	require.NoError(t, cs.Close())

	require.Nil(t, agent.ServiceIngress())
	require.True(t, sub.IsClosed())
	require.Nil(t, cs.Subscription())

	// Idempotent: second Close does not re-touch the agent.
	require.NoError(t, cs.Close())
	require.Nil(t, agent.ServiceIngress())
	require.Nil(t, cs.Subscription())
}

func TestServiceControlSubscription_closeStaleAfterRebind(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{ServiceCount: 1})
	adder := &fakeLogStreamAdder{}

	cs1, err := cluster.OpenServiceControlSubscription(cluster.ServiceControlSubscriptionDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.NoError(t, err)
	require.NoError(t, cs1.Close())
	require.Nil(t, agent.ServiceIngress())

	cs2, err := cluster.OpenServiceControlSubscription(cluster.ServiceControlSubscriptionDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs2.Close() })

	ingress2 := agent.ServiceIngress()
	require.NotNil(t, ingress2)
	sub2 := cs2.Subscription()
	require.NotNil(t, sub2)

	// Stale owner must not clear the rebound ingress or close the new sub.
	require.NoError(t, cs1.Close())
	require.Same(t, ingress2, agent.ServiceIngress())
	require.Same(t, sub2, cs2.Subscription())
	require.False(t, sub2.IsClosed())
}

func TestServiceControlSubscription_nilReceiver(t *testing.T) {
	t.Parallel()

	var cs *cluster.ServiceControlSubscription
	require.NoError(t, cs.Close())
	require.Nil(t, cs.Subscription())

	// Zero value has no subscription. Close is a no-op (no agent panic).
	require.NoError(t, (&cluster.ServiceControlSubscription{}).Close())
}
