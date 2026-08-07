package cluster_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/consensus"
)

func TestOpenClientIngressSubscription_nilDeps(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{})
	adder := &fakeLogStreamAdder{}

	t.Run("nil adder", func(t *testing.T) {
		t.Parallel()
		cs, err := cluster.OpenClientIngressSubscription(cluster.ClientIngressSubscriptionDeps{
			Adder:    nil,
			Topology: topology,
			Agent:    agent,
		})
		require.EqualError(t, err, "cluster: subscription adder is required")
		require.Nil(t, cs)
	})

	t.Run("nil topology", func(t *testing.T) {
		t.Parallel()
		cs, err := cluster.OpenClientIngressSubscription(cluster.ClientIngressSubscriptionDeps{
			Adder:    adder,
			Topology: nil,
			Agent:    agent,
		})
		require.EqualError(t, err, "cluster: topology is required")
		require.Nil(t, cs)
	})

	t.Run("nil agent", func(t *testing.T) {
		t.Parallel()
		cs, err := cluster.OpenClientIngressSubscription(cluster.ClientIngressSubscriptionDeps{
			Adder:    adder,
			Topology: topology,
			Agent:    nil,
		})
		require.EqualError(t, err, "cluster: consensus agent is required")
		require.Nil(t, cs)
	})
}

func TestOpenClientIngressSubscription_openAndBind(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{})
	adder := &fakeLogStreamAdder{}

	cs, err := cluster.OpenClientIngressSubscription(cluster.ClientIngressSubscriptionDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.NoError(t, err)
	require.NotNil(t, cs)
	t.Cleanup(func() { _ = cs.Close() })

	// Add arguments: local ingress stream and bind channel.
	require.Len(t, adder.calls, 1)
	require.Equal(t, topology.IngressStreamID(), adder.calls[0].streamID)
	require.Equal(t, topology.IngressSubscriptionChannel(), adder.calls[0].channel)

	// Agent holds SubscriptionConsensusIngress.
	require.IsType(t, (*consensus.SubscriptionConsensusIngress)(nil), agent.ClientIngress())

	// Subscription accessor is the added handle.
	require.Same(t, adder.lastSub, cs.Subscription())
}

func TestOpenClientIngressSubscription_addFailure(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{})
	cause := errors.New("driver timeout")
	adder := &fakeLogStreamAdder{}
	adder.failAllSubscriptionAdds(cause)

	cs, err := cluster.OpenClientIngressSubscription(cluster.ClientIngressSubscriptionDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.Nil(t, cs)
	channel := topology.IngressSubscriptionChannel()
	want := fmt.Sprintf(
		"cluster: add client ingress subscription on %s: %s",
		channel,
		cause.Error(),
	)
	require.EqualError(t, err, want)
	require.ErrorIs(t, err, cause)
	require.Nil(t, agent.ClientIngress())
}

func TestOpenClientIngressSubscription_adderReturnsNilNil(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{})
	adder := &fakeLogStreamAdder{returnNilSub: true}

	cs, err := cluster.OpenClientIngressSubscription(cluster.ClientIngressSubscriptionDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.Nil(t, cs)
	channel := topology.IngressSubscriptionChannel()
	want := fmt.Sprintf(
		"cluster: add client ingress subscription on %s: adder returned no subscription",
		channel,
	)
	require.EqualError(t, err, want)
	require.Nil(t, agent.ClientIngress())
}

func TestClientIngressSubscription_close(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{})
	adder := &fakeLogStreamAdder{}

	cs, err := cluster.OpenClientIngressSubscription(cluster.ClientIngressSubscriptionDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.NoError(t, err)

	// Capture handle before Close. Accessor nils after.
	sub := adder.lastSub
	require.NotNil(t, sub)
	require.False(t, sub.IsClosed())

	require.NoError(t, cs.Close())

	// Clear then close: agent ingress nil, subscription closed, accessor nil.
	require.Nil(t, agent.ClientIngress())
	require.True(t, sub.IsClosed())
	require.Nil(t, cs.Subscription())

	// Idempotent: second Close does not re-touch the agent.
	require.NoError(t, cs.Close())
	require.Nil(t, agent.ClientIngress())
	require.Nil(t, cs.Subscription())
}

func TestClientIngressSubscription_closeStaleAfterRebind(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{})
	adder := &fakeLogStreamAdder{}

	cs1, err := cluster.OpenClientIngressSubscription(cluster.ClientIngressSubscriptionDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.NoError(t, err)
	require.NoError(t, cs1.Close())
	require.Nil(t, agent.ClientIngress())

	cs2, err := cluster.OpenClientIngressSubscription(cluster.ClientIngressSubscriptionDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs2.Close() })

	ingress2 := agent.ClientIngress()
	require.NotNil(t, ingress2)
	sub2 := cs2.Subscription()
	require.NotNil(t, sub2)

	// Stale owner must not clear the rebound ingress or close the new sub.
	require.NoError(t, cs1.Close())
	require.Same(t, ingress2, agent.ClientIngress())
	require.Same(t, sub2, cs2.Subscription())
	require.False(t, sub2.IsClosed())
}

func TestClientIngressSubscription_nilReceiver(t *testing.T) {
	t.Parallel()

	var cs *cluster.ClientIngressSubscription
	require.NoError(t, cs.Close())
	require.Nil(t, cs.Subscription())

	// Zero value has no subscription. Close is a no-op (no agent panic).
	require.NoError(t, (&cluster.ClientIngressSubscription{}).Close())
}
