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

func TestOpenServiceControlPublication_errors(t *testing.T) {
	t.Parallel()

	cause := errors.New("driver timeout")

	tests := []struct {
		name string
		// mutate turns a fully wired deps set into the error case under test.
		mutate func(*cluster.ServiceControlPublicationDeps, *fakeLogStreamAdder)
		// want builds the expected error text from the resolved channel.
		want func(channel string) string
		// wantIs, when set, is the wrapped cause require.ErrorIs must match.
		wantIs error
	}{
		{
			name: "nil adder",
			mutate: func(d *cluster.ServiceControlPublicationDeps, _ *fakeLogStreamAdder) {
				d.Adder = nil
			},
			want: func(string) string { return "cluster: publication adder is required" },
		},
		{
			name: "nil topology",
			mutate: func(d *cluster.ServiceControlPublicationDeps, _ *fakeLogStreamAdder) {
				d.Topology = nil
			},
			want: func(string) string { return "cluster: topology is required" },
		},
		{
			name: "nil agent",
			mutate: func(d *cluster.ServiceControlPublicationDeps, _ *fakeLogStreamAdder) {
				d.Agent = nil
			},
			want: func(string) string { return "cluster: consensus agent is required" },
		},
		{
			name: "add failure",
			mutate: func(_ *cluster.ServiceControlPublicationDeps, a *fakeLogStreamAdder) {
				a.failConcPubErr = cause
			},
			want: func(channel string) string {
				return fmt.Sprintf(
					"cluster: add service control publication on %s: %s",
					channel,
					cause.Error(),
				)
			},
			wantIs: cause,
		},
		{
			name: "adder returns nil nil",
			mutate: func(_ *cluster.ServiceControlPublicationDeps, a *fakeLogStreamAdder) {
				a.returnNilConcPub = true
			},
			want: func(channel string) string {
				return fmt.Sprintf(
					"cluster: add service control publication on %s: "+
						"adder returned no publication",
					channel,
				)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			topology := singleMemberTopology(t)
			agent := consensus.NewAgent(consensus.AgentConfig{ServiceCount: 1})
			adder := &fakeLogStreamAdder{}
			deps := cluster.ServiceControlPublicationDeps{
				Adder:    adder,
				Topology: topology,
				Agent:    agent,
			}
			tc.mutate(&deps, adder)

			cp, err := cluster.OpenServiceControlPublication(deps)
			require.EqualError(t, err, tc.want(topology.ServiceControlChannel()))
			if tc.wantIs != nil {
				require.ErrorIs(t, err, tc.wantIs)
			}
			require.Nil(t, cp)
			require.Nil(t, agent.ServiceProxy())
		})
	}
}

func TestOpenServiceControlPublication_openAndBind(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{ServiceCount: 1})
	adder := &fakeLogStreamAdder{}

	cp, err := cluster.OpenServiceControlPublication(cluster.ServiceControlPublicationDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.NoError(t, err)
	require.NotNil(t, cp)
	t.Cleanup(func() { _ = cp.Close() })

	require.Len(t, adder.calls, 1)
	require.Equal(t, "concurrent-publication", adder.calls[0].kind)
	require.Equal(t, topology.ServiceStreamID(), adder.calls[0].streamID)
	require.Equal(t, topology.ServiceControlChannel(), adder.calls[0].channel)

	require.NotNil(t, agent.ServiceProxy())
	require.Same(t, adder.lastConcPub, agent.ServiceProxy().Publication())
	require.Same(t, adder.lastConcPub, cp.Publication())
}

func TestServiceControlPublication_close(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{ServiceCount: 1})
	adder := &fakeLogStreamAdder{}

	cp, err := cluster.OpenServiceControlPublication(cluster.ServiceControlPublicationDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.NoError(t, err)

	pub := adder.lastConcPub
	require.NotNil(t, pub)
	require.False(t, pub.IsClosed())
	require.NotNil(t, agent.ServiceProxy())

	require.NoError(t, cp.Close())

	require.Nil(t, agent.ServiceProxy())
	require.True(t, pub.IsClosed())
	require.Nil(t, cp.Publication())

	// Idempotent: second Close does not re-touch the agent.
	require.NoError(t, cp.Close())
	require.Nil(t, agent.ServiceProxy())
	require.Nil(t, cp.Publication())
}

func TestServiceControlPublication_closeStaleAfterRebind(t *testing.T) {
	t.Parallel()

	topology := singleMemberTopology(t)
	agent := consensus.NewAgent(consensus.AgentConfig{ServiceCount: 1})
	adder := &fakeLogStreamAdder{}

	cp1, err := cluster.OpenServiceControlPublication(cluster.ServiceControlPublicationDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.NoError(t, err)
	require.NoError(t, cp1.Close())
	require.Nil(t, agent.ServiceProxy())

	cp2, err := cluster.OpenServiceControlPublication(cluster.ServiceControlPublicationDeps{
		Adder:    adder,
		Topology: topology,
		Agent:    agent,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cp2.Close() })

	proxy2 := agent.ServiceProxy()
	require.NotNil(t, proxy2)
	pub2 := cp2.Publication()
	require.NotNil(t, pub2)

	// Stale owner must not clear the rebound proxy or close the new pub.
	require.NoError(t, cp1.Close())
	require.Same(t, proxy2, agent.ServiceProxy())
	require.Same(t, pub2, cp2.Publication())
	require.False(t, pub2.IsClosed())
}

func TestServiceControlPublication_nilReceiver(t *testing.T) {
	t.Parallel()

	var cp *cluster.ServiceControlPublication
	require.NoError(t, cp.Close())
	require.Nil(t, cp.Publication())

	// Zero value has no publication. Close is a no-op (no agent panic).
	require.NoError(t, (&cluster.ServiceControlPublication{}).Close())
}

func TestConsensusModule_RequestServiceAck_nilReceiver(t *testing.T) {
	t.Parallel()

	var nilModule *cluster.ConsensusModule
	require.ErrorIs(t, nilModule.RequestServiceAck(1), consensus.ErrServiceControlUnbound)
}
