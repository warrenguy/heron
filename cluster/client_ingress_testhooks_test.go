//go:build testhooks

package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/consensus"
)

// seedClientIngressPendingBegin attaches one image with a BEGIN-only frame and
// polls so the open-wired assembler holds a session builder. Returns the
// attached image.
func seedClientIngressPendingBegin(
	t *testing.T,
	sub *aeron.Subscription,
	ingress *consensus.SubscriptionClientIngress,
	correlationID int64,
) *aeron.Image {
	t.Helper()

	image := aeron.NewTestImageForSubscriptionHooksWithCorrelation(t, sub, 0, correlationID)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	aeron.WriteCommittedDataFrameForHooks(
		aeron.ImageTermBufferForHooks(image),
		0,
		image.SessionID(),
		image.StreamID(),
		7,
		0,
		logbuffer.BeginFlag,
		[]byte("client-ingress-pending-begin"),
	)
	require.Equal(t, 1, ingress.Poll(1, func([]byte, int) aeron.ControlledAction {
		t.Fatal("BEGIN must not reach onFragment")
		return aeron.ControlledContinue
	}))
	return image
}

// Asserts the open-path unavailable-image handler frees the ingress assembler
// builder for the image session id.
func TestOpenClientIngressSubscription_unavailableImageFreesAssembler(t *testing.T) {
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
	t.Cleanup(func() { _ = cs.Close() })

	sub := cs.Subscription()
	handler := aeron.SubscriptionUnavailableImageHandlerForHooks(sub)
	require.NotNil(t, handler)

	ingress, ok := agent.ClientIngress().(*consensus.SubscriptionClientIngress)
	require.True(t, ok)

	image := seedClientIngressPendingBegin(t, sub, ingress, 1)
	handler(image)

	require.False(t, ingress.FreeSessionBuffer(image.SessionID()))
	// Nil image must not free a builder or panic.
	require.NotPanics(t, func() { handler(nil) })
}

// Asserts Close drops pending reassembly builders before unbinding the agent
// ingress.
func TestClientIngressSubscription_Close_clearsAssembler(t *testing.T) {
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

	sub := cs.Subscription()
	ingress, ok := agent.ClientIngress().(*consensus.SubscriptionClientIngress)
	require.True(t, ok)

	image := seedClientIngressPendingBegin(t, sub, ingress, 1)

	require.NoError(t, cs.Close())

	// Assembler cleared on Close. Agent unbound.
	require.False(t, ingress.FreeSessionBuffer(image.SessionID()))
	require.Nil(t, agent.ClientIngress())
}
