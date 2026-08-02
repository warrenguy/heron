package cluster_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/consensus"
)

type logStreamAddCall struct {
	kind     string
	streamID int32
	channel  string
}

type fakeLogStreamAdder struct {
	calls        []logStreamAddCall
	lastSub      *aeron.Subscription
	lastPub      *aeron.ExclusivePublication
	failSubErr   error
	failPubErr   error
	returnNilSub bool
	returnNilPub bool
}

func (f *fakeLogStreamAdder) AddSubscription(
	streamID int32,
	channel string,
) (*aeron.Subscription, error) {
	f.calls = append(f.calls, logStreamAddCall{
		kind:     "subscription",
		streamID: streamID,
		channel:  channel,
	})
	if f.returnNilSub {
		return nil, nil
	}
	if f.failSubErr != nil {
		return nil, f.failSubErr
	}
	sub := new(aeron.Subscription)
	f.lastSub = sub
	return sub, nil
}

func (f *fakeLogStreamAdder) AddExclusivePublication(
	streamID int32,
	channel string,
) (*aeron.ExclusivePublication, error) {
	f.calls = append(f.calls, logStreamAddCall{
		kind:     "publication",
		streamID: streamID,
		channel:  channel,
	})
	if f.returnNilPub {
		return nil, nil
	}
	if f.failPubErr != nil {
		return nil, f.failPubErr
	}
	pub := new(aeron.ExclusivePublication)
	f.lastPub = pub
	return pub, nil
}

func sharedLogTopology(t *testing.T, memberID int32, logChannel string) *cluster.Topology {
	t.Helper()
	cfg := cluster.Config{
		MemberID:                      memberID,
		ClusterMembers:                goldenClusterMembersSharedLog,
		ClusterDir:                    t.TempDir(),
		AeronDir:                      t.TempDir(),
		LogChannel:                    logChannel,
		ArchiveControlRequestChannel:  testArchiveControlRequestChannel,
		ArchiveControlResponseChannel: testArchiveControlResponseChannel,
	}.Normalize()
	tp, err := cluster.NewTopology(cfg)
	require.NoError(t, err)
	return tp
}

func openLogStreamsDeps(
	t *testing.T,
	topology *cluster.Topology,
	adder cluster.LogStreamAdder,
) (
	*consensus.Agent,
	*consensus.LogPublisher,
	*consensus.LogAdapter,
	cluster.LogStreamsDeps,
) {
	t.Helper()
	agent := consensus.NewAgent(consensus.AgentConfig{})
	logPublisher := consensus.NewLogPublisher()
	logAdapter := consensus.NewLogAdapter(0)
	return agent, logPublisher, logAdapter, cluster.LogStreamsDeps{
		Adder:        adder,
		Topology:     topology,
		Agent:        agent,
		LogPublisher: logPublisher,
		LogAdapter:   logAdapter,
	}
}

func TestOpenLogStreams_nilDeps(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	agent := consensus.NewAgent(consensus.AgentConfig{})
	logPublisher := consensus.NewLogPublisher()
	logAdapter := consensus.NewLogAdapter(0)
	adder := &fakeLogStreamAdder{}

	tests := []struct {
		name string
		deps cluster.LogStreamsDeps
		want string
	}{
		{
			name: "nil adder",
			deps: cluster.LogStreamsDeps{
				Adder:        nil,
				Topology:     topology,
				Agent:        agent,
				LogPublisher: logPublisher,
				LogAdapter:   logAdapter,
			},
			want: "cluster: log stream adder is required",
		},
		{
			name: "nil topology",
			deps: cluster.LogStreamsDeps{
				Adder:        adder,
				Topology:     nil,
				Agent:        agent,
				LogPublisher: logPublisher,
				LogAdapter:   logAdapter,
			},
			want: "cluster: topology is required",
		},
		{
			name: "nil agent",
			deps: cluster.LogStreamsDeps{
				Adder:        adder,
				Topology:     topology,
				Agent:        nil,
				LogPublisher: logPublisher,
				LogAdapter:   logAdapter,
			},
			want: "cluster: consensus agent is required",
		},
		{
			name: "nil publisher",
			deps: cluster.LogStreamsDeps{
				Adder:        adder,
				Topology:     topology,
				Agent:        agent,
				LogPublisher: nil,
				LogAdapter:   logAdapter,
			},
			want: "cluster: log publisher is required",
		},
		{
			name: "nil adapter",
			deps: cluster.LogStreamsDeps{
				Adder:        adder,
				Topology:     topology,
				Agent:        agent,
				LogPublisher: logPublisher,
				LogAdapter:   nil,
			},
			want: "cluster: log adapter is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls, err := cluster.OpenLogStreams(tc.deps)
			require.EqualError(t, err, tc.want)
			require.Nil(t, ls)
		})
	}
}

func TestOpenLogStreams_openOrderAndBind(t *testing.T) {
	t.Parallel()

	const logTemplate = "aeron:udp?term-length=64k|interface=127.0.0.1|ttl=0"
	topology := sharedLogTopology(t, 0, logTemplate)
	adder := &fakeLogStreamAdder{}
	agent, logPublisher, logAdapter, deps := openLogStreamsDeps(t, topology, adder)

	ls, err := cluster.OpenLogStreams(deps)
	require.NoError(t, err)
	require.NotNil(t, ls)
	t.Cleanup(func() { _ = ls.Close() })

	// Open order: subscription then publication; both on log stream and channel.
	require.Len(t, adder.calls, 2)
	require.Equal(t, "subscription", adder.calls[0].kind)
	require.Equal(t, "publication", adder.calls[1].kind)
	require.Equal(t, topology.LogStreamID(), adder.calls[0].streamID)
	require.Equal(t, topology.LogStreamID(), adder.calls[1].streamID)
	require.Equal(t, topology.LogChannel(), adder.calls[0].channel)
	require.Equal(t, topology.LogChannel(), adder.calls[1].channel)

	// Multicast channel form from shared-log members + loopback template.
	const wantChannel = "aeron:udp?term-length=64k" +
		"|interface=127.0.0.1|ttl=0|endpoint=239.255.12.1:9004"
	require.Equal(t, wantChannel, adder.calls[0].channel)
	require.Equal(t, wantChannel, topology.LogChannel())

	// Bind set complete.
	require.Equal(t, adder.lastPub.SessionID(), agent.AddLogPublication(0))
	require.IsType(t, (*consensus.LogAppendPosition)(nil), agent.AppendSource())
	require.IsType(t, (*consensus.SubscriptionLogJoinSource)(nil), agent.LogJoinSource())
	require.Same(t, adder.lastPub, logPublisher.Publication())

	// Adapter bound with no image.
	require.Nil(t, logAdapter.Image())
	require.True(t, logAdapter.IsImageClosed())

	// Handles exposed.
	require.Same(t, adder.lastPub, ls.Publication())
	require.Same(t, adder.lastSub, ls.Subscription())
}

func TestOpenLogStreams_channelIdenticalAcrossMembers(t *testing.T) {
	t.Parallel()

	const logTemplate = "aeron:udp?term-length=64k|interface=127.0.0.1|ttl=0"
	adder0 := &fakeLogStreamAdder{}
	adder1 := &fakeLogStreamAdder{}
	tp0 := sharedLogTopology(t, 0, logTemplate)
	tp1 := sharedLogTopology(t, 1, logTemplate)

	_, _, _, deps0 := openLogStreamsDeps(t, tp0, adder0)
	_, _, _, deps1 := openLogStreamsDeps(t, tp1, adder1)

	ls0, err := cluster.OpenLogStreams(deps0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ls0.Close() })
	ls1, err := cluster.OpenLogStreams(deps1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ls1.Close() })

	require.Equal(t, adder0.calls[0].channel, adder1.calls[0].channel)
	require.Equal(t, adder0.calls[1].channel, adder1.calls[1].channel)
	require.Equal(t, tp0.LogChannel(), tp1.LogChannel())
}

// TestOpenLogStreams_publicationInitialPosition asserts OpenLogStreams seeds
// the exclusive publication channel only. The subscription keeps
// Topology.LogChannel.
func TestOpenLogStreams_publicationInitialPosition(t *testing.T) {
	t.Parallel()

	const logTemplate = "aeron:udp?term-length=64k|interface=127.0.0.1|ttl=0"
	topology := sharedLogTopology(t, 0, logTemplate)
	adder := &fakeLogStreamAdder{}
	_, _, _, deps := openLogStreamsDeps(t, topology, adder)

	// Term length differs from topology default (64k) so replace is exercised.
	seed := &cluster.LogPublicationInitialPosition{
		Position:         128,
		InitialTermID:    7,
		TermBufferLength: 128 * 1024,
		MtuLength:        1408,
	}
	deps.PublicationInitialPosition = seed

	ls, err := cluster.OpenLogStreams(deps)
	require.NoError(t, err)
	require.NotNil(t, ls)
	t.Cleanup(func() { _ = ls.Close() })

	require.Len(t, adder.calls, 2)
	require.Equal(t, "subscription", adder.calls[0].kind)
	require.Equal(t, "publication", adder.calls[1].kind)
	require.Equal(t, topology.LogStreamID(), adder.calls[0].streamID)
	require.Equal(t, topology.LogStreamID(), adder.calls[1].streamID)
	require.Equal(t, topology.LogChannel(), adder.calls[0].channel)

	// Seed geometry lands on the publication channel only. Param values are
	// covered in-package. Presence asserts the seed reached the add. Delimit
	// term-id so the assert cannot match inside init-term-id.
	pubChannel := adder.calls[1].channel
	require.Contains(t, pubChannel, "init-term-id=7")
	require.Contains(t, pubChannel, "|term-id=7")
	require.Contains(t, pubChannel, "term-offset=128")
	require.Contains(t, pubChannel, "term-length=131072")
	require.Contains(t, pubChannel, "mtu=1408")
	require.NotEqual(t, topology.LogChannel(), pubChannel)
	require.NotContains(t, adder.calls[0].channel, "init-term-id=")
}

func TestOpenLogStreams_publicationInitialPositionInvalid(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	adder := &fakeLogStreamAdder{}
	agent, logPublisher, _, deps := openLogStreamsDeps(t, topology, adder)
	deps.PublicationInitialPosition = &cluster.LogPublicationInitialPosition{
		Position:         17,
		InitialTermID:    0,
		TermBufferLength: 64 * 1024,
		MtuLength:        1408,
	}

	ls, err := cluster.OpenLogStreams(deps)
	require.Nil(t, ls)
	require.ErrorContains(t, err, "not frame-aligned")

	// Seed validated before any add; agent and publisher untouched.
	require.Empty(t, adder.calls)
	require.Equal(t, int32(-1), agent.AddLogPublication(0))
	require.Nil(t, agent.AppendSource())
	require.Nil(t, agent.LogJoinSource())
	require.Nil(t, logPublisher.Publication())
}

func TestOpenLogStreams_subscriptionAddFailure(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	cause := errors.New("driver timeout")
	adder := &fakeLogStreamAdder{failSubErr: cause}
	agent, logPublisher, _, deps := openLogStreamsDeps(t, topology, adder)

	ls, err := cluster.OpenLogStreams(deps)
	require.Nil(t, ls)
	channel := topology.LogChannel()
	want := fmt.Sprintf(
		"cluster: add cluster log subscription on %s: %s",
		channel,
		cause.Error(),
	)
	require.EqualError(t, err, want)
	require.ErrorIs(t, err, cause)

	// Agent and publisher untouched; no publication add attempted.
	require.Equal(t, int32(-1), agent.AddLogPublication(0))
	require.Nil(t, agent.AppendSource())
	require.Nil(t, agent.LogJoinSource())
	require.Nil(t, logPublisher.Publication())
	require.Len(t, adder.calls, 1)
	require.Equal(t, "subscription", adder.calls[0].kind)
}

func TestOpenLogStreams_publicationAddFailureRollsBack(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	cause := errors.New("driver timeout")
	adder := &fakeLogStreamAdder{failPubErr: cause}
	agent, logPublisher, _, deps := openLogStreamsDeps(t, topology, adder)

	ls, err := cluster.OpenLogStreams(deps)
	require.Nil(t, ls)
	channel := topology.LogChannel()
	want := fmt.Sprintf(
		"cluster: add cluster log publication on %s: %s",
		channel,
		cause.Error(),
	)
	require.EqualError(t, err, want)
	require.ErrorIs(t, err, cause)

	// Subscription already added is closed; agent untouched.
	require.NotNil(t, adder.lastSub)
	require.True(t, adder.lastSub.IsClosed())
	require.Equal(t, int32(-1), agent.AddLogPublication(0))
	require.Nil(t, agent.AppendSource())
	require.Nil(t, agent.LogJoinSource())
	require.Nil(t, logPublisher.Publication())
	require.Len(t, adder.calls, 2)
}

func TestOpenLogStreams_adderReturnsNilNil(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	channel := topology.LogChannel()

	t.Run("subscription", func(t *testing.T) {
		t.Parallel()
		adder := &fakeLogStreamAdder{returnNilSub: true}
		agent, logPublisher, _, deps := openLogStreamsDeps(t, topology, adder)

		ls, err := cluster.OpenLogStreams(deps)
		require.Nil(t, ls)
		want := fmt.Sprintf(
			"cluster: add cluster log subscription on %s: adder returned no subscription",
			channel,
		)
		require.EqualError(t, err, want)
		require.Equal(t, int32(-1), agent.AddLogPublication(0))
		require.Nil(t, agent.AppendSource())
		require.Nil(t, agent.LogJoinSource())
		require.Nil(t, logPublisher.Publication())
		require.Len(t, adder.calls, 1)
	})

	t.Run("publication", func(t *testing.T) {
		t.Parallel()
		adder := &fakeLogStreamAdder{returnNilPub: true}
		agent, logPublisher, _, deps := openLogStreamsDeps(t, topology, adder)

		ls, err := cluster.OpenLogStreams(deps)
		require.Nil(t, ls)
		want := fmt.Sprintf(
			"cluster: add cluster log publication on %s: adder returned no publication",
			channel,
		)
		require.EqualError(t, err, want)
		require.NotNil(t, adder.lastSub)
		require.True(t, adder.lastSub.IsClosed())
		require.Equal(t, int32(-1), agent.AddLogPublication(0))
		require.Nil(t, agent.AppendSource())
		require.Nil(t, agent.LogJoinSource())
		require.Nil(t, logPublisher.Publication())
	})
}

func TestLogStreams_close(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	adder := &fakeLogStreamAdder{}
	agent, logPublisher, _, deps := openLogStreamsDeps(t, topology, adder)

	ls, err := cluster.OpenLogStreams(deps)
	require.NoError(t, err)

	// Capture before Close; accessors nil after.
	pub := adder.lastPub
	sub := adder.lastSub
	require.NotNil(t, pub)
	require.NotNil(t, sub)
	require.False(t, pub.IsClosed())
	require.False(t, sub.IsClosed())

	require.NoError(t, ls.Close())

	// Clear then close: binds cleared; both handles closed; accessors nil.
	require.Equal(t, int32(-1), agent.AddLogPublication(0))
	require.Nil(t, agent.AppendSource())
	require.Nil(t, agent.LogJoinSource())
	require.Nil(t, logPublisher.Publication())
	require.True(t, pub.IsClosed())
	require.True(t, sub.IsClosed())
	require.Nil(t, ls.Publication())
	require.Nil(t, ls.Subscription())

	// Idempotent: second Close does not re-clear agent.
	require.NoError(t, ls.Close())
	require.Equal(t, int32(-1), agent.AddLogPublication(0))
	require.Nil(t, agent.AppendSource())
	require.Nil(t, ls.Publication())
	require.Nil(t, ls.Subscription())
}

func TestLogStreams_closeStaleAfterRebind(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	adder := &fakeLogStreamAdder{}
	agent, _, _, deps := openLogStreamsDeps(t, topology, adder)

	ls1, err := cluster.OpenLogStreams(deps)
	require.NoError(t, err)
	require.NoError(t, ls1.Close())
	require.Equal(t, int32(-1), agent.AddLogPublication(0))
	require.Nil(t, agent.AppendSource())

	// Re-open on same agent with a fresh publisher/adapter pair.
	logPublisher2 := consensus.NewLogPublisher()
	logAdapter2 := consensus.NewLogAdapter(0)
	deps.LogPublisher = logPublisher2
	deps.LogAdapter = logAdapter2
	ls2, err := cluster.OpenLogStreams(deps)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ls2.Close() })

	require.NotEqual(t, int32(-1), agent.AddLogPublication(0))
	require.NotNil(t, agent.AppendSource())
	require.NotNil(t, agent.LogJoinSource())
	require.Same(t, adder.lastPub, logPublisher2.Publication())
	pub2 := ls2.Publication()
	sub2 := ls2.Subscription()
	require.NotNil(t, pub2)
	require.NotNil(t, sub2)

	// Stale owner must not clear rebound agent state or close new handles.
	require.NoError(t, ls1.Close())
	require.NotEqual(t, int32(-1), agent.AddLogPublication(0))
	require.NotNil(t, agent.AppendSource())
	require.NotNil(t, agent.LogJoinSource())
	require.Same(t, pub2, ls2.Publication())
	require.Same(t, sub2, ls2.Subscription())
	require.False(t, pub2.IsClosed())
	require.False(t, sub2.IsClosed())
	require.Same(t, pub2, logPublisher2.Publication())
}

func TestLogStreams_nilReceiver(t *testing.T) {
	t.Parallel()

	var ls *cluster.LogStreams
	require.NoError(t, ls.Close())
	require.Nil(t, ls.Publication())
	require.Nil(t, ls.Subscription())

	// Zero value has no owned handles; Close is a no-op (no agent panic).
	require.NoError(t, (&cluster.LogStreams{}).Close())
}
