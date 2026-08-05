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

// testLogChannelTemplate is a loopback UDP log channel without endpoint.
// Topology fills the endpoint from shared-log members.
const testLogChannelTemplate = "aeron:udp?term-length=64k|interface=127.0.0.1|ttl=0"

type logStreamAddCall struct {
	kind     string
	streamID int32
	channel  string
}

type fakeLogStreamAdder struct {
	calls        []logStreamAddCall
	lastSub      *aeron.Subscription
	lastPub      *aeron.ExclusivePublication
	lastDestHook *aeron.RecordingReceiveDestinationHook
	// failSubLeft is remaining subscription adds that return failSubErr. Negative
	// means permanent (every subscription add fails). Zero means no scripted
	// failure (unset or countdown exhausted).
	failSubLeft    int
	failSubErr     error
	failPubErr     error
	failDestErr    error
	returnNilSub   bool
	returnNilPub   bool
	correlationSeq int64
}

func (f *fakeLogStreamAdder) NextCorrelationID() int64 {
	f.correlationSeq++
	return f.correlationSeq
}

// failNextSubscriptionAdds scripts the next n subscription adds to fail with
// err. After n failures, later adds succeed.
func (f *fakeLogStreamAdder) failNextSubscriptionAdds(n int, err error) {
	f.failSubErr = err
	f.failSubLeft = n
}

// failAllSubscriptionAdds scripts every subscription add to fail with err.
func (f *fakeLogStreamAdder) failAllSubscriptionAdds(err error) {
	f.failSubErr = err
	f.failSubLeft = -1
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
	if f.failSubErr != nil && f.failSubLeft != 0 {
		if f.failSubLeft > 0 {
			f.failSubLeft--
		}
		return nil, f.failSubErr
	}
	sub := new(aeron.Subscription)
	hook := &aeron.RecordingReceiveDestinationHook{AddErr: f.failDestErr}
	aeron.SubscriptionSetReceiveDestinationHookForHooks(sub, hook)
	f.lastSub = sub
	f.lastDestHook = hook
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

// requireManualLogSubscriptionChannel asserts the control-mode=manual channel
// shape that OpenLogStreams and ReseedSubscription build.
//
// Accepts require.TestingT so unit (*testing.T) and property (*rapid.T) callers
// share one helper.
func requireManualLogSubscriptionChannel(t require.TestingT, channel string) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	require.Contains(t, channel, "control-mode=manual")
	require.Contains(t, channel, "tags=")
	require.Contains(t, channel, "rejoin=false")
	require.Contains(t, channel, "alias=log-cm")
	require.NotContains(t, channel, "session-id=")
	require.NotContains(t, channel, "group=true")
	require.NotContains(t, channel, "endpoint=")
}

// requireLogStreamOpenOrder asserts subscription-then-publication open order
// and stream ids. Callers assert publication channel content separately.
//
// Accepts require.TestingT so unit (*testing.T) and property (*rapid.T) callers
// share one helper.
func requireLogStreamOpenOrder(t require.TestingT, adder *fakeLogStreamAdder, streamID int32) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	require.Len(t, adder.calls, 2)
	require.Equal(t, "subscription", adder.calls[0].kind)
	require.Equal(t, "publication", adder.calls[1].kind)
	require.Equal(t, streamID, adder.calls[0].streamID)
	require.Equal(t, streamID, adder.calls[1].streamID)
	requireManualLogSubscriptionChannel(t, adder.calls[0].channel)
}

// requireAgentLogUnbound asserts the agent and log publisher hold no log binds.
//
// Accepts require.TestingT so unit (*testing.T) and property (*rapid.T) callers
// share one helper.
func requireAgentLogUnbound(
	t require.TestingT,
	agent *consensus.Agent,
	logPublisher *consensus.LogPublisher,
) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	require.Equal(t, int32(-1), agent.AddLogPublication(0))
	require.Nil(t, agent.AppendSource())
	require.Nil(t, agent.LogJoinSource())
	require.Empty(t, agent.LiveLogDestination())
	require.Nil(t, logPublisher.Publication())
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

// openLogStreamsOK opens log streams and registers Close cleanup. For happy-path
// tests that keep a live LogStreams.
func openLogStreamsOK(
	t *testing.T,
	topology *cluster.Topology,
	adder cluster.LogStreamAdder,
) (
	*cluster.LogStreams,
	*consensus.Agent,
	*consensus.LogPublisher,
	*consensus.LogAdapter,
) {
	t.Helper()
	agent, logPublisher, logAdapter, deps := openLogStreamsDeps(t, topology, adder)
	ls, err := cluster.OpenLogStreams(deps)
	require.NoError(t, err)
	require.NotNil(t, ls)
	t.Cleanup(func() { _ = ls.Close() })
	return ls, agent, logPublisher, logAdapter
}

func TestOpenLogStreams_nilDeps(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	adder := &fakeLogStreamAdder{}
	agent, logPublisher, logAdapter, _ := openLogStreamsDeps(t, topology, adder)

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

	topology := sharedLogTopology(t, 0, testLogChannelTemplate)
	adder := &fakeLogStreamAdder{}
	ls, agent, logPublisher, logAdapter := openLogStreamsOK(t, topology, adder)

	// Open order: subscription then publication. Sub is manual-mode; pub is live.
	requireLogStreamOpenOrder(t, adder, topology.LogStreamID())
	require.Equal(t, topology.LogChannel(), adder.calls[1].channel)

	// Multicast live URI from shared-log members plus loopback template.
	const wantLive = "aeron:udp?term-length=64k" +
		"|interface=127.0.0.1|ttl=0|endpoint=239.255.12.1:9004"
	require.Equal(t, wantLive, topology.LogChannel())
	require.Equal(t, wantLive, agent.LiveLogDestination())
	require.Equal(t, []string{wantLive}, adder.lastDestHook.Adds)

	// Two correlation ids for tags.
	require.Equal(t, int64(2), adder.correlationSeq)

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
	require.Equal(t, wantLive, ls.LiveURI())
}

func TestOpenLogStreams_channelIdenticalAcrossMembers(t *testing.T) {
	t.Parallel()

	adder0 := &fakeLogStreamAdder{}
	adder1 := &fakeLogStreamAdder{}
	tp0 := sharedLogTopology(t, 0, testLogChannelTemplate)
	tp1 := sharedLogTopology(t, 1, testLogChannelTemplate)

	openLogStreamsOK(t, tp0, adder0)
	openLogStreamsOK(t, tp1, adder1)

	// Live publication channel and live destination match across members.
	// Manual subscription shape matches. Tags come from each member's client.
	requireManualLogSubscriptionChannel(t, adder0.calls[0].channel)
	requireManualLogSubscriptionChannel(t, adder1.calls[0].channel)
	require.Equal(t, adder0.calls[1].channel, adder1.calls[1].channel)
	require.Equal(t, tp0.LogChannel(), tp1.LogChannel())
	require.Equal(t, tp0.LogChannel(), adder0.lastDestHook.Adds[0])
	require.Equal(t, tp1.LogChannel(), adder1.lastDestHook.Adds[0])
}

// TestOpenLogStreams_publicationInitialPosition asserts OpenLogStreams seeds
// the exclusive publication channel only. The subscription is manual-mode.
func TestOpenLogStreams_publicationInitialPosition(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, testLogChannelTemplate)
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

	requireLogStreamOpenOrder(t, adder, topology.LogStreamID())

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
	require.Equal(t, []string{topology.LogChannel()}, adder.lastDestHook.Adds)
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
	requireAgentLogUnbound(t, agent, logPublisher)
}

func TestOpenLogStreams_subscriptionAddFailure(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	cause := errors.New("driver timeout")
	adder := &fakeLogStreamAdder{}
	adder.failAllSubscriptionAdds(cause)
	agent, logPublisher, _, deps := openLogStreamsDeps(t, topology, adder)

	ls, err := cluster.OpenLogStreams(deps)
	require.Nil(t, ls)
	require.ErrorIs(t, err, cause)
	require.ErrorContains(t, err, "cluster: add cluster log subscription on ")
	require.ErrorContains(t, err, "control-mode=manual")

	// Agent and publisher untouched. No publication add attempted.
	requireAgentLogUnbound(t, agent, logPublisher)
	require.Len(t, adder.calls, 1)
	require.Equal(t, "subscription", adder.calls[0].kind)
}

func TestOpenLogStreams_liveDestinationAddFailureRollsBack(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	cause := errors.New("destination refused")
	adder := &fakeLogStreamAdder{failDestErr: cause}
	agent, logPublisher, _, deps := openLogStreamsDeps(t, topology, adder)

	ls, err := cluster.OpenLogStreams(deps)
	require.Nil(t, ls)
	liveURI := topology.LogChannel()
	want := fmt.Sprintf(
		"cluster: add live log destination on %s: %s",
		liveURI,
		cause.Error(),
	)
	require.EqualError(t, err, want)
	require.ErrorIs(t, err, cause)

	// Subscription closed. Agent and publication untouched.
	require.NotNil(t, adder.lastSub)
	require.True(t, adder.lastSub.IsClosed())
	requireAgentLogUnbound(t, agent, logPublisher)
	require.Len(t, adder.calls, 1)
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
	requireAgentLogUnbound(t, agent, logPublisher)
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
		require.ErrorContains(t, err, "adder returned no subscription")
		require.ErrorContains(t, err, "control-mode=manual")
		requireAgentLogUnbound(t, agent, logPublisher)
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
		requireAgentLogUnbound(t, agent, logPublisher)
	})
}

func TestOpenLogStreams_copiesRcvbufParams(t *testing.T) {
	t.Parallel()

	const logTemplate = "aeron:udp?term-length=64k|so-rcvbuf=2m|rcv-wnd=1m" +
		"|interface=127.0.0.1|ttl=0"
	topology := sharedLogTopology(t, 0, logTemplate)
	adder := &fakeLogStreamAdder{}
	openLogStreamsOK(t, topology, adder)

	subChannel := adder.calls[0].channel
	requireManualLogSubscriptionChannel(t, subChannel)
	require.Contains(t, subChannel, "so-rcvbuf=2m")
	require.Contains(t, subChannel, "rcv-wnd=1m")
}

func TestLogStreams_reseedSubscription(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	adder := &fakeLogStreamAdder{}
	ls, agent, _, _ := openLogStreamsOK(t, topology, adder)

	oldSub := ls.Subscription()
	oldJoin := agent.LogJoinSource()
	require.NotNil(t, oldSub)
	require.NotNil(t, oldJoin)
	require.Len(t, adder.calls, 2)
	firstSubChannel := adder.calls[0].channel
	require.Equal(t, int64(2), adder.correlationSeq)

	require.NoError(t, ls.ReseedSubscription(adder))

	newSub := ls.Subscription()
	require.NotNil(t, newSub)
	require.NotSame(t, oldSub, newSub)
	require.True(t, oldSub.IsClosed())
	require.False(t, newSub.IsClosed())
	require.IsType(t, (*consensus.SubscriptionLogJoinSource)(nil), agent.LogJoinSource())
	require.NotSame(t, oldJoin, agent.LogJoinSource())

	// Fresh tags and no destinations on the reseeded subscription.
	require.Len(t, adder.calls, 3)
	require.Equal(t, "subscription", adder.calls[2].kind)
	requireManualLogSubscriptionChannel(t, adder.calls[2].channel)
	require.NotEqual(t, firstSubChannel, adder.calls[2].channel)
	require.Equal(t, int64(4), adder.correlationSeq)
	require.Empty(t, adder.lastDestHook.Adds)
}

func TestLogStreams_reseedSubscription_closed(t *testing.T) {
	t.Parallel()

	require.EqualError(
		t,
		(*cluster.LogStreams)(nil).ReseedSubscription(&fakeLogStreamAdder{}),
		"cluster: log streams closed",
	)
	require.EqualError(
		t,
		(&cluster.LogStreams{}).ReseedSubscription(&fakeLogStreamAdder{}),
		"cluster: log streams closed",
	)
}

func TestLogStreams_reseedSubscription_nilAdder(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	adder := &fakeLogStreamAdder{}
	ls, _, _, _ := openLogStreamsOK(t, topology, adder)

	subBefore := ls.Subscription()
	require.EqualError(
		t,
		ls.ReseedSubscription(nil),
		"cluster: subscription adder is required",
	)
	// Nil-adder check fails before close. Subscription stays intact.
	require.Same(t, subBefore, ls.Subscription())
	require.False(t, subBefore.IsClosed())
}

// Asserts a failed reseed open recovers a live-fed subscription instead of
// leaving the member with no receive destinations.
func TestLogStreams_reseedSubscription_openFailureRestoresLive(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	adder := &fakeLogStreamAdder{}
	ls, agent, _, _ := openLogStreamsOK(t, topology, adder)

	oldSub := ls.Subscription()
	liveURI := topology.LogChannel()
	require.Equal(t, liveURI, agent.LiveLogDestination())

	// Clear live as Ensure does before reseed. Recovery must re-set it.
	agent.LiveLogDestinationSet("")
	cause := errors.New("driver refused reseed")
	adder.failNextSubscriptionAdds(1, cause)

	err := ls.ReseedSubscription(adder)
	require.ErrorIs(t, err, cause)
	require.True(t, oldSub.IsClosed())

	// Recovery opened a second subscription, re-added live, and set the agent
	// field.
	newSub := ls.Subscription()
	require.NotNil(t, newSub)
	require.NotSame(t, oldSub, newSub)
	require.False(t, newSub.IsClosed())
	require.IsType(t, (*consensus.SubscriptionLogJoinSource)(nil), agent.LogJoinSource())
	require.Equal(t, liveURI, agent.LiveLogDestination())
	require.Equal(t, []string{liveURI}, adder.lastDestHook.Adds)

	// Open plus failed reseed plus recovery: three subscription adds.
	subCalls := 0
	for _, c := range adder.calls {
		if c.kind == "subscription" {
			subCalls++
		}
	}
	require.Equal(t, 3, subCalls)
}

// Asserts total reseed and recovery failure leaves no subscription.
func TestLogStreams_reseedSubscription_recoveryFailureLeavesNil(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	adder := &fakeLogStreamAdder{}
	agent, _, _, deps := openLogStreamsDeps(t, topology, adder)

	ls, err := cluster.OpenLogStreams(deps)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ls.Close() })

	oldSub := ls.Subscription()
	cause := errors.New("driver down")
	adder.failNextSubscriptionAdds(2, cause)

	err = ls.ReseedSubscription(adder)
	require.ErrorIs(t, err, cause)
	require.True(t, oldSub.IsClosed())
	require.Nil(t, ls.Subscription())
	require.Nil(t, agent.LogJoinSource())
	require.Empty(t, agent.LiveLogDestination())
}

// Asserts recovery open success with live AddDestination failure. Binds the
// subscription. Clears LiveLogDestination. Joins both error causes.
func TestLogStreams_reseedSubscription_recoveryAddDestinationFailure(t *testing.T) {
	t.Parallel()

	topology := sharedLogTopology(t, 0, "")
	adder := &fakeLogStreamAdder{}
	ls, agent, _, _ := openLogStreamsOK(t, topology, adder)

	oldSub := ls.Subscription()
	liveURI := topology.LogChannel()
	require.Equal(t, liveURI, agent.LiveLogDestination())

	// Clear live as Ensure does before reseed. Recovery open succeeds. Live re-add
	// fails and must keep the field empty for a later restore.
	agent.LiveLogDestinationSet("")
	reseedCause := errors.New("driver refused reseed")
	destCause := errors.New("driver refused live destination")
	adder.failNextSubscriptionAdds(1, reseedCause)
	adder.failDestErr = destCause

	err := ls.ReseedSubscription(adder)
	require.ErrorIs(t, err, reseedCause)
	require.ErrorIs(t, err, destCause)
	require.True(t, oldSub.IsClosed())

	newSub := ls.Subscription()
	require.NotNil(t, newSub)
	require.NotSame(t, oldSub, newSub)
	require.False(t, newSub.IsClosed())
	require.IsType(t, (*consensus.SubscriptionLogJoinSource)(nil), agent.LogJoinSource())
	// Add failed after the hook recorded the attempt. Agent field must stay empty
	// so restoreLiveLogDestination can re-add on a later tick.
	require.Empty(t, agent.LiveLogDestination())
	require.Equal(t, []string{liveURI}, adder.lastDestHook.Adds)
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
	requireAgentLogUnbound(t, agent, logPublisher)
	require.Empty(t, agent.CatchupLogDestination())
	require.True(t, pub.IsClosed())
	require.True(t, sub.IsClosed())
	require.Nil(t, ls.Publication())
	require.Nil(t, ls.Subscription())
	require.Empty(t, ls.LiveURI())

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
