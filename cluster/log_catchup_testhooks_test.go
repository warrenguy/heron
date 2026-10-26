//go:build testhooks

package cluster

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/consensus"
)

const (
	testCatchupEndpoint       = "127.0.0.1:9105"
	testCatchupLogSessionID   = 55
	testCatchupRecordingID    = 88
	testCatchupStartPosition  = 4096
	testCatchupCustomLogChan  = "aeron:udp?term-length=1m|mtu=8k|endpoint=239.255.12.1:9004"
	testCatchupDefaultLogChan = DefaultLogChannel + "|endpoint=239.255.12.1:9004"

	// testCatchupDefaultDest is the golden destination URI from the default
	// template. Geometry matches DefaultLogChannel with the catchup endpoint.
	testCatchupDefaultDest = "aeron:udp?term-length=64k|endpoint=" + testCatchupEndpoint
)

var errTestArchiveRefused = errors.New("archive refused replay")

// catchupLogStreamAdder is a LogStreamAdder with destination recording for
// catchup unit tests. Package-local. log_streams_test uses a parallel fake.
type catchupLogStreamAdder struct {
	subCalls       []catchupAddSubCall
	lastSub        *aeron.Subscription
	lastDestHook   *aeron.RecordingReceiveDestinationHook
	correlationSeq int64
	failSubErr     error
	// failSubLeft is remaining subscription adds that return failSubErr. Zero means
	// later adds succeed.
	failSubLeft int
	// failFirstDestOnNextSub makes the next created subscription fail its first
	// AddDestination only. Later adds on that subscription succeed.
	failFirstDestOnNextSub bool
	failDestErr            error
}

type catchupAddSubCall struct {
	streamID int32
	channel  string
}

func (f *catchupLogStreamAdder) NextCorrelationID() int64 {
	f.correlationSeq++
	return f.correlationSeq
}

// failNextSubscriptionAdds scripts the next n subscription adds to fail with
// err. After n failures, later adds succeed.
func (f *catchupLogStreamAdder) failNextSubscriptionAdds(n int, err error) {
	f.failSubErr = err
	f.failSubLeft = n
}

// failFirstDestinationOnNextSub scripts the next created subscription to fail
// its first AddDestination only.
func (f *catchupLogStreamAdder) failFirstDestinationOnNextSub(err error) {
	f.failFirstDestOnNextSub = true
	f.failDestErr = err
}

func (f *catchupLogStreamAdder) AddSubscription(
	streamID int32,
	channel string,
) (*aeron.Subscription, error) {
	f.subCalls = append(f.subCalls, catchupAddSubCall{
		streamID: streamID,
		channel:  channel,
	})
	if f.failSubLeft > 0 {
		f.failSubLeft--
		return nil, f.failSubErr
	}
	sub := new(aeron.Subscription)
	hook := &aeron.RecordingReceiveDestinationHook{}
	if f.failFirstDestOnNextSub {
		hook.FailNextAddErr = f.failDestErr
		f.failFirstDestOnNextSub = false
	}
	aeron.SubscriptionSetReceiveDestinationHookForHooks(sub, hook)
	f.lastSub = sub
	f.lastDestHook = hook
	return sub, nil
}

func (f *catchupLogStreamAdder) AddExclusivePublication(
	streamID int32,
	channel string,
) (*aeron.ExclusivePublication, error) {
	return new(aeron.ExclusivePublication), nil
}

// catchupTestStreams opens LogStreams with a recording destination hook so
// Ensure and Remove can reseed and attach destinations without a media driver.
type catchupTestStreams struct {
	tb       testing.TB
	streams  *LogStreams
	adder    *catchupLogStreamAdder
	agent    *consensus.Agent
	adapter  *consensus.LogAdapter
	liveURI  string
	streamID int32
}

func openCatchupTestStreams(t *testing.T) *catchupTestStreams {
	t.Helper()

	// Shared-log topology resolves the live multicast URI from members.
	cfg := Config{
		MemberID: 0,
		ClusterMembers: "0,127.0.0.1:9002,127.0.0.1:9003,239.255.12.1:9004," +
			"127.0.0.1:9005,127.0.0.1:9006,127.0.0.1:9007|" +
			"1,127.0.0.1:9102,127.0.0.1:9103,239.255.12.1:9004," +
			"127.0.0.1:9105,127.0.0.1:9106,127.0.0.1:9107|" +
			"2,127.0.0.1:9202,127.0.0.1:9203,239.255.12.1:9004," +
			"127.0.0.1:9205,127.0.0.1:9206,127.0.0.1:9207",
		ClusterDir:                    t.TempDir(),
		AeronDir:                      t.TempDir(),
		LogChannel:                    "aeron:udp?term-length=64k|interface=127.0.0.1|ttl=0",
		ReplicationChannel:            testReplicationChannel,
		ArchiveControlRequestChannel:  "aeron:ipc",
		ArchiveControlResponseChannel: "aeron:ipc",
	}.Normalize()
	tp, err := NewTopology(cfg)
	require.NoError(t, err)

	adder := &catchupLogStreamAdder{}
	agent := testReplayAgent(testCatchupRecordingID)
	logPublisher := consensus.NewLogPublisher()
	logAdapter := consensus.NewLogAdapter(0)
	ls, err := OpenLogStreams(LogStreamsDeps{
		Adder:        adder,
		Topology:     tp,
		Agent:        agent,
		LogPublisher: logPublisher,
		LogAdapter:   logAdapter,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ls.Close() })

	return &catchupTestStreams{
		tb:       t,
		streams:  ls,
		adder:    adder,
		agent:    agent,
		adapter:  logAdapter,
		liveURI:  tp.LogChannel(),
		streamID: tp.LogStreamID(),
	}
}

func newTestCatchupControl(
	t *testing.T,
	fixture *catchupTestStreams,
	control replayControl,
) *catchupControl {
	t.Helper()
	return newCatchupControl(
		context.Background(),
		control,
		fixture.adder,
		fixture.agent,
		fixture.streams,
		fixture.streamID,
		fixture.liveURI,
	)
}

// Asserts catchup channels build on the configured log channel, not the
// package default. Term geometry must match the live log and the recording
// taken from that channel. A replay onto a shorter term is a different stream.
func TestCatchupControl_channelsFollowConfiguredLogChannel(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		logChannel string
		want       string
	}{
		{
			name:       "default template",
			logChannel: testCatchupDefaultLogChan,
			want:       testCatchupDefaultDest,
		},
		{
			name:       "custom term length and mtu",
			logChannel: testCatchupCustomLogChan,
			want: "aeron:udp?term-length=1m|mtu=8k|endpoint=" +
				testCatchupEndpoint,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(
				t,
				tt.want,
				catchupDestinationURI(tt.logChannel, testCatchupEndpoint),
			)

			// Replay channel: same template plus session scope, linger, eos, and
			// alias=log-catchup.
			replay := catchupReplayChannel(
				tt.logChannel,
				testCatchupEndpoint,
				testCatchupLogSessionID,
			)
			require.Equal(
				t,
				tt.want+"|session-id=55|linger=0|eos=false|alias=log-catchup",
				replay,
			)
		})
	}
}

// Asserts StartCatchupReplay forwards follow-mode args (MaxInt64 length,
// session-scoped channel with alias=log-catchup) and retains session ids until
// stop.
func TestCatchupControl_stopDrainsStartedReplays(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	control := &fakeReplayControl{sessionID: 11}
	c := newTestCatchupControl(t, fixture, control)

	first, err := c.StartCatchupReplay(
		testCatchupStartPosition,
		testCatchupEndpoint,
		testCatchupLogSessionID,
	)
	require.NoError(t, err)
	require.Equal(t, int64(11), first)

	require.Len(t, control.startCalls, 1)
	require.Equal(t, int64(testCatchupRecordingID), control.startCalls[0].recordingID)
	require.Equal(t, int64(testCatchupStartPosition), control.startCalls[0].position)
	require.Equal(t, int64(math.MaxInt64), control.startCalls[0].length)
	require.Equal(t, fixture.streamID, control.startCalls[0].replayStreamID)
	wantChannel := catchupReplayChannel(
		fixture.liveURI,
		testCatchupEndpoint,
		testCatchupLogSessionID,
	)
	require.Equal(t, wantChannel, control.startCalls[0].replayChannel)

	control.sessionID = 12
	second, err := c.StartCatchupReplay(
		testCatchupStartPosition,
		"127.0.0.1:9106",
		testCatchupLogSessionID,
	)
	require.NoError(t, err)
	require.Equal(t, int64(12), second)
	require.Len(t, control.startCalls, 2)
	require.Equal(t, int64(math.MaxInt64), control.startCalls[1].length)
	wantChannel2 := catchupReplayChannel(
		fixture.liveURI,
		"127.0.0.1:9106",
		testCatchupLogSessionID,
	)
	require.Equal(t, wantChannel2, control.startCalls[1].replayChannel)

	require.NoError(t, c.StopCatchupReplays())
	require.Equal(t, []int64{11, 12}, control.stopCalls)

	// Idempotent: a second stop has nothing left to drain.
	require.NoError(t, c.StopCatchupReplays())
	require.Len(t, control.stopCalls, 2)
}

// Asserts StopCatchupReplay removes one retained session and leaves others.
func TestCatchupControl_stopOneReplayLeavesOthers(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	control := &fakeReplayControl{sessionID: 21}
	c := newTestCatchupControl(t, fixture, control)

	first, err := c.StartCatchupReplay(
		testCatchupStartPosition,
		testCatchupEndpoint,
		testCatchupLogSessionID,
	)
	require.NoError(t, err)
	require.Equal(t, int64(21), first)

	control.sessionID = 22
	second, err := c.StartCatchupReplay(
		testCatchupStartPosition,
		"127.0.0.1:9107",
		testCatchupLogSessionID,
	)
	require.NoError(t, err)
	require.Equal(t, int64(22), second)

	require.NoError(t, c.StopCatchupReplay(first))
	require.Equal(t, []int64{21}, control.stopCalls)
	require.Equal(t, []int64{22}, c.replaySessionIDs)

	// Unknown session is a no-op. Ledger stays intact.
	require.NoError(t, c.StopCatchupReplay(first))
	require.Equal(t, []int64{21}, control.stopCalls)
	require.Equal(t, []int64{22}, c.replaySessionIDs)

	require.NoError(t, c.StopCatchupReplays())
	require.Equal(t, []int64{21, 22}, control.stopCalls)
	require.Empty(t, c.replaySessionIDs)
}

// Asserts a failed StopCatchupReplay keeps the session in the ledger. A later
// call can retry the archive stop.
func TestCatchupControl_stopOneReplayFailureKeepsLedger(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	control := &fakeReplayControl{
		sessionID: 31,
		stopErr:   errTestArchiveRefused,
	}
	c := newTestCatchupControl(t, fixture, control)

	sessionID, err := c.StartCatchupReplay(
		testCatchupStartPosition,
		testCatchupEndpoint,
		testCatchupLogSessionID,
	)
	require.NoError(t, err)
	require.Equal(t, int64(31), sessionID)

	require.ErrorIs(t, c.StopCatchupReplay(sessionID), errTestArchiveRefused)
	require.Equal(t, []int64{31}, control.stopCalls)
	require.Equal(t, []int64{31}, c.replaySessionIDs)

	// Retry after archive recovers: ledger drops only on success.
	control.stopErr = nil
	require.NoError(t, c.StopCatchupReplay(sessionID))
	require.Equal(t, []int64{31, 31}, control.stopCalls)
	require.Empty(t, c.replaySessionIDs)
}

// Asserts a failed start is not retained. No replay exists to stop.
func TestCatchupControl_startFailureRetainsNothing(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	control := &fakeReplayControl{startErr: errTestArchiveRefused}
	c := newTestCatchupControl(t, fixture, control)

	_, err := c.StartCatchupReplay(
		testCatchupStartPosition,
		testCatchupEndpoint,
		testCatchupLogSessionID,
	)
	require.ErrorIs(t, err, errTestArchiveRefused)

	require.NoError(t, c.StopCatchupReplays())
	require.Empty(t, control.stopCalls)
}

// Asserts Ensure reseeds, adds only the catchup destination, and is
// idempotent once open. Remove restores the live destination alone.
func TestCatchupControl_ensureReseedAndRemoveRestoresLive(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	c := newTestCatchupControl(t, fixture, &fakeReplayControl{})

	// Open left the live destination attached.
	require.Equal(t, fixture.liveURI, fixture.agent.LiveLogDestination())
	require.Equal(t, []string{fixture.liveURI}, fixture.adder.lastDestHook.Adds)
	openSub := fixture.streams.Subscription()
	openJoin := fixture.agent.LogJoinSource()
	require.Len(t, fixture.adder.subCalls, 1)

	want := catchupDestinationURI(fixture.liveURI, testCatchupEndpoint)
	require.NotEmpty(t, want)
	require.Equal(t, want, c.EnsureCatchupLogDestination(testCatchupEndpoint))

	// Reseed opened a fresh manual subscription. Live field is cleared. Only
	// the catchup destination is attached.
	require.Len(t, fixture.adder.subCalls, 2)
	require.NotSame(t, openSub, fixture.streams.Subscription())
	require.True(t, openSub.IsClosed())
	require.NotSame(t, openJoin, fixture.agent.LogJoinSource())
	require.IsType(t, (*consensus.SubscriptionLogJoinSource)(nil), fixture.agent.LogJoinSource())
	require.Empty(t, fixture.agent.LiveLogDestination())
	require.Equal(t, want, fixture.agent.CatchupLogDestination())
	require.Equal(t, []string{want}, fixture.adder.lastDestHook.Adds)

	// Idempotent: second Ensure returns the stored URI and reseeds nothing.
	require.Equal(t, want, c.EnsureCatchupLogDestination(testCatchupEndpoint))
	require.Len(t, fixture.adder.subCalls, 2)

	require.NoError(t, c.RemoveCatchupLogDestination())
	require.Empty(t, fixture.agent.CatchupLogDestination())
	require.Equal(t, fixture.liveURI, fixture.agent.LiveLogDestination())
	require.Equal(t, []string{want}, fixture.adder.lastDestHook.Removes)
	require.Equal(t, []string{want, fixture.liveURI}, fixture.adder.lastDestHook.Adds)

	// Destination set after teardown is live only.
	require.Equal(t, []string{fixture.liveURI}, currentDestinations(fixture.adder.lastDestHook))

	// Idempotent: a second remove neither reopens nor re-adds.
	require.NoError(t, c.RemoveCatchupLogDestination())
	require.Equal(t, []string{want}, fixture.adder.lastDestHook.Removes)
	require.Equal(t, fixture.liveURI, fixture.agent.LiveLogDestination())

	// A later Ensure reseeds and re-attaches catchup.
	require.Equal(t, want, c.EnsureCatchupLogDestination(testCatchupEndpoint))
	require.Len(t, fixture.adder.subCalls, 3)
	require.Equal(t, want, fixture.agent.CatchupLogDestination())
	require.Empty(t, fixture.agent.LiveLogDestination())
}

// Asserts abandoned-catchup teardown leaves only the live destination.
// Prepare-style remove after a reseed with live still empty must not leave the
// subscription with zero destinations.
func TestCatchupControl_abandonedCatchupRestoresLiveOnly(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	c := newTestCatchupControl(t, fixture, &fakeReplayControl{})

	want := c.EnsureCatchupLogDestination(testCatchupEndpoint)
	require.NotEmpty(t, want)
	require.Empty(t, fixture.agent.LiveLogDestination())

	require.NoError(t, c.RemoveCatchupLogDestination())
	require.Empty(t, fixture.agent.CatchupLogDestination())
	require.Equal(t, fixture.liveURI, fixture.agent.LiveLogDestination())
	require.Equal(t, []string{fixture.liveURI}, currentDestinations(fixture.adder.lastDestHook))
}

// Asserts AddLiveLogDestination attaches live beside catchup.
// Idempotent when already attached. Retries when AddDestination fails.
func TestCatchupControl_addLiveLogDestination(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	c := newTestCatchupControl(t, fixture, &fakeReplayControl{})

	wantCatchup := c.EnsureCatchupLogDestination(testCatchupEndpoint)
	require.NotEmpty(t, wantCatchup)
	require.Empty(t, fixture.agent.LiveLogDestination())

	// Failure leaves LiveLogDestination empty. The next call retries.
	fixture.adder.lastDestHook.FailNextAddErr = errors.New("live add refused")
	require.ErrorContains(t, c.AddLiveLogDestination(), "live add refused")
	require.Empty(t, fixture.agent.LiveLogDestination())
	require.Equal(t, []string{wantCatchup}, currentDestinations(fixture.adder.lastDestHook))

	// Retry attaches live beside catchup.
	require.NoError(t, c.AddLiveLogDestination())
	require.Equal(t, fixture.liveURI, fixture.agent.LiveLogDestination())
	require.Equal(
		t,
		[]string{wantCatchup, fixture.liveURI},
		currentDestinations(fixture.adder.lastDestHook),
	)

	// Idempotent: already attached. No second AddDestination.
	addsBefore := len(fixture.adder.lastDestHook.Adds)
	require.NoError(t, c.AddLiveLogDestination())
	require.Equal(t, fixture.liveURI, fixture.agent.LiveLogDestination())
	require.Len(t, fixture.adder.lastDestHook.Adds, addsBefore)
}

// Asserts module-close remove does not restore the live destination.
func TestCatchupControl_closeRemovesWithoutLiveRestore(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	c := newTestCatchupControl(t, fixture, &fakeReplayControl{})

	want := c.EnsureCatchupLogDestination(testCatchupEndpoint)
	require.NotEmpty(t, want)
	require.Empty(t, fixture.agent.LiveLogDestination())

	require.NoError(t, c.close())
	require.Empty(t, fixture.agent.CatchupLogDestination())
	require.Empty(t, fixture.agent.LiveLogDestination())
	require.Equal(t, []string{want}, fixture.adder.lastDestHook.Removes)
	// No live re-add after the catchup destination on the reseeded subscription.
	require.Equal(t, []string{want}, fixture.adder.lastDestHook.Adds)
}

// Asserts catchup AddDestination failure after reseed restores the live feed.
func TestCatchupControl_ensureCatchupAddFailureRestoresLive(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	c := newTestCatchupControl(t, fixture, &fakeReplayControl{})

	// Next reseeded subscription fails the first destination add (catchup),
	// then succeeds on live restore.
	fixture.adder.failFirstDestinationOnNextSub(errors.New("catchup dest refused"))

	require.Empty(t, c.EnsureCatchupLogDestination(testCatchupEndpoint))
	require.Empty(t, fixture.agent.CatchupLogDestination())
	require.Equal(t, fixture.liveURI, fixture.agent.LiveLogDestination())
	require.Equal(
		t,
		[]string{fixture.liveURI},
		currentDestinations(fixture.adder.lastDestHook),
	)
}

// Asserts reseed open failure restores live via fail-closed recovery.
func TestCatchupControl_ensureReseedFailureRestoresLive(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	c := newTestCatchupControl(t, fixture, &fakeReplayControl{})
	openSub := fixture.streams.Subscription()

	cause := errors.New("reseed refused")
	// First reseed open fails. Recovery open succeeds and re-adds live.
	fixture.adder.failNextSubscriptionAdds(1, cause)

	require.Empty(t, c.EnsureCatchupLogDestination(testCatchupEndpoint))
	require.Empty(t, fixture.agent.CatchupLogDestination())
	require.Equal(t, fixture.liveURI, fixture.agent.LiveLogDestination())
	require.True(t, openSub.IsClosed())
	require.NotNil(t, fixture.streams.Subscription())
	require.NotSame(t, openSub, fixture.streams.Subscription())
	require.Equal(
		t,
		[]string{fixture.liveURI},
		currentDestinations(fixture.adder.lastDestHook),
	)
}

// Asserts a later catchup entry rebuilds the log subscription after a failed
// reseed and recovery.
func TestCatchupControl_ensureRetryAfterReseedRecoveryFailure(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	c := newTestCatchupControl(t, fixture, &fakeReplayControl{})
	openSub := fixture.streams.Subscription()

	// Phase: first entry. Both adds fail.
	fixture.adder.failNextSubscriptionAdds(2, errors.New("driver down"))
	require.Empty(t, c.EnsureCatchupLogDestination(testCatchupEndpoint))
	require.Nil(t, fixture.streams.Subscription())
	require.Nil(t, fixture.agent.LogJoinSource())
	require.Empty(t, fixture.agent.CatchupLogDestination())
	require.Empty(t, fixture.agent.LiveLogDestination())
	require.True(t, openSub.IsClosed())

	// Phase: later entry. The reseed adds a fresh subscription and attaches
	// catchup.
	want := catchupDestinationURI(fixture.liveURI, testCatchupEndpoint)
	subCallsBefore := len(fixture.adder.subCalls)
	require.Equal(t, want, c.EnsureCatchupLogDestination(testCatchupEndpoint))
	require.Len(t, fixture.adder.subCalls, subCallsBefore+1)
	require.Same(t, fixture.adder.lastSub, fixture.streams.Subscription())
	require.IsType(t, (*consensus.SubscriptionLogJoinSource)(nil), fixture.agent.LogJoinSource())
	require.Equal(t, want, fixture.agent.CatchupLogDestination())
	require.Empty(t, fixture.agent.LiveLogDestination())
	require.Equal(t, []string{want}, currentDestinations(fixture.adder.lastDestHook))

	// Phase: near live. Live joins catchup on the rebuilt subscription.
	require.NoError(t, c.AddLiveLogDestination())
	require.Equal(t, fixture.liveURI, fixture.agent.LiveLogDestination())
	require.Equal(
		t,
		[]string{want, fixture.liveURI},
		currentDestinations(fixture.adder.lastDestHook),
	)

	// Phase: stop catchup. Only live remains.
	require.NoError(t, c.RemoveCatchupLogDestination())
	require.Empty(t, fixture.agent.CatchupLogDestination())
	require.Equal(t, fixture.liveURI, fixture.agent.LiveLogDestination())
	require.Equal(t, []string{fixture.liveURI}, currentDestinations(fixture.adder.lastDestHook))
}

// catchupTB is the testing seam the catchup helpers need. Both *testing.T and
// *rapid.T satisfy it.
type catchupTB interface {
	require.TestingT
	Helper()
}

// Asserts prepare replaces the log subscription after the bound image closes
// without end of stream. The fresh subscription carries the live destination.
// The next join binds the session from it.
func TestCatchupControl_prepareReplacesSubscriptionAfterImageLoss(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	fixture.agent.SetCatchupControl(newTestCatchupControl(t, fixture, &fakeReplayControl{}))
	oldSub := fixture.streams.Subscription()

	// Phase: bind.
	image := joinTestLogImage(t, fixture, testCatchupLogSessionID, 1, 0)

	// Phase: loss. The join query after the close misses.
	aeron.ImageInternalCloseForHooks(image)
	_, ready := fixture.agent.FollowerLogJoinReady(testCatchupLogSessionID)
	require.False(t, ready)

	// Phase: prepare.
	requirePrepareReplacedSubscription(t, fixture, oldSub)

	// Phase: rejoin.
	joinTestLogImage(t, fixture, testCatchupLogSessionID, 2, testCatchupStartPosition)
	require.True(t, fixture.agent.IsLogJoinedForSession(testCatchupLogSessionID))
}

// Asserts prepare replaces the log subscription after a missed follower join
// query, even while the bound image of another session stays open.
func TestCatchupControl_prepareReplacesSubscriptionAfterMissedJoin(t *testing.T) {
	t.Parallel()

	const otherSessionID int32 = testCatchupLogSessionID + 1

	fixture := openCatchupTestStreams(t)
	fixture.agent.SetCatchupControl(newTestCatchupControl(t, fixture, &fakeReplayControl{}))
	oldSub := fixture.streams.Subscription()

	// Phase: bind.
	bound := joinTestLogImage(t, fixture, testCatchupLogSessionID, 1, 0)

	// Phase: loss. The other session's image closes. The bound image stays
	// open.
	other := addTestLogImage(t, fixture, otherSessionID, 2, 0)
	aeron.ImageInternalCloseForHooks(other)

	// Phase: first prepare. No miss and an open image keep the subscription.
	_, prepared, err := fixture.agent.PrepareForNewLeadership(0, 0)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Same(t, oldSub, fixture.streams.Subscription())
	require.Same(t, bound, fixture.adapter.Image())

	// Phase: miss.
	_, ready := fixture.agent.FollowerLogJoinReady(otherSessionID)
	require.False(t, ready)

	// Phase: second prepare.
	requirePrepareReplacedSubscription(t, fixture, oldSub)

	// Phase: rejoin.
	joinTestLogImage(t, fixture, otherSessionID, 3, 0)
	require.True(t, fixture.agent.IsLogJoinedForSession(otherSessionID))
}

// requirePrepareReplacedSubscription runs one prepare and requires it to
// replace oldSub with a fresh subscription that carries only the live
// destination. The adapter holds no image afterwards.
func requirePrepareReplacedSubscription(
	t catchupTB,
	fixture *catchupTestStreams,
	oldSub *aeron.Subscription,
) {
	t.Helper()
	_, prepared, err := fixture.agent.PrepareForNewLeadership(0, 0)
	require.NoError(t, err)
	require.True(t, prepared)
	require.NotSame(t, oldSub, fixture.streams.Subscription())
	require.True(t, oldSub.IsClosed())
	require.Nil(t, fixture.adapter.Image())
	requireLogStreamsReseeded(t, fixture, true)
}

// requireLogStreamsReseeded requires the streams and agent state a reseed
// leaves. wantSub false is the double add failure: no subscription, no join
// source, no live destination.
func requireLogStreamsReseeded(t catchupTB, fixture *catchupTestStreams, wantSub bool) {
	t.Helper()
	require.Empty(t, fixture.agent.CatchupLogDestination())
	if !wantSub {
		require.Nil(t, fixture.streams.Subscription())
		require.Nil(t, fixture.agent.LogJoinSource())
		require.Empty(t, fixture.agent.LiveLogDestination())
		return
	}
	require.Same(t, fixture.adder.lastSub, fixture.streams.Subscription())
	require.False(t, fixture.adder.lastSub.IsClosed())
	require.IsType(t, (*consensus.SubscriptionLogJoinSource)(nil), fixture.agent.LogJoinSource())
	require.Equal(t, fixture.liveURI, fixture.agent.LiveLogDestination())
	require.Equal(t, []string{fixture.liveURI}, currentDestinations(fixture.adder.lastDestHook))
}

// joinTestLogImage adds an open image of sessionID at joinPosition to the
// current log subscription, then joins it through the agent. Returns the image.
func joinTestLogImage(
	t catchupTB,
	fixture *catchupTestStreams,
	sessionID int32,
	correlationID int64,
	joinPosition int64,
) *aeron.Image {
	t.Helper()
	image := addTestLogImage(t, fixture, sessionID, correlationID, joinPosition)
	params, ready := fixture.agent.FollowerLogJoinReady(sessionID)
	require.True(t, ready)
	require.Equal(t, joinPosition, params.LogJoinPosition)
	joined, err := fixture.agent.JoinLogAsFollower(params, 0)
	require.NoError(t, err)
	require.True(t, joined)
	require.Same(t, image, fixture.adapter.Image())
	return image
}

// addTestLogImage adds an open image of sessionID at joinPosition to the
// current log subscription. Returns the image.
func addTestLogImage(
	t catchupTB,
	fixture *catchupTestStreams,
	sessionID int32,
	correlationID int64,
	joinPosition int64,
) *aeron.Image {
	t.Helper()
	sub := fixture.streams.Subscription()
	require.NotNil(t, sub)
	image := aeron.NewTestImageForSubscriptionHooksWithSessionID(
		fixture.tb,
		sub,
		sessionID,
		correlationID,
		joinPosition,
	)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	return image
}

// reseedAddOutcome scripts the subscription adds of one reseed and the state
// the reseed leaves.
type reseedAddOutcome struct {
	name string
	// arrange scripts the add failures. Nil scripts none.
	arrange func(adder *catchupLogStreamAdder, cause error)
	// wantAdds is the number of subscription adds the reseed makes.
	wantAdds int
	wantErr  bool
	wantSub  bool
}

// reseedAddOutcomes covers every add failure shape of one reseed.
var reseedAddOutcomes = []reseedAddOutcome{
	{name: "adds succeed", wantAdds: 1, wantSub: true},
	{
		name: "fresh add fails, recovery succeeds",
		arrange: func(adder *catchupLogStreamAdder, cause error) {
			adder.failNextSubscriptionAdds(1, cause)
		},
		wantAdds: 2,
		wantErr:  true,
		wantSub:  true,
	},
	{
		name: "recovery live add fails once",
		arrange: func(adder *catchupLogStreamAdder, cause error) {
			adder.failNextSubscriptionAdds(1, cause)
			adder.failFirstDestinationOnNextSub(cause)
		},
		wantAdds: 2,
		wantErr:  true,
		wantSub:  true,
	},
	{
		name: "both adds fail",
		arrange: func(adder *catchupLogStreamAdder, cause error) {
			adder.failNextSubscriptionAdds(2, cause)
		},
		wantAdds: 2,
		wantErr:  true,
	},
}

// Asserts ReseedLogSubscription replaces the subscription with one that holds
// only the live destination, and the state each add failure leaves.
func TestCatchupControl_reseedLogSubscription(t *testing.T) {
	t.Parallel()

	cause := errors.New("driver refused")

	for _, tt := range reseedAddOutcomes {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fixture := openCatchupTestStreams(t)
			c := newTestCatchupControl(t, fixture, &fakeReplayControl{})
			requireReseedLogSubscription(t, fixture, c, tt, cause)
		})
	}

	t.Run("after ensure", func(t *testing.T) {
		t.Parallel()

		fixture := openCatchupTestStreams(t)
		c := newTestCatchupControl(t, fixture, &fakeReplayControl{})
		require.NotEmpty(t, c.EnsureCatchupLogDestination(testCatchupEndpoint))
		requireReseedLogSubscription(t, fixture, c, reseedAddOutcomes[0], cause)
	})

	t.Run("closed streams", func(t *testing.T) {
		t.Parallel()

		fixture := openCatchupTestStreams(t)
		c := newTestCatchupControl(t, fixture, &fakeReplayControl{})
		require.NoError(t, fixture.streams.Close())
		addsBefore := len(fixture.adder.subCalls)

		require.EqualError(t, c.ReseedLogSubscription(), "cluster: log streams closed")
		require.Len(t, fixture.adder.subCalls, addsBefore)
	})
}

// requireReseedLogSubscription scripts outcome, runs one ReseedLogSubscription
// and requires the adds, error and state outcome describes.
func requireReseedLogSubscription(
	t *testing.T,
	fixture *catchupTestStreams,
	c *catchupControl,
	outcome reseedAddOutcome,
	cause error,
) {
	t.Helper()
	if outcome.arrange != nil {
		outcome.arrange(fixture.adder, cause)
	}
	oldSub := fixture.streams.Subscription()
	addsBefore := len(fixture.adder.subCalls)

	err := c.ReseedLogSubscription()
	if outcome.wantErr {
		require.ErrorIs(t, err, cause)
	} else {
		require.NoError(t, err)
	}
	require.Len(t, fixture.adder.subCalls, addsBefore+outcome.wantAdds)
	require.True(t, oldSub.IsClosed())
	requireLogStreamsReseeded(t, fixture, outcome.wantSub)
}

// Asserts RemoveDestination error keeps the catchup field for retry.
func TestCatchupControl_removeDestinationErrorKeepsField(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	c := newTestCatchupControl(t, fixture, &fakeReplayControl{})

	want := c.EnsureCatchupLogDestination(testCatchupEndpoint)
	require.NotEmpty(t, want)

	fixture.adder.lastDestHook.RemoveErr = errors.New("remove refused")
	err := c.RemoveCatchupLogDestination()
	require.ErrorContains(t, err, "remove refused")
	require.Equal(t, want, fixture.agent.CatchupLogDestination())
	require.Empty(t, fixture.agent.LiveLogDestination())

	// Retry after the remove path is healthy.
	fixture.adder.lastDestHook.RemoveErr = nil
	require.NoError(t, c.RemoveCatchupLogDestination())
	require.Empty(t, fixture.agent.CatchupLogDestination())
	require.Equal(t, fixture.liveURI, fixture.agent.LiveLogDestination())
}

// Asserts ReseedSubscription rejects a nil correlation allocator before
// closing the current subscription.
func TestLogStreams_reseedSubscription_nilCorrelationID(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	subBefore := fixture.streams.Subscription()
	joinBefore := fixture.agent.LogJoinSource()
	fixture.streams.nextCorrelationID = nil

	require.EqualError(
		t,
		fixture.streams.ReseedSubscription(fixture.adder),
		"cluster: correlation id allocator is required",
	)
	require.Same(t, subBefore, fixture.streams.Subscription())
	require.False(t, subBefore.IsClosed())
	require.Same(t, joinBefore, fixture.agent.LogJoinSource())
}

// Asserts catchup remove succeeds but live restore AddDestination failure
// leaves catchup cleared, live field empty, and returns the restore error.
func TestCatchupControl_removeRestoresLiveAddFailure(t *testing.T) {
	t.Parallel()

	fixture := openCatchupTestStreams(t)
	c := newTestCatchupControl(t, fixture, &fakeReplayControl{})

	want := c.EnsureCatchupLogDestination(testCatchupEndpoint)
	require.NotEmpty(t, want)
	require.Empty(t, fixture.agent.LiveLogDestination())

	// Catchup remove succeeds. Next AddDestination (live restore) fails once.
	fixture.adder.lastDestHook.FailNextAddErr = errors.New("live restore refused")
	err := c.RemoveCatchupLogDestination()
	require.ErrorContains(t, err, "live restore refused")
	require.Empty(t, fixture.agent.CatchupLogDestination())
	require.Empty(t, fixture.agent.LiveLogDestination())
	require.Equal(t, []string{want}, fixture.adder.lastDestHook.Removes)
	require.Empty(t, currentDestinations(fixture.adder.lastDestHook))

	// Retry restore only. Catchup is already cleared.
	require.NoError(t, c.RemoveCatchupLogDestination())
	require.Equal(t, fixture.liveURI, fixture.agent.LiveLogDestination())
	require.Equal(t, []string{fixture.liveURI}, currentDestinations(fixture.adder.lastDestHook))
}

// currentDestinations returns destinations still attached after recorded
// add/remove pairs on a single reseeded subscription hook.
func currentDestinations(hook *aeron.RecordingReceiveDestinationHook) []string {
	if hook == nil {
		return nil
	}
	present := make(map[string]int, len(hook.Adds))
	order := make([]string, 0, len(hook.Adds))
	for _, uri := range hook.Adds {
		if present[uri] == 0 {
			order = append(order, uri)
		}
		present[uri]++
	}
	for _, uri := range hook.Removes {
		present[uri]--
	}
	out := make([]string, 0, len(order))
	for _, uri := range order {
		if present[uri] > 0 {
			out = append(out, uri)
		}
	}
	return out
}
