//go:build integration

package integration_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/integration/harness"
)

// sessionMsg is a handler-visible SessionMessage capture.
type sessionMsg struct {
	clusterSessionID int64
	timestamp        int64
}

// nltMsg is a handler-visible NewLeadershipTermEvent capture.
//
// LogAdapterHandler omits LeaderMemberID and LogSessionID.
type nltMsg struct {
	leadershipTermID    int64
	logPosition         int64
	timestamp           int64
	termBaseLogPosition int64
	timeUnit            clusterwire.ClusterTimeUnit
	appVersion          int32
}

// recordingLogHandler implements consensus.LogAdapterHandler for spy asserts.
type recordingLogHandler struct {
	sessions []sessionMsg
	nlts     []nltMsg
}

func (h *recordingLogHandler) SessionMessage(clusterSessionID, timestamp int64) {
	h.sessions = append(h.sessions, sessionMsg{
		clusterSessionID: clusterSessionID,
		timestamp:        timestamp,
	})
}

func (h *recordingLogHandler) NewLeadershipTermEvent(
	leadershipTermID int64,
	logPosition int64,
	timestamp int64,
	termBaseLogPosition int64,
	timeUnit clusterwire.ClusterTimeUnit,
	appVersion int32,
) {
	h.nlts = append(h.nlts, nltMsg{
		leadershipTermID:    leadershipTermID,
		logPosition:         logPosition,
		timestamp:           timestamp,
		termBaseLogPosition: termBaseLogPosition,
		timeUnit:            timeUnit,
		appVersion:          appVersion,
	})
}

// connectExclusiveIPC opens a SharedParallel exclusive IPC pub/sub pair.
func connectExclusiveIPC(
	t *testing.T,
	streamID int32,
) (
	md *harness.MediaDriver,
	client *aeron.Client,
	hadClientError *atomic.Bool,
	pub *aeron.ExclusivePublication,
	sub *aeron.Subscription,
) {
	t.Helper()

	md = harness.SharedParallel(t)
	client, hadClientError = harness.ConnectIPCClient(t, md)
	pub, sub = harness.ConnectExclusiveIPCPubSubClients(t, client, client, streamID)
	return md, client, hadClientError, pub, sub
}

// newLogPublisher builds a LogPublisher bound to pub.
func newLogPublisher(pub *aeron.ExclusivePublication) *consensus.LogPublisher {
	lp := consensus.NewLogPublisher()
	lp.SetPublication(pub)
	return lp
}

// bindRecordingLogAdapter wires LogAdapter to image 0 with a recording spy.
func bindRecordingLogAdapter(
	t testing.TB,
	sub *aeron.Subscription,
) (*consensus.LogAdapter, *recordingLogHandler) {
	t.Helper()

	spy := &recordingLogHandler{}
	adapter := consensus.NewLogAdapter(0)
	adapter.SetImage(harness.RequireFirstImage(t, sub))
	adapter.SetHandler(spy)
	return adapter, spy
}

// waitLogAdapterPoll pumps DoWork and adapter.Poll until spy has recorded
// wantSessions session messages and wantNLTs new leadership term events, or
// until timeout.
func waitLogAdapterPoll(
	t testing.TB,
	client *aeron.Client,
	hadClientError *atomic.Bool,
	adapter *consensus.LogAdapter,
	bound int64,
	spy *recordingLogHandler,
	wantSessions int,
	wantNLTs int,
) {
	t.Helper()

	harness.PollClientUntilTransportClean(
		t,
		client,
		hadClientError,
		harness.ConnectedPollTimeout,
		"client transport error during log adapter poll",
		func() bool {
			adapter.Poll(bound)
			return len(spy.sessions) >= wantSessions && len(spy.nlts) >= wantNLTs
		},
		func() string {
			return fmt.Sprintf(
				"log adapter poll condition not met within %s: bound=%d position=%d sessions=%d nlts=%d",
				harness.ConnectedPollTimeout,
				bound,
				adapter.Position(),
				len(spy.sessions),
				len(spy.nlts),
			)
		},
	)
}

func requireSessionAt(
	t testing.TB,
	spy *recordingLogHandler,
	i int,
	clusterSessionID, timestamp int64,
) {
	t.Helper()

	require.Greater(t, len(spy.sessions), i)
	require.Equal(t, clusterSessionID, spy.sessions[i].clusterSessionID)
	require.Equal(t, timestamp, spy.sessions[i].timestamp)
}

// TestLogCommit_AppendMessageAndNLTRoundtrip proves LogPublisher SessionMessage
// and NewLeadershipTermEvent roundtrip through LogAdapter on live exclusive IPC.
func TestLogCommit_AppendMessageAndNLTRoundtrip(t *testing.T) {
	md, client, hadClientError, pub, sub := connectExclusiveIPC(
		t, harness.LogCommitAppendNLTStreamID,
	)
	lp := newLogPublisher(pub)
	adapter, spy := bindRecordingLogAdapter(t, sub)

	const (
		leadershipTermID int64 = 11
		clusterSessionID int64 = 22
		sessionTimestamp int64 = 33
		nltTimestamp     int64 = 1001
		termBaseLogPos   int64 = 42
		appVersion       int32 = 1
	)
	payload := []byte("append-nlt")

	pos0 := lp.Position()
	appendPos := lp.AppendMessage(leadershipTermID, clusterSessionID, sessionTimestamp, payload)
	require.Positive(t, appendPos)
	require.Greater(t, appendPos, pos0)

	require.True(t, lp.AppendNewLeadershipTermEvent(
		leadershipTermID,
		nltTimestamp,
		termBaseLogPos,
		3, // leaderMemberID (handler omits)
		9, // logSessionID (handler omits)
		clusterwire.ClusterTimeUnit_NANOS,
		appVersion,
	))

	endPos := lp.Position()
	require.Greater(t, endPos, appendPos)

	waitLogAdapterPoll(t, client, hadClientError, adapter, endPos, spy, 1, 1)

	require.Len(t, spy.sessions, 1)
	requireSessionAt(t, spy, 0, clusterSessionID, sessionTimestamp)

	require.Len(t, spy.nlts, 1)
	nlt := spy.nlts[0]
	require.Equal(t, leadershipTermID, nlt.leadershipTermID)

	// LogPosition on wire is claim end (publication position after NLT).
	require.Equal(t, endPos, nlt.logPosition)
	require.Equal(t, nltTimestamp, nlt.timestamp)
	require.Equal(t, termBaseLogPos, nlt.termBaseLogPosition)
	require.Equal(t, clusterwire.ClusterTimeUnit_NANOS, nlt.timeUnit)
	require.Equal(t, appVersion, nlt.appVersion)

	require.Equal(t, endPos, adapter.Position())
	finishPubSubIPCTest(t, md, hadClientError)
}

// TestLogCommit_BoundIsolationLive proves LogAdapter.Poll delivers only frames
// before the bound and rests at frame-boundary offer positions on live IPC.
func TestLogCommit_BoundIsolationLive(t *testing.T) {
	md, client, hadClientError, pub, sub := connectExclusiveIPC(
		t, harness.LogCommitBoundIsolationStreamID,
	)
	lp := newLogPublisher(pub)

	const (
		termID     int64 = 1
		sessionID  int64 = 10
		timestamp1 int64 = 100
		timestamp2 int64 = 200
	)

	mid := lp.AppendMessage(termID, sessionID, timestamp1, []byte("bound-msg-one"))
	require.Positive(t, mid)

	end := lp.AppendMessage(termID, sessionID, timestamp2, []byte("bound-msg-two"))
	require.Positive(t, end)
	require.Greater(t, end, mid)

	adapter, spy := bindRecordingLogAdapter(t, sub)

	// Bound at first-frame end: second frame starts at mid (start >= bound).
	waitLogAdapterPoll(t, client, hadClientError, adapter, mid, spy, 1, 0)
	require.Len(t, spy.sessions, 1)
	requireSessionAt(t, spy, 0, sessionID, timestamp1)
	require.Equal(t, mid, adapter.Position())

	// Second poll drains remaining frame.
	waitLogAdapterPoll(t, client, hadClientError, adapter, end, spy, 2, 0)
	require.Len(t, spy.sessions, 2)
	requireSessionAt(t, spy, 1, sessionID, timestamp2)
	require.Equal(t, end, adapter.Position())

	finishPubSubIPCTest(t, md, hadClientError)
}

// TestLogCommit_AeronPublisherCommitPosition proves single-peer CommitPosition
// TryClaim advances position and delivers a decodable frame on live exclusive IPC.
func TestLogCommit_AeronPublisherCommitPosition(t *testing.T) {
	md, client, hadClientError, pub, sub := connectExclusiveIPC(
		t, harness.LogCommitAeronPublisherStreamID,
	)

	const peerID int32 = 1
	ap := election.NewAeronPublisher()
	ap.SetPublication(peerID, pub)
	require.Same(t, pub, ap.Publication(peerID))

	fields := codecs.CommitPositionFields{
		LeadershipTermID: 7,
		LogPosition:      99,
		LeaderMemberID:   3,
	}

	pos0 := pub.Position()
	ap.CommitPosition(election.PublicationTarget{MemberID: peerID}, fields)

	require.Greater(t, pub.Position(), pos0, "CommitPosition must advance publication position")

	received := harness.PollDelivered(t, client, sub, 1)
	require.Len(t, received, 1)

	decoded, ok := codecs.TryDecodeCommitPositionAt(received[0], 0)
	require.True(t, ok)
	require.Equal(t, fields, decoded)

	finishPubSubIPCTest(t, md, hadClientError)
}
