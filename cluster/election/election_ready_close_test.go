package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// javaDefaultAppVersion matches Java ConsensusModule.Context default appVersion
// (SemanticVersion.compose(0, 0, 1)).
const javaDefaultAppVersion int32 = 1

func TestElection_shouldBaseStartupValueOnLeader(t *testing.T) {
	t.Parallel()

	const (
		leadershipTermID  int64 = 0
		logPosition       int64 = 0
		leaderRecordingID int64 = 367234
		commitPosition    int64 = 1024
	)

	for _, tc := range []struct {
		name          string
		isLeaderStart bool
		isNodeStart   bool
	}{
		{"leader and node startup", true, true},
		{"leader startup only", true, false},
		{"neither startup", false, false},
		{"node startup only", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := newElectionFixture(
				t,
				withMemberID(1),
				withNodeStartup(tc.isNodeStart),
				withPositions(leadershipTermID, logPosition, logPosition),
			)
			fx.host.followerLogJoinPosition = logPosition

			fx.clock.nowNs = 1
			require.Equal(t, 1, fx.doWork())
			require.Equal(t, election.ElectionStateCanvass, fx.election.State())

			fx.advance(1)
			isStartup := clusterwire.BooleanType_FALSE
			if tc.isLeaderStart {
				isStartup = clusterwire.BooleanType_TRUE
			}
			fx.election.OnNewLeadershipTerm(codecs.NewLeadershipTermFields{
				LogLeadershipTermID:     leadershipTermID,
				NextLeadershipTermID:    election.NullValue,
				NextTermBaseLogPosition: election.NullPosition,
				NextLogPosition:         election.NullPosition,
				LeadershipTermID:        leadershipTermID,
				TermBaseLogPosition:     logPosition,
				LogPosition:             logPosition,
				CommitPosition:          commitPosition,
				LeaderRecordingID:       leaderRecordingID,
				LeaderMemberID:          0,
				LogSessionID:            0,
				IsStartup:               isStartup,
			})

			// installLeadershipTerm routes synchronously; ElectionTest oracle
			// expects the first DoWork below after NLT install.
			require.Equal(t, election.ElectionStateFollowerReplay, fx.election.State())
			require.Equal(t, tc.isLeaderStart, election.IsLeaderStartupForTest(fx.election))

			fx.advance(1)
			require.Equal(t, 1, fx.doWork())
			require.Equal(t, election.ElectionStateFollowerLogInit, fx.election.State())

			fx.advance(1)
			require.Equal(t, 1, fx.doWork())
			require.Equal(t, election.ElectionStateFollowerLogAwait, fx.election.State())

			fx.advance(1)
			timeAtFollowerLogAwaitNs := fx.clock.nowNs
			require.Equal(t, 1, fx.doWork())
			require.Equal(t, []int32{0}, fx.host.followerLogJoinReadyCalls)
			require.Equal(t, 1, fx.agent.joinFollowerCalls)
			require.Len(t, fx.agent.joinFollowerParams, 1)
			require.Equal(t, timeAtFollowerLogAwaitNs, fx.agent.joinFollowerNowNs[0])
			require.Equal(t, election.ElectionStateFollowerReady, fx.election.State())

			params := fx.agent.joinFollowerParams[0]
			require.Equal(t, "log=test", params.LogChannel)
			require.Equal(t, int32(100), params.LogStreamID)
			require.Equal(t, int32(0), params.LogSessionID)
			require.Equal(t, logPosition, params.LogJoinPosition)
			require.Equal(
				t,
				tc.isLeaderStart,
				params.IsLeaderStartup,
				"IsLeaderStartup follows leader NLT flag, not host stub or node startup",
			)
		})
	}
}

func TestElection_shouldThrowNonZeroLogPositionAndNullRecordingIdSpecified(t *testing.T) {
	t.Parallel()

	// Oracle rows for NullValue recording id; broader paths live in
	// TestEnsureRecordingLogCoherent_nullRecordingIDSkips.
	spy := &recordingLogCoherencySpy{}

	require.NoError(t, election.EnsureRecordingLogCoherent(
		spy,
		recordinglog.EnsureCoherentParams{RecordingID: election.NullValue, LogPosition: 0},
	))
	require.NoError(t, election.EnsureRecordingLogCoherent(
		spy,
		recordinglog.EnsureCoherentParams{RecordingID: election.NullValue, LogPosition: 1000},
	))
	require.Empty(t, spy.calls)
}

func TestElection_shouldSendCommitPositionAndNewLeadershipTermEventsWithTheSameLeadershipTerm(
	t *testing.T,
) {
	t.Parallel()

	const (
		logPosition      int64 = 100
		leadershipTermID int64 = 42
		quorumPosition1  int64 = 1024 * 1024
		quorumPosition2  int64 = 2 * 1024 * 1024
		logSessionID     int32 = 5
		logRecordingID   int64 = 842384023
	)

	fx := newElectionFixture(
		t,
		withMemberID(1),
		withNodeStartup(true),
		withPositions(leadershipTermID, logPosition, logPosition),
		withAppVersion(javaDefaultAppVersion),
	)
	fx.host.localLogRecordingID = logRecordingID

	fx.host.quorumSequence = []int64{quorumPosition1, quorumPosition2}

	fx.clock.nowNs = 1
	election.SetStateForTest(fx.election, election.ElectionStateLeaderLogReplication)
	election.SetLogSessionIDForTest(fx.election, logSessionID)
	fx.publisher.Reset()

	fx.advance(1)
	replicationEpochMs := fx.epochClock.nowMs
	require.Equal(t, 3, fx.doWork())
	require.Equal(t, election.ElectionStateLeaderReplay, fx.election.State())

	requireNewLeadershipTermBatch(
		t, fx.publisher, leadershipTermID, logPosition, quorumPosition1,
		logRecordingID, logSessionID, javaDefaultAppVersion, replicationEpochMs,
		clusterwire.BooleanType_FALSE,
	)
	require.Equal(t, []publishCommitPositionCall{
		{commitPosition: quorumPosition1, leadershipTermID: leadershipTermID},
	}, fx.host.publishCommitPositionCalls)

	fx.advance(1)
	replayEpochMs := fx.epochClock.nowMs
	fx.publisher.Reset()
	fx.host.publishCommitPositionCalls = nil

	require.Equal(t, 3, fx.doWork())
	require.Equal(t, election.ElectionStateLeaderInit, fx.election.State())

	requireNewLeadershipTermBatch(
		t, fx.publisher, leadershipTermID, logPosition, quorumPosition2,
		logRecordingID, logSessionID, javaDefaultAppVersion, replayEpochMs,
		clusterwire.BooleanType_TRUE,
	)
	require.Equal(t, []publishCommitPositionCall{
		{commitPosition: quorumPosition2, leadershipTermID: leadershipTermID},
	}, fx.host.publishCommitPositionCalls)
}

func requireNewLeadershipTermBatch(
	t *testing.T,
	pub *election.SbePublisher,
	leadershipTermID, logPosition, commitPosition, leaderRecordingID int64,
	logSessionID, appVersion int32,
	timestamp int64,
	isStartup clusterwire.BooleanType,
) {
	t.Helper()

	require.Len(t, pub.Sent, 2)

	targets := map[int32]struct{}{}
	for _, msg := range pub.Sent {
		targets[msg.Dst.MemberID] = struct{}{}
		assertNewLeadershipTermFields(
			t, msg, leadershipTermID, logPosition, commitPosition,
			leaderRecordingID, logSessionID, appVersion, timestamp, isStartup,
		)
	}
	require.Equal(t, map[int32]struct{}{0: {}, 2: {}}, targets)
}

func assertNewLeadershipTermFields(
	t *testing.T,
	msg election.SentMessage,
	leadershipTermID, logPosition, commitPosition, leaderRecordingID int64,
	logSessionID, appVersion int32,
	timestamp int64,
	isStartup clusterwire.BooleanType,
) {
	t.Helper()

	require.Equal(t, uint16(clusterwire.NewLeadershipTermSbeTemplateID), msg.TemplateID)
	fields := msg.Fields.(codecs.NewLeadershipTermFields)
	require.Equal(t, leadershipTermID, fields.LogLeadershipTermID)
	require.Equal(t, leadershipTermID, fields.NextLeadershipTermID)
	require.Equal(t, logPosition, fields.NextTermBaseLogPosition)
	require.Equal(t, election.NullPosition, fields.NextLogPosition)
	require.Equal(t, leadershipTermID, fields.LeadershipTermID)
	require.Equal(t, logPosition, fields.TermBaseLogPosition)
	require.Equal(t, logPosition, fields.LogPosition)
	require.Equal(t, commitPosition, fields.CommitPosition)
	require.Equal(t, leaderRecordingID, fields.LeaderRecordingID)
	require.Equal(t, timestamp, fields.Timestamp)
	require.Equal(t, int32(1), fields.LeaderMemberID)
	require.Equal(t, logSessionID, fields.LogSessionID)
	require.Equal(t, appVersion, fields.AppVersion)
	require.Equal(t, isStartup, fields.IsStartup)
}
