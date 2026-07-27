package election_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func TestHandleConsensusMessage_roundtrip(t *testing.T) {
	t.Parallel()

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{ActingVersion: clusterwire.CanvassPositionSbeSchemaVersion}

	cases := []struct {
		name    string
		publish func()
		spy     func(*testing.T, *recordingNodeState, *FakeEpochClock)
	}{
		{
			name: "CanvassPosition",
			publish: func() {
				pub.CanvassPosition(
					dst,
					codecs.CanvassPositionFields{
						LogLeadershipTermID: 1,
						LogPosition:         2,
						LeadershipTermID:    3,
						FollowerMemberID:    4,
					},
				)
			},
		},
		{
			name: "RequestVote",
			publish: func() {
				pub.RequestVote(
					dst,
					codecs.RequestVoteFields{
						LogLeadershipTermID: 10,
						LogPosition:         20,
						CandidateTermID:     30,
						CandidateMemberID:   40,
					},
				)
			},
			spy: func(t *testing.T, nodeState *recordingNodeState, epochClock *FakeEpochClock) {
				t.Helper()
				require.Len(t, nodeState.proposeCalls, 1)
				require.Equal(t, int64(30), nodeState.proposeCalls[0].candidateTermID)
				require.Equal(t, int64(20), nodeState.proposeCalls[0].logPosition)
				require.Equal(t, epochClock.nowMs, nodeState.proposeCalls[0].timestampMs)
			},
		},
		{
			name: "Vote",
			publish: func() {
				pub.PlaceVote(
					dst,
					codecs.VoteFields{
						CandidateTermID:     1,
						LogLeadershipTermID: 2,
						LogPosition:         3,
						CandidateMemberID:   4,
						FollowerMemberID:    5,
						Vote:                clusterwire.BooleanType_TRUE,
					},
				)
			},
		},
		{
			name: "NewLeadershipTerm",
			publish: func() {
				pub.NewLeadershipTerm(
					dst,
					codecs.NewLeadershipTermFields{
						LogLeadershipTermID:  1,
						NextLeadershipTermID: 2,
						LeaderMemberID:       3,
					},
				)
			},
		},
		{
			name: "AppendPosition",
			publish: func() {
				pub.AppendPosition(
					dst,
					codecs.AppendPositionFields{
						LeadershipTermID: 7,
						LogPosition:      8,
						FollowerMemberID: 9,
						Flags:            0x01,
					},
				)
			},
		},
		{
			name: "CommitPosition",
			publish: func() {
				pub.CommitPosition(
					dst,
					codecs.CommitPositionFields{
						LeadershipTermID: 11,
						LogPosition:      12,
						LeaderMemberID:   13,
					},
				)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, deps, _, epochClock, nodeState := testElectionDeps(t)
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, election.ElectionStateCanvass)

			pub.Reset()
			tc.publish()
			require.Len(t, pub.Sent, 1)
			require.True(t, e.HandleConsensusMessage(pub.Sent[0].Wire, 0))
			if tc.spy != nil {
				tc.spy(t, nodeState, epochClock)
			}
		})
	}
}

func TestHandleConsensusMessage_rejectsBadInput(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCanvass)

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{ActingVersion: clusterwire.CanvassPositionSbeSchemaVersion}
	pub.CanvassPosition(
		dst,
		codecs.CanvassPositionFields{
			LogLeadershipTermID: 1,
			LogPosition:         2,
			LeadershipTermID:    3,
			FollowerMemberID:    4,
		},
	)
	wire := pub.Sent[0].Wire

	t.Run("bad schema id", func(t *testing.T) {
		bad := append([]byte(nil), wire...)
		bad[4] = 0
		bad[5] = 0
		require.False(t, e.HandleConsensusMessage(bad, 0))
	})

	t.Run("truncated buffer", func(t *testing.T) {
		require.False(t, e.HandleConsensusMessage(wire[:4], 0))
	})

	t.Run("truncated body", func(t *testing.T) {
		headerLen := codecs.MessageHeaderEncodedLen()
		require.Greater(t, len(wire), headerLen+4)
		require.False(t, e.HandleConsensusMessage(wire[:headerLen+4], 0))
	})

	t.Run("unknown template", func(t *testing.T) {
		bad := append([]byte(nil), wire...)
		bad[2] = 0xFF
		bad[3] = 0xFF
		require.False(t, e.HandleConsensusMessage(bad, 0))
	})
}

func TestHandleConsensusMessage_gatedInInit(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, nodeState := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{ActingVersion: clusterwire.CanvassPositionSbeSchemaVersion}
	pub.RequestVote(
		dst,
		codecs.RequestVoteFields{
			LogLeadershipTermID: 10,
			LogPosition:         20,
			CandidateTermID:     30,
			CandidateMemberID:   40,
		},
	)
	require.Len(t, pub.Sent, 1)
	require.True(t, e.HandleConsensusMessage(pub.Sent[0].Wire, 0))
	require.Empty(t, nodeState.proposeCalls)
}

func TestElection_HandleError_resetsToInit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"non-nil error", errors.New("test error")},
		{"nil error", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, _, _, _ := testElectionDeps(t)
			observer := &recordingStateObserver{}
			deps.StateObserver = observer
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, election.ElectionStateCanvass)

			e.HandleError(5_000, tc.err)

			require.Equal(t, election.ElectionStateInit, e.State())
			require.Equal(t, []election.ElectionState{
				election.ElectionStateInit,
				election.ElectionStateInit,
			}, observer.states)
		})
	}
}

func TestOnRequestVote_branches(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		fields       codecs.RequestVoteFields
		wantPropose  bool
		wantVoteSent bool
		wantVote     clusterwire.BooleanType
		wantState    election.ElectionState
	}{
		{
			name: "self candidate ignored",
			fields: codecs.RequestVoteFields{
				CandidateTermID:   99,
				LogPosition:       100,
				CandidateMemberID: 0,
			},
			wantState: election.ElectionStateCanvass,
		},
		{
			name: "ahead of candidate log places false vote",
			fields: codecs.RequestVoteFields{
				CandidateTermID:     99,
				LogLeadershipTermID: 0,
				LogPosition:         100,
				CandidateMemberID:   1,
			},
			wantPropose:  true,
			wantVoteSent: true,
			wantVote:     clusterwire.BooleanType_FALSE,
			wantState:    election.ElectionStateCanvass,
		},
		{
			name: "grant enters follower ballot",
			fields: codecs.RequestVoteFields{
				CandidateTermID:     99,
				LogLeadershipTermID: 40,
				LogPosition:         900,
				CandidateMemberID:   1,
			},
			wantPropose:  true,
			wantVoteSent: true,
			wantVote:     clusterwire.BooleanType_TRUE,
			wantState:    election.ElectionStateFollowerBallot,
		},
		{
			name: "stale term places false vote",
			fields: codecs.RequestVoteFields{
				CandidateTermID:   4,
				LogPosition:       100,
				CandidateMemberID: 1,
			},
			wantVoteSent: true,
			wantVote:     clusterwire.BooleanType_FALSE,
			wantState:    election.ElectionStateCanvass,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, _, epochClock, nodeState := testElectionDeps(t)
			pub := deps.Publisher.(*election.SbePublisher)
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, election.ElectionStateCanvass)

			e.OnRequestVote(tc.fields)

			require.Equal(t, tc.wantState, e.State())
			if tc.wantPropose {
				require.Len(t, nodeState.proposeCalls, 1)
				require.Equal(t, tc.fields.CandidateTermID, nodeState.proposeCalls[0].candidateTermID)
				require.Equal(t, tc.fields.LogPosition, nodeState.proposeCalls[0].logPosition)
				require.Equal(t, epochClock.nowMs, nodeState.proposeCalls[0].timestampMs)
			} else {
				require.Empty(t, nodeState.proposeCalls)
			}
			if !tc.wantVoteSent {
				require.Empty(t, pub.Sent)
				return
			}
			require.Len(t, pub.Sent, 1)
			require.Equal(t, uint16(clusterwire.VoteSbeTemplateID), pub.Sent[0].TemplateID)
			vote := pub.Sent[0].Fields.(codecs.VoteFields)
			require.Equal(t, tc.wantVote, vote.Vote)
			require.Equal(t, e.AppendPosition(), vote.LogPosition)
		})
	}
}

func TestHandleNominate_deadlineBecomesCandidate(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, nodeState := testElectionDeps(t)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateNominate)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCandidateBallot, e.State())
	require.Len(t, nodeState.proposeCalls, 1)
	require.Equal(t, cfg.LeadershipTermID+1, nodeState.proposeCalls[0].candidateTermID)
}

func TestHandleNominate_proposeErrorResetsToInit(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, nodeState := testElectionDeps(t)
	deps.NodeState = &failingNodeState{store: nodeState, err: errors.New("sync failed")}
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateNominate)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Equal(t, cfg.LeadershipTermID, election.CandidateTermIDForTest(e))
}

func TestOnRequestVote_proposeErrorResetsToInit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		fields codecs.RequestVoteFields
	}{
		{
			name: "ahead of candidate log",
			fields: codecs.RequestVoteFields{
				CandidateTermID:     99,
				LogLeadershipTermID: 0,
				LogPosition:         100,
				CandidateMemberID:   1,
			},
		},
		{
			name: "grant path",
			fields: codecs.RequestVoteFields{
				CandidateTermID:     99,
				LogLeadershipTermID: 40,
				LogPosition:         900,
				CandidateMemberID:   1,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, _, _, nodeState := testElectionDeps(t)
			deps.NodeState = &failingNodeState{store: nodeState, err: errors.New("sync failed")}
			pub := deps.Publisher.(*election.SbePublisher)
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, election.ElectionStateCanvass)

			e.OnRequestVote(tc.fields)

			require.Equal(t, election.ElectionStateInit, e.State())
			require.Equal(t, cfg.LeadershipTermID, election.CandidateTermIDForTest(e))
			require.Empty(t, pub.Sent)
		})
	}
}

func TestHandleInit_singleNodeUpdateErrorStaysInit(t *testing.T) {
	t.Parallel()

	cfg, deps := singleNodeElectionDeps(t)
	deps.NodeState = &failingNodeState{store: deps.NodeState, err: errors.New("sync failed")}
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(1_000_000_000))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Nil(t, e.Leader())
	require.Equal(t, cfg.LeadershipTermID, e.LeadershipTermID())
}

func TestHandleNominate_republishesBeforeDeadline(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, nodeState := testElectionDeps(t)
	pub := deps.Publisher.(*election.SbePublisher)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateNominate)
	election.SetNominationDeadlineNsForTest(e, clock.nowNs+cfg.ElectionTimeoutNs)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateNominate, e.State())
	require.Empty(t, nodeState.proposeCalls)
	require.Len(t, pub.Sent, 2)
	for _, m := range pub.Sent {
		require.Equal(t, uint16(clusterwire.CanvassPositionSbeTemplateID), m.TemplateID)
	}
}

func TestHandleFollowerBallot_timeout(t *testing.T) {
	t.Parallel()

	timeoutNs := election.ElectionTestConfig().ElectionTimeoutNs
	for _, tc := range []struct {
		name    string
		deltaNs int64
		want    election.ElectionState
		work    int
	}{
		{"just before timeout", timeoutNs - 1, election.ElectionStateFollowerBallot, 0},
		{"at timeout", timeoutNs, election.ElectionStateCanvass, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, election.ElectionStateFollowerBallot)

			require.Equal(t, tc.work, e.DoWork(clock.nowNs+tc.deltaNs))
			require.Equal(t, tc.want, e.State())
		})
	}
}

func TestHandleCandidateBallot_decisionBranches(t *testing.T) {
	t.Parallel()

	timeoutNs := election.ElectionTestConfig().ElectionTimeoutNs
	for _, tc := range []struct {
		name    string
		votes   map[int32]bool
		deltaNs int64
		want    election.ElectionState
	}{
		{
			"unanimous wins immediately",
			map[int32]bool{0: true, 1: true, 2: true},
			0,
			election.ElectionStateLeaderLogReplication,
		},
		{
			"quorum at deadline wins",
			map[int32]bool{0: true, 1: true},
			timeoutNs,
			election.ElectionStateLeaderLogReplication,
		},
		{
			"quorum fail at deadline recanvasses",
			map[int32]bool{0: true},
			timeoutNs,
			election.ElectionStateCanvass,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, election.ElectionStateCandidateBallot)

			for _, m := range election.MembersForTest(e) {
				if yes, ok := tc.votes[m.ID]; ok {
					m.CandidateTermID = cfg.LeadershipTermID
					m.Vote = new(yes)
				}
			}

			e.DoWork(clock.nowNs + tc.deltaNs)
			require.Equal(t, tc.want, e.State())
		})
	}
}

// leaderLogReplicationFixture drives a 3-node election to LEADER_LOG_REPLICATION
// with a real log session id and quorumPosition holding below the append
// position so the state does not advance.
func leaderLogReplicationFixture(
	t *testing.T,
	quorumPosition int64,
	opts ...fixtureOption,
) *electionFixture {
	t.Helper()

	fx := newElectionFixture(t, opts...)
	fx.host.quorumPosition = quorumPosition
	fx.canvassTo(election.ElectionStateCandidateBallot)
	fx.injectVote(1, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition, true)
	fx.injectVote(2, fx.cfg.LeadershipTermID, fx.cfg.AppendPosition, true)
	fx.doWork()
	require.Equal(t, election.ElectionStateLeaderLogReplication, fx.election.State())
	fx.publisher.Reset()
	return fx
}

func TestHandleLeaderLogReplication_publishesOnLeaderHeartbeatInterval(t *testing.T) {
	t.Parallel()

	fx := leaderLogReplicationFixture(t, 0)

	// First cycle publishes NewLeadershipTerm and the commit position.
	require.Equal(t, 2, fx.doWork())
	require.Len(t, fx.publisher.Sent, 2)

	// Past the election status interval but inside the leader heartbeat
	// interval: no republish.
	fx.advance(fx.cfg.ElectionStatusIntervalNs)
	require.Equal(t, 0, fx.doWork())
	require.Len(t, fx.publisher.Sent, 2)

	fx.advance(fx.cfg.LeaderHeartbeatIntervalNs - fx.cfg.ElectionStatusIntervalNs)
	require.Equal(t, 2, fx.doWork())
	require.Len(t, fx.publisher.Sent, 4)
	for _, m := range fx.publisher.Sent {
		require.Equal(t, uint16(clusterwire.NewLeadershipTermSbeTemplateID), m.TemplateID)
	}
}

func TestPublishNewLeadershipTerm_commitPositionField(t *testing.T) {
	t.Parallel()

	const quorumPosition int64 = 123
	const recordingID int64 = 842384023
	const appVersion int32 = -98

	fx := leaderLogReplicationFixture(t, quorumPosition, withAppVersion(appVersion))
	fx.host.localLogRecordingID = recordingID

	wantTimestamp := fx.epochClock.nowMs
	fx.doWork()

	require.Len(t, fx.publisher.Sent, 2)
	term := fx.election.LeadershipTermID()
	for _, m := range fx.publisher.Sent {
		require.Equal(t, uint16(clusterwire.NewLeadershipTermSbeTemplateID), m.TemplateID)
		fields := m.Fields.(codecs.NewLeadershipTermFields)
		require.Equal(t, quorumPosition, fields.CommitPosition)
		require.Equal(t, term, fields.LeadershipTermID)
		require.Equal(t, term, fields.NextLeadershipTermID)
		require.Equal(t, election.NullPosition, fields.NextLogPosition)
		require.Equal(t, fx.election.AppendPosition(), fields.NextTermBaseLogPosition)
		require.Equal(t, fx.election.AppendPosition(), fields.TermBaseLogPosition)
		require.Equal(t, fx.election.AppendPosition(), fields.LogPosition)
		require.Equal(t, recordingID, fields.LeaderRecordingID)
		require.Equal(t, wantTimestamp, fields.Timestamp)
		require.Equal(t, fx.election.ThisMemberID(), fields.LeaderMemberID)
		require.Equal(t, fixtureLogSessionID, fields.LogSessionID)
		require.Equal(t, appVersion, fields.AppVersion)
		require.Equal(t, clusterwire.BooleanType_FALSE, fields.IsStartup)
	}

	require.Equal(t, []publishCommitPositionCall{
		{commitPosition: quorumPosition, leadershipTermID: term},
	}, fx.host.publishCommitPositionCalls)
}

func TestOnAppendPosition_leaderRepublishesToLaggingFollower(t *testing.T) {
	t.Parallel()

	fx := leaderLogReplicationFixture(t, 0)
	laggingTerm := fx.election.LeadershipTermID() - 1

	fx.injectAppendPosition(1, laggingTerm, 50)

	require.Equal(t, laggingTerm, fx.member(1).LeadershipTermID)
	require.Equal(t, int64(50), fx.member(1).LogPosition)

	require.Len(t, fx.publisher.Sent, 1)
	sent := fx.publisher.Sent[0]
	require.Equal(t, uint16(clusterwire.NewLeadershipTermSbeTemplateID), sent.TemplateID)
	require.Equal(t, int32(1), sent.Dst.MemberID)
	fields := sent.Fields.(codecs.NewLeadershipTermFields)
	require.Equal(t, laggingTerm, fields.LogLeadershipTermID)
	require.Equal(t, fx.election.LeadershipTermID(), fields.LeadershipTermID)
}

func TestPublishCommitPositionOnInterval_quorumRegression(t *testing.T) {
	t.Parallel()

	fx := leaderLogReplicationFixture(t, 0)
	fx.host.quorumPosition = 123

	fx.doWork()
	term := fx.election.LeadershipTermID()
	require.Equal(t, []publishCommitPositionCall{
		{commitPosition: 123, leadershipTermID: term},
	}, fx.host.publishCommitPositionCalls)
	require.Len(t, fx.publisher.Sent, 2)

	// The quorum position regressed: the commit position is withheld even
	// after the heartbeat interval, while NewLeadershipTerm republishes.
	fx.host.quorumPosition = 50
	fx.advance(fx.cfg.LeaderHeartbeatIntervalNs)
	fx.doWork()
	require.Equal(t, []publishCommitPositionCall{
		{commitPosition: 123, leadershipTermID: term},
	}, fx.host.publishCommitPositionCalls)
	require.Len(t, fx.publisher.Sent, 4)
	for _, m := range fx.publisher.Sent {
		require.Equal(t, uint16(clusterwire.NewLeadershipTermSbeTemplateID), m.TemplateID)
	}
}

func TestHandleLeaderReplay_firstTickUpdatesThisMember(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = true
	cfg.AppendPosition = cfg.LogPosition
	handler := &recordingAgentHandler{}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateLeaderReplay)

	require.Positive(t, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderInit, e.State())

	member := election.MemberForTest(e, cfg.MemberID)
	require.Equal(t, cfg.LeadershipTermID, member.LeadershipTermID)
	require.Equal(t, cfg.AppendPosition, member.LogPosition)
	require.Equal(t, clock.nowNs, member.TimeOfLastAppendPositionNs)

	// isLeaderStartup reverts to the node's own startup flag on replay start;
	// the next cycle joins the log as leader with it.
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Len(t, handler.joinLeaderCalls, 1)
	require.True(t, handler.joinLeaderCalls[0].isStartup)
}

func TestHandleFollowerReplay_logPositionGteAppendToLogInit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		logPosition int64
	}{
		{"log position equals append", testAppendPosition},
		{"log position beyond append", testAppendPosition + 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			cfg.LogPosition = tc.logPosition
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, election.ElectionStateFollowerReplay)

			require.Equal(t, 1, e.DoWork(clock.nowNs))
			require.Equal(t, election.ElectionStateFollowerLogInit, e.State())
		})
	}
}

func TestHandleFollowerReplay_branches(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string

		// commitPosition rides the installing NewLeadershipTerm and becomes
		// the follower's notified commit position (baseline log 100, append 200).
		commitPosition int64
		wantStates     []election.ElectionState
		wantAppendSent bool
	}{
		{
			name:           "zero notified commit publishes append position",
			commitPosition: 0,
			wantStates: []election.ElectionState{
				election.ElectionStateFollowerReplay,
			},
			wantAppendSent: true,
		},
		{
			name:           "log at or past notified commit rejects replay",
			commitPosition: 80,
			wantStates: []election.ElectionState{
				election.ElectionStateCanvass,
			},
		},
		{
			name:           "replay to notified short of append recanvasses",
			commitPosition: 150,
			wantStates: []election.ElectionState{
				election.ElectionStateFollowerReplay,
				election.ElectionStateCanvass,
			},
		},
		{
			name:           "replay reaching append enters log init",
			commitPosition: 250,
			wantStates: []election.ElectionState{
				election.ElectionStateFollowerReplay,
				election.ElectionStateFollowerLogInit,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := newElectionFixture(t)
			fx.doWork()
			require.Equal(t, election.ElectionStateCanvass, fx.election.State())

			fields := installNewLeadershipTermFields(fx.cfg, 1, fx.cfg.LeadershipTermID)
			fields.CommitPosition = tc.commitPosition
			fx.election.OnNewLeadershipTerm(fields)
			require.Equal(t, election.ElectionStateFollowerReplay, fx.election.State())

			fx.publisher.Reset()
			for _, want := range tc.wantStates {
				fx.doWork()
				require.Equal(t, want, fx.election.State())
			}

			if !tc.wantAppendSent {
				require.Empty(t, fx.publisher.Sent)
				return
			}
			require.Len(t, fx.publisher.Sent, 1)
			sent := fx.publisher.Sent[0]
			require.Equal(t, uint16(clusterwire.AppendPositionSbeTemplateID), sent.TemplateID)
			require.Equal(t, int32(1), sent.Dst.MemberID)
			appended := sent.Fields.(codecs.AppendPositionFields)
			require.Equal(t, fx.cfg.AppendPosition, appended.LogPosition)
		})
	}
}

func TestHandleFollowerLogAwait_branches(t *testing.T) {
	t.Parallel()

	timeoutNs := election.ElectionTestConfig().LeaderHeartbeatTimeoutNs
	for _, tc := range []struct {
		name          string
		joinOK        bool
		joinPosition  int64
		joinAccepted  bool
		deltaNs       int64
		wantWork      int
		wantState     election.ElectionState
		wantJoinCalls int
	}{
		{
			name:         "image not ready before timeout waits",
			joinPosition: testLogPosition,
			wantState:    election.ElectionStateFollowerLogAwait,
		},
		{
			name:         "image not ready at timeout resets to init",
			joinPosition: testLogPosition,
			deltaNs:      timeoutNs,
			wantWork:     1,
			wantState:    election.ElectionStateInit,
		},
		{
			name:         "join position mismatch resets to init",
			joinOK:       true,
			joinPosition: testLogPosition + 1,
			wantWork:     1,
			wantState:    election.ElectionStateInit,
		},
		{
			name:          "join rejected before timeout waits",
			joinOK:        true,
			joinPosition:  testLogPosition,
			wantState:     election.ElectionStateFollowerLogAwait,
			wantJoinCalls: 1,
		},
		{
			name:          "join rejected at timeout resets to init",
			joinOK:        true,
			joinPosition:  testLogPosition,
			deltaNs:       timeoutNs,
			wantWork:      1,
			wantState:     election.ElectionStateInit,
			wantJoinCalls: 1,
		},
		{
			name:          "join accepted enters follower ready",
			joinOK:        true,
			joinPosition:  testLogPosition,
			joinAccepted:  true,
			wantWork:      1,
			wantState:     election.ElectionStateFollowerReady,
			wantJoinCalls: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			host := &recordingElectionHost{
				clock:                   clock,
				localLogRecordingID:     int64(election.NullValue),
				followerLogJoinOK:       tc.joinOK,
				followerLogJoinPosition: tc.joinPosition,
			}
			deps.Host = host
			handler := &recordingAgentHandler{joinFollowerReturn: tc.joinAccepted}
			deps.Handler = handler
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, election.ElectionStateFollowerLogAwait)

			require.Equal(t, tc.wantWork, e.DoWork(clock.nowNs+tc.deltaNs))
			require.Equal(t, tc.wantState, e.State())

			require.Len(t, handler.joinFollowerParams, tc.wantJoinCalls)
			if tc.wantJoinCalls == 0 {
				return
			}
			require.Equal(t, cfg.LogPosition, handler.joinFollowerParams[0].LogJoinPosition)

			// The host stub reports IsLeaderStartup true; the election must
			// overwrite it with its own false before the join.
			require.False(t, handler.joinFollowerParams[0].IsLeaderStartup)
		})
	}
}

func TestHandleFollowerLogInit_waitsForLogSession(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerLogInit)

	// No log session id has been installed, so the state holds.
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateFollowerLogInit, e.State())
}

func TestHandleFollowerCatchupInit_nearLiveSkipsToLogInit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		nearLive  bool
		wantState election.ElectionState
		wantWork  int
	}{
		{"near live skips to log init", true, election.ElectionStateFollowerLogInit, 1},
		{"not near live stalls", false, election.ElectionStateFollowerCatchupInit, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Log equals append so the install routes through the no-replay
			// branch; the leader's live log position ahead of the append
			// position becomes the catchup join position.
			fx := newElectionFixture(
				t,
				withPositions(testLeadershipTermID, testAppendPosition, testAppendPosition),
			)
			fx.host.catchupNearLive = tc.nearLive
			catchupJoinPosition := fx.cfg.AppendPosition + 100

			fx.doWork()
			require.Equal(t, election.ElectionStateCanvass, fx.election.State())

			fields := installNewLeadershipTermFields(fx.cfg, 1, fx.cfg.LeadershipTermID)
			fields.LogPosition = catchupJoinPosition
			fx.election.OnNewLeadershipTerm(fields)
			require.Equal(t, election.ElectionStateFollowerReplay, fx.election.State())

			fx.doWork()
			require.Equal(t, election.ElectionStateFollowerCatchupInit, fx.election.State())

			require.Equal(t, tc.wantWork, fx.doWork())
			require.Equal(t, tc.wantState, fx.election.State())
			require.Equal(t, []int64{catchupJoinPosition}, fx.host.catchupNearLiveCalls)

			// Neither branch calls catchup start on the host: near-live is
			// skip-to-log-init only, and stall ticks idle.
			require.Empty(t, fx.host.catchupInitiatedCalls)
		})
	}
}

func TestHandleFollowerCatchupInit_notifiedCommitFallback(t *testing.T) {
	t.Parallel()

	// Injected state only: the install path always sets catchupJoinPosition
	// before routing here, so the fallback to the notified commit position is
	// observable only when the state is entered directly.
	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{clock: clock}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerCatchupInit)

	require.Equal(t, 0, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateFollowerCatchupInit, e.State())
	require.Equal(t, []int64{0}, host.catchupNearLiveCalls)
	require.Empty(t, host.catchupInitiatedCalls)
}

func TestHandleFollowerCatchup_pollLimit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		commitPosition int64
		// LogPosition on install is AppendPosition + joinAhead.
		joinAhead  int64
		pollResult int
		// When true, want limit is AppendPosition+joinAhead; else wantLimit.
		wantJoinLimit bool
		wantLimit     int64
	}{
		{
			name:           "notified commit",
			commitPosition: 500,
			pollResult:     5,
			wantLimit:      500,
		},
		{
			name:           "zero commit falls back to join position",
			commitPosition: 0,
			joinAhead:      100,
			pollResult:     2,
			wantJoinLimit:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			host := &recordingElectionHost{clock: clock, catchupPollResult: tc.pollResult}
			deps.Host = host
			e := election.NewElection(cfg, deps)

			// Install so leader identity and commit bound are set, then inject catchup.
			election.SetStateForTest(e, election.ElectionStateCanvass)
			fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
			fields.CommitPosition = tc.commitPosition
			fields.LogPosition = cfg.AppendPosition + tc.joinAhead
			e.OnNewLeadershipTerm(fields)
			require.Equal(t, tc.commitPosition, e.NotifiedCommitPosition())

			election.SetStateForTest(e, election.ElectionStateFollowerCatchup)
			t0 := clock.nowNs
			require.Equal(t, tc.pollResult, e.DoWork(t0))
			require.Equal(t, election.ElectionStateFollowerCatchup, e.State())

			wantLimit := tc.wantLimit
			if tc.wantJoinLimit {
				wantLimit = cfg.AppendPosition + tc.joinAhead
			}
			require.Equal(t, []catchupPollCall{{
				limitPosition: wantLimit,
				nowNs:         t0,
			}}, host.catchupPollCalls)
		})
	}
}

func TestHandleFollowerCatchup_noImageReturnsZero(t *testing.T) {
	t.Parallel()

	// Default recording host poll result is 0 (no image -> zero work).
	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{clock: clock}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerCatchup)

	require.Equal(t, 0, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateFollowerCatchup, e.State())
	require.Len(t, host.catchupPollCalls, 1)

	// Both bounds unset: notified 0 falls back to NullPosition join.
	require.Equal(t, int64(election.NullPosition), host.catchupPollCalls[0].limitPosition)
}

func TestEnsureRecordingLogCoherent_nullRecordingIDSkips(t *testing.T) {
	t.Parallel()

	spy := &recordingLogCoherencySpy{}

	require.NoError(t, election.EnsureRecordingLogCoherent(
		spy,
		recordinglog.EnsureCoherentParams{RecordingID: election.NullValue},
	))
	require.NoError(t, election.EnsureRecordingLogCoherent(
		spy,
		recordinglog.EnsureCoherentParams{RecordingID: election.NullValue, LogPosition: 1000},
	))
	require.Empty(t, spy.calls)

	// Nil log is a no-op.
	require.NoError(t, election.EnsureRecordingLogCoherent(
		nil,
		recordinglog.EnsureCoherentParams{RecordingID: 42},
	))

	// A real recording id reaches the log; a NullValue term base falls back to
	// the initial term base position.
	require.NoError(t, election.EnsureRecordingLogCoherent(
		spy,
		recordinglog.EnsureCoherentParams{
			RecordingID:                42,
			InitialLogLeadershipTermID: 1,
			InitialTermBaseLogPosition: 100,
			LeadershipTermID:           5,
			TermBaseLogPosition:        election.NullValue,
			LogPosition:                200,
			Timestamp:                  7,
		},
	))
	require.Equal(t, []recordinglog.EnsureCoherentParams{{
		RecordingID:                42,
		InitialLogLeadershipTermID: 1,
		InitialTermBaseLogPosition: 100,
		LeadershipTermID:           5,
		TermBaseLogPosition:        100,
		LogPosition:                200,
		Timestamp:                  7,
	}}, spy.calls)
}

func TestUpdateRecordingLog_propagation(t *testing.T) {
	t.Parallel()

	t.Run("leader init coherency error resets to init", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{clock: clock, localLogRecordingID: 42}
		deps.Host = host
		deps.RecordingLog = &recordingLogCoherencySpy{err: errors.New("write failed")}
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateLeaderInit)

		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateInit, e.State())
	})

	t.Run("follower log await success advances log leadership term", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, epochClock, _ := testElectionDeps(t)
		spy := &recordingLogCoherencySpy{}
		deps.RecordingLog = spy
		adoptedTerm := driveFollowerLogJoinToReady(t, cfg, deps, clock)

		require.Equal(t, []recordinglog.EnsureCoherentParams{{
			RecordingID:                testRecordingID,
			InitialLogLeadershipTermID: cfg.LeadershipTermID,
			InitialTermBaseLogPosition: cfg.TermBaseLogPosition,
			LeadershipTermID:           adoptedTerm,
			TermBaseLogPosition:        cfg.LogPosition,
			LogPosition:                election.NullValue,
			Timestamp:                  epochClock.nowMs,
		}}, spy.calls)
	})

	t.Run("forwards file sync level from deps", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, epochClock, _ := testElectionDeps(t)
		host := &recordingElectionHost{clock: clock, localLogRecordingID: 42}
		deps.Host = host
		spy := &recordingLogCoherencySpy{}
		deps.RecordingLog = spy
		deps.RecordingLogFileSyncLevel = recordinglog.FileSyncData
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateLeaderInit)

		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateLeaderReady, e.State())
		require.Equal(t, []recordinglog.EnsureCoherentParams{{
			RecordingID:                42,
			InitialLogLeadershipTermID: cfg.LeadershipTermID,
			InitialTermBaseLogPosition: cfg.TermBaseLogPosition,
			LeadershipTermID:           cfg.LeadershipTermID,
			TermBaseLogPosition:        cfg.LogPosition,
			LogPosition:                election.NullValue,
			Timestamp:                  epochClock.nowMs,
			FileSyncLevel:              recordinglog.FileSyncData,
		}}, spy.calls)
	})
}

// driveFollowerLogJoinToReady adopts a successor term via NewLeadershipTerm,
// replays, and joins the follower log, asserting the FollowerReady transition
// and returning the adopted term. Callers set deps.RecordingLog before the
// drive; Host and Handler are installed here.
func driveFollowerLogJoinToReady(
	t *testing.T,
	cfg election.ElectionConfig,
	deps election.ElectionDeps,
	clock *FakeClock,
) int64 {
	t.Helper()

	deps.Host = &recordingElectionHost{
		clock:                   clock,
		localLogRecordingID:     testRecordingID,
		followerLogJoinOK:       true,
		followerLogJoinPosition: cfg.LogPosition,
	}
	deps.Handler = &recordingAgentHandler{joinFollowerReturn: true}
	e := election.NewElection(cfg, deps)

	adoptedTerm := cfg.LeadershipTermID + 2
	election.SetStateForTest(e, election.ElectionStateCanvass)
	e.OnNewLeadershipTerm(installNewLeadershipTermFields(cfg, 1, adoptedTerm))
	require.Equal(t, election.ElectionStateFollowerReplay, e.State())
	require.Equal(t, cfg.LeadershipTermID, election.LogLeadershipTermIDForTest(e))

	election.SetStateForTest(e, election.ElectionStateFollowerLogAwait)
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateFollowerReady, e.State())
	require.Equal(t, adoptedTerm, election.LogLeadershipTermIDForTest(e))
	return adoptedTerm
}

// driveToFollowerLogReplication installs a successor term whose base position
// this node has not reached, routing the follower into
// FOLLOWER_LOG_REPLICATION.
func driveToFollowerLogReplication(
	t *testing.T,
	e *election.Election,
	cfg election.ElectionConfig,
	nextTermBase, nextLogPosition int64,
) {
	t.Helper()

	election.SetStateForTest(e, election.ElectionStateCanvass)
	fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
	fields.TermBaseLogPosition = cfg.AppendPosition + 100
	fields.NextLeadershipTermID = cfg.LeadershipTermID + 1
	fields.NextTermBaseLogPosition = nextTermBase
	fields.NextLogPosition = nextLogPosition
	e.OnNewLeadershipTerm(fields)
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
}

func TestHandleFollowerLogReplication_branches(t *testing.T) {
	t.Parallel()

	t.Run("caught up commits replicated term and recanvasses", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, epochClock, _ := testElectionDeps(t)
		host := &recordingElectionHost{clock: clock, localLogRecordingID: 42}
		deps.Host = host
		spy := &recordingLogCoherencySpy{}
		deps.RecordingLog = spy
		e := election.NewElection(cfg, deps)

		// Successor term base equals the append position with a known stop
		// position at it: nothing to replicate.
		driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition, cfg.AppendPosition)

		require.Equal(t, 0, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateCanvass, e.State())
		require.Equal(t, cfg.LeadershipTermID+1, election.LogLeadershipTermIDForTest(e))
		require.Equal(t, []recordinglog.EnsureCoherentParams{{
			RecordingID:                42,
			InitialLogLeadershipTermID: cfg.LeadershipTermID,
			InitialTermBaseLogPosition: cfg.TermBaseLogPosition,
			LeadershipTermID:           cfg.LeadershipTermID + 1,
			TermBaseLogPosition:        cfg.AppendPosition,
			LogPosition:                cfg.AppendPosition,
			Timestamp:                  epochClock.nowMs,
		}}, spy.calls)
	})

	t.Run("caught up coherency error resets to init", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{clock: clock, localLogRecordingID: 42}
		deps.Host = host
		deps.RecordingLog = &recordingLogCoherencySpy{err: errors.New("write failed")}
		e := election.NewElection(cfg, deps)
		driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition, cfg.AppendPosition)

		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, cfg.LeadershipTermID, election.LogLeadershipTermIDForTest(e))
	})

	t.Run("bare injected state times out without committing", func(t *testing.T) {
		t.Parallel()

		// Without an install the replication stop position is the NullPosition
		// sentinel: the caught-up branch must not commit it as a term, and the
		// unarmed deadline resets the election on the first tick.
		cfg, deps, clock, _, _ := testElectionDeps(t)
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateFollowerLogReplication)

		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, cfg.LeadershipTermID, election.LogLeadershipTermIDForTest(e))
	})

	t.Run("waits on install deadline then times out", func(t *testing.T) {
		t.Parallel()

		// The installing NewLeadershipTerm sets the replication deadline one
		// heartbeat timeout out.
		cfg, deps, clock, _, _ := testElectionDeps(t)
		e := election.NewElection(cfg, deps)
		driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition+50, election.NullPosition)

		t0 := clock.nowNs
		require.Equal(t, 0, e.DoWork(t0))
		require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())

		require.Equal(t, 0, e.DoWork(t0+cfg.LeaderHeartbeatTimeoutNs-1))
		require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())

		require.Equal(t, 1, e.DoWork(t0+cfg.LeaderHeartbeatTimeoutNs))
		require.Equal(t, election.ElectionStateInit, e.State())
	})

	t.Run("leader commit position extends deadline", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		e := election.NewElection(cfg, deps)
		driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition+50, election.NullPosition)

		t0 := clock.nowNs
		require.Equal(t, 0, e.DoWork(t0))

		extendNs := t0 + cfg.LeaderHeartbeatTimeoutNs - 1
		clock.nowNs = extendNs
		e.OnCommitPosition(
			codecs.CommitPositionFields{
				LeadershipTermID: cfg.LeadershipTermID,
				LogPosition:      0,
				LeaderMemberID:   1,
			},
		)

		require.Equal(t, 0, e.DoWork(t0+cfg.LeaderHeartbeatTimeoutNs))
		require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())

		require.Equal(t, 1, e.DoWork(extendNs+cfg.LeaderHeartbeatTimeoutNs))
		require.Equal(t, election.ElectionStateInit, e.State())
	})
}

func TestHandleFollowerLogReplication_timeoutClearsHelperForReinstall(t *testing.T) {
	t.Parallel()

	// IsDone keeps the helper resident while lagging.
	//
	// Timeout leave must clear it so a later FOLLOWER_LOG_REPLICATION visit
	// can install again.
	cfg, deps, clock, _, _ := testElectionDeps(t)
	installs := 0
	host := &recordingElectionHost{
		clock: clock,
		newLogReplication: func(_, _ string, _, _, _ int64) election.LogReplication {
			installs++
			return &countingLogReplicationForTest{
				work:        1,
				done:        true,
				recordingID: int64(90 + installs),
			}
		},
	}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition+50, election.NullPosition)

	t0 := clock.nowNs
	require.Positive(t, e.DoWork(t0))
	require.Equal(t, 1, installs)
	require.Len(t, host.newLogReplicationCalls, 1)
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())

	// Resident done helper blocks reinstall while still in this visit.
	require.Equal(t, 0, e.DoWork(t0+1))
	require.Equal(t, 1, installs)

	require.Equal(t, 1, e.DoWork(t0+cfg.LeaderHeartbeatTimeoutNs))
	require.Equal(t, election.ElectionStateInit, e.State())

	// Re-enter FOLLOWER_LOG_REPLICATION after leave: factory must run again.
	driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition+50, election.NullPosition)
	require.Positive(t, e.DoWork(clock.nowNs))
	require.Equal(t, 2, installs)
	require.Len(t, host.newLogReplicationCalls, 2)
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
}

func TestHandleFollowerLogReplication_caughtUpClearsHelperForReinstall(t *testing.T) {
	t.Parallel()

	// Already at stop: install + IsDone then caught-up leave must clear the
	// helper so a later FOLLOWER_LOG_REPLICATION visit can install again.
	cfg, deps, clock, _, _ := testElectionDeps(t)
	installs := 0
	host := &recordingElectionHost{
		clock:               clock,
		localLogRecordingID: 42,
		newLogReplication: func(_, _ string, _, _, _ int64) election.LogReplication {
			installs++
			return &countingLogReplicationForTest{
				work:        0,
				done:        true,
				recordingID: int64(50 + installs),
			}
		},
	}
	deps.Host = host
	deps.RecordingLog = &recordingLogCoherencySpy{}
	e := election.NewElection(cfg, deps)

	driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition, cfg.AppendPosition)
	_ = e.DoWork(clock.nowNs)
	require.Equal(t, 1, installs)
	require.Equal(t, election.ElectionStateCanvass, e.State())

	// Lagging re-enter after caught-up leave: factory runs again.
	//
	// Caught-up commit advances log term; NewLeadershipTerm must match for
	// install.
	logTerm := election.LogLeadershipTermIDForTest(e)
	appendPos := e.AppendPosition()
	fields := installNewLeadershipTermFields(cfg, 1, logTerm)
	fields.LogLeadershipTermID = logTerm
	fields.LeadershipTermID = logTerm
	fields.TermBaseLogPosition = appendPos + 100
	fields.NextLeadershipTermID = logTerm + 1
	fields.NextTermBaseLogPosition = appendPos + 50
	fields.NextLogPosition = election.NullPosition
	e.OnNewLeadershipTerm(fields)
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())

	require.Positive(t, e.DoWork(clock.nowNs))
	require.Equal(t, 2, installs)
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
}

func TestOnNewLeadershipTerm_installRoutes(t *testing.T) {
	t.Parallel()

	t.Run("next term base below append truncates and resets", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{clock: clock, localLogRecordingID: 42}
		deps.Host = host
		spy := &recordingLogCoherencySpy{}
		deps.RecordingLog = spy
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)

		fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
		fields.NextLeadershipTermID = cfg.LeadershipTermID + 1
		fields.NextTermBaseLogPosition = cfg.AppendPosition - 50
		e.OnNewLeadershipTerm(fields)

		require.Equal(t, []truncateCall{{
			leadershipTermID: cfg.LeadershipTermID,
			logPosition:      cfg.AppendPosition - 50,
		}}, host.truncateCalls)
		require.Empty(t, spy.calls)
		require.Equal(t, cfg.AppendPosition-50, e.AppendPosition())
		require.Equal(t, election.ElectionStateInit, e.State())
	})

	t.Run("lagging append without successor term resets", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{clock: clock}
		deps.Host = host
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)

		fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
		fields.TermBaseLogPosition = cfg.AppendPosition + 100
		e.OnNewLeadershipTerm(fields)

		require.Empty(t, host.truncateCalls)
		require.Equal(t, cfg.AppendPosition, e.AppendPosition())
		require.Equal(t, election.ElectionStateInit, e.State())
	})

	t.Run("open successor term routes to replication", func(t *testing.T) {
		t.Parallel()

		cfg, deps, _, _, _ := testElectionDeps(t)
		e := election.NewElection(cfg, deps)
		driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition+50, election.NullPosition)

		termID, stopPosition, termBase := election.ReplicationForTest(e)
		require.Equal(t, cfg.LeadershipTermID, termID)
		require.Equal(t, cfg.AppendPosition+50, stopPosition)
		require.Equal(t, int64(election.NullValue), termBase)
	})

	t.Run("committed successor term routes to replication", func(t *testing.T) {
		t.Parallel()

		cfg, deps, _, _, _ := testElectionDeps(t)
		e := election.NewElection(cfg, deps)
		driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition, cfg.AppendPosition+300)

		termID, stopPosition, termBase := election.ReplicationForTest(e)
		require.Equal(t, cfg.LeadershipTermID+1, termID)
		require.Equal(t, cfg.AppendPosition+300, stopPosition)
		require.Equal(t, cfg.AppendPosition, termBase)
	})

	t.Run("committed successor without stop position stays put", func(t *testing.T) {
		t.Parallel()

		cfg, deps, _, _, _ := testElectionDeps(t)
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)

		fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
		fields.TermBaseLogPosition = cfg.AppendPosition + 100
		fields.NextLeadershipTermID = cfg.LeadershipTermID + 1
		fields.NextTermBaseLogPosition = cfg.AppendPosition
		fields.NextLogPosition = election.NullPosition
		e.OnNewLeadershipTerm(fields)

		require.Equal(t, election.ElectionStateCanvass, e.State())
		require.NotNil(t, e.Leader())
	})
}

func TestOnCommitPosition_branches(t *testing.T) {
	t.Parallel()

	t.Run("from leader raises notified commit position monotonically", func(t *testing.T) {
		t.Parallel()

		cfg, deps, _, _, _ := testElectionDeps(t)
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)
		e.OnNewLeadershipTerm(installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID))
		require.Equal(t, election.ElectionStateFollowerReplay, e.State())

		e.OnCommitPosition(
			codecs.CommitPositionFields{
				LeadershipTermID: cfg.LeadershipTermID,
				LogPosition:      500,
				LeaderMemberID:   1,
			},
		)
		require.Equal(t, int64(500), e.NotifiedCommitPosition())

		e.OnCommitPosition(
			codecs.CommitPositionFields{
				LeadershipTermID: cfg.LeadershipTermID,
				LogPosition:      400,
				LeaderMemberID:   1,
			},
		)
		require.Equal(t, int64(500), e.NotifiedCommitPosition())
	})

	t.Run("non-leader member ignored", func(t *testing.T) {
		t.Parallel()

		cfg, deps, _, _, _ := testElectionDeps(t)
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)
		e.OnNewLeadershipTerm(installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID))

		e.OnCommitPosition(
			codecs.CommitPositionFields{
				LeadershipTermID: cfg.LeadershipTermID,
				LogPosition:      500,
				LeaderMemberID:   2,
			},
		)
		require.Equal(t, int64(0), e.NotifiedCommitPosition())
	})

	t.Run("ignored in init", func(t *testing.T) {
		t.Parallel()

		cfg, deps, _, _, _ := testElectionDeps(t)
		e := election.NewElection(cfg, deps)

		e.OnCommitPosition(
			codecs.CommitPositionFields{
				LeadershipTermID: cfg.LeadershipTermID + 1,
				LogPosition:      500,
				LeaderMemberID:   1,
			},
		)
		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, int64(0), e.NotifiedCommitPosition())
	})

	t.Run("newer term while leader ready resets", func(t *testing.T) {
		t.Parallel()

		cfg, deps, _, _, _ := testElectionDeps(t)
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateLeaderReady)

		e.OnCommitPosition(
			codecs.CommitPositionFields{
				LeadershipTermID: cfg.LeadershipTermID + 1,
				LogPosition:      500,
				LeaderMemberID:   1,
			},
		)
		require.Equal(t, election.ElectionStateInit, e.State())
	})

	t.Run("unsticks follower replay waiting on commit position", func(t *testing.T) {
		t.Parallel()

		fx := newElectionFixture(t)
		fx.doWork()
		fx.injectNewLeadershipTerm(1, fx.cfg.LeadershipTermID)
		require.Equal(t, election.ElectionStateFollowerReplay, fx.election.State())

		// No commit position notified yet: replay waits, publishing its
		// append position to the leader.
		fx.doWork()
		require.Equal(t, election.ElectionStateFollowerReplay, fx.election.State())

		fx.election.OnCommitPosition(
			codecs.CommitPositionFields{
				LeadershipTermID: fx.election.LeadershipTermID(),
				LogPosition:      fx.cfg.AppendPosition,
				LeaderMemberID:   1,
			},
		)

		fx.doWork()
		require.Equal(t, election.ElectionStateFollowerReplay, fx.election.State())
		fx.doWork()
		require.Equal(t, election.ElectionStateFollowerLogInit, fx.election.State())
	})
}

// installNewLeadershipTermFields builds a NewLeadershipTerm anchored at cfg's
// baseline log term and position with null successor-term sentinels, matching
// what a leader with no recording log publishes.
func installNewLeadershipTermFields(
	cfg election.ElectionConfig,
	leaderID int32,
	term int64,
) codecs.NewLeadershipTermFields {
	return codecs.NewLeadershipTermFields{
		LogLeadershipTermID:     cfg.LeadershipTermID,
		NextLeadershipTermID:    election.NullValue,
		NextTermBaseLogPosition: election.NullPosition,
		NextLogPosition:         election.NullPosition,
		LeadershipTermID:        term,
		TermBaseLogPosition:     cfg.LogPosition,
		LogPosition:             cfg.LogPosition,
		LeaderMemberID:          leaderID,
		LogSessionID:            fixtureLogSessionID,
	}
}

func TestOnNewLeadershipTerm_branches(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		state      election.ElectionState
		leaderID   int32
		term       int64
		wantState  election.ElectionState
		wantLeader bool
	}{
		{
			"accept in follower ballot",
			election.ElectionStateFollowerBallot,
			1, 5,
			election.ElectionStateFollowerReplay,
			true,
		},
		{
			"unknown leader ignored",
			election.ElectionStateFollowerBallot,
			99, 5,
			election.ElectionStateFollowerBallot,
			false,
		},
		{
			"stale term ignored",
			election.ElectionStateFollowerBallot,
			1, 4,
			election.ElectionStateFollowerBallot,
			false,
		},
		{
			"wrong state ignored",
			election.ElectionStateLeaderReady,
			1, 5,
			election.ElectionStateLeaderReady,
			false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, _, _, _ := testElectionDeps(t)
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, tc.state)

			e.OnNewLeadershipTerm(installNewLeadershipTermFields(cfg, tc.leaderID, tc.term))

			require.Equal(t, tc.wantState, e.State())
			if tc.wantLeader {
				require.NotNil(t, e.Leader())
				require.Equal(t, tc.leaderID, e.Leader().ID)
				require.Equal(t, tc.term, e.LeadershipTermID())
				require.Equal(t, fixtureLogSessionID, e.LogSessionID())
			}
		})
	}
}

func TestOnNewLeadershipTerm_inBallotOrCanvassGuards(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		state       election.ElectionState
		term        int64
		wantInstall bool
	}{
		{
			"follower ballot on candidate term installs",
			election.ElectionStateFollowerBallot,
			testLeadershipTermID,
			true,
		},
		{
			"follower ballot off candidate term ignores",
			election.ElectionStateFollowerBallot,
			testLeadershipTermID + 1,
			false,
		},
		{
			"candidate ballot on candidate term installs",
			election.ElectionStateCandidateBallot,
			testLeadershipTermID,
			true,
		},
		{
			"canvass installs regardless of candidate term",
			election.ElectionStateCanvass,
			testLeadershipTermID + 2,
			true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, _, _, _ := testElectionDeps(t)
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, tc.state)

			e.OnNewLeadershipTerm(installNewLeadershipTermFields(cfg, 1, tc.term))

			if !tc.wantInstall {
				require.Equal(t, tc.state, e.State())
				require.Nil(t, e.Leader())
				require.Equal(t, cfg.LeadershipTermID, e.LeadershipTermID())
				return
			}
			require.Equal(t, election.ElectionStateFollowerReplay, e.State())
			require.NotNil(t, e.Leader())
			require.Equal(t, int32(1), e.Leader().ID)
			require.Equal(t, tc.term, e.LeadershipTermID())
		})
	}
}

func TestHandleFollowerReady_publishCompletesElection(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	handler := &recordingAgentHandler{}
	deps.Handler = handler
	pub := deps.Publisher.(*election.SbePublisher)
	e := election.NewElection(cfg, deps)

	election.SetStateForTest(e, election.ElectionStateFollowerBallot)
	e.OnNewLeadershipTerm(installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID))
	election.SetStateForTest(e, election.ElectionStateFollowerReady)

	require.Equal(t, 1, e.DoWork(clock.nowNs))

	require.Equal(t, election.ElectionStateClosed, e.State())
	require.Equal(t, []int64{clock.nowNs}, handler.electionCompleteNs)
	require.Len(t, pub.Sent, 1)
	sent := pub.Sent[0]
	require.Equal(t, uint16(clusterwire.AppendPositionSbeTemplateID), sent.TemplateID)
	require.Equal(t, int32(1), sent.Dst.MemberID)
	fields := sent.Fields.(codecs.AppendPositionFields)
	require.Equal(t, cfg.LeadershipTermID, fields.LeadershipTermID)
	require.Equal(t, cfg.LogPosition, fields.LogPosition)
	require.Equal(t, cfg.MemberID, fields.FollowerMemberID)
}

func TestHandleFollowerReady_publishFailure(t *testing.T) {
	t.Parallel()

	timeoutNs := election.ElectionTestConfig().LeaderHeartbeatTimeoutNs
	for _, tc := range []struct {
		name    string
		deltaNs int64
		want    election.ElectionState
	}{
		{"retries before timeout", timeoutNs - 1, election.ElectionStateFollowerReady},
		{"resets to init at timeout", timeoutNs, election.ElectionStateInit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			handler := &recordingAgentHandler{}
			deps.Handler = handler
			pub := &appendPositionFailPublisher{SbePublisher: &election.SbePublisher{}}
			deps.Publisher = pub
			e := election.NewElection(cfg, deps)

			election.SetStateForTest(e, election.ElectionStateFollowerBallot)
			e.OnNewLeadershipTerm(installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID))
			election.SetStateForTest(e, election.ElectionStateFollowerReady)

			require.Equal(t, 1, e.DoWork(clock.nowNs))
			require.Equal(t, election.ElectionStateFollowerReady, e.State())
			require.Equal(t, 1, e.DoWork(clock.nowNs+tc.deltaNs))

			require.Equal(t, tc.want, e.State())
			require.Equal(t, 2, pub.attempts)
			require.Empty(t, handler.electionCompleteNs)
		})
	}
}

func TestOnRecordingSignal_initNoOp(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	lr := &noopLogReplicationForTest{}
	election.SetLogReplicationForTest(e, lr)

	e.OnRecordingSignal(1, 2, 3, election.RecordingSignal(4))
	require.False(t, lr.called)
}

func TestOnRecordingSignal_withLogReplication(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerLogReplication)

	lr := &noopLogReplicationForTest{}
	election.SetLogReplicationForTest(e, lr)

	e.OnRecordingSignal(1, 2, 3, election.RecordingSignal(4))
	require.True(t, lr.called)
}

func TestHandleInit_multiNodeEntersCanvass(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	observer := &recordingStateObserver{}
	deps.StateObserver = observer
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, []election.ElectionState{
		election.ElectionStateInit,
		election.ElectionStateCanvass,
	}, observer.states)
}

func TestHandleInit_singleNodeShortcut(t *testing.T) {
	t.Parallel()

	cfg, deps := singleNodeElectionDeps(t)
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(1_000_000_000))
	require.Equal(t, election.ElectionStateLeaderLogReplication, e.State())
	require.Equal(t, e.ThisMemberID(), e.Leader().ID)
	require.Equal(t, int64(1), e.LeadershipTermID())
}

func TestHandleCanvass_publishesToPeers(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	pub := deps.Publisher.(*election.SbePublisher)
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())

	pub.Reset()
	require.Positive(t, e.DoWork(clock.nowNs))
	require.Len(t, pub.Sent, 2)
	for _, m := range pub.Sent {
		require.Equal(t, uint16(clusterwire.CanvassPositionSbeTemplateID), m.TemplateID)
	}
	require.Equal(t, election.ElectionStateCanvass, e.State())
}

func TestHandleCanvass_quorumAtDeadlineEntersNominate(t *testing.T) {
	t.Parallel()

	timeoutNs := election.ElectionTestConfig().StartupCanvassTimeoutNs
	for _, tc := range []struct {
		name    string
		deltaNs int64
		want    election.ElectionState
	}{
		{"just before deadline", timeoutNs - 1, election.ElectionStateCanvass},
		{"at deadline", timeoutNs, election.ElectionStateNominate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			cfg.IsNodeStartup = true
			host := &recordingElectionHost{clock: clock, role: consensus.RoleFollower}
			deps.Host = host
			e := election.NewElection(cfg, deps)

			require.Equal(t, 1, e.DoWork(clock.nowNs))
			require.Equal(t, election.ElectionStateCanvass, e.State())
			require.True(t, election.IsExtendedCanvassForTest(e))

			// Quorum via self plus one peer; peer 2 stays unreported so unanimity cannot fire early.
			e.OnCanvassPosition(
				codecs.CanvassPositionFields{
					LogLeadershipTermID: cfg.LeadershipTermID,
					LogPosition:         cfg.AppendPosition,
					LeadershipTermID:    cfg.LeadershipTermID,
					FollowerMemberID:    1,
				},
			)
			require.Equal(t, cfg.AppendPosition, election.MemberForTest(e, 1).LogPosition)

			require.Positive(t, e.DoWork(clock.nowNs+tc.deltaNs))
			require.Equal(t, tc.want, e.State())
		})
	}
}

func TestOnAppendPosition_updatesMemberLogPositionDuringCanvass(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		leadershipTerm int64
		wantUpdated    bool
	}{
		{
			name:           "current term updates member",
			leadershipTerm: testLeadershipTermID,
			wantUpdated:    true,
		},
		{
			name:           "stale term ignored",
			leadershipTerm: testLeadershipTermID + 1,
			wantUpdated:    false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			host := &recordingElectionHost{clock: clock, role: consensus.RoleFollower}
			deps.Host = host
			e := election.NewElection(cfg, deps)

			require.Equal(t, 1, e.DoWork(clock.nowNs))
			require.Equal(t, election.ElectionStateCanvass, e.State())

			follower := election.MemberForTest(e, 1)
			require.Equal(t, election.NullPosition, follower.LogPosition)

			const flags uint8 = 0x02
			e.OnAppendPosition(
				codecs.AppendPositionFields{
					LeadershipTermID: tc.leadershipTerm,
					LogPosition:      cfg.AppendPosition + 50,
					FollowerMemberID: 1,
					Flags:            flags,
				},
			)

			if !tc.wantUpdated {
				require.Equal(t, election.NullPosition, follower.LogPosition)
				require.Empty(t, host.trackCatchupCalls)
				return
			}
			require.Equal(t, tc.leadershipTerm, follower.LeadershipTermID)
			require.Equal(t, cfg.AppendPosition+50, follower.LogPosition)
			require.Equal(t, clock.nowNs, follower.TimeOfLastAppendPositionNs)
			require.Len(t, host.trackCatchupCalls, 1)
			require.Equal(t, int32(1), host.trackCatchupCalls[0].memberID)
			require.Equal(t, tc.leadershipTerm, host.trackCatchupCalls[0].leadershipTermID)
			require.Equal(t, flags, host.trackCatchupCalls[0].flags)
		})
	}
}

func TestHandleInit_nonStartupPreparesForNewLeadershipOnce(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = false
	preparedAppend := cfg.AppendPosition + 1
	handler := &recordingAgentHandler{appendPositionReturn: preparedAppend}
	host := &recordingElectionHost{clock: clock, role: consensus.RoleLeader}
	deps.Handler = handler
	deps.Host = host
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, 1, handler.prepareCalls)
	require.Equal(t, cfg.LogPosition, handler.prepareLogPosition)
	require.Equal(t, clock.nowNs, handler.prepareNowNs)
	require.Equal(t, consensus.RoleFollower, host.role)
	require.Equal(t, preparedAppend, e.AppendPosition())
}

// markFollowersCaughtUp reports every other member at this node's term and
// append position, so the next canvass tick sees a unanimous candidacy.
func markFollowersCaughtUp(e *election.Election) {
	for _, m := range election.MembersForTest(e) {
		if m.ID != e.ThisMemberID() {
			m.LeadershipTermID = e.LeadershipTermID()
			m.LogPosition = e.AppendPosition()
		}
	}
}

func TestHandleCanvass_unanimousEntersNominate(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())

	markFollowersCaughtUp(e)

	require.Positive(t, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateNominate, e.State())
}

func TestState_extendedCanvassLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("startup init to canvass keeps extended window", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		cfg.IsNodeStartup = true
		e := election.NewElection(cfg, deps)

		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateCanvass, e.State())
		require.True(t, election.IsExtendedCanvassForTest(e))
	})

	t.Run("leaving canvass drops extended window", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		cfg.IsNodeStartup = true
		e := election.NewElection(cfg, deps)

		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateCanvass, e.State())
		require.True(t, election.IsExtendedCanvassForTest(e))

		markFollowersCaughtUp(e)
		require.Positive(t, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateNominate, e.State())
		require.False(t, election.IsExtendedCanvassForTest(e))
	})
}

func TestHandleCandidateBallot_requestVoteUsesAppendPosition(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	pub := deps.Publisher.(*election.SbePublisher)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCandidateBallot)

	for _, m := range election.MembersForTest(e) {
		if m.ID == e.ThisMemberID() {
			m.IsBallotSent = true
		}
	}

	require.Equal(t, 2, e.DoWork(clock.nowNs))
	require.Len(t, pub.Sent, 2)
	for _, m := range pub.Sent {
		require.Equal(t, uint16(clusterwire.RequestVoteSbeTemplateID), m.TemplateID)
		rv := m.Fields.(codecs.RequestVoteFields)
		require.Equal(t, e.AppendPosition(), rv.LogPosition)
	}
}

func TestOnCanvassPosition_ignoredBranches(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		state  election.ElectionState
		fields codecs.CanvassPositionFields
	}{
		{"init", election.ElectionStateInit, codecs.CanvassPositionFields{FollowerMemberID: 1}},
		{
			"unknown member",
			election.ElectionStateCanvass,
			codecs.CanvassPositionFields{FollowerMemberID: 99},
		},
		{"self", election.ElectionStateCanvass, codecs.CanvassPositionFields{FollowerMemberID: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, _, _, _ := testElectionDeps(t)
			pub := deps.Publisher.(*election.SbePublisher)
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, tc.state)

			e.OnCanvassPosition(tc.fields)

			require.Empty(t, pub.Sent)
			require.Equal(t, tc.state, e.State())
		})
	}
}

func TestOnVote_recordsOnlyInCandidateBallot(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		state        election.ElectionState
		wantRecorded bool
	}{
		{"candidate ballot records vote", election.ElectionStateCandidateBallot, true},
		{"other state ignored", election.ElectionStateCanvass, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, _, _, _ := testElectionDeps(t)
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, tc.state)

			e.OnVote(
				codecs.VoteFields{
					CandidateTermID:   cfg.LeadershipTermID,
					CandidateMemberID: e.ThisMemberID(),
					FollowerMemberID:  1,
					Vote:              clusterwire.BooleanType_TRUE,
				},
			)

			voter := election.MemberForTest(e, 1)
			if !tc.wantRecorded {
				require.Nil(t, voter.Vote)
				return
			}
			require.NotNil(t, voter.Vote)
			require.True(t, *voter.Vote)
			require.Equal(t, cfg.LeadershipTermID, voter.CandidateTermID)
		})
	}
}

type noopLogReplicationForTest struct {
	called bool
}

func (n *noopLogReplicationForTest) DoWork(int64) int { return 0 }

func (n *noopLogReplicationForTest) RecordingID() int64 { return 42 }

func (n *noopLogReplicationForTest) IsDone() bool { return false }

func (n *noopLogReplicationForTest) OnSignal(int64, int64, int64, election.RecordingSignal) {
	n.called = true
}

// countingLogReplicationForTest is a duty-cycle double for install / IsDone
// wire tests. It does not advance append position.
type countingLogReplicationForTest struct {
	doWorkCalls int
	work        int
	done        bool
	recordingID int64
}

func (c *countingLogReplicationForTest) DoWork(int64) int {
	c.doWorkCalls++
	return c.work
}

func (c *countingLogReplicationForTest) RecordingID() int64 { return c.recordingID }

func (c *countingLogReplicationForTest) IsDone() bool { return c.done }

func (c *countingLogReplicationForTest) OnSignal(int64, int64, int64, election.RecordingSignal) {}
