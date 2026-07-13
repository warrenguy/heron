package election_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/election"
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
				pub.CanvassPosition(dst, 1, 2, 3, 4)
			},
		},
		{
			name: "RequestVote",
			publish: func() {
				pub.RequestVote(dst, 10, 20, 30, 40)
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
				pub.PlaceVote(dst, 1, 2, 3, 4, 5, true)
			},
		},
		{
			name: "NewLeadershipTerm",
			publish: func() {
				pub.NewLeadershipTerm(dst, codecs.NewLeadershipTermFields{
					LogLeadershipTermID:  1,
					NextLeadershipTermID: 2,
					LeaderMemberID:       3,
				})
			},
		},
		{
			name: "AppendPosition",
			publish: func() {
				pub.AppendPosition(dst, 7, 8, 9, 0x01)
			},
		},
		{
			name: "CommitPosition",
			publish: func() {
				pub.CommitPosition(dst, 11, 12, 13)
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
	pub.CanvassPosition(dst, 1, 2, 3, 4)
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
	pub.RequestVote(dst, 10, 20, 30, 40)
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

func TestHandleLeaderLogReplication_publishesOnLeaderHeartbeatInterval(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	pub := deps.Publisher.(*election.SbePublisher)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateLeaderLogReplication)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Len(t, pub.Sent, 2)

	// Past the election status interval but inside the leader heartbeat
	// interval: no republish.
	require.Equal(t, 0, e.DoWork(clock.nowNs+cfg.ElectionStatusIntervalNs))
	require.Len(t, pub.Sent, 2)

	require.Equal(t, 1, e.DoWork(clock.nowNs+cfg.LeaderHeartbeatIntervalNs))
	require.Len(t, pub.Sent, 4)
	for _, m := range pub.Sent {
		require.Equal(t, uint16(clusterwire.NewLeadershipTermSbeTemplateID), m.TemplateID)
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

			e.OnNewLeadershipTerm(codecs.NewLeadershipTermFields{
				LeaderMemberID:   tc.leaderID,
				LeadershipTermID: tc.term,
				LogSessionID:     777,
			})

			require.Equal(t, tc.wantState, e.State())
			if tc.wantLeader {
				require.NotNil(t, e.Leader())
				require.Equal(t, tc.leaderID, e.Leader().ID)
				require.Equal(t, tc.term, e.LeadershipTermID())
				require.Equal(t, int32(777), e.LogSessionID())
			}
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
	e.OnNewLeadershipTerm(codecs.NewLeadershipTermFields{
		LeaderMemberID:   1,
		LeadershipTermID: cfg.LeadershipTermID,
		LogSessionID:     777,
	})
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
			e.OnNewLeadershipTerm(codecs.NewLeadershipTermFields{
				LeaderMemberID:   1,
				LeadershipTermID: cfg.LeadershipTermID,
				LogSessionID:     777,
			})
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

func TestHandleCanvass_unanimousEntersNominate(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())

	for _, m := range election.MembersForTest(e) {
		if m.ID != e.ThisMemberID() {
			m.LeadershipTermID = e.LeadershipTermID()
			m.LogPosition = e.AppendPosition()
		}
	}

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

	t.Run("recanvass from ballot drops extended window", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		cfg.IsNodeStartup = true
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCandidateBallot)
		require.True(t, election.IsExtendedCanvassForTest(e))

		require.Equal(t, 1, e.DoWork(clock.nowNs+cfg.ElectionTimeoutNs))
		require.Equal(t, election.ElectionStateCanvass, e.State())
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

			e.OnVote(codecs.VoteFields{
				CandidateTermID:   cfg.LeadershipTermID,
				CandidateMemberID: e.ThisMemberID(),
				FollowerMemberID:  1,
				Vote:              clusterwire.BooleanType_TRUE,
			})

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
