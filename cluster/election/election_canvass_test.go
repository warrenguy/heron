package election_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
)

func TestOnRequestVote_branches(t *testing.T) {
	t.Parallel()

	const (
		voteTemplate              = uint16(clusterwire.VoteSbeTemplateID)
		newLeadershipTermTemplate = uint16(clusterwire.NewLeadershipTermSbeTemplateID)
	)
	aheadOfCandidate := func(candidateID int32) codecs.RequestVoteFields {
		return codecs.RequestVoteFields{
			CandidateTermID:     99,
			LogLeadershipTermID: 0,
			LogPosition:         100,
			CandidateMemberID:   candidateID,
		}
	}

	for _, tc := range []struct {
		name string

		// role picks the state: LEADER_READY for a leader, CANVASS otherwise.
		role consensus.Role

		hasLogSession bool
		fields        codecs.RequestVoteFields
		wantPropose   bool
		wantSent      []uint16
		wantVote      clusterwire.BooleanType
		wantState     election.ElectionState
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
			name:        "ahead of candidate log places false vote",
			fields:      aheadOfCandidate(1),
			wantPropose: true,
			wantSent:    []uint16{voteTemplate},
			wantVote:    clusterwire.BooleanType_FALSE,
			wantState:   election.ElectionStateCanvass,
		},
		{
			name:          "follower with log session ahead of candidate log only votes",
			hasLogSession: true,
			fields:        aheadOfCandidate(1),
			wantPropose:   true,
			wantSent:      []uint16{voteTemplate},
			wantVote:      clusterwire.BooleanType_FALSE,
			wantState:     election.ElectionStateCanvass,
		},
		{
			name:          "leader ahead of candidate log votes then republishes",
			role:          consensus.RoleLeader,
			hasLogSession: true,
			fields:        aheadOfCandidate(1),
			wantPropose:   true,
			wantSent:      []uint16{voteTemplate, newLeadershipTermTemplate},
			wantVote:      clusterwire.BooleanType_FALSE,
			wantState:     election.ElectionStateLeaderReady,
		},
		{
			name:          "leader ahead of unknown candidate only proposes",
			role:          consensus.RoleLeader,
			hasLogSession: true,
			fields:        aheadOfCandidate(99),
			wantPropose:   true,
			wantState:     election.ElectionStateLeaderReady,
		},
		{
			name: "grant enters follower ballot",
			fields: codecs.RequestVoteFields{
				CandidateTermID:     99,
				LogLeadershipTermID: 40,
				LogPosition:         900,
				CandidateMemberID:   1,
			},
			wantPropose: true,
			wantSent:    []uint16{voteTemplate},
			wantVote:    clusterwire.BooleanType_TRUE,
			wantState:   election.ElectionStateFollowerBallot,
		},
		{
			name: "stale term places false vote",
			fields: codecs.RequestVoteFields{
				CandidateTermID:   4,
				LogPosition:       100,
				CandidateMemberID: 1,
			},
			wantSent:  []uint16{voteTemplate},
			wantVote:  clusterwire.BooleanType_FALSE,
			wantState: election.ElectionStateCanvass,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, epochClock, nodeState := testElectionDeps(t)
			useRecordingHost(&deps, clock, cfg, tc.role)
			pub := deps.Publisher.(*election.SbePublisher)
			e := election.NewElection(cfg, deps)
			state := election.ElectionStateCanvass
			if tc.role == consensus.RoleLeader {
				state = election.ElectionStateLeaderReady
			}
			election.SetStateForTest(e, state)
			if tc.hasLogSession {
				election.SetLogSessionIDForTest(e, fixtureLogSessionID)
			}

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

			var sentTemplates []uint16
			for _, m := range pub.Sent {
				sentTemplates = append(sentTemplates, m.TemplateID)
				require.Equal(t, tc.fields.CandidateMemberID, m.Dst.MemberID)
			}
			require.Equal(t, tc.wantSent, sentTemplates)
			if len(pub.Sent) == 0 {
				return
			}
			vote := pub.Sent[0].Fields.(codecs.VoteFields)
			require.Equal(t, tc.wantVote, vote.Vote)
			require.Equal(t, e.AppendPosition(), vote.LogPosition)
			if len(pub.Sent) < 2 {
				return
			}
			term := pub.Sent[1].Fields.(codecs.NewLeadershipTermFields)
			require.Equal(t, tc.fields.LogLeadershipTermID, term.LogLeadershipTermID)
			require.Equal(t, cfg.LeadershipTermID, term.LeadershipTermID)
			require.Equal(t, clock.nowNs, term.Timestamp)
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
		role   consensus.Role
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
		{
			name: "leader ahead of candidate log",
			role: consensus.RoleLeader,
			fields: codecs.RequestVoteFields{
				CandidateTermID:     99,
				LogLeadershipTermID: 0,
				LogPosition:         100,
				CandidateMemberID:   1,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, nodeState := testElectionDeps(t)
			useRecordingHost(&deps, clock, cfg, tc.role)
			deps.NodeState = &failingNodeState{store: nodeState, err: errors.New("sync failed")}
			pub := deps.Publisher.(*election.SbePublisher)
			e := election.NewElection(cfg, deps)
			state := election.ElectionStateCanvass
			if tc.role == consensus.RoleLeader {
				state = election.ElectionStateLeaderReady
				election.SetLogSessionIDForTest(e, fixtureLogSessionID)
			}
			election.SetStateForTest(e, state)

			e.OnRequestVote(tc.fields)

			require.Equal(t, election.ElectionStateInit, e.State())
			require.Equal(t, cfg.LeadershipTermID, election.CandidateTermIDForTest(e))
			require.Empty(t, pub.Sent)
		})
	}
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
			logPos := cfg.AppendPosition + 50
			e.OnAppendPosition(
				codecs.AppendPositionFields{
					LeadershipTermID: tc.leadershipTerm,
					LogPosition:      logPos,
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
			require.Equal(t, logPos, follower.LogPosition)
			require.Equal(t, clock.nowNs, follower.TimeOfLastAppendPositionNs)
			require.Len(t, host.trackCatchupCalls, 1)
			require.Equal(t, int32(1), host.trackCatchupCalls[0].memberID)
			require.Equal(t, logPos, host.trackCatchupCalls[0].logPosition)
			require.Equal(t, flags, host.trackCatchupCalls[0].flags)
		})
	}
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

func TestOnCanvassPosition_branches(t *testing.T) {
	t.Parallel()

	const olderLogTerm int64 = testLeadershipTermID - 1
	older := func(id int32) codecs.CanvassPositionFields {
		return codecs.CanvassPositionFields{
			LogLeadershipTermID: olderLogTerm,
			LogPosition:         50,
			LeadershipTermID:    olderLogTerm,
			FollowerMemberID:    id,
		}
	}

	type canvassCase struct {
		name         string
		state        election.ElectionState
		role         consensus.Role
		logSessionID int32
		fields       codecs.CanvassPositionFields
		wantSent     bool
		wantReset    bool
	}
	cases := []canvassCase{
		{
			name:         "init",
			state:        election.ElectionStateInit,
			role:         consensus.RoleLeader,
			logSessionID: fixtureLogSessionID,
			fields:       older(1),
		},
		{
			name:         "unknown member",
			state:        election.ElectionStateLeaderReady,
			role:         consensus.RoleLeader,
			logSessionID: fixtureLogSessionID,
			fields:       older(99),
		},
		{
			name:         "self",
			state:        election.ElectionStateLeaderReady,
			role:         consensus.RoleLeader,
			logSessionID: fixtureLogSessionID,
			fields:       older(0),
		},
		{
			name:         "older log term with follower role",
			state:        election.ElectionStateFollowerReady,
			role:         consensus.RoleFollower,
			logSessionID: fixtureLogSessionID,
			fields:       older(1),
		},
		{
			name:         "older log term with candidate role",
			state:        election.ElectionStateCandidateBallot,
			role:         consensus.RoleCandidate,
			logSessionID: fixtureLogSessionID,
			fields:       older(1),
		},
		{
			name:         "older log term with null log session",
			state:        election.ElectionStateLeaderReady,
			role:         consensus.RoleLeader,
			logSessionID: election.NullSessionID,
			fields:       older(1),
		},
		{
			name:         "older log term with leader role",
			state:        election.ElectionStateLeaderReady,
			role:         consensus.RoleLeader,
			logSessionID: fixtureLogSessionID,
			fields:       older(1),
			wantSent:     true,
		},
		{
			name:         "log term equal to election term",
			state:        election.ElectionStateLeaderReady,
			role:         consensus.RoleLeader,
			logSessionID: fixtureLogSessionID,
			fields: codecs.CanvassPositionFields{
				LogLeadershipTermID: testLeadershipTermID,
				LogPosition:         50,
				LeadershipTermID:    testLeadershipTermID,
				FollowerMemberID:    1,
			},
		},
	}
	for state := election.ElectionStateInit; state <= election.ElectionStateClosed; state++ {
		cases = append(cases, canvassCase{
			name:         "log term above election term in " + state.String(),
			state:        state,
			role:         consensus.RoleLeader,
			logSessionID: fixtureLogSessionID,
			fields: codecs.CanvassPositionFields{
				LogLeadershipTermID: testLeadershipTermID + 1,
				LogPosition:         50,
				LeadershipTermID:    testLeadershipTermID + 1,
				FollowerMemberID:    1,
			},
			wantReset: state == election.ElectionStateLeaderLogReplication ||
				state == election.ElectionStateLeaderReady,
		})
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			useRecordingHost(&deps, clock, cfg, tc.role)
			pub := deps.Publisher.(*election.SbePublisher)
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, tc.state)
			election.SetLogSessionIDForTest(e, tc.logSessionID)

			e.OnCanvassPosition(tc.fields)

			if tc.wantReset {
				require.Equal(t, election.ElectionStateInit, e.State())
				require.EqualError(
					t,
					e.LastError(),
					"election: new election detected from canvass position",
				)
				require.Empty(t, pub.Sent)
				return
			}
			require.Equal(t, tc.state, e.State())
			require.NoError(t, e.LastError())
			if !tc.wantSent {
				require.Empty(t, pub.Sent)
				return
			}
			require.Len(t, pub.Sent, 1)
			sent := pub.Sent[0]
			require.Equal(t, uint16(clusterwire.NewLeadershipTermSbeTemplateID), sent.TemplateID)
			require.Equal(t, tc.fields.FollowerMemberID, sent.Dst.MemberID)
			fields := sent.Fields.(codecs.NewLeadershipTermFields)
			require.Equal(t, olderLogTerm, fields.LogLeadershipTermID)
			require.Equal(t, cfg.LeadershipTermID, fields.LeadershipTermID)
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
