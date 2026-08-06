package election_test

import (
	"cmp"
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

	t.Run("non-nil error", func(t *testing.T) {
		t.Parallel()

		const hostCommit int64 = 80
		cfg, deps, _, _, _ := testElectionDeps(t)
		require.Greater(t, cfg.LogPosition, hostCommit)
		host := &recordingElectionHost{commitPosition: hostCommit}
		deps.Host = host
		observer := &recordingStateObserver{}
		deps.StateObserver = observer
		err := errors.New("test error")
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)
		appendBefore := e.AppendPosition()

		e.HandleError(5_000, err)

		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, err, e.LastError())
		require.Equal(t, hostCommit, e.LogPosition())
		require.Equal(t, appendBefore, e.AppendPosition())
		require.Equal(t, []election.ElectionState{
			election.ElectionStateInit,
			election.ElectionStateInit,
		}, observer.states)
	})

	t.Run("nil error", func(t *testing.T) {
		t.Parallel()

		const hostCommit int64 = 40
		cfg, deps, _, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{commitPosition: hostCommit}
		deps.Host = host
		observer := &recordingStateObserver{}
		deps.StateObserver = observer
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)
		e.HandleError(1_000, errors.New("prior"))
		require.Error(t, e.LastError())

		e.HandleError(5_000, nil)

		require.Equal(t, election.ElectionStateInit, e.State())
		require.NoError(t, e.LastError())
		require.Equal(t, hostCommit, e.LogPosition())
		// Same-state Init is a no-op for entry side effects and the observer.
		require.Equal(t, []election.ElectionState{
			election.ElectionStateInit,
			election.ElectionStateInit,
		}, observer.states)
	})

	t.Run("ErrorHandler set", func(t *testing.T) {
		t.Parallel()

		const hostCommit int64 = 40
		cfg, deps, _, _, _ := testElectionDeps(t)
		require.Greater(t, cfg.LogPosition, hostCommit)
		deps.Host = &recordingElectionHost{commitPosition: hostCommit}

		// Capture pre-transition fields inside the handler. LastError is set first.
		// State and logPosition stay pre-HandleError until the handler returns.
		type handlerSnapshot struct {
			err         error
			lastError   error
			state       election.ElectionState
			logPosition int64
		}
		var e *election.Election
		var snaps []handlerSnapshot
		deps.ErrorHandler = func(err error) {
			snaps = append(snaps, handlerSnapshot{
				err:         err,
				lastError:   e.LastError(),
				state:       e.State(),
				logPosition: e.LogPosition(),
			})
		}
		e = election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)
		appendBefore := e.AppendPosition()
		err := errors.New("handler sees this")

		e.HandleError(5_000, err)

		require.Equal(t, []handlerSnapshot{{
			err:         err,
			lastError:   err,
			state:       election.ElectionStateCanvass,
			logPosition: cfg.LogPosition,
		}}, snaps)
		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, hostCommit, e.LogPosition())
		require.Equal(t, appendBefore, e.AppendPosition())

		// Nil error skips the handler and clears LastError.
		e.HandleError(6_000, nil)

		require.Len(t, snaps, 1)
		require.NoError(t, e.LastError())
	})

	t.Run("ErrorHandler nil", func(t *testing.T) {
		t.Parallel()

		cfg, deps, _, _, _ := testElectionDeps(t)
		deps.Host = &recordingElectionHost{}
		e := election.NewElection(cfg, deps)

		require.NotPanics(t, func() {
			e.HandleError(5_000, errors.New("no handler"))
		})
		require.Equal(t, election.ElectionStateInit, e.State())
		require.EqualError(t, e.LastError(), "no handler")
	})

	t.Run("host commit not notified commit", func(t *testing.T) {
		t.Parallel()

		const hostCommit int64 = 77
		const notifiedCommit int64 = 999
		cfg, deps, _, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{commitPosition: hostCommit}
		deps.Host = host
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)

		// Install sets NotifiedCommitPosition from the wire CommitPosition field.
		fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
		fields.CommitPosition = notifiedCommit
		e.OnNewLeadershipTerm(fields)
		require.Equal(t, notifiedCommit, e.NotifiedCommitPosition())
		require.NotEqual(t, hostCommit, e.NotifiedCommitPosition())
		appendBefore := e.AppendPosition()

		e.HandleError(5_000, errors.New("commit source"))

		require.Equal(t, hostCommit, e.LogPosition())
		require.NotEqual(t, e.NotifiedCommitPosition(), e.LogPosition())
		require.Equal(t, appendBefore, e.AppendPosition())
	})

	t.Run("sticky role after HandleError", func(t *testing.T) {
		t.Parallel()

		cfg, deps, _, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{role: consensus.RoleLeader}
		deps.Host = host
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateLeaderReady)

		e.HandleError(5_000, errors.New("keep role"))

		require.Equal(t, election.ElectionStateInit, e.State())
		require.Equal(t, consensus.RoleLeader, host.Role())
	})
}

func TestElection_HandleError_truncateAndPrepare(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		hostCommit int64
		wantLog    int64
	}{
		{
			name:       "truncate success commit at or below newPosition",
			hostCommit: truncateInstallBase - 8,
			wantLog:    truncateInstallBase - 8,
		},
		{
			name:       "truncate success commit above newPosition",
			hostCommit: truncateInstallBase + 64,
			wantLog:    truncateInstallBase,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := runTruncateInstall(t, truncateInstallErrs{}, tc.hostCommit)

			require.Equal(t, truncateInstallBase, r.e.AppendPosition())
			require.Equal(t, tc.wantLog, r.e.LogPosition())
			require.EqualError(t, r.e.LastError(), "election: truncating cluster log")
			require.Equal(t, election.ElectionStateInit, r.e.State())
		})
	}

	t.Run("truncate success zero commit prepare collapses append", func(t *testing.T) {
		t.Parallel()

		// Host commit 0 and prepare return of logPosition document the collapse path.
		r := runTruncateInstall(t, truncateInstallErrs{}, 0)
		require.Equal(t, truncateInstallBase, r.e.AppendPosition())
		require.Equal(t, int64(0), r.e.LogPosition())

		cfg, deps, clock, _, _ := testElectionDeps(t)
		cfg.IsNodeStartup = false
		cfg.LogPosition = r.e.LogPosition()
		cfg.AppendPosition = r.e.AppendPosition()
		deps.Host = &recordingElectionHost{commitPosition: 0}
		deps.Handler = &recordingAgentHandler{appendPositionReturn: r.e.LogPosition()}
		e := election.NewElection(cfg, deps)
		require.Equal(t, truncateInstallBase, e.AppendPosition())
		require.Equal(t, int64(0), e.LogPosition())

		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateCanvass, e.State())
		require.Equal(t, int64(0), e.AppendPosition())
		require.Equal(t, int64(0), e.LogPosition())
	})

	t.Run("truncate success stop-derived prepare", func(t *testing.T) {
		t.Parallel()

		const hostCommit = truncateInstallBase - 16
		const stopPos int64 = 180
		r := runTruncateInstall(t, truncateInstallErrs{}, hostCommit)
		require.Equal(t, hostCommit, r.e.LogPosition())
		require.Equal(t, truncateInstallBase, r.e.AppendPosition())

		cfg, deps, clock, _, _ := testElectionDeps(t)
		cfg.IsNodeStartup = false
		cfg.LogPosition = r.e.LogPosition()
		cfg.AppendPosition = r.e.AppendPosition()
		host := &recordingElectionHost{commitPosition: hostCommit}
		deps.Host = host
		deps.Handler = &recordingAgentHandler{appendPositionReturn: stopPos}
		e := election.NewElection(cfg, deps)

		require.Equal(t, 1, e.DoWork(clock.nowNs))
		require.Equal(t, election.ElectionStateCanvass, e.State())
		require.Equal(t, stopPos, e.AppendPosition())
		require.Equal(t, hostCommit, e.LogPosition())
	})
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

func TestHandleLeaderReplay_doneAtBound(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	spy := &spyLogReplay{position: testAppendPosition, done: true}
	host := &recordingElectionHost{
		clock: clock,
		newLogReplay: func(_, _ int64) election.LogReplay {
			return spy
		},
	}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateLeaderReplay)

	// Install tick.
	require.Positive(t, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderReplay, e.State())
	require.Equal(t, 0, spy.closeCalls)

	// Done at the append bound enters LeaderInit. logPosition equals the append
	// bound. The exit closes the helper. DoWork may be 0 with a done spy. State
	// and closeCalls are the contract.
	_ = e.DoWork(clock.nowNs + 1)
	require.Equal(t, election.ElectionStateLeaderInit, e.State())
	require.Equal(t, e.AppendPosition(), e.LogPosition())
	require.Equal(t, int64(testAppendPosition), e.LogPosition())
	require.Equal(t, 1, spy.closeCalls)

	// Second Close is a no-op after the pointer is cleared.
	e.Close()
	require.Equal(t, 1, spy.closeCalls)
}

func TestHandleLeaderReplay_doneShort(t *testing.T) {
	t.Parallel()

	replayed := int64(testAppendPosition - 50)
	cfg, deps, clock, _, _ := testElectionDeps(t)
	spy := &spyLogReplay{position: replayed, done: true}
	host := &recordingElectionHost{
		clock: clock,
		newLogReplay: func(_, _ int64) election.LogReplay {
			return spy
		},
	}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateLeaderReplay)

	// Seed a live log session so a fall-through publish path would fan out.
	election.SetLogSessionIDForTest(e, fixtureLogSessionID)

	// Install tick.
	require.Positive(t, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderReplay, e.State())
	appendBefore := e.AppendPosition()
	require.Equal(t, 0, spy.closeCalls)

	// Clear install-tick host side effects before the done tick.
	host.publishCommitPositionCalls = nil

	// Done short of the append bound enters Canvass. logPosition is the replayed
	// position. Append position is unchanged. The exit closes the helper and must
	// not publish leadership as a Follower. Incomplete replay must not call
	// HandleError.
	_ = e.DoWork(clock.nowNs + 1)
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, consensus.RoleFollower, host.Role())
	require.Equal(t, int64(replayed), e.LogPosition())
	require.Equal(t, appendBefore, e.AppendPosition())
	require.Equal(t, int64(testAppendPosition), e.AppendPosition())
	require.NotEqual(t, e.AppendPosition(), e.LogPosition())
	require.NoError(t, e.LastError())
	require.Equal(t, 1, spy.closeCalls)
	require.Empty(t, host.publishCommitPositionCalls)

	// Second Close is a no-op after the pointer is cleared.
	e.Close()
	require.Equal(t, 1, spy.closeCalls)
}

func TestHandleFollowerReplay_closesOnExit(t *testing.T) {
	t.Parallel()

	fx := newElectionFixture(t)
	spy := &spyLogReplay{position: testAppendPosition, done: true}
	fx.host.newLogReplay = func(_, _ int64) election.LogReplay {
		return spy
	}

	fx.doWork()
	require.Equal(t, election.ElectionStateCanvass, fx.election.State())

	fields := installNewLeadershipTermFields(fx.cfg, 1, fx.cfg.LeadershipTermID)
	fields.CommitPosition = 250
	fx.election.OnNewLeadershipTerm(fields)
	require.Equal(t, election.ElectionStateFollowerReplay, fx.election.State())

	// Install tick.
	fx.doWork()
	require.Equal(t, election.ElectionStateFollowerReplay, fx.election.State())
	require.Equal(t, 0, spy.closeCalls)

	// Done tick closes and routes to FollowerLogInit. DoWork may return 0 when
	// the spy reports no work. The exit path still runs.
	fx.doWork()
	require.Equal(t, election.ElectionStateFollowerLogInit, fx.election.State())
	require.Equal(t, 1, spy.closeCalls)

	fx.election.Close()
	require.Equal(t, 1, spy.closeCalls)
}

func TestHandleInit_resetsReplay(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)

	// Keep prepare from collapsing the log-to-append gap on first init.
	cfg.IsNodeStartup = true
	spy := &spyLogReplay{position: testLogPosition, done: false}
	host := &recordingElectionHost{
		clock: clock,
		newLogReplay: func(_, _ int64) election.LogReplay {
			return spy
		},
	}
	deps.Host = host
	deps.Handler = &recordingAgentHandler{
		appendPositionReturn: election.NullPosition,
	}
	e := election.NewElection(cfg, deps)

	// First init sets isFirstInit false.
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())

	// Install a live helper without finishing it.
	election.SetStateForTest(e, election.ElectionStateLeaderReplay)
	require.Positive(t, e.DoWork(clock.nowNs+1))
	require.Equal(t, election.ElectionStateLeaderReplay, e.State())
	require.Equal(t, 0, spy.closeCalls)

	// Force Init. Non-first branch must Close the helper.
	election.SetStateForTest(e, election.ElectionStateInit)
	require.Equal(t, 1, e.DoWork(clock.nowNs+2))
	require.Equal(t, 1, spy.closeCalls)

	// Pointer cleared: a second Close is a no-op.
	e.Close()
	require.Equal(t, 1, spy.closeCalls)
}

// Asserts re-entering Init clears the catchup join bound so the next install
// derives the bound from its own leadership term. Agent-side catchup teardown
// runs inside PrepareForNewLeadership and is covered by consensus tests.
func TestHandleInit_resetsCatchupJoinPosition(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	// First init sets isFirstInit false.
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())

	// Install with the leader log ahead so the join bound is recorded.
	fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
	fields.LogPosition = cfg.AppendPosition + 100
	e.OnNewLeadershipTerm(fields)
	require.Equal(
		t,
		cfg.AppendPosition+100,
		election.CatchupJoinPositionForTest(e),
	)

	// Force Init. Non-first branch must clear the bound.
	election.SetStateForTest(e, election.ElectionStateInit)
	require.Equal(t, 1, e.DoWork(clock.nowNs+1))
	require.Equal(t, election.NullPosition, election.CatchupJoinPositionForTest(e))
}

func TestHandleInit_resetsReplication(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	// First init sets isFirstInit false.
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())

	lr := &noopLogReplicationForTest{}
	election.SetLogReplicationForTest(e, lr)

	// Installed helper still receives signals before the non-first init.
	e.OnRecordingSignal(1, 2, 3, election.RecordingSignal(4))
	require.True(t, lr.called)
	lr.called = false

	election.SetStateForTest(e, election.ElectionStateInit)
	require.Equal(t, 1, e.DoWork(clock.nowNs+1))

	// Cleared helper receives no further signals.
	e.OnRecordingSignal(1, 2, 3, election.RecordingSignal(4))
	require.False(t, lr.called)
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
		wantLastError string
	}{
		{
			name:         "image not ready before timeout waits",
			joinPosition: testLogPosition,
			wantState:    election.ElectionStateFollowerLogAwait,
		},
		{
			name:          "image not ready at timeout resets to init",
			joinPosition:  testLogPosition,
			deltaNs:       timeoutNs,
			wantWork:      1,
			wantState:     election.ElectionStateInit,
			wantLastError: "election: failed to join live log",
		},
		{
			name:          "join position mismatch resets to init",
			joinOK:        true,
			joinPosition:  testLogPosition + 1,
			wantWork:      1,
			wantState:     election.ElectionStateInit,
			wantLastError: "election: log join position mismatch",
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
			wantLastError: "election: failed to join live log as follower",
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
			if tc.wantLastError == "" {
				require.NoError(t, e.LastError())
			} else {
				require.EqualError(t, e.LastError(), tc.wantLastError)
			}

			require.Len(t, handler.joinFollowerParams, tc.wantJoinCalls)
			if tc.wantJoinCalls == 0 {
				return
			}
			require.Equal(t, cfg.LogPosition, handler.joinFollowerParams[0].LogJoinPosition)
			require.False(t, handler.joinFollowerParams[0].IsBoundRejoin)

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

// TestHandleFollowerLogInit_rejoinsBoundSession asserts LOG_INIT re-joins a
// bound leader session after prepare stopped the recording. Cases cover ready,
// await on missing image, await on refuse, and init on join error.
func TestHandleFollowerLogInit_rejoinsBoundSession(t *testing.T) {
	t.Parallel()

	const sessionID int32 = 7

	for _, tc := range []struct {
		name          string
		joinReadyOK   bool
		joinAccepted  bool
		joinErr       error
		wantState     election.ElectionState
		wantJoinCalls int
	}{
		{
			name:          "bound session re-joins and enters ready",
			joinReadyOK:   true,
			joinAccepted:  true,
			wantState:     election.ElectionStateFollowerReady,
			wantJoinCalls: 1,
		},
		{
			name:      "bound session without open image awaits fresh image",
			wantState: election.ElectionStateFollowerLogAwait,
		},
		{
			name:          "refused join awaits fresh image",
			joinReadyOK:   true,
			wantState:     election.ElectionStateFollowerLogAwait,
			wantJoinCalls: 1,
		},
		{
			name:          "join error resets to init",
			joinReadyOK:   true,
			joinErr:       errors.New("join failed"),
			wantState:     election.ElectionStateInit,
			wantJoinCalls: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			host := &recordingElectionHost{
				clock:             clock,
				logJoined:         true,
				followerLogJoinOK: tc.joinReadyOK,

				// Offset from the election log position on purpose. Bound-session
				// join must not compare join positions.
				followerLogJoinPosition: cfg.LogPosition + 50,
			}
			deps.Host = host
			handler := &recordingAgentHandler{
				joinFollowerReturn: tc.joinAccepted,
				joinFollowerErr:    tc.joinErr,
			}
			deps.Handler = handler
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, election.ElectionStateFollowerLogInit)
			election.SetLogSessionIDForTest(e, sessionID)

			require.Equal(t, 1, e.DoWork(clock.nowNs))
			require.Equal(t, tc.wantState, e.State())

			require.Len(t, host.followerLogJoinReadyCalls, 1)
			require.Len(t, handler.joinFollowerParams, tc.wantJoinCalls)
			if tc.wantJoinCalls == 0 {
				return
			}
			require.Equal(t, sessionID, handler.joinFollowerParams[0].LogSessionID)
			require.False(t, handler.joinFollowerParams[0].IsLeaderStartup)
			require.True(t, handler.joinFollowerParams[0].IsBoundRejoin)
		})
	}
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
			// branch.
			fx := newElectionFixture(
				t,
				withPositions(testLeadershipTermID, testAppendPosition, testAppendPosition),
			)
			fx.host.catchupNearLive = tc.nearLive

			// Empty catchup endpoint keeps the not-near-live case on the stall
			// branch.
			catchupJoinPosition := driveToFollowerCatchupInit(fx, "")

			require.Equal(t, tc.wantWork, fx.doWork())
			require.Equal(t, tc.wantState, fx.election.State())
			require.Equal(t, []int64{catchupJoinPosition}, fx.host.catchupNearLiveCalls)

			// Near-live skips to log-init without catchup start. Not near-live
			// stalls when CatchupEndpoint is empty (no send until timeout).
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
	}{
		{
			name:           "notified commit",
			commitPosition: 500,
			pollResult:     5,
		},
		{
			name:           "zero commit falls back to join position",
			commitPosition: 0,
			joinAhead:      100,
			pollResult:     2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			host := &recordingElectionHost{clock: clock, catchupPollResult: tc.pollResult}
			deps.Host = host
			e, joinPos := forceFollowerCatchup(t, cfg, deps, tc.commitPosition, tc.joinAhead)
			require.Equal(t, tc.commitPosition, e.NotifiedCommitPosition())

			t0 := clock.nowNs
			require.Equal(t, tc.pollResult, e.DoWork(t0))
			require.Equal(t, election.ElectionStateFollowerCatchup, e.State())

			// Zero notified commit falls back to the catchup join bound.
			wantLimit := tc.commitPosition
			if wantLimit == 0 {
				wantLimit = joinPos
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

// Near-live catchup adds the live destination once and counts the attempt as work.
// A second tick with live already set does not re-add.
func TestHandleFollowerCatchup_addsLiveWhenNearLive(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	const joinAhead int64 = 200
	const notifiedCommit int64 = 150
	host := &recordingElectionHost{
		clock:             clock,
		catchupPollResult: 2,
		catchupNearLive:   true,
		// Commit below join: stay in CATCHUP. Observe the live add alone.
		commitPosition: cfg.AppendPosition,
	}
	deps.Host = host
	e, joinPos := forceFollowerCatchup(t, cfg, deps, notifiedCommit, joinAhead)

	t0 := clock.nowNs
	// Poll (2) + live add (1).
	require.Equal(t, 3, e.DoWork(t0))
	require.Equal(t, election.ElectionStateFollowerCatchup, e.State())
	require.Equal(t, 1, host.addLiveLogDestinationCalls)
	require.NotEmpty(t, host.LiveLogDestination())
	require.Equal(t, []int64{max(notifiedCommit, joinPos)}, host.catchupNearLiveCalls)

	// Live already set: no second add.
	require.Equal(t, 2, e.DoWork(t0+1))
	require.Equal(t, 1, host.addLiveLogDestinationCalls)
	require.Len(t, host.catchupNearLiveCalls, 1)
}

// Failed live add leaves LiveLogDestination empty. The next tick retries.
func TestHandleFollowerCatchup_liveAddFailureRetries(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	const joinAhead int64 = 200
	host := &recordingElectionHost{
		clock:                         clock,
		catchupPollResult:             1,
		catchupNearLive:               true,
		addLiveLogDestinationFailLeft: 1,
		commitPosition:                cfg.AppendPosition,
	}
	deps.Host = host
	e, _ := forceFollowerCatchup(t, cfg, deps, 0, joinAhead)

	t0 := clock.nowNs
	// First tick: poll + failed add. LiveLogDestination stays empty.
	require.Equal(t, 2, e.DoWork(t0))
	require.Equal(t, 1, host.addLiveLogDestinationCalls)
	require.Empty(t, host.LiveLogDestination())
	require.Equal(t, election.ElectionStateFollowerCatchup, e.State())

	// Second tick: near-live re-check. Add succeeds.
	require.Equal(t, 2, e.DoWork(t0+1))
	require.Equal(t, 2, host.addLiveLogDestinationCalls)
	require.NotEmpty(t, host.LiveLogDestination())
	require.Len(t, host.catchupNearLiveCalls, 2)

	// Third tick: live set. No further add.
	require.Equal(t, 1, e.DoWork(t0+2))
	require.Equal(t, 2, host.addLiveLogDestinationCalls)
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

// forceFollowerCatchup installs a NewLeadershipTerm that records the notified
// commit and catchup join bounds, then injects FOLLOWER_CATCHUP.
//
// joinAhead is added to cfg.AppendPosition for fields.LogPosition. Caller must
// wire deps.Host before the call.
//
// Returns: the election and the recorded join position.
func forceFollowerCatchup(
	t *testing.T,
	cfg election.ElectionConfig,
	deps election.ElectionDeps,
	commitPosition, joinAhead int64,
) (*election.Election, int64) {
	t.Helper()

	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCanvass)
	joinPos := cfg.AppendPosition + joinAhead
	fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
	fields.CommitPosition = commitPosition
	fields.LogPosition = joinPos
	e.OnNewLeadershipTerm(fields)
	election.SetStateForTest(e, election.ElectionStateFollowerCatchup)
	return e, joinPos
}

// driveToFollowerCatchupInit installs a leader term whose live log position is
// ahead of this member, routing the follower through the no-replay branch into
// FOLLOWER_CATCHUP_INIT. Returns the catchup join position.
//
// Sets catchupEndpoint on this member and clears recorded publisher traffic, so
// the caller sees catchup sends only.
func driveToFollowerCatchupInit(fx *electionFixture, catchupEndpoint string) int64 {
	fx.t.Helper()

	e := fx.election
	fx.doWork()
	require.Equal(fx.t, election.ElectionStateCanvass, e.State())

	catchupJoinPosition := fx.cfg.AppendPosition + 100
	fields := installNewLeadershipTermFields(fx.cfg, 1, fx.cfg.LeadershipTermID)
	fields.LogPosition = catchupJoinPosition
	e.OnNewLeadershipTerm(fields)
	require.Equal(fx.t, election.ElectionStateFollowerReplay, e.State())

	fx.doWork()
	require.Equal(fx.t, election.ElectionStateFollowerCatchupInit, e.State())

	thisMember := fx.member(fx.cfg.MemberID)
	require.NotNil(fx.t, thisMember)
	thisMember.CatchupEndpoint = catchupEndpoint

	fx.publisher.Reset()
	return catchupJoinPosition
}

// followerLogReplicationFields builds an installing NewLeadershipTerm whose
// successor term base sits past this node's append position.
func followerLogReplicationFields(
	cfg election.ElectionConfig,
	nextTermBase int64,
	nextLogPosition int64,
) codecs.NewLeadershipTermFields {
	fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
	fields.TermBaseLogPosition = cfg.AppendPosition + 100
	fields.NextLeadershipTermID = cfg.LeadershipTermID + 1
	fields.NextTermBaseLogPosition = nextTermBase
	fields.NextLogPosition = nextLogPosition
	return fields
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
	e.OnNewLeadershipTerm(followerLogReplicationFields(cfg, nextTermBase, nextLogPosition))
	require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
}

// hostWithLogReplication wires a recordingElectionHost whose replication
// factory returns the given helper.
func hostWithLogReplication(
	clock *FakeClock,
	lr election.LogReplication,
) *recordingElectionHost {
	return &recordingElectionHost{
		clock: clock,
		newLogReplication: func(_, _ string, _, _, _ int64) election.LogReplication {
			return lr
		},
	}
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
		require.EqualError(t, e.LastError(), "election: timeout awaiting commit position")
	})

	// Matching leader traffic refreshes the replication deadline only while a
	// helper is installed. With a nil factory the install-time deadline fires
	// unchanged, so lag still returns to Init under leader heartbeats.
	commitRefresh := func(e *election.Election, cfg election.ElectionConfig) {
		e.OnCommitPosition(
			codecs.CommitPositionFields{
				LeadershipTermID: cfg.LeadershipTermID,
				LogPosition:      0,
				LeaderMemberID:   1,
			},
		)
	}
	lateTermRefresh := func(e *election.Election, cfg election.ElectionConfig) {
		e.OnNewLeadershipTerm(
			followerLogReplicationFields(cfg, cfg.AppendPosition+50, election.NullPosition),
		)
	}
	deadlineCases := []struct {
		name       string
		withHelper bool
		refresh    func(e *election.Election, cfg election.ElectionConfig)
	}{
		{
			name:    "leader commit position does not extend deadline without helper",
			refresh: commitRefresh,
		},
		{
			name:       "leader commit position extends deadline with helper",
			withHelper: true,
			refresh:    commitRefresh,
		},
		{
			name:    "late leadership term does not extend deadline without helper",
			refresh: lateTermRefresh,
		},
		{
			name:       "late leadership term extends deadline with helper",
			withHelper: true,
			refresh:    lateTermRefresh,
		},
	}
	for _, tc := range deadlineCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, clock, _, _ := testElectionDeps(t)
			if tc.withHelper {
				deps.Host = hostWithLogReplication(clock, &countingLogReplicationForTest{})
			}
			e := election.NewElection(cfg, deps)
			driveToFollowerLogReplication(t, e, cfg, cfg.AppendPosition+50, election.NullPosition)

			// Install tick: one unit of work builds the helper when the factory is wired.
			t0 := clock.nowNs
			installWork := 0
			if tc.withHelper {
				installWork = 1
			}
			require.Equal(t, installWork, e.DoWork(t0))
			require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())

			refreshNs := t0 + cfg.LeaderHeartbeatTimeoutNs - 1
			clock.nowNs = refreshNs
			tc.refresh(e, cfg)

			if !tc.withHelper {
				require.Equal(t, 1, e.DoWork(t0+cfg.LeaderHeartbeatTimeoutNs))
				require.Equal(t, election.ElectionStateInit, e.State())
				return
			}

			// Refresh moved the deadline one heartbeat past refreshNs.
			require.Equal(t, 0, e.DoWork(t0+cfg.LeaderHeartbeatTimeoutNs))
			require.Equal(t, election.ElectionStateFollowerLogReplication, e.State())
			require.Equal(t, 1, e.DoWork(refreshNs+cfg.LeaderHeartbeatTimeoutNs))
			require.Equal(t, election.ElectionStateInit, e.State())
		})
	}
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

// truncateInstallErrs holds the inject errors for one truncate install round.
type truncateInstallErrs struct {
	truncate error
	commit   error
	force    error
}

// truncateInstallResult holds the election, spies, and pre-install tracked
// positions after one OnNewLeadershipTerm that took the truncate path.
type truncateInstallResult struct {
	e            *election.Election
	host         *recordingElectionHost
	spy          *recordingLogCoherencySpy
	termID       int64
	appendBefore int64
	termBefore   int64
	fields       codecs.NewLeadershipTermFields
}

const truncateInstallBase int64 = 128

// runTruncateInstall calls OnNewLeadershipTerm with next-term base below the
// append position so the install takes the truncate path.
//
// hostCommit seeds Host.CommitPosition. HandleError syncs logPosition from that
// value. On truncate success, logPosition ends capped at the truncate end when
// host commit is higher.
func runTruncateInstall(
	t *testing.T,
	errs truncateInstallErrs,
	hostCommit int64,
) truncateInstallResult {
	t.Helper()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.LogPosition = cfg.AppendPosition
	host := &recordingElectionHost{
		clock:               clock,
		localLogRecordingID: 42,
		truncateErr:         errs.truncate,
		commitPosition:      hostCommit,
	}
	deps.Host = host
	spy := &recordingLogCoherencySpy{commitErr: errs.commit, forceErr: errs.force}
	deps.RecordingLog = spy

	// Init reruns prepare on a retry drive; a zero return would clobber the
	// append position and skip the truncate branch on the second install.
	deps.Handler = &recordingAgentHandler{appendPositionReturn: election.NullPosition}
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCanvass)

	result := truncateInstallResult{
		e:            e,
		host:         host,
		spy:          spy,
		termID:       cfg.LeadershipTermID,
		appendBefore: e.AppendPosition(),
		termBefore:   election.LogLeadershipTermIDForTest(e),
	}
	require.Less(t, truncateInstallBase, result.appendBefore)

	fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
	fields.NextLeadershipTermID = cfg.LeadershipTermID + 1
	fields.NextTermBaseLogPosition = truncateInstallBase
	result.fields = fields
	e.OnNewLeadershipTerm(fields)
	return result
}

func TestOnNewLeadershipTerm_installRoutes(t *testing.T) {
	t.Parallel()

	t.Run("next term base below append truncates and resets", func(t *testing.T) {
		t.Parallel()

		// Seed Host.CommitPosition at the truncate end so post-HandleError logPosition
		// stays at the truncated bound (not the zero default commit).
		r := runTruncateInstall(t, truncateInstallErrs{}, truncateInstallBase)

		require.Equal(t, []truncateCall{{
			leadershipTermID: r.termID,
			logPosition:      truncateInstallBase,
		}}, r.host.truncateCalls)
		require.Equal(t, []commitLogPositionCall{{
			leadershipTermID: r.termID,
			logPosition:      truncateInstallBase,
		}}, r.spy.commitCalls)
		require.Equal(t, 1, r.spy.forceCalls)
		require.Empty(t, r.spy.calls)
		require.Equal(t, truncateInstallBase, r.e.AppendPosition())
		require.Equal(t, truncateInstallBase, r.e.LogPosition())
		require.EqualError(t, r.e.LastError(), "election: truncating cluster log")
		require.Equal(t, r.termBefore, election.LogLeadershipTermIDForTest(r.e))
		require.Equal(t, election.ElectionStateInit, r.e.State())
	})

	t.Run("announced term above log term truncates on log term", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		cfg.LogPosition = cfg.AppendPosition
		host := &recordingElectionHost{
			clock:               clock,
			localLogRecordingID: 42,
			commitPosition:      truncateInstallBase,
		}
		deps.Host = host
		spy := &recordingLogCoherencySpy{}
		deps.RecordingLog = spy
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)

		// Rollback shape: the leader announces a higher term while the log
		// still ends in the anchored log term. Commit and truncate must use
		// the log term, not the announced one.
		fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID+1)
		fields.NextLeadershipTermID = cfg.LeadershipTermID + 1
		fields.NextTermBaseLogPosition = truncateInstallBase
		e.OnNewLeadershipTerm(fields)

		require.Equal(t, []truncateCall{{
			leadershipTermID: cfg.LeadershipTermID,
			logPosition:      truncateInstallBase,
		}}, host.truncateCalls)
		require.Equal(t, []commitLogPositionCall{{
			leadershipTermID: cfg.LeadershipTermID,
			logPosition:      truncateInstallBase,
		}}, spy.commitCalls)
		require.Equal(t, truncateInstallBase, e.AppendPosition())
		require.Equal(t, truncateInstallBase, e.LogPosition())
		require.EqualError(t, e.LastError(), "election: truncating cluster log")
		require.Equal(t, election.ElectionStateInit, e.State())
	})

	t.Run("truncate failure keeps append high", func(t *testing.T) {
		t.Parallel()

		const hostCommit = truncateInstallBase
		tests := []struct {
			name              string
			errs              truncateInstallErrs
			wantForceCalls    int
			wantTruncateCalls int
			retryConverges    bool
		}{
			{
				// Recording-log commit runs before archive truncate. Host refuse
				// still applies commit and force. The next round re-runs commit,
				// force, and truncate.
				name:              "host refuses",
				errs:              truncateInstallErrs{truncate: errors.New("host refuse")},
				wantForceCalls:    1,
				wantTruncateCalls: 1,
				retryConverges:    true,
			},
			{
				name: "term commit fails",
				errs: truncateInstallErrs{commit: recordinglog.ErrUnknownLeadershipTermID},
			},
			{
				name:           "force fails",
				errs:           truncateInstallErrs{force: errors.New("force failed")},
				wantForceCalls: 1,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				r := runTruncateInstall(t, tc.errs, hostCommit)

				require.Len(t, r.spy.commitCalls, 1)
				require.Equal(t, tc.wantForceCalls, r.spy.forceCalls)
				require.Len(t, r.host.truncateCalls, tc.wantTruncateCalls)
				require.Equal(t, r.appendBefore, r.e.AppendPosition())

				// Half-fail still syncs logPosition from host commit. The inject error reaches
				// HandleError unwrapped. Each row sets exactly one inject.
				require.Equal(t, hostCommit, r.e.LogPosition())
				wantErr := cmp.Or(tc.errs.truncate, tc.errs.commit, tc.errs.force)
				require.ErrorIs(t, r.e.LastError(), wantErr)
				require.Equal(t, election.ElectionStateInit, r.e.State())

				if !tc.retryConverges {
					return
				}

				// Cleared refusal: Init drives back to Canvass, the same install
				// re-runs commit, force, and truncate, then lowers positions.
				r.host.truncateErr = nil
				r.e.DoWork(1_000_000_000)
				require.Equal(t, election.ElectionStateCanvass, r.e.State())
				r.e.OnNewLeadershipTerm(r.fields)

				require.Equal(t, []truncateCall{
					{leadershipTermID: r.termID, logPosition: truncateInstallBase},
					{leadershipTermID: r.termID, logPosition: truncateInstallBase},
				}, r.host.truncateCalls)
				require.Len(t, r.spy.commitCalls, 2)
				require.Equal(t, 2, r.spy.forceCalls)
				require.Equal(t, truncateInstallBase, r.e.AppendPosition())
				require.Equal(t, truncateInstallBase, r.e.LogPosition())
				require.Equal(t, election.ElectionStateInit, r.e.State())
			})
		}
	})

	t.Run("truncate with a null log term id skips the commit", func(t *testing.T) {
		t.Parallel()

		cfg, deps, clock, _, _ := testElectionDeps(t)
		cfg.LogPosition = cfg.AppendPosition
		cfg.LeadershipTermID = election.NullValue
		host := &recordingElectionHost{
			clock:               clock,
			localLogRecordingID: 42,
			commitPosition:      128,
		}
		deps.Host = host
		spy := &recordingLogCoherencySpy{}
		deps.RecordingLog = spy
		e := election.NewElection(cfg, deps)
		election.SetLogLeadershipTermIDForTest(e, election.NullValue)
		election.SetStateForTest(e, election.ElectionStateCanvass)

		const newBase int64 = 128
		fields := installNewLeadershipTermFields(cfg, 1, election.NullValue)
		fields.LogLeadershipTermID = election.NullValue
		fields.NextLeadershipTermID = 1
		fields.NextTermBaseLogPosition = newBase
		e.OnNewLeadershipTerm(fields)

		require.Len(t, host.truncateCalls, 1)
		require.Empty(t, spy.commitCalls)
		require.Equal(t, 0, spy.forceCalls)
		require.Equal(t, newBase, e.AppendPosition())
		require.Equal(t, newBase, e.LogPosition())
		require.Equal(t, election.ElectionStateInit, e.State())
	})

	t.Run("lagging append without successor term resets", func(t *testing.T) {
		t.Parallel()

		const hostCommit int64 = 88
		cfg, deps, clock, _, _ := testElectionDeps(t)
		host := &recordingElectionHost{clock: clock, commitPosition: hostCommit}
		deps.Host = host
		e := election.NewElection(cfg, deps)
		election.SetStateForTest(e, election.ElectionStateCanvass)

		fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
		fields.TermBaseLogPosition = cfg.AppendPosition + 100
		e.OnNewLeadershipTerm(fields)

		require.Empty(t, host.truncateCalls)
		require.Equal(t, cfg.AppendPosition, e.AppendPosition())
		require.Equal(t, hostCommit, e.LogPosition())
		require.EqualError(t, e.LastError(), "election: invalid newLeadershipTerm")
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

func TestHandleInit_prepareErrorFailsClosed(t *testing.T) {
	t.Parallel()

	const hostCommit int64 = 90
	prepareErr := errors.New("prepare stop failed")
	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = false
	host := &recordingElectionHost{commitPosition: hostCommit}
	deps.Host = host
	handler := &recordingAgentHandler{
		appendPositionReturn: cfg.AppendPosition + 1,
		prepareErr:           prepareErr,
	}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)

	seedAppend := e.AppendPosition()
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Equal(t, seedAppend, e.AppendPosition())
	require.Equal(t, prepareErr, e.LastError())
	require.Equal(t, hostCommit, e.LogPosition())
	require.Equal(t, 1, handler.prepareCalls)

	// Bounded attempt is retried rather than abandoned.
	require.Equal(t, 1, e.DoWork(clock.nowNs+1))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Equal(t, 2, handler.prepareCalls)
	require.Equal(t, seedAppend, e.AppendPosition())
	require.Equal(t, prepareErr, e.LastError())
	require.Equal(t, hostCommit, e.LogPosition())
}

func TestHandleInit_prepareSuccessAssignsAppend(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = false
	preparedAppend := cfg.AppendPosition + 50
	handler := &recordingAgentHandler{appendPositionReturn: preparedAppend}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, preparedAppend, e.AppendPosition())
	require.Equal(t, 1, handler.prepareCalls)
}

func TestHandleInit_prepareNullPositionLeavesAppendUnchanged(t *testing.T) {
	t.Parallel()

	// Sentinel guard: NullPosition from prepare must not overwrite appendPosition.
	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = false
	handler := &recordingAgentHandler{appendPositionReturn: election.NullPosition}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)

	seedAppend := e.AppendPosition()
	require.NotEqual(t, election.NullPosition, seedAppend)
	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateCanvass, e.State())
	require.Equal(t, seedAppend, e.AppendPosition())
	require.Equal(t, 1, handler.prepareCalls)
}

func TestHandleLeaderInit_joinLeaderErrorFailsClosed(t *testing.T) {
	t.Parallel()

	joinErr := errors.New("join leader recording failed")
	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{clock: clock, localLogRecordingID: 42}
	deps.Host = host
	spy := &recordingLogCoherencySpy{}
	deps.RecordingLog = spy
	handler := &recordingAgentHandler{joinLeaderErr: joinErr}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateLeaderInit)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Len(t, handler.joinLeaderCalls, 1)
	require.Empty(t, spy.calls)
}

func TestHandleLeaderInit_joinLeaderSuccessWritesTerm(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, epochClock, _ := testElectionDeps(t)
	host := &recordingElectionHost{clock: clock, localLogRecordingID: 42}
	deps.Host = host
	spy := &recordingLogCoherencySpy{}
	deps.RecordingLog = spy
	handler := &recordingAgentHandler{}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateLeaderInit)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateLeaderReady, e.State())
	require.Len(t, handler.joinLeaderCalls, 1)
	require.Equal(t, []recordinglog.EnsureCoherentParams{{
		RecordingID:                42,
		InitialLogLeadershipTermID: cfg.LeadershipTermID,
		InitialTermBaseLogPosition: cfg.TermBaseLogPosition,
		LeadershipTermID:           cfg.LeadershipTermID,
		TermBaseLogPosition:        cfg.LogPosition,
		LogPosition:                election.NullValue,
		Timestamp:                  epochClock.nowMs,
	}}, spy.calls)
}

func TestHandleFollowerLogAwait_joinFollowerErrorFailsClosed(t *testing.T) {
	t.Parallel()

	joinErr := errors.New("join follower recording failed")
	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{
		clock:                   clock,
		localLogRecordingID:     int64(election.NullValue),
		followerLogJoinOK:       true,
		followerLogJoinPosition: cfg.LogPosition,
	}
	deps.Host = host
	spy := &recordingLogCoherencySpy{}
	deps.RecordingLog = spy
	handler := &recordingAgentHandler{
		joinFollowerReturn: true,
		joinFollowerErr:    joinErr,
	}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerLogAwait)

	require.Equal(t, 1, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Equal(t, 1, handler.joinFollowerCalls)
	require.Empty(t, spy.calls)
}

func TestHandleFollowerLogAwait_notReadyRetriesUntilTimeout(t *testing.T) {
	t.Parallel()

	// (false, nil) keeps the retry-until-heartbeat-timeout path.
	cfg, deps, clock, _, _ := testElectionDeps(t)
	timeoutNs := cfg.LeaderHeartbeatTimeoutNs
	host := &recordingElectionHost{
		clock:                   clock,
		followerLogJoinOK:       true,
		followerLogJoinPosition: cfg.LogPosition,
	}
	deps.Host = host
	handler := &recordingAgentHandler{joinFollowerReturn: false}
	deps.Handler = handler
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerLogAwait)

	require.Equal(t, 0, e.DoWork(clock.nowNs))
	require.Equal(t, election.ElectionStateFollowerLogAwait, e.State())
	require.Equal(t, 1, handler.joinFollowerCalls)

	require.Equal(t, 1, e.DoWork(clock.nowNs+timeoutNs))
	require.Equal(t, election.ElectionStateInit, e.State())
}

func TestOnNewLeadershipTerm_initInstallsNothing(t *testing.T) {
	t.Parallel()

	// While stuck in Init, acceptsConsensusInbound is false, so a divergent
	// NextTermBaseLogPosition installs nothing and never truncates.
	cfg, deps, clock, _, _ := testElectionDeps(t)
	host := &recordingElectionHost{clock: clock}
	deps.Host = host
	e := election.NewElection(cfg, deps)
	require.Equal(t, election.ElectionStateInit, e.State())

	fields := installNewLeadershipTermFields(cfg, 1, cfg.LeadershipTermID)
	fields.NextLeadershipTermID = cfg.LeadershipTermID + 1
	fields.NextTermBaseLogPosition = cfg.AppendPosition - 50
	e.OnNewLeadershipTerm(fields)

	require.Empty(t, host.truncateCalls)
	require.Equal(t, election.ElectionStateInit, e.State())
	require.Equal(t, cfg.AppendPosition, e.AppendPosition())
	require.Nil(t, e.Leader())
}
