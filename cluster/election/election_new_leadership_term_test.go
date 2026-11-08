package election_test

import (
	"cmp"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/election"
	"github.com/warrenguy/luas/cluster/recordinglog"
)

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
