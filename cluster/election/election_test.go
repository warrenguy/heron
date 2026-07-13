package election_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
)

func TestSnapshotForTest_capturesObservableState(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	pub := deps.Publisher.(*election.SbePublisher)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCandidateBallot)
	pub.RequestVote(election.PublicationTarget{MemberID: 1}, 1, 2, 3, 4)

	snap := election.SnapshotForTest(e, consensus.RoleCandidate, pub)

	require.Equal(t, election.ElectionStateCandidateBallot, snap.State)
	require.Equal(t, consensus.RoleCandidate, snap.Role)
	require.Equal(t, cfg.LeadershipTermID, snap.LeadershipTermID)
	require.Equal(t, cfg.LeadershipTermID, snap.CandidateTermID)
	require.Len(t, snap.MemberSnapshots, 3)
	require.Equal(t, int32(0), snap.MemberSnapshots[0].ID)
	require.Equal(t, 1, snap.PublishedCount)
}

func TestElection_DoWork_closedReturnsZero(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	election.SetStateForTest(e, election.ElectionStateClosed)
	require.Equal(t, 0, e.DoWork(1_000))
}

func TestElection_DoWork_panicsOnUnknownState(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	election.SetStateForTest(e, election.ElectionState(99))
	require.PanicsWithValue(t, "election: unknown state ElectionState(99)", func() {
		e.DoWork(1_000)
	})
}

func TestNewElection_initializesFields(t *testing.T) {
	t.Parallel()

	cfg, deps, clock, _, _ := testElectionDeps(t)
	cfg.IsNodeStartup = true
	cfg.GracefulClosedLeaderID = 7
	observer := &recordingStateObserver{}
	deps.StateObserver = observer

	e := election.NewElection(cfg, deps)

	require.Equal(t, election.ElectionStateInit, e.State())
	require.Equal(t, cfg.LeadershipTermID, e.LeadershipTermID())
	require.Equal(t, cfg.LogPosition, e.LogPosition())
	require.Equal(t, cfg.AppendPosition, e.AppendPosition())
	require.Equal(t, cfg.MemberID, e.ThisMemberID())
	require.Nil(t, e.Leader())
	require.Equal(t, int32(cnc.NullValue), e.LogSessionID())
	require.Equal(t, int64(0), e.NotifiedCommitPosition())
	require.Equal(t, []election.ElectionState{election.ElectionStateInit}, observer.states)

	initialUpdate := clock.nowNs - int64(24*time.Hour)
	require.Equal(t, initialUpdate, election.TimeOfLastUpdateNsForTest(e))
	require.Equal(t, initialUpdate, election.TimeOfLastCommitPositionUpdateNsForTest(e))
}

func TestElectionConfig_defaults(t *testing.T) {
	t.Parallel()

	require.Equal(t, election.DefaultElectionConfig(), election.ElectionTestConfig())

	cfg := election.DefaultElectionConfig()
	require.Equal(t, int64(1_000_000_000), cfg.ElectionTimeoutNs)
	require.Equal(t, int64(100_000_000), cfg.ElectionStatusIntervalNs)
	require.Equal(t, int64(60_000_000_000), cfg.StartupCanvassTimeoutNs)
	require.Equal(t, int64(10_000_000_000), cfg.LeaderHeartbeatTimeoutNs)
	require.Equal(t, int64(200_000_000), cfg.LeaderHeartbeatIntervalNs)
	require.NoError(t, election.ValidateElectionConfig(cfg))
}

func TestValidateElectionConfig_rejectsZeroTimeouts(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		mut  func(*election.ElectionConfig)
		msg  string
	}{
		{
			"zero ElectionTimeoutNs",
			func(c *election.ElectionConfig) { c.ElectionTimeoutNs = 0 },
			"ElectionTimeoutNs must be positive",
		},
		{
			"zero ElectionStatusIntervalNs",
			func(c *election.ElectionConfig) { c.ElectionStatusIntervalNs = 0 },
			"ElectionStatusIntervalNs must be positive",
		},
		{
			"zero LeaderHeartbeatTimeoutNs",
			func(c *election.ElectionConfig) { c.LeaderHeartbeatTimeoutNs = 0 },
			"LeaderHeartbeatTimeoutNs must be positive",
		},
		{
			"zero StartupCanvassTimeoutNs",
			func(c *election.ElectionConfig) { c.StartupCanvassTimeoutNs = 0 },
			"StartupCanvassTimeoutNs must be positive",
		},
		{
			"zero LeaderHeartbeatIntervalNs",
			func(c *election.ElectionConfig) { c.LeaderHeartbeatIntervalNs = 0 },
			"LeaderHeartbeatIntervalNs must be positive",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := election.DefaultElectionConfig()
			tc.mut(&cfg)
			err := election.ValidateElectionConfig(cfg)
			require.EqualError(t, err, "election: "+tc.msg)
		})
	}
}

func TestNewElection_rejectsZeroTimeout(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	cfg.ElectionTimeoutNs = 0

	require.PanicsWithValue(t, "election: ElectionTimeoutNs must be positive", func() {
		election.NewElection(cfg, deps)
	})
}

func TestNewElection_validationPanics(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)

	panics := []struct {
		name string
		mut  func(*election.ElectionDeps)
		msg  string
	}{
		{"nil Handler", func(d *election.ElectionDeps) { d.Handler = nil }, "nil Handler"},
		{"nil Host", func(d *election.ElectionDeps) { d.Host = nil }, "nil Host"},
		{"nil Publisher", func(d *election.ElectionDeps) { d.Publisher = nil }, "nil Publisher"},
		{"nil NanoClock", func(d *election.ElectionDeps) { d.NanoClock = nil }, "nil NanoClock"},
		{"nil EpochClock", func(d *election.ElectionDeps) { d.EpochClock = nil }, "nil EpochClock"},
		{"nil NodeState", func(d *election.ElectionDeps) { d.NodeState = nil }, "nil NodeState"},
		{"nil ThisMember", func(d *election.ElectionDeps) { d.ThisMember = nil }, "nil ThisMember"},
		{"empty Members", func(d *election.ElectionDeps) { d.Members = nil }, "empty Members"},
		{
			"duplicate member id",
			func(d *election.ElectionDeps) {
				m := &election.Member{ID: 0}
				d.Members = []*election.Member{m, m}
			},
			"duplicate member id",
		},
		{
			"ThisMember not in Members",
			func(d *election.ElectionDeps) { d.ThisMember = &election.Member{ID: 99} },
			"ThisMember not in Members",
		},
	}

	cfgMismatch := cfg
	cfgMismatch.MemberID = 99
	require.PanicsWithValue(t, "election: MemberID mismatch", func() {
		election.NewElection(cfgMismatch, deps)
	})

	for _, tc := range panics {
		t.Run(tc.name, func(t *testing.T) {
			d := deps
			tc.mut(&d)
			require.PanicsWithValue(t, "election: "+tc.msg, func() {
				election.NewElection(cfg, d)
			})
		})
	}
}
