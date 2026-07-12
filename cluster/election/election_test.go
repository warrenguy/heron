package election_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/election"
)

func TestElection_DoWork_allStatesReturnZero(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	for code := range int64(election.ElectionStateNameCount()) {
		s, err := election.ElectionStateFromCode(code)
		require.NoError(t, err)
		election.SetStateForTest(e, s)
		require.Equal(t, 0, e.DoWork(1_000), "state %s", s)
	}
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
