package sim

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/election"
)

func TestNewElectionSim_emptyMemberSpec(t *testing.T) {
	t.Parallel()

	_, err := NewElectionSim(t, Config{Seed: 1, MemberSpec: "|"})
	require.ErrorIs(t, err, ErrEmptyMemberSpec)
}

func TestNewElectionSim_partialBuildRollback(t *testing.T) {
	t.Parallel()

	base := t.TempDir()

	// Block node-1, not node-0, so node 0 builds first and the build loop
	// must roll back an already-created node when node 1 fails mid-sequence.
	blocker := filepath.Join(base, "node-1")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))

	_, err := NewElectionSim(t, Config{
		Seed:         1,
		NodeStateDir: base,
	})
	require.Error(t, err)
}

func TestElectionSim_deliverUnknownTargetNoFollowUpTick(t *testing.T) {
	t.Parallel()

	s, err := NewElectionSim(t, Config{Seed: 8, MemberSpec: defaultThreeNodeMemberSpec})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	dst := election.PublicationTarget{MemberID: 99}
	s.Node(0).Publisher().CanvassPosition(
		dst,
		codecs.CanvassPositionFields{
			LogLeadershipTermID: 1,
			LogPosition:         2,
			LeadershipTermID:    3,
			FollowerMemberID:    4,
		},
	)
	require.NoError(t, s.RunSteps(4))

	deliverFound := false
	for _, e := range s.Timeline() {
		if e.Kind == "deliver" && e.To == 99 {
			deliverFound = true
			require.False(t, e.HandedOff)
		}
	}
	require.True(t, deliverFound)

	// Unknown-target deliver hands off nothing, so it never schedules a tick for
	// member 99. Drain the queue and confirm no such tick ever runs.
	require.NoError(t, s.RunUntil(func(*ElectionSim) bool { return false }))
	for _, e := range s.Timeline() {
		if e.Kind == "tick" {
			require.NotEqual(t, int32(99), e.NodeID)
		}
	}
}

func TestElectionSim_initialTickMemberIDOrder(t *testing.T) {
	t.Parallel()

	s, err := NewElectionSim(t, Config{Seed: 42, MemberSpec: defaultThreeNodeMemberSpec})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.RunSteps(3))

	var tickOrder []int32
	for _, e := range s.Timeline() {
		if e.Kind == "tick" {
			tickOrder = append(tickOrder, e.NodeID)
		}
	}
	require.Equal(t, []int32{0, 1, 2}, tickOrder)
}

func TestElectionSim_maxStepsExceeded(t *testing.T) {
	t.Parallel()

	s, err := NewElectionSim(t, Config{
		Seed:       1,
		MemberSpec: defaultThreeNodeMemberSpec,
		MaxSteps:   2,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	err = s.RunSteps(3)
	require.ErrorIs(t, err, ErrMaxSteps)
}

func TestElectionSim_advanceTo(t *testing.T) {
	t.Parallel()

	s, err := NewElectionSim(t, Config{Seed: 42, MemberSpec: defaultThreeNodeMemberSpec})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	deadline := simStartTimeNs + 500
	require.NoError(t, s.AdvanceTo(deadline))
	require.Equal(t, deadline, s.scheduler.Now())

	before := s.scheduler.Now()
	require.NoError(t, s.AdvanceTo(simStartTimeNs))
	require.Equal(t, before, s.scheduler.Now())
}

func TestNetwork_sameAtNsDeliverOrder(t *testing.T) {
	t.Parallel()

	s, err := NewElectionSim(t, Config{Seed: 17, MemberSpec: defaultThreeNodeMemberSpec})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	dst := election.PublicationTarget{MemberID: 2}
	fields := codecs.CanvassPositionFields{
		LogLeadershipTermID: 1,
		LogPosition:         2,
		LeadershipTermID:    3,
		FollowerMemberID:    4,
	}
	s.Node(0).Publisher().CanvassPosition(dst, fields)
	s.Node(1).Publisher().CanvassPosition(dst, fields)
	require.NoError(t, s.RunSteps(5))

	require.Equal(t, []int32{0, 1}, s.Node(2).ReceivedFromOrderForTest())
}

func TestStateObserver_recordsStateTimeline(t *testing.T) {
	t.Parallel()

	s, err := NewElectionSim(t, Config{Seed: 1, MemberSpec: defaultThreeNodeMemberSpec})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	found := false
	for _, e := range s.Timeline() {
		if e.Kind == "state" && e.NodeID == 0 {
			found = true
			require.Equal(t, election.ElectionStateInit, e.State)
		}
	}
	require.True(t, found)
}
