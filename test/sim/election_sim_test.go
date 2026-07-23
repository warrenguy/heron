//go:build testhooks

package sim_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/sim"
)

func TestElectionSim_threeNode_happyPath(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{Seed: 42}, func(s *sim.ElectionSim) {
		require.NoError(t, s.RunSteps(3))

		for _, node := range s.Nodes() {
			require.Equal(t, election.ElectionStateCanvass, node.Election().State())
		}
		require.GreaterOrEqual(t, len(s.Timeline()), 3)
	})
}

func TestElectionSim_deterministicReplay(t *testing.T) {
	t.Parallel()

	run := func(seed uint64) []byte {
		s, err := sim.NewElectionSim(t, sim.Config{Seed: seed})
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()
		require.NoError(t, s.RunSteps(3))
		return sim.TimelineCanonicalBytes(s.Timeline())
	}

	a1 := run(99)
	a2 := run(99)

	require.Equal(t, a1, a2)
}

func TestElectionSim_manualMessageDelivery(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{Seed: 11}, func(s *sim.ElectionSim) {
		dst := election.PublicationTarget{MemberID: 1}
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

		found := false
		for _, e := range s.Timeline() {
			if e.Kind == "deliver" && e.NodeID == 1 {
				found = true
				require.Equal(t, int32(0), e.From)
				require.Equal(t, uint16(clusterwire.CanvassPositionSbeTemplateID), e.TemplateID)
			}
		}
		require.True(t, found)
		require.Equal(t, 1, s.Node(1).Snapshot().InboundCount)
	})
}

func TestElectionSim_dumpSmoke(t *testing.T) {
	t.Parallel()

	s, err := sim.NewElectionSim(t, sim.Config{Seed: 13})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.RunSteps(1))
	require.NotPanics(t, func() { s.Dump(t) })
}

func TestElectionSim_dumpOnFailure(t *testing.T) {
	t.Parallel()

	t.Run("dumpOnFail", func(t *testing.T) {
		s, err := sim.NewElectionSim(t, sim.Config{Seed: 13})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		require.NoError(t, s.RunSteps(1))

		dt := &dumpTB{TB: t, failed: true}
		require.NotPanics(t, func() { s.DumpOnFail(dt) })
		require.Contains(t, dt.logs(), "ElectionSim failure dump")
	})
}

func TestElectionSim_dumpSaveToDisk(t *testing.T) {
	t.Setenv("HERON_SIM_SAVE", "1")

	s, err := sim.NewElectionSim(t, sim.Config{
		Seed: 17,
		SeedAttribution: sim.SeedAttribution{
			BaseSeed: 1,
			Scenario: "dump_save",
			RunIndex: 0,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, s.RunSteps(1))

	// Dump always logs; with HERON_SIM_SAVE=1 also writes under module root.
	dir := s.Dump(t)
	require.NotEmpty(t, dir)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	// Path must be module-root/test/sim/testdata/sim/failures/..., not package-cwd nested.
	require.Contains(t, dir, filepath.FromSlash("test/sim/testdata/sim/failures"))
	require.NotContains(t, dir, filepath.FromSlash("test/sim/test/sim"))

	require.FileExists(t, filepath.Join(dir, "dump.txt"))
	require.FileExists(t, filepath.Join(dir, "timeline.txt"))
	require.FileExists(t, filepath.Join(dir, "nodes.txt"))

	header, err := os.ReadFile(filepath.Join(dir, "dump.txt"))
	require.NoError(t, err)
	require.Contains(t, string(header), "derived_seed: 17")
	require.Contains(t, string(header), "base_seed: 1")
	require.Contains(t, string(header), "scenario: dump_save")
	require.Contains(t, string(header), "run_index: 0")
}

type dumpTB struct {
	testing.TB
	failed bool
	logBuf strings.Builder
}

func (d *dumpTB) Failed() bool { return d.failed || d.TB.Failed() }

func (d *dumpTB) Log(args ...any) {
	_, _ = fmt.Fprint(&d.logBuf, fmt.Sprintln(args...))
}

func (d *dumpTB) Logf(format string, args ...any) {
	_, _ = fmt.Fprintf(&d.logBuf, format+"\n", args...)
}

func (d *dumpTB) logs() string { return d.logBuf.String() }

func TestElectionSim_tempdirNodeState(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	sim.Run(t, sim.Config{
		Seed:         3,
		MemberSpec:   sim.DefaultThreeNodeMemberSpec(),
		NodeStateDir: baseDir,
	}, func(s *sim.ElectionSim) {
		require.Len(t, s.Nodes(), 3)
		for i := range 3 {
			path := filepath.Join(baseDir, fmt.Sprintf("node-%d", i), election.NodeStateFileName)
			require.FileExists(t, path)
		}
	})
}

func TestElectionSim_setStateForTestHook(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{Seed: 1}, func(s *sim.ElectionSim) {
		node := s.Node(0)
		before := node.ObserverStatesForTest()
		election.SetStateForTest(node.Election(), election.ElectionStateCanvass)
		require.Equal(t, election.ElectionStateCanvass, node.Election().State())
		require.Equal(t, before, node.ObserverStatesForTest())
	})
}

func TestElectionSim_runUntilEmptyQueue(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{Seed: 42}, func(s *sim.ElectionSim) {
		require.NoError(t, s.RunSteps(3))
		require.NoError(t, s.RunUntil(func(*sim.ElectionSim) bool { return false }))
		require.ErrorIs(t, s.RunSteps(1), sim.ErrQueueEmpty)
	})
}

func TestElectionSim_runUntilPredicateSteps(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{Seed: 42}, func(s *sim.ElectionSim) {
		calls := 0
		require.NoError(t, s.RunUntil(func(*sim.ElectionSim) bool {
			calls++
			return calls > 2
		}))
		require.Equal(t, 3, calls)

		ticks := 0
		for _, e := range s.Timeline() {
			if e.Kind == "tick" {
				ticks++
			}
		}
		require.Equal(t, 2, ticks)
	})
}

func TestElectionSim_invalidSeedFromEnv(t *testing.T) {
	t.Setenv("HERON_SIM_SEED", "not-a-number")

	// Seed 0 so SeedFromEnv reads HERON_SIM_SEED (non-zero short-circuits env).
	s, err := sim.NewElectionSim(t, sim.Config{Seed: 0})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
}
