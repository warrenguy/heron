//go:build electionsim && testhooks

package scenarios

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/test/sim"
)

// scenarioRestart crashes one node after progress, restarts it, then expects
// soft liveness once the cluster settles.
func scenarioRestart() sim.ScenarioSpec {
	const deadline = sim.DefaultLivenessDeadlineSteps
	return sim.ScenarioSpec{
		Name:           "restart",
		Weight:         200,
		ExpectLiveness: true,
		Build: func(seed uint64, _ int) (sim.Config, func(*sim.ElectionSim) error) {
			cfg := baseConfig(seed)
			cfg.Nemesis = sim.RestartNemesis{}
			return cfg, func(s *sim.ElectionSim) error {
				// Elect first so the crash exercises post-leadership recovery.
				if err := runUntilSoftLiveness(s, deadline); err != nil {
					return err
				}

				// Crash a follower when possible; fall back to member 1.
				victim := int32(1)
				if lead := leaderNode(s); lead != nil && lead.ID() == victim {
					victim = 0
				}
				if err := applyNemesis(s, sim.NemesisPayload{
					Action: sim.ActionCrash,
					NodeID: victim,
				}, deadline); err != nil {
					return err
				}

				// Apply restart before soft liveness: leadership may already hold.
				if err := applyNemesis(s, sim.NemesisPayload{
					Action: sim.ActionRestart,
					NodeID: victim,
				}, deadline); err != nil {
					return err
				}
				return runUntilSoftLiveness(s, deadline)
			}
		},
	}
}

// scenarioRestartRecordingLog is a thin durability check excluded from the
// weighted 1000 soak. ExpectLiveness is false; TERM identity is the assert.
func scenarioRestartRecordingLog() sim.ScenarioSpec {
	return sim.ScenarioSpec{
		Name:           "restart_recordinglog",
		Weight:         0,
		ExpectLiveness: false,
		Build: func(seed uint64, _ int) (sim.Config, func(*sim.ElectionSim) error) {
			cfg := baseConfig(seed)
			cfg.AttachRecordingLog = true
			return cfg, func(s *sim.ElectionSim) error {
				return sim.PumpUntil(s, 0, hasRecordingLogTerm)
			}
		},
	}
}

func TestScenario_restart(t *testing.T) {
	t.Parallel()
	RunScenario(t, scenarioRestart(), 1, 0)
}

func TestScenario_restartRecordingLog(t *testing.T) {
	t.Parallel()

	// Fixed few seeds; not part of the 1000-weight catalog soak.
	for _, runIndex := range []int{0, 1, 2} {
		t.Run("seed-"+strconv.Itoa(runIndex), func(t *testing.T) {
			t.Parallel()
			runRestartRecordingLog(t, 1, runIndex)
		})
	}
}

func runRestartRecordingLog(t *testing.T, base uint64, runIndex int) {
	t.Helper()
	sc := scenarioRestartRecordingLog()
	seed := sim.NonZeroSeed(sim.DeriveSeed(base, sc.Name, runIndex))
	stateDir := t.TempDir()

	cfg, body := sc.Build(seed, runIndex)
	cfg.Seed = seed
	cfg.NodeStateDir = stateDir

	var termID int64
	var ok bool
	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		// TERM is written on leader-init / follower-join adoption, not on
		// the first transition into LeaderLogReplication as self-leader.
		require.NoError(t, body(s))
		termID, ok = firstRecordingLogTerm(s)
		require.True(t, ok)
		require.NotEqual(t, int64(election.NullValue), termID)
	})

	// Cross-sim reopen: same NodeStateDir loads durable TERM rows.
	found := false
	for _, id := range []int32{0, 1, 2} {
		recDir := filepath.Join(stateDir, "node-"+strconv.Itoa(int(id)), "recording")
		reopened, err := recordinglog.Open(recDir, false)
		require.NoError(t, err)
		entry := reopened.FindTermEntry(termID)
		require.NoError(t, reopened.Close())
		if entry != nil {
			found = true
			require.Equal(t, sim.LocalLogRecordingID, entry.RecordingID)
			break
		}
	}
	require.True(t, found, "TERM %d missing after reopen", termID)
}

func hasRecordingLogTerm(s *sim.ElectionSim) bool {
	_, found := firstRecordingLogTerm(s)
	return found
}

func firstRecordingLogTerm(s *sim.ElectionSim) (int64, bool) {
	for _, n := range s.Nodes() {
		el := n.Election()
		log := n.RecordingLog()
		if el == nil || log == nil {
			continue
		}
		term := el.LeadershipTermID()
		if term == election.NullValue {
			continue
		}
		if entry := log.FindTermEntry(term); entry != nil {
			return term, true
		}
	}
	return 0, false
}
