package sim

import (
	"errors"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
)

// SeedFromEnv returns HERON_SIM_SEED when set and defaultSeed is 0.
//
// Non-zero defaultSeed is authoritative (env does not override).
//
// Invalid env values are logged and fall back to defaultSeed.
func SeedFromEnv(tb testing.TB, defaultSeed uint64) uint64 {
	tb.Helper()
	if defaultSeed != 0 {
		return defaultSeed
	}
	v := os.Getenv("HERON_SIM_SEED")
	if v == "" {
		return defaultSeed
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		tb.Logf("sim: ignoring invalid HERON_SIM_SEED %q: %v", v, err)
		return defaultSeed
	}
	return n
}

// Run builds an ElectionSim, registers cleanup and DumpOnFail, then runs fn.
func Run(tb testing.TB, cfg Config, fn func(s *ElectionSim)) {
	tb.Helper()
	s, err := NewElectionSim(tb, cfg)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, s.Close()) })
	defer s.DumpOnFail(tb)
	fn(s)
}

// pumpAdvanceNs advances idle master time by one election status interval.
//
// Matches ElectionStatusIntervalNs (100ms) so quiet gaps re-tick on the same
// cadence as production status polls instead of oversampling every 10ms.
const pumpAdvanceNs = int64(100_000_000)

// AdvanceLive advances every live node to targetNs with a tick each.
//
// live is false when no node is live.
func AdvanceLive(s *ElectionSim, targetNs int64) (live bool, err error) {
	for _, n := range s.Nodes() {
		if !s.IsLive(n.ID()) {
			continue
		}
		live = true
		if err = s.AdvanceToPhase(n.ID(), targetNs); err != nil {
			return live, err
		}
	}
	return live, nil
}

// PumpIdle advances master time one status interval and ticks live nodes.
//
// Returns ErrQueueEmpty when no node is live.
func PumpIdle(s *ElectionSim) error {
	live, err := AdvanceLive(s, s.Now()+pumpAdvanceNs)
	if err != nil {
		return err
	}
	if !live {
		return ErrQueueEmpty
	}
	return nil
}

// PumpNodes advances live nodes by advanceNs for rounds ticks.
//
// Each round: one status-interval idle pump, then extra advanceNs for
// heartbeat/commit cadence.
func PumpNodes(s *ElectionSim, advanceNs int64, rounds int) error {
	for range rounds {
		if err := PumpIdle(s); err != nil {
			return err
		}
		if _, err := AdvanceLive(s, s.Now()+advanceNs); err != nil {
			return err
		}
	}
	return nil
}

// PumpUntil steps until pred is true, deadlineSteps from the call site elapse
// (ErrMaxSteps), or Config.MaxSteps is hit. Empty queues idle-pump live nodes
// instead of stopping.
//
// deadlineSteps <= 0 disables the local budget (sim MaxSteps still applies).
// Local budget is measured with ElectionSim.Steps so PumpIdle AdvanceToPhase
// steps count.
func PumpUntil(s *ElectionSim, deadlineSteps int, pred func(*ElectionSim) bool) error {
	start := s.Steps()
	for {
		if pred(s) {
			return nil
		}
		if deadlineSteps > 0 && s.Steps()-start >= deadlineSteps {
			return ErrMaxSteps
		}
		err := s.RunSteps(1)
		if err == nil {
			continue
		}
		if !errors.Is(err, ErrQueueEmpty) {
			return err
		}
		if err := PumpIdle(s); err != nil {
			return err
		}
	}
}

// PumpUntilLeaderReady pumps until nodeID has a live election in LEADER_READY.
func PumpUntilLeaderReady(s *ElectionSim, nodeID int32, deadlineSteps int) error {
	return PumpUntil(s, deadlineSteps, func(s *ElectionSim) bool {
		n := s.Node(nodeID)
		if n == nil {
			return false
		}
		el := n.Election()
		return el != nil && el.State() == election.ElectionStateLeaderReady
	})
}

// ApplyNemesisBatch runs schedule then waits for count matching action rows.
//
// Waits on the timeline, not on a liveness predicate: liveness can already
// hold before the nemesis lands and would short-circuit the wait.
func ApplyNemesisBatch(
	s *ElectionSim,
	action string,
	count int,
	deadlineSteps int,
	schedule func(),
) error {
	mark := s.TimelineLen()
	schedule()
	return runUntilNemesisCount(s, mark, action, count, deadlineSteps)
}

// runUntilNemesisCount steps until at least count timeline entries with
// Kind "nemesis" and the given Action appear after mark (TimelineLen before
// ScheduleNemesis).
//
// Scans with a cursor so each step only clones the unprocessed tail.
func runUntilNemesisCount(
	s *ElectionSim,
	mark int,
	action string,
	count int,
	deadlineSteps int,
) error {
	if count <= 0 {
		return nil
	}
	scanned := mark
	seen := 0
	return PumpUntil(s, deadlineSteps, func(s *ElectionSim) bool {
		entries := s.TimelineSince(scanned)
		scanned += len(entries)
		for _, e := range entries {
			if e.Kind == "nemesis" && e.Action == action {
				seen++
				if seen >= count {
					return true
				}
			}
		}
		return false
	})
}

// SchedulePartitionEdges schedules cut/heal for each edge at atNs.
func SchedulePartitionEdges(
	s *ElectionSim,
	atNs int64,
	action string,
	edges [][2]int32,
	bidirectional bool,
) {
	for _, e := range edges {
		s.ScheduleNemesis(atNs, NemesisPayload{
			Action: action,
			Data: PartitionSpec{
				From:          e[0],
				To:            e[1],
				Bidirectional: bidirectional,
			},
		})
	}
}
