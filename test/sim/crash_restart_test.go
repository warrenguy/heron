//go:build testhooks

package sim_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/sim"
)

func TestCrash_deliverToCrashedNeverPanics(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{Seed: 21}, func(s *sim.ElectionSim) {
		require.NoError(t, s.RunSteps(3))
		require.NoError(t, s.CrashNode(1))
		require.False(t, s.IsLive(1))

		// Drain residual in-flight events; pre-crash 0->1 delivers also skip
		// and must not satisfy the assertion for the explicit publish below.
		require.NoError(t, s.RunUntil(func(*sim.ElectionSim) bool { return false }))

		mark := s.TimelineLen()
		beforeInbound := s.Node(1).Snapshot().InboundCount
		publishCanvass(s, 0, 1, 1)

		// Queue may drain before n steps; any other error (safety, max steps)
		// must fail the test.
		if err := s.RunSteps(8); err != nil {
			require.ErrorIs(t, err, sim.ErrQueueEmpty)
		}

		skipFound := false
		for _, e := range s.TimelineSince(mark) {
			if e.Kind == "deliver_skip" && e.From == 0 && e.To == 1 {
				skipFound = true
				require.False(t, e.HandedOff)
				require.True(t, e.Skipped)
			}
		}
		require.True(t, skipFound, "expected deliver_skip for crashed node")

		// Skip short-circuits before Network.Deliver bookkeeping.
		require.Equal(t, beforeInbound, s.Node(1).Snapshot().InboundCount)
	})
}

func TestCrash_tickWhileCrashedNeverPanics(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{Seed: 22}, func(s *sim.ElectionSim) {
		require.NoError(t, s.RunSteps(2))
		require.NoError(t, s.CrashNode(0))

		if err := s.RunSteps(10); err != nil {
			require.ErrorIs(t, err, sim.ErrQueueEmpty)
		}

		// Residual queue may hold no node-0 tick; force one on the dead node.
		mark := s.TimelineLen()
		require.NoError(t, s.AdvanceToPhase(0, s.Now()))

		skipFound := false
		for _, e := range s.TimelineSince(mark) {
			if e.Kind == "tick_skip" && e.NodeID == 0 {
				skipFound = true
				require.True(t, e.Skipped)
			}
		}
		require.True(t, skipFound, "expected tick_skip for crashed node")
	})
}

func TestRestart_restoresLogIdentity(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{Seed: 23}, func(s *sim.ElectionSim) {
		require.NoError(t, s.RunSteps(5))

		node := s.Node(1)
		el := node.Election()
		require.NotNil(t, el)

		wantTerm := el.LeadershipTermID()
		wantLogPos := el.LogPosition()
		diverged := wantTerm + 7
		if wantTerm == election.NullValue {
			diverged = 42
		}
		election.SetLogLeadershipTermIDForTest(el, diverged)
		require.Equal(t, diverged, election.LogLeadershipTermIDForTest(el))

		require.NoError(t, s.CrashNode(1))
		require.Nil(t, s.Node(1).Election())

		require.NoError(t, s.RestartNode(1))
		restoredNode := s.Node(1)
		restored := restoredNode.Election()
		require.NotNil(t, restored)
		require.Equal(t, diverged, election.LogLeadershipTermIDForTest(restored))
		require.Equal(t, wantTerm, restored.LeadershipTermID())
		require.Equal(t, wantLogPos, restored.LogPosition())
		require.NotEqual(t, election.ElectionStateInit, restored.State())
		require.True(t, s.IsLive(1))
		require.True(t, restoredNode.InitLoadedForTest())
		require.Equal(
			t,
			election.CandidateTermIDForTest(restored),
			restoredNode.MaxCandidateSeenForTest(),
		)
	})
}

func TestRestart_focusNoFollowUpTickOnNonFocus(t *testing.T) {
	t.Parallel()

	// Focused sims schedule only subject ticks (no coalesce re-tick).
	focus := int32(0)
	sim.Run(t, sim.Config{Seed: 72, FocusNodeID: &focus}, func(s *sim.ElectionSim) {
		require.NoError(t, s.RunSteps(1))

		const nonFocus int32 = 1
		require.NoError(t, s.CrashNode(nonFocus))
		require.NoError(t, s.RestartNode(nonFocus))
		require.True(t, s.IsLive(nonFocus))
		require.NotEqual(t, election.ElectionStateInit, s.Node(nonFocus).Election().State())

		// Restart drives leave Init via direct DoWork; no follow-up tick for non-focus peers.
		mark := s.TimelineLen()
		require.NoError(t, s.RunUntil(func(*sim.ElectionSim) bool { return false }))
		for _, e := range s.TimelineSince(mark) {
			if e.Kind == "tick" {
				require.NotEqual(t, nonFocus, e.NodeID, "post-restart tick on non-focus")
			}
		}
	})
}

// TestRestart_postCrashInjectDeliversAgain proves reattach invalidates peer
// delivered keys for the restarted source. A pre-crash deliveredKey for this
// source must not make a post-restart inject report success without a new
// delivery.
func TestRestart_postCrashInjectDeliversAgain(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{Seed: 28}, func(s *sim.ElectionSim) {
		require.NoError(t, s.RunSteps(2))

		const from, to int32 = 0, 1
		require.NoError(t, sim.InjectCanvassPosition(s, from, to, election.NullValue, 0))
		require.Contains(t, s.Node(to).ReceivedFromOrderForTest(), from)

		require.NoError(t, s.CrashNode(from))
		require.NoError(t, s.RestartNode(from))
		require.Empty(t, s.Node(from).Publisher().Sent)
		require.NotContains(t, s.Node(to).ReceivedFromOrderForTest(), from)

		// Inject drains the scheduler; source canvass ticks can fan out extra Sent.
		// Pin progress on inbound + peer bookkeeping, not absolute Sent length.
		beforeInbound := s.Node(to).Snapshot().InboundCount
		beforeSent := len(s.Node(from).Publisher().Sent)
		require.NoError(t, sim.InjectCanvassPosition(s, from, to, election.NullValue, 0))
		require.Greater(t, len(s.Node(from).Publisher().Sent), beforeSent)
		require.Greater(t, s.Node(to).Snapshot().InboundCount, beforeInbound)
		require.Contains(t, s.Node(to).ReceivedFromOrderForTest(), from)
	})
}

func TestCrashRestart_errorPaths(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{Seed: 26}, func(s *sim.ElectionSim) {
		require.NoError(t, s.RunSteps(2))

		err := s.CrashNode(99)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown")

		require.NoError(t, s.CrashNode(0))
		require.False(t, s.IsLive(0))
		require.NoError(t, s.CrashNode(0))

		err = s.RestartNode(99)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown")

		err = s.RestartNode(1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "live")
	})
}

func TestRestart_nemesisActions(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{
		Seed:    25,
		Nemesis: sim.RestartNemesis{},
	}, func(s *sim.ElectionSim) {
		require.NoError(t, s.RunSteps(3))
		runNemesisUntilAction(t, s, sim.NemesisPayload{
			Action: sim.ActionCrash,
			NodeID: 0,
		})
		require.False(t, s.IsLive(0))

		runNemesisUntilAction(t, s, sim.NemesisPayload{
			Action: sim.ActionRestart,
			NodeID: 0,
		})
		require.True(t, s.IsLive(0))
		require.NotNil(t, s.Node(0).Election())
	})
}

func TestComposite_restartCrashRestart(t *testing.T) {
	t.Parallel()

	comp := sim.NewCompositeNemesis(
		sim.NewPartitionNemesis(),
		sim.RestartNemesis{},
	)
	sim.Run(t, sim.Config{Seed: 27, Nemesis: comp}, func(s *sim.ElectionSim) {
		require.NoError(t, s.RunSteps(3))
		runNemesisUntilAction(t, s, sim.NemesisPayload{
			Action: sim.ActionCrash,
			NodeID: 2,
		})
		require.False(t, s.IsLive(2))

		runNemesisUntilAction(t, s, sim.NemesisPayload{
			Action: sim.ActionRestart,
			NodeID: 2,
		})
		require.True(t, s.IsLive(2))
		require.NotNil(t, s.Node(2).Election())
	})
}
