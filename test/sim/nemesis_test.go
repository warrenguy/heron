//go:build testhooks

package sim_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/sim"
)

// runNemesisUntilAction schedules at Now and advances until that action
// appears on the timeline. Ticks may interleave at the same master time;
// do not drain first or the nemesis event can race past an empty check.
func runNemesisUntilAction(t *testing.T, s *sim.ElectionSim, payload sim.NemesisPayload) {
	t.Helper()
	scanned := s.TimelineLen()
	s.ScheduleNemesis(s.Now(), payload)
	require.NoError(t, s.RunUntil(func(es *sim.ElectionSim) bool {
		entries := es.TimelineSince(scanned)
		scanned += len(entries)
		for _, e := range entries {
			if e.Kind == "nemesis" && e.Action == payload.Action {
				return true
			}
		}
		return false
	}))
}

// publishCanvass enqueues one CanvassPosition so network policy can drop or
// deliver it. Drop decisions land at Enqueue; delivers need a scheduler step.
func publishCanvass(s *sim.ElectionSim, from, to int32, logLeadershipTermID int64) {
	s.Node(from).Publisher().CanvassPosition(
		election.PublicationTarget{MemberID: to},
		codecs.CanvassPositionFields{
			LogLeadershipTermID: logLeadershipTermID,
		},
	)
}

// requireDropNoDeliver asserts a drop on from->to and no deliver on that edge.
func requireDropNoDeliver(
	t *testing.T,
	entries []sim.TimelineEntry,
	from, to int32,
	failMsg string,
) {
	t.Helper()
	dropFound := false
	for _, e := range entries {
		if e.Kind == "drop" && e.NodeID == from && e.To == to {
			dropFound = true
		}
		if e.Kind == "deliver" && e.From == from && e.To == to {
			require.Fail(t, failMsg)
		}
	}
	require.True(t, dropFound)
}

// Partition cut blocks delivery on the edge; heal restores it.
//
// Bidirectional cut mutates both directions. Cut path uses a bounded drain
// (queue may empty before the step budget); heal path waits for a real
// deliver so policy state alone is not the only proof.
func TestPartition_cutAndHeal(t *testing.T) {
	t.Parallel()

	part := sim.NewPartitionNemesis()
	sim.Run(t, sim.Config{
		Seed:    31,
		Nemesis: part,
	}, func(s *sim.ElectionSim) {
		runNemesisUntilAction(t, s, sim.NemesisPayload{
			Action: sim.ActionPartition,
			Data:   sim.PartitionSpec{From: 0, To: 1, Bidirectional: true},
		})
		require.True(t, part.Policy.IsCut(0, 1))
		require.True(t, part.Policy.IsCut(1, 0))
		require.False(t, part.Policy.IsBidirectionallyConnected(0, 1))

		// Cut path: canvass 0->1 must drop, never deliver.
		publishCanvass(s, 0, 1, 1)
		if err := s.RunSteps(20); err != nil {
			// Empty queue is fine; drop already recorded at Enqueue.
			require.ErrorIs(t, err, sim.ErrQueueEmpty)
		}
		requireDropNoDeliver(t, s.Timeline(), 0, 1, "cut edge should drop 0->1")

		runNemesisUntilAction(t, s, sim.NemesisPayload{
			Action: sim.ActionHeal,
			Data:   sim.PartitionSpec{From: 0, To: 1, Bidirectional: true},
		})
		require.True(t, part.Policy.IsBidirectionallyConnected(0, 1))

		// Heal path: same edge must deliver once the edge is open again.
		mark := s.TimelineLen()
		publishCanvass(s, 0, 1, 2)
		delivered := false
		require.NoError(t, s.RunUntil(func(es *sim.ElectionSim) bool {
			entries := es.TimelineSince(mark)
			mark += len(entries)
			for _, e := range entries {
				if e.Kind == "deliver" && e.From == 0 && e.To == 1 {
					delivered = true
					return true
				}
			}
			return false
		}))
		require.True(t, delivered, "expected post-heal deliver 0->1")
	})
}

// Partition without Bidirectional cuts only From->To; reverse stays open.
func TestPartition_asymmetricCut(t *testing.T) {
	t.Parallel()

	part := sim.NewPartitionNemesis()
	sim.Run(t, sim.Config{
		Seed:    32,
		Nemesis: part,
	}, func(s *sim.ElectionSim) {
		runNemesisUntilAction(t, s, sim.NemesisPayload{
			Action: sim.ActionPartition,
			Data:   sim.PartitionSpec{From: 0, To: 1},
		})
		require.True(t, part.Policy.IsCut(0, 1))
		require.False(t, part.Policy.IsCut(1, 0))
	})
}

// Focus wrap and partition stack as outer and inner. Nemesis OnBuild installs
// the partition as InnerNetworkPolicy; FocusNodeID still wraps outside so
// non-focus destinations never deliver even when the partition leaves that
// edge open.
//
// Two publishes separate the layers:
//   - 0->2 is open under the partition, so only focus wrap can drop it.
//   - 0->1 is to the focus node, so only the cut can drop it.
func TestFocusPlusPartition_dropsNonFocusAndCutEdges(t *testing.T) {
	t.Parallel()

	focus := int32(1)
	part := sim.NewPartitionNemesis()
	sim.Run(t, sim.Config{
		Seed:        33,
		FocusNodeID: &focus,
		Nemesis:     part,
	}, func(s *sim.ElectionSim) {
		// Partition is the mutable inner; focus wrap is not exposed here.
		require.Equal(t, part.Policy, s.InnerNetworkPolicy())

		runNemesisUntilAction(t, s, sim.NemesisPayload{
			Action: sim.ActionPartition,
			Data:   sim.PartitionSpec{From: 0, To: 1},
		})

		// Open under partition; drop proves focus wrap is outer.
		publishCanvass(s, 0, 2, 1)

		// To focus, so wrap admits; drop proves cut is the inner.
		publishCanvass(s, 0, 1, 1)

		// Drops land at Enqueue; no scheduler step required.
		requireDropNoDeliver(t, s.Timeline(), 0, 2, "focus must drop non-focus dest")
		requireDropNoDeliver(t, s.Timeline(), 0, 1, "partition must drop cut edge to focus")
	})
}

// set_loss mutates the live LossyPolicy DropProb. DropProb=1 drops every
// message at Enqueue; no deliver event and no scheduler drain needed.
func TestLossy_setLoss(t *testing.T) {
	t.Parallel()

	lossy := sim.NewLossyNetworkNemesis(sim.LossyPolicy{DropProb: 0})
	sim.Run(t, sim.Config{
		Seed:    34,
		Nemesis: lossy,
	}, func(s *sim.ElectionSim) {
		runNemesisUntilAction(t, s, sim.NemesisPayload{
			Action: sim.ActionSetLoss,
			Data:   sim.LossyParams{DropProb: 1.0},
		})
		require.InDelta(t, 1.0, lossy.Policy.DropProb, 1e-9)

		publishCanvass(s, 0, 1, 1)
		requireDropNoDeliver(t, s.Timeline(), 0, 1, "DropProb=1 must drop all")
	})
}

// skew advances only the epoch clock (TimeMs) on listed nodes. Nano time is
// the master schedule; skew must not move it or event ordering drifts.
func TestClockSkew_advanceEpoch(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{
		Seed:    35,
		Nemesis: sim.ClockSkewNemesis{},
	}, func(s *sim.ElectionSim) {
		before := s.Node(0).Clock().TimeMs()
		nano := s.Node(0).Clock().TimeNs()

		runNemesisUntilAction(t, s, sim.NemesisPayload{
			Action: sim.ActionSkew,
			Data:   sim.SkewSpec{DeltaMs: 5_000, NodeIDs: []int32{0}},
		})

		require.Equal(t, nano, s.Node(0).Clock().TimeNs(), "nano unchanged")
		require.Equal(t, before+5_000, s.Node(0).Clock().TimeMs())
	})
}

// Composite routes each action to the child that owns it and rejects two
// children claiming the same action at build time (including nested).
//
// Unknown actions no-op: every child ignores them, so the timeline still
// records the nemesis event without error.
func TestComposite_routesAndForbidsDuplicateActions(t *testing.T) {
	t.Parallel()

	t.Run("routes", func(t *testing.T) {
		t.Parallel()
		part := sim.NewPartitionNemesis()
		lossy := sim.NewLossyNetworkNemesis(sim.LossyPolicy{})
		comp := sim.NewCompositeNemesis(part, lossy, sim.ClockSkewNemesis{}, sim.RestartNemesis{})
		sim.Run(t, sim.Config{
			Seed:    36,
			Nemesis: comp,
		}, func(s *sim.ElectionSim) {
			// Each action mutates only its owner; others stay at defaults.
			runNemesisUntilAction(t, s, sim.NemesisPayload{
				Action: sim.ActionPartition,
				Data:   sim.PartitionSpec{From: 0, To: 2},
			})
			runNemesisUntilAction(t, s, sim.NemesisPayload{
				Action: sim.ActionSetLoss,
				Data:   sim.LossyParams{DropProb: 0.5},
			})
			before := s.Node(1).Clock().TimeMs()
			runNemesisUntilAction(t, s, sim.NemesisPayload{
				Action: sim.ActionSkew,
				Data:   sim.SkewSpec{DeltaMs: 1, NodeIDs: []int32{1}},
			})
			require.Equal(t, before+1, s.Node(1).Clock().TimeMs())
			require.True(t, part.Policy.IsCut(0, 2))
			require.InDelta(t, 0.5, lossy.Policy.DropProb, 1e-9)

			// Unknown action: no child errors; run still completes.
			runNemesisUntilAction(t, s, sim.NemesisPayload{Action: "no_such_action"})
		})
	})

	t.Run("duplicate actions rejected", func(t *testing.T) {
		t.Parallel()
		comp := sim.NewCompositeNemesis(
			sim.NewPartitionNemesis(),
			sim.NewPartitionNemesis(),
		)
		_, err := sim.NewElectionSim(t, sim.Config{Seed: 37, Nemesis: comp})
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate action")
	})

	t.Run("nested composite duplicate actions rejected", func(t *testing.T) {
		t.Parallel()
		// Nested child still claims ActionPartition; ownership is flat.
		partA := sim.NewPartitionNemesis()
		partB := sim.NewPartitionNemesis()
		comp := sim.NewCompositeNemesis(partA, sim.NewCompositeNemesis(partB))
		_, err := sim.NewElectionSim(t, sim.Config{Seed: 40, Nemesis: comp})
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate action")
	})
}

// Unknown action is a no-op for a single nemesis too: OnEvent returns nil and
// the scheduler still records the timeline entry.
func TestNemesis_unknownActionNoError(t *testing.T) {
	t.Parallel()

	sim.Run(t, sim.Config{
		Seed:    38,
		Nemesis: sim.NewPartitionNemesis(),
	}, func(s *sim.ElectionSim) {
		runNemesisUntilAction(t, s, sim.NemesisPayload{Action: "bogus"})
	})
}

// SetNetworkPolicy replaces the inner policy only and re-wraps focus around
// it. Without rewrap, a bare Set would strip focusOnlyPolicy and non-focus
// destinations would start delivering.
//
// Two publishes separate the layers after the swap:
//   - 1->2 is open under the partition, so only focus wrap can drop it.
//   - 1->0 is to the focus node, so only the new cut can drop it.
func TestSetNetworkPolicy_rewrapsFocus(t *testing.T) {
	t.Parallel()

	focus := int32(0)
	sim.Run(t, sim.Config{
		Seed:        39,
		FocusNodeID: &focus,
	}, func(s *sim.ElectionSim) {
		part := sim.NewPartitionPolicy()
		part.Cut(1, 0)
		s.SetNetworkPolicy(part)
		require.Equal(t, part, s.InnerNetworkPolicy())

		// Open under partition; drop proves focus wrap still outer.
		publishCanvass(s, 1, 2, 1)

		// To focus, so wrap admits; drop proves new cut is the inner.
		publishCanvass(s, 1, 0, 1)

		// Drops land at Enqueue; no deliver events expected.
		requireDropNoDeliver(t, s.Timeline(), 1, 2, "focus wrap must drop non-focus dest")
		requireDropNoDeliver(t, s.Timeline(), 1, 0, "cut edge to focus must drop")
	})
}
