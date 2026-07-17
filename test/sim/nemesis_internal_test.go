//go:build testhooks

package sim

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
)

func TestNemesis_payloadParseErrors(t *testing.T) {
	t.Parallel()

	type caseData struct {
		name       string
		data       any
		wantSubstr string
	}
	tests := []struct {
		name    string
		seed    uint64
		nemesis Nemesis
		action  string
		cases   []caseData
	}{
		{
			name:    "partition",
			seed:    60,
			nemesis: NewPartitionNemesis(),
			action:  ActionPartition,
			cases: []caseData{
				{"nil", nil, "payload missing data"},
				{"wrong type", "not-a-spec", "payload type"},
			},
		},
		{
			name:    "lossy",
			seed:    61,
			nemesis: NewLossyNetworkNemesis(LossyPolicy{}),
			action:  ActionSetLoss,
			cases: []caseData{
				{"nil", nil, "payload missing data"},
				{"wrong type", 42, "payload type"},
			},
		},
		{
			name:    "clock_skew",
			seed:    62,
			nemesis: ClockSkewNemesis{},
			action:  ActionSkew,
			cases: []caseData{
				{"nil", nil, "payload missing data"},
				{"wrong type", true, "payload type"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := tc.nemesis
			Run(t, Config{Seed: tc.seed, Nemesis: n}, func(s *ElectionSim) {
				for _, c := range tc.cases {
					t.Run(c.name, func(t *testing.T) {
						err := n.OnEvent(s, ScheduledEvent{
							Payload: NemesisPayload{Action: tc.action, Data: c.data},
						})
						require.ErrorContains(t, err, c.wantSubstr)
					})
				}
			})
		})
	}
}

func TestClockSkew_nodeIDZeroOnly(t *testing.T) {
	t.Parallel()

	n := ClockSkewNemesis{}
	Run(t, Config{Seed: 63, Nemesis: n}, func(s *ElectionSim) {
		before0 := s.Node(0).clock.TimeMs()
		before1 := s.Node(1).clock.TimeMs()
		before2 := s.Node(2).clock.TimeMs()

		// Empty NodeIDs + NodeID 0 must skew only member 0 (not all nodes).
		require.NoError(t, n.OnEvent(s, ScheduledEvent{
			Payload: NemesisPayload{
				Action: ActionSkew,
				NodeID: 0,
				Data:   SkewSpec{DeltaMs: 9_000},
			},
		}))
		require.Equal(t, before0+9_000, s.Node(0).clock.TimeMs())
		require.Equal(t, before1, s.Node(1).clock.TimeMs())
		require.Equal(t, before2, s.Node(2).clock.TimeMs())
	})
}

func TestClockSkew_allNodes(t *testing.T) {
	t.Parallel()

	n := ClockSkewNemesis{}
	Run(t, Config{Seed: 64, Nemesis: n}, func(s *ElectionSim) {
		before := make([]int64, 3)
		for i := range before {
			before[i] = s.Node(int32(i)).clock.TimeMs()
		}
		require.NoError(t, n.OnEvent(s, ScheduledEvent{
			Payload: NemesisPayload{
				Action: ActionSkew,
				Data:   SkewSpec{DeltaMs: 100, All: true},
			},
		}))
		for i := range before {
			require.Equal(t, before[i]+100, s.Node(int32(i)).clock.TimeMs())
		}
	})
}

func TestClockSkew_unknownNodeErrors(t *testing.T) {
	t.Parallel()

	n := ClockSkewNemesis{}
	Run(t, Config{Seed: 67, Nemesis: n}, func(s *ElectionSim) {
		t.Run("single unknown id", func(t *testing.T) {
			before0 := s.Node(0).clock.TimeMs()
			err := n.OnEvent(s, ScheduledEvent{
				Payload: NemesisPayload{
					Action: ActionSkew,
					NodeID: 99,
					Data:   SkewSpec{DeltaMs: 1_000},
				},
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "skew unknown node 99")
			require.Equal(t, before0, s.Node(0).clock.TimeMs())
		})

		t.Run("NodeIDs with one bad id no partial skew", func(t *testing.T) {
			before0 := s.Node(0).clock.TimeMs()
			before1 := s.Node(1).clock.TimeMs()
			before2 := s.Node(2).clock.TimeMs()
			err := n.OnEvent(s, ScheduledEvent{
				Payload: NemesisPayload{
					Action: ActionSkew,
					Data:   SkewSpec{DeltaMs: 1_000, NodeIDs: []int32{0, 99, 1}},
				},
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "skew unknown node 99")
			require.Equal(t, before0, s.Node(0).clock.TimeMs())
			require.Equal(t, before1, s.Node(1).clock.TimeMs())
			require.Equal(t, before2, s.Node(2).clock.TimeMs())
		})
	})
}

func TestClockSkew_duplicateNodeErrors(t *testing.T) {
	t.Parallel()

	n := ClockSkewNemesis{}
	Run(t, Config{Seed: 69, Nemesis: n}, func(s *ElectionSim) {
		before0 := s.Node(0).clock.TimeMs()
		err := n.OnEvent(s, ScheduledEvent{
			Payload: NemesisPayload{
				Action: ActionSkew,
				Data:   SkewSpec{DeltaMs: 1_000, NodeIDs: []int32{0, 0}},
			},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "skew duplicate node 0")
		require.Equal(t, before0, s.Node(0).clock.TimeMs())
	})
}

func TestPartition_invalidEdgeErrors(t *testing.T) {
	t.Parallel()

	part := NewPartitionNemesis()
	Run(t, Config{Seed: 68, Nemesis: part}, func(s *ElectionSim) {
		tests := []struct {
			name       string
			action     string
			spec       PartitionSpec
			wantSubstr string
		}{
			{"unknown from", ActionPartition, PartitionSpec{From: 99, To: 1}, "partition unknown node 99"},
			{"unknown to", ActionPartition, PartitionSpec{From: 0, To: 99}, "partition unknown node 99"},
			{"self edge", ActionPartition, PartitionSpec{From: 1, To: 1}, "partition self edge node 1"},
			{"heal unknown node", ActionHeal, PartitionSpec{From: 0, To: 99}, "heal unknown node 99"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				err := part.OnEvent(s, ScheduledEvent{
					Payload: NemesisPayload{Action: tc.action, Data: tc.spec},
				})
				require.ErrorContains(t, err, tc.wantSubstr)
				require.Empty(t, part.Policy.cut)
			})
		}
	})
}

// Composite install order must not change final chain shape: partition first
// so cut drops win, lossy second. Each order exercises a different OnBuild
// wrap-existing branch (lossy after part vs partition around lossy).
func TestComposite_partitionLossyPolicyChain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		seed uint64
		// partFirst true: NewCompositeNemesis(part, lossy); false: reverse.
		partFirst bool
	}{
		{name: "part_then_lossy", seed: 65, partFirst: true},
		{name: "lossy_then_part", seed: 70, partFirst: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			part := NewPartitionNemesis()
			lossy := NewLossyNetworkNemesis(LossyPolicy{DropProb: 0})
			var comp *CompositeNemesis
			if tc.partFirst {
				comp = NewCompositeNemesis(part, lossy)
			} else {
				comp = NewCompositeNemesis(lossy, part)
			}
			Run(t, Config{Seed: tc.seed, Nemesis: comp}, func(s *ElectionSim) {
				chain, ok := s.InnerNetworkPolicy().(PolicyChain)
				require.True(t, ok, "composite should install PolicyChain")
				require.Len(t, chain, 2)
				require.Equal(t, part.Policy, chain[0])
				require.Equal(t, lossy.Policy, chain[1])

				// Cut still drops; lossy does not block open edges at DropProb 0.
				part.Policy.Cut(0, 1)
				drop, _ := chain.Allow(0, 1, election.SentMessage{}, s.rng)
				require.True(t, drop)
				drop, lat := chain.Allow(1, 0, election.SentMessage{}, s.rng)
				require.False(t, drop)
				require.Equal(t, int64(0), lat)
			})
		})
	}
}

func TestComposite_firstErrorWins(t *testing.T) {
	t.Parallel()

	part := NewPartitionNemesis()
	lossy := NewLossyNetworkNemesis(LossyPolicy{})
	comp := NewCompositeNemesis(part, lossy)
	Run(t, Config{Seed: 66, Nemesis: comp}, func(s *ElectionSim) {
		err := comp.OnEvent(s, ScheduledEvent{
			Payload: NemesisPayload{Action: ActionPartition, Data: nil},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "partition")
	})
}

func TestLossyPolicy_latencyRange(t *testing.T) {
	t.Parallel()

	p := &LossyPolicy{DropProb: 0, LatencyMin: 100, LatencyMax: 200}
	rng := NewRNG(1)
	for range 50 {
		drop, lat := p.Allow(0, 1, election.SentMessage{}, rng)
		require.False(t, drop)
		require.GreaterOrEqual(t, lat, int64(100))
		require.LessOrEqual(t, lat, int64(200))
	}
}
