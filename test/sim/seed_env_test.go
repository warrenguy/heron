package sim

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
)

func TestSeedFromEnv(t *testing.T) {
	cases := []struct {
		name        string
		env         string
		defaultSeed uint64
		want        uint64
	}{
		{name: "nonZeroAuthoritative", env: "999", defaultSeed: 42, want: 42},
		{name: "zeroUsesEnv", env: "77", defaultSeed: 0, want: 77},
		{name: "zeroEmptyEnv", env: "", defaultSeed: 0, want: 0},
		{name: "invalidFallsBackZero", env: "not-a-uint", defaultSeed: 0, want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HERON_SIM_SEED", tc.env)
			require.Equal(t, tc.want, SeedFromEnv(t, tc.defaultSeed))
		})
	}
}

func TestAdvanceEpoch_shiftsEpochOnly(t *testing.T) {
	t.Parallel()

	c := NewNodeClock(simStartTimeNs, defaultEpochMs)
	beforeNs := c.TimeNs()
	beforeMs := c.TimeMs()
	c.AdvanceEpoch(100)
	require.Equal(t, beforeNs, c.TimeNs())
	require.Equal(t, beforeMs+100, c.TimeMs())
}

func TestPolicyChain_firstDropWins(t *testing.T) {
	t.Parallel()

	part := NewPartitionPolicy()
	part.Cut(0, 1)
	lossy := &LossyPolicy{LatencyMin: 10, LatencyMax: 10}
	chain := PolicyChain{part, lossy}

	drop, lat := chain.Allow(0, 1, election.SentMessage{}, nil)
	require.True(t, drop)
	require.Equal(t, int64(0), lat)

	drop, lat = chain.Allow(1, 0, election.SentMessage{}, nil)
	require.False(t, drop)
	require.Equal(t, int64(10), lat)
}
