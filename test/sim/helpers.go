package sim

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
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
