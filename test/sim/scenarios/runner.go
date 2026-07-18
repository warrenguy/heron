//go:build electionsim && testhooks

// Package scenarios holds the election fault scenario catalog and soak runner.
//
// Build with -tags=electionsim,testhooks. Not part of default unit packages.
package scenarios

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/sim"
)

// DefaultMaxSteps is the per-seed step budget for catalog scenarios.
const DefaultMaxSteps = 20_000

// DefaultLivenessDeadlineSteps bounds soft liveness waits after faults apply.
const DefaultLivenessDeadlineSteps = 10_000

// DefaultCatalogWeight is the sum of soak scenario weights.
const DefaultCatalogWeight = 1000

// ScenarioSpec describes one catalog scenario and how to build/run a seed.
type ScenarioSpec struct {
	Name           string
	Weight         int
	ExpectLiveness bool

	// Build returns sim config and a run body for (seed, runIndex).
	// seed is already DeriveSeed(...); Build must set Config.Seed to seed.
	Build func(seed uint64, runIndex int) (sim.Config, func(*sim.ElectionSim) error)
}

// SoakConfig configures a weighted serial catalog run.
type SoakConfig struct {
	// BaseSeed defaults to 1. HERON_SIM_SEED at runner start overrides base
	// (shifts all derived seeds; does not collapse run indices).
	BaseSeed uint64

	// Scenarios is the weighted catalog (weights sum to 1000 at full scale).
	Scenarios []ScenarioSpec

	// TargetRuns is the total weighted seeds to execute. Zero uses the sum of
	// scenario weights. ELECTION_SIM_RUNS env overrides when set.
	TargetRuns int
}

// DeriveSeed mixes base, scenario name, and run index with FNV-1a 64.
//
// Non-commutative: changing only scenario or only index changes the result.
func DeriveSeed(base uint64, scenario string, runIndex int) uint64 {
	h := fnv.New64a()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], base)
	_, _ = h.Write(b[:])
	_, _ = h.Write([]byte(scenario))
	binary.LittleEndian.PutUint64(b[:], uint64(runIndex))
	_, _ = h.Write(b[:])
	return h.Sum64()
}

// nonZeroSeed returns seed, or ^uint64(0) when seed is 0.
//
// Config.Seed must be non-zero so SeedFromEnv cannot override mid-run.
// All-ones avoids colliding with natural FNV output of 1.
func nonZeroSeed(seed uint64) uint64 {
	if seed == 0 {
		return ^uint64(0)
	}
	return seed
}

// RunCatalog executes every weighted seed serially (no t.Parallel).
//
// ErrMaxSteps and safety errors hard-fail the seed with DumpOnFail via sim.Run.
func RunCatalog(t *testing.T, cfg SoakConfig) {
	t.Helper()

	base := cfg.BaseSeed
	if base == 0 {
		base = 1
	}
	if v := os.Getenv("HERON_SIM_SEED"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("scenarios: invalid HERON_SIM_SEED %q: %v", v, err)
		}
		base = n
	}

	scenarios := cfg.Scenarios
	if len(scenarios) == 0 {
		t.Fatal("scenarios: empty catalog")
	}

	target := cfg.TargetRuns
	if v := os.Getenv("ELECTION_SIM_RUNS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			t.Fatalf("scenarios: invalid ELECTION_SIM_RUNS %q", v)
		}
		target = n
	}
	if target == 0 {
		target = sumWeights(scenarios)
	}
	scenarios = scaleWeights(scenarios, target)

	for _, sc := range scenarios {
		for i := 0; i < sc.Weight; i++ {
			name := fmt.Sprintf("%s/%03d", sc.Name, i)
			t.Run(name, func(t *testing.T) {
				// Serial soak: no t.Parallel.
				runOne(t, base, sc, i)
			})
		}
	}
}

// RunScenario executes one named scenario at a fixed base and run index.
func RunScenario(t *testing.T, sc ScenarioSpec, base uint64, runIndex int) {
	t.Helper()
	if base == 0 {
		base = 1
	}
	runOne(t, base, sc, runIndex)
}

func runOne(t *testing.T, base uint64, sc ScenarioSpec, runIndex int) {
	t.Helper()
	seed := nonZeroSeed(DeriveSeed(base, sc.Name, runIndex))

	simCfg, body := sc.Build(seed, runIndex)
	simCfg.Seed = seed
	simCfg.SeedAttribution = sim.SeedAttribution{
		BaseSeed: base,
		Scenario: sc.Name,
		RunIndex: runIndex,
	}
	if simCfg.MaxSteps == 0 {
		simCfg.MaxSteps = DefaultMaxSteps
	}

	sim.Run(t, simCfg, func(s *sim.ElectionSim) {
		require.NoError(t, body(s))
		if sc.ExpectLiveness {
			RequireSoftLiveness(t, s)
		}
		require.NoError(t, s.SafetyErr())
		require.NoError(t, sim.CheckElectionSafetyHistory(s))
	})
}

func sumWeights(scenarios []ScenarioSpec) int {
	sum := 0
	for _, sc := range scenarios {
		sum += sc.Weight
	}
	return sum
}

// scaleWeights allocates target runs across scenarios by largest remainder.
//
// Zero-weight entries stay zero. When target equals the full weight sum, weights
// are unchanged. When target is smaller, each positive-weight scenario gets at
// least one run only if target >= number of positive-weight scenarios; otherwise
// the largest weights take the budget first.
func scaleWeights(scenarios []ScenarioSpec, target int) []ScenarioSpec {
	total := sumWeights(scenarios)
	if target <= 0 || target == total {
		return scenarios
	}
	out := make([]ScenarioSpec, len(scenarios))
	copy(out, scenarios)
	if total <= 0 {
		return out
	}

	type rem struct {
		i    int
		frac float64
	}
	allocated := 0
	remainders := make([]rem, 0, len(out))
	for i := range out {
		if out[i].Weight <= 0 {
			out[i].Weight = 0
			continue
		}
		exact := float64(out[i].Weight) * float64(target) / float64(total)
		w := int(exact)
		out[i].Weight = w
		allocated += w
		remainders = append(remainders, rem{i: i, frac: exact - float64(w)})
	}

	// Largest remainder fills the rest.
	for allocated < target {
		best := -1
		bestFrac := -1.0
		for _, r := range remainders {
			if out[r.i].Weight == 0 && r.frac == 0 {
				continue
			}
			if r.frac > bestFrac {
				bestFrac = r.frac
				best = r.i
			}
		}
		if best < 0 {
			// Give remaining to first positive original weight.
			for i := range out {
				if scenarios[i].Weight > 0 {
					out[i].Weight++
					allocated++
					if allocated >= target {
						break
					}
				}
			}
			break
		}
		out[best].Weight++
		allocated++

		// Zero out this remainder so next pass picks another.
		for j := range remainders {
			if remainders[j].i == best {
				remainders[j].frac = -1
				break
			}
		}
	}
	return out
}

// baseConfig returns a three-node sim config with NominationRand from seed.
func baseConfig(seed uint64) sim.Config {
	cfg := sim.Config{
		Seed:           seed,
		MemberSpec:     sim.DefaultThreeNodeMemberSpec(),
		ElectionConfig: sim.DefaultElectionConfig(),
		MaxSteps:       DefaultMaxSteps,
	}
	return sim.ConfigWithSimRand(cfg)
}
