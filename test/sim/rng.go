package sim

import "math/rand/v2"

// NewRNG creates a seeded deterministic PCG RNG for nemesis and network
// fault injection.
func NewRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
}
