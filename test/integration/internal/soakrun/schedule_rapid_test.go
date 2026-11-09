//go:build rapid

package soakrun

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

const stepsPerRound = 10

func TestSchedule_model_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		seed := rapid.Uint64().Draw(rt, "seed")
		count := rapid.IntRange(0, 40).Draw(rt, "count")

		first := takeSteps(seed, count)
		longer := takeSteps(seed, count+10)
		require.Equal(rt, first, longer[:count])

		full := takeSteps(seed, 3*stepsPerRound)
		requireRoundPairs(rt, full)
		requireLossScopes(rt, full)

		for _, step := range full {
			requireStepBounds(rt, step)
		}
	})
}

func requireRoundPairs(rt *rapid.T, steps []Step) {
	rt.Helper()

	for round := range slices.Chunk(steps, stepsPerRound) {
		seen := make(map[kindRole]struct{})

		for _, step := range round {
			seen[kindRole{kind: step.Kind, role: step.VictimRole}] = struct{}{}
		}

		require.Len(rt, seen, stepsPerRound)
	}
}

func requireLossScopes(rt *rapid.T, steps []Step) {
	rt.Helper()

	var loss int

	for _, step := range steps {
		if step.Index >= stepsPerRound || step.Kind != PacketLoss {
			continue
		}

		require.Equal(rt, OneMember, step.LossScope)
		loss++
	}

	require.Equal(rt, 2, loss)
}

func requireStepBounds(rt *rapid.T, step Step) {
	rt.Helper()

	require.GreaterOrEqual(rt, step.FollowerPick, 0)
	require.LessOrEqual(rt, step.FollowerPick, 1)
	require.GreaterOrEqual(rt, step.Settle, 5*time.Second)
	require.LessOrEqual(rt, step.Settle, 15*time.Second)

	switch step.Kind {
	case PacketLoss:
		require.GreaterOrEqual(rt, step.Hold, 5*time.Second)
		require.LessOrEqual(rt, step.Hold, 60*time.Second)
		require.Contains(rt, []int{1, 5, 20}, step.LossPercent)
		require.Contains(rt, []LossScope{OneMember, AllMembers}, step.LossScope)

	case Partition:
		require.GreaterOrEqual(rt, step.Hold, 5*time.Second)
		require.LessOrEqual(rt, step.Hold, 30*time.Second)
		require.Zero(rt, step.LossPercent)
		require.Zero(rt, step.LossScope)

	default:
		require.Zero(rt, step.Hold)
		require.Zero(rt, step.LossPercent)
		require.Zero(rt, step.LossScope)
	}
}
