//go:build rapid && testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// TestClusterTermination_canTerminate_rapid checks canTerminate against a
// model of peer marks, service shutdown, and the deadline.
func TestClusterTermination_canTerminate_rapid(t *testing.T) {
	t.Parallel()

	ops := rapid.SampledFrom([]string{
		"peer", "self", "unknown", "services", "clock", "reset",
	})

	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 4).Draw(rt, "peers")
		peers := make([]int32, n)

		for i := range peers {
			peers[i] = int32(i + 1)
		}

		deadline := rapid.Int64Range(0, 1000).Draw(rt, "deadline")
		term := newClusterTermination(peers)
		term.deadlineNs = deadline
		marks := make([]bool, n)
		awaiting := true
		modelDeadline := deadline
		now := int64(0)

		matches := func(at int64) bool {
			if awaiting {
				return false
			}

			for _, marked := range marks {
				if !marked {
					return at >= modelDeadline
				}
			}

			return true
		}

		steps := rapid.IntRange(1, 40).Draw(rt, "steps")

		for range steps {
			switch ops.Draw(rt, "op") {
			case "peer":
				if n == 0 {
					break
				}

				i := rapid.IntRange(0, n-1).Draw(rt, "peer")
				require.True(rt, term.markTerminated(peers[i]))
				marks[i] = true

			case "self":
				require.False(rt, term.markTerminated(0))

			case "unknown":
				id := rapid.Int32Range(100, 200).Draw(rt, "unknown")
				require.False(rt, term.markTerminated(id))

			case "services":
				term.servicesTerminated()
				awaiting = false

			case "clock":
				now += rapid.Int64Range(0, 500).Draw(rt, "step")

			case "reset":
				modelDeadline = rapid.Int64Range(0, 2000).Draw(rt, "newDeadline")
				term.deadlineNs = modelDeadline
			}

			require.Equal(rt, matches(now), term.canTerminate(now))
		}
	})
}
