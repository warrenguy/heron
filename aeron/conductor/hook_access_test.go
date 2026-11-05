package conductor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConductor_lingerSweepBlocker_clearedWhenTestEnds(t *testing.T) {
	conductorInstance := NewConductorForTest(t, Config{ClientID: 42}, nil)

	calls := 0
	t.Run("install", func(t *testing.T) {
		conductorInstance.SetLingerSweepBlockerForTest(t, func() {
			calls++
		})
		invokeLingerSweepBlocker(conductorInstance)
		require.Equal(t, 1, calls)
	})

	// Phase: the subtest cleanup removed the blocker.
	invokeLingerSweepBlocker(conductorInstance)
	require.Equal(t, 1, calls)
}
