//go:build rapid

package recordinglog

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// TestSelectRecoveryAnchor_selectsLastValidCM_rapid checks backward CM selection on
// comparator-sorted caches.
func TestSelectRecoveryAnchor_selectsLastValidCM_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 10).Draw(rt, "n")
		entries := DrawIndexedEntries(rt, n, "")
		sortEntries(entries)

		sel, err := selectRecoveryAnchor(entries, 0)
		require.NoError(rt, err)
		if !sel.hasSnapshot {
			require.False(rt, slices.ContainsFunc(entries, isValidConsensusModuleSnapshot))
			return
		}

		require.True(rt, isValidConsensusModuleSnapshot(entries[sel.snapshotIndex]))
		afterCM := entries[sel.snapshotIndex+1:]
		require.False(rt, slices.ContainsFunc(afterCM, isValidConsensusModuleSnapshot))
	})
}
