//go:build integration

package harness

import "testing"

func TestSharedDriverStreamIDs_unique(t *testing.T) {
	t.Parallel()

	seen := make(map[int32]string, len(sharedDriverStreamIDs))
	for name, id := range sharedDriverStreamIDs {
		if prev, ok := seen[id]; ok {
			t.Fatalf("stream ID %d used by both %s and %s", id, prev, name)
		}
		seen[id] = name
	}
}
