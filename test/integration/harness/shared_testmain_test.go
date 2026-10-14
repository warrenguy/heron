//go:build integration

package harness

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAcceptSharedDriver_testMainGlobals asserts that TestMain leaves the
// unexported shared-driver globals set. TestMain and the wiring and fallback
// tests live in shared_testmain_external_test.go.
func TestAcceptSharedDriver_testMainGlobals(t *testing.T) {
	if sharedMediaDriverDisabled() {
		t.Skip("HERON_SHARED_MEDIA_DRIVER=0")
	}
	if !sharedTestMainSeen.Load() {
		t.Skip("aeronmd not found; run make test-integration (requires Docker)")
	}
	sharedDriverStateMu.RLock()
	md := sharedDriver
	sharedDriverStateMu.RUnlock()
	require.NotNil(t, md, "TestMain must leave sharedDriver set")
}
