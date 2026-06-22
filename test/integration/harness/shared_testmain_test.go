//go:build integration

// White-box checks for harness package shared-driver TestMain globals.
// TestMain lives in shared_testmain_external_test.go (harness_test); one TestMain
// per directory test binary. Public acceptance tests are in that file.
// Disabled via HERON_SHARED_MEDIA_DRIVER=0.
package harness

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAcceptSharedDriver_testMainGlobals proves TestMain leaves unexported globals set.
// Public wiring and fallback behavior are covered in shared_testmain_external_test.go.
func TestAcceptSharedDriver_testMainGlobals(t *testing.T) {
	if sharedMediaDriverDisabled() {
		t.Skip("HERON_SHARED_MEDIA_DRIVER=0")
	}
	if !sharedTestMainSeen.Load() {
		t.Skip("aeronmd not found; run make test-integration (requires Docker)")
	}
	require.NotNil(t, sharedDriver, "TestMain must leave sharedDriver set")
}
