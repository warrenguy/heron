//go:build integration

// Package-scoped shared media driver for harness_test; migrated tests use harness.Shared(t).
// Disabled via HERON_SHARED_MEDIA_DRIVER=0.
package harness_test

import (
	"os"
	"testing"

	"github.com/warrenguy/heron/test/integration/harness"
)

// TestMain starts one aeronmd for the harness_test package.
// harness.Shared(t) returns it for migrated tests.
func TestMain(m *testing.M) {
	os.Exit(harness.RunTestMainWithSharedDriver(m))
}

func TestAcceptSharedDriver_testMainWiring(t *testing.T) {
	harness.AssertSharedDriverWiring(t)
}

func TestAcceptSharedDriver_fallbackDisabled(t *testing.T) {
	harness.AssertSharedDriverFallback(t)
}
