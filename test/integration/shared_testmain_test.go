//go:build integration

// Package-scoped shared media driver for integration tests; migrated tests use Shared(t).
// Disabled via HERON_SHARED_MEDIA_DRIVER=0.
package integration_test

import (
	"os"
	"testing"

	"github.com/warrenguy/heron/test/integration/harness"
)

// TestMain starts one aeronmd for the integration_test package.
// Shared(t) returns it for migrated tests.
func TestMain(m *testing.M) {
	os.Exit(harness.RunTestMainWithSharedDriver(m))
}

func TestAcceptSharedDriver_testMainWiring(t *testing.T) {
	harness.AssertSharedDriverWiring(t)
}

func TestAcceptSharedDriver_fallbackDisabled(t *testing.T) {
	harness.AssertSharedDriverFallback(t)
}
