//go:build integration

// Package-scoped shared media driver for integration_test; migrated IPC tests use Shared(t).
// Disabled via HERON_SHARED_MEDIA_DRIVER=0.
package integration_test

import (
	"os"
	"testing"

	"github.com/warrenguy/heron/test/integration/harness"
)

// TestMain starts one aeronmd for integration tests in this package.
// Shared(t) and SharedAttachedCnc(t) return it for migrated IPC and ring/CNC tests.
func TestMain(m *testing.M) {
	os.Exit(harness.RunTestMainWithSharedDriver(m))
}

func TestAcceptSharedDriver_testMainWiring(t *testing.T) {
	harness.AssertSharedDriverWiring(t)
}

func TestAcceptSharedDriver_fallbackDisabled(t *testing.T) {
	harness.AssertSharedDriverFallback(t)
}
