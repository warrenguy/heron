//go:build integration

// Package-scoped shared media driver for ring/CNC tests (Shared, SharedAttachedCnc).
// IPC-only tests use StartParallel; disabled via HERON_SHARED_MEDIA_DRIVER=0.
package integration_test

import (
	"os"
	"testing"

	"github.com/warrenguy/heron/test/integration/harness"
)

// TestMain starts one aeronmd for ring/CNC tests in this package.
// Shared(t) and SharedAttachedCnc(t) return it; IPC tests use StartParallel.
func TestMain(m *testing.M) {
	os.Exit(harness.RunTestMainWithSharedDriver(m))
}

func TestAcceptSharedDriver_testMainWiring(t *testing.T) {
	harness.AssertSharedDriverWiring(t)
}

func TestAcceptSharedDriver_fallbackDisabled(t *testing.T) {
	harness.AssertSharedDriverFallback(t)
}
