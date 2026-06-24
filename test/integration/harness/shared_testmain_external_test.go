//go:build integration

// Package-scoped shared media driver for harness directory tests.
// Migrated IPC tests use harness.Shared(t).
// Disabled via HERON_SHARED_MEDIA_DRIVER=0.
package harness_test

import (
	"os"
	"testing"

	"github.com/warrenguy/heron/test/integration/harness"
)

// TestMain starts one aeronmd for all tests in this directory (harness and harness_test).
// harness.Shared(t) returns it for migrated IPC and timeout-message tests.
func TestMain(m *testing.M) {
	os.Exit(harness.RunTestMainWithSharedDriver(m))
}

func TestAcceptSharedDriver_testMainWiring(t *testing.T) {
	harness.AssertSharedDriverWiring(t)
}

func TestAcceptSharedDriver_fallbackDisabled(t *testing.T) {
	harness.AssertSharedDriverFallback(t)
}
