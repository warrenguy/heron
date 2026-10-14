//go:build integration

package integration_test

import (
	"os"
	"testing"

	"github.com/warrenguy/heron/test/integration/harness"
)

// TestMain starts one shared aeronmd for the tests in this package. Tests get
// it from harness.Shared(t) or harness.SharedAttachedCnc(t).
// HERON_SHARED_MEDIA_DRIVER=0 disables the shared driver.
func TestMain(m *testing.M) {
	os.Exit(harness.RunTestMainWithSharedDriver(m))
}

func TestAcceptSharedDriver_testMainWiring(t *testing.T) {
	harness.AssertSharedDriverWiring(t)
}

func TestAcceptSharedDriver_fallbackDisabled(t *testing.T) {
	harness.AssertSharedDriverFallback(t)
}
