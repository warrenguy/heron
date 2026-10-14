//go:build integration

package harness_test

import (
	"os"
	"testing"

	"github.com/warrenguy/heron/test/integration/harness"
)

// TestMain starts one shared aeronmd for every test in this directory. Go
// builds harness and harness_test into one test binary, so one TestMain serves
// both. Tests get the driver from harness.Shared(t).
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
