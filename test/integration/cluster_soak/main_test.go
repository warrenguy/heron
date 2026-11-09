//go:build integration && cluster_soak

package cluster_soak_test

import (
	"os"
	"testing"

	"luas.fast/test/integration/harness"
)

// TestMain runs one member in a StartProcessCluster child, else the tests.
func TestMain(m *testing.M) {
	harness.RunProcessMemberIfChild()
	os.Exit(m.Run())
}
