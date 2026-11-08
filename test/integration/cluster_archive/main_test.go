//go:build integration && archive_live

package cluster_archive_test

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
