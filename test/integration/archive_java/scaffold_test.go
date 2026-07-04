//go:build integration && archive_java

package archive_java_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/archive_java/archivejava"
	"github.com/warrenguy/heron/test/integration/harness"
)

func TestHarnessScaffold(t *testing.T) {
	if os.Getenv(archivejava.ComposeGateEnvKey) != "1" {
		t.Skip(archivejava.ComposeSkipHint)
	}

	dir := os.Getenv("AERON_DIR")
	require.NotEmpty(t, dir)

	md := harness.JoinExternal(t, dir)
	md.AssertAlive(t)
	t.Log("archive java harness scaffold passed")
}
