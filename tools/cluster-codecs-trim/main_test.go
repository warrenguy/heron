package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunUsage(t *testing.T) {
	withArgs(t, []string{"cluster-codecs-trim"})
	require.Equal(t, 2, run())
}

func TestRunMissingInput(t *testing.T) {
	withArgs(t, []string{"cluster-codecs-trim", "/no/such/file.xml"})
	require.Equal(t, 1, run())
}

func TestRunSuccess(t *testing.T) {
	tmp := t.TempDir()
	oldResolve := resolveSpecsDir
	resolveSpecsDir = func() (string, error) { return tmp, nil }
	t.Cleanup(func() { resolveSpecsDir = oldResolve })

	input := filepath.Join("testdata", "cluster-codecs-fixture.xml")
	withArgs(t, []string{"cluster-codecs-trim", input})
	require.Equal(t, 0, run())

	outPath := filepath.Join(tmp, "cluster-codecs-client.xml")
	info, err := os.Stat(outPath)
	require.NoError(t, err)
	require.Positive(t, info.Size())
}

func withArgs(t *testing.T, args []string) {
	t.Helper()

	old := os.Args
	os.Args = args
	t.Cleanup(func() { os.Args = old })
}
