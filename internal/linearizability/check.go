package linearizability

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
)

const visualizationFile = "linearizability.html"

// Check runs [Model] over ops and stops at the first illegal key. A timeout of 0
// does not limit the check.
func Check(ops []porcupine.Operation, timeout time.Duration) porcupine.CheckResult {
	return porcupine.CheckOperationsTimeout(Model(), ops, timeout)
}

// RequireLinearizable fails tb unless [Model] accepts ops with [porcupine.Ok]. A
// timeout fails tb too.
//
// A failed check writes linearizability.html under tb.ArtifactDir. A passing
// check does not create that directory.
func RequireLinearizable(
	tb testing.TB,
	ops []porcupine.Operation,
	timeout time.Duration,
) {
	tb.Helper()

	require.NoError(tb, checkAndVisualize(ops, timeout, tb.ArtifactDir))
}

// checkAndVisualize calls dir only on a result other than [porcupine.Ok]. The
// visualization needs each key's search, so an illegal key does not stop the
// others.
func checkAndVisualize(
	ops []porcupine.Operation,
	timeout time.Duration,
	dir func() string,
) error {
	result, info := porcupine.CheckOperationsVerbose(Model(), ops, timeout)
	if result == porcupine.Ok {
		return nil
	}

	path := filepath.Join(dir(), visualizationFile)
	if err := porcupine.VisualizePath(Model(), info, path); err != nil {
		return fmt.Errorf("check history: result %s: write %s: %w", result, path, err)
	}
	return fmt.Errorf("check history: result %s: visualization at %s", result, path)
}
