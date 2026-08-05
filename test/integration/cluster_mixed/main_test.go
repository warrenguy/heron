//go:build integration && cluster_mixed && cluster_java

package cluster_mixed_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
)

// mixedSmokeCompleted is the name of the proof that ran to completion, or ""
// when none did. Each proof writes this as its final statement.
//
// Concurrency: written on the test goroutine; read in TestMain after m.Run
// returns. Proofs must not call t.Parallel.
var mixedSmokeCompleted string

// TestMain fails the binary when an overlay gate names a proof that did not
// run to completion.
//
// Behaviour: rejects a zero m.Run exit when the required proof never marked
// completion. go test exits zero when -test.run selects nothing and when the
// only selected test skips; the compose smoke reads only that exit code. A
// run with no overlay gate set requires nothing. A developer may run the
// package outside compose and see the usual skips.
func TestMain(m *testing.M) {
	code := m.Run()
	if code == 0 {
		name, required := clusterjava.MixedRequiredSmoke(os.Getenv)
		if required && mixedSmokeCompleted != name {
			fmt.Fprintf(
				os.Stderr,
				"heron: mixed smoke required %q; completed proof %q\n",
				name,
				mixedSmokeCompleted,
			)
			code = 1
		}
	}
	os.Exit(code)
}

// markMixedSmokeCompleted marks that the calling proof reached its end.
//
// Behaviour: call as the final statement of a proof. A skip or fatal before
// this call leaves the marker unset, so the marker means completion rather
// than entry.
//
// Concurrency: invoker-only. Runs on the test goroutine.
func markMixedSmokeCompleted(t *testing.T) {
	t.Helper()
	mixedSmokeCompleted = t.Name()
}
