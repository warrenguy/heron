package consensus_test

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPackageDoesNotDependOnElection(t *testing.T) {
	cmd := exec.Command(
		"go",
		"list",
		"-tags=bench,testhooks",
		"-deps",
		"github.com/warrenguy/heron/cluster/consensus",
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "go list -deps: %s", out)

	const electionPkg = "github.com/warrenguy/heron/cluster/election"
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		dep := strings.TrimSpace(line)

		// Match the exact package or a subpackage path boundary so a sibling like
		// cluster/electionfoo cannot false-positive-fail the guard.
		forbidden := dep == electionPkg || strings.HasPrefix(dep, electionPkg+"/")
		require.False(t, forbidden, "cluster/consensus must not import %s", dep)
	}
}
