//go:build integration

package harness

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAliveError_stoppedDriver(t *testing.T) {
	t.Parallel()
	md := Start(t)

	pid, err := md.ProcessPID()
	require.NoError(t, err)
	require.Positive(t, pid)

	md.Stop()

	err = md.aliveError()
	require.ErrorIs(t, err, os.ErrProcessDone)
	require.ErrorContains(t, err, "harness: aeronmd process "+strconv.Itoa(pid)+" not alive")

	pidAfter, err := md.ProcessPID()
	require.Error(t, err)
	require.Equal(t, 0, pidAfter)
}
