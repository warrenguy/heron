//go:build !integration

package harness

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMediaDriver_notStarted(t *testing.T) {
	var md MediaDriver

	t.Run("ProcessPID", func(t *testing.T) {
		pid, err := md.ProcessPID()
		require.Equal(t, 0, pid)
		require.ErrorIs(t, err, errMediaDriverProcessNotStarted)
	})

	t.Run("AliveError", func(t *testing.T) {
		err := md.aliveError()
		require.ErrorIs(t, err, errMediaDriverProcessNotStarted)
	})
}
