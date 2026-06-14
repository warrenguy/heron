//go:build integration

package harness_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/harness"
)

func TestJoinExternal_liveCnc(t *testing.T) {
	dir := harness.Start(t).Dir

	joined := harness.JoinExternal(t, dir)
	require.Equal(t, dir, joined.Dir)
	harness.AssertCncAlive(t, dir)
}
