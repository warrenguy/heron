//go:build integration

package integration_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/internal/cnctest"
	"github.com/warrenguy/luas/test/integration/harness"
)

func TestCncRingsLive(t *testing.T) {
	_, attach := harness.SharedAttachedCnc(t)
	require.Positive(t, attach.Rings.Meta.PID)
	// Construct only: shared driver may publish to ToClients after drain.
	cnctest.AssertWiredRingsConstruct(t, attach.Mapped, attach.Rings)
}
