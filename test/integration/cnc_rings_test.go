//go:build integration

package integration_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/internal/cnctest"
	"luas.fast/test/integration/harness"
)

func TestCncRingsLive(t *testing.T) {
	_, attach := harness.SharedAttachedCnc(t)
	require.Positive(t, attach.Rings.Meta.PID)
	// Construct only: shared driver may publish to ToClients after drain.
	cnctest.AssertWiredRingsConstruct(t, attach.Mapped, attach.Rings)
}
