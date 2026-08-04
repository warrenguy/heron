//go:build integration

package integration_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc/cnctest"
	"github.com/warrenguy/heron/test/integration/harness"
)

func TestCncRingsLive(t *testing.T) {
	_, attach := harness.SharedAttachedCnc(t)
	require.Positive(t, attach.Rings.Meta.PID)
	// Construct only: shared driver may publish to ToClients after drain.
	cnctest.AssertWiredRingsConstruct(t, attach.Mapped, attach.Rings)
}
