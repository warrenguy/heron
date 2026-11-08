package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/aeron/conductor"
)

func TestControlProtocolVersionConstants(t *testing.T) {
	require.Equal(t, cnc.ComposeSemanticVersion(1, 0, 0), conductor.ControlProtocolVersion)
	require.Equal(
		t,
		cnc.SystemCounterControlProtocolVersionID,
		conductor.SystemCounterControlProtocolVersion,
	)
}
