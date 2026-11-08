package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/aeron/cnc"
	"luas.fast/aeron/conductor"
)

func TestControlProtocolVersionConstants(t *testing.T) {
	require.Equal(t, cnc.ComposeSemanticVersion(1, 0, 0), conductor.ControlProtocolVersion)
	require.Equal(
		t,
		cnc.SystemCounterControlProtocolVersionID,
		conductor.SystemCounterControlProtocolVersion,
	)
}
