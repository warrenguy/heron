package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
)

func TestProtocolSemanticVersion(t *testing.T) {
	version := ProtocolSemanticVersion()
	require.Equal(t, cnc.ComposeSemanticVersion(1, 12, 0), version)
	require.NotEqual(t, cnc.ComposeSemanticVersion(5, 2, 0), version)
}
