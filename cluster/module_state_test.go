package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModuleStateCounterKeyAndLabel(t *testing.T) {
	t.Parallel()

	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x00}, moduleStateCounterKey)
	require.Equal(t, "Consensus Module state - clusterId=0", moduleStateCounterLabel)
}
