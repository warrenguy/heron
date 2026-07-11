package consensus_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/consensus"
)

func TestClusterCounterTypeIDsMatchAeron(t *testing.T) {
	require.Equal(t, int32(201), consensus.ClusterNodeRoleCounterTypeID)
	require.Equal(t, int32(207), consensus.ClusterElectionStateCounterTypeID)
}
