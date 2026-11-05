//go:build testhooks

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/consensus"
)

func TestConsensusModule_Close_clearsControlToggles(t *testing.T) {
	t.Parallel()

	agent := consensus.NewAgent(consensus.AgentConfig{})
	agent.SetClusterControlToggle(Toggle{})
	agent.SetNodeControlToggle(Toggle{})
	module := &ConsensusModule{agent: agent}

	require.NoError(t, module.Close())
	require.Nil(t, agent.ClusterControlToggleForTest())
	require.Nil(t, agent.NodeControlToggleForTest())
}
