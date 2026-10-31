//go:build testhooks

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/consensus"
)

type recordingModuleStateCounter struct {
	states []consensus.ModuleState
}

func (c *recordingModuleStateCounter) StoreModuleState(state consensus.ModuleState) {
	c.states = append(c.states, state)
}

func TestConsensusModule_Close_marksModuleClosed(t *testing.T) {
	t.Parallel()

	agent := consensus.NewAgent(consensus.AgentConfig{})
	counter := &recordingModuleStateCounter{}
	agent.SetModuleStateCounter(counter)
	module := &ConsensusModule{agent: agent}
	want := []consensus.ModuleState{
		consensus.ModuleStateActive,
		consensus.ModuleStateClosed,
	}

	require.NoError(t, module.Close())
	require.Equal(t, want, counter.states)
	require.Equal(t, consensus.ModuleStateClosed, agent.ModuleState())
	require.Equal(t, consensus.ModuleStateClosed, module.ModuleState())
	require.Nil(t, agent.ModuleStateCounterForTest())

	require.NoError(t, module.Close())
	require.Equal(t, want, counter.states)
}
