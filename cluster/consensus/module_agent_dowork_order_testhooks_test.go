//go:build testhooks

package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDoWork_stepOrder_electionSet(t *testing.T) {
	var log []string
	SetDoWorkStepRecorderForTest(t, func(step string) {
		log = append(log, step)
	})
	r := &orderRunner{n: 2}
	a := NewAgent(AgentConfig{Election: r, Commit: new(atomic.Int64)})
	require.Equal(t, 2, a.DoWork(1))
	require.Equal(t, []string{"slowTick", "pollIngress", "pollClientIngress", "election.DoWork"}, log)
}

func TestDoWork_stepOrder_electionNil(t *testing.T) {
	var log []string
	SetDoWorkStepRecorderForTest(t, func(step string) {
		log = append(log, step)
	})
	a := NewAgent(AgentConfig{Commit: new(atomic.Int64)})
	require.Equal(t, 0, a.DoWork(1))
	require.Equal(t, []string{"slowTick", "pollIngress", "pollClientIngress", "consensusWork"}, log)
}
