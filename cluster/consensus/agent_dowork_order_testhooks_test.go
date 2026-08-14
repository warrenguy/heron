//go:build testhooks

package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDoWork_stepOrder(t *testing.T) {
	tests := []struct {
		name     string
		election ElectionRunner
		wantWork int
		wantLog  []string
	}{
		{
			name:     "electionSet",
			election: &orderRunner{n: 2},
			wantWork: 2,
			wantLog: []string{
				"slowTick",
				"pollIngress",
				"pollClientIngress",
				"pollServiceControl",
				"serviceControlWork",
				"election.DoWork",
				"retryPendingJoinLog",
			},
		},
		{
			name:     "electionNil",
			wantWork: 0,
			wantLog: []string{
				"slowTick",
				"pollIngress",
				"pollClientIngress",
				"pollServiceControl",
				"serviceControlWork",
				"sessionWork",
				"timerWork",
				"consensusWork",
				"retryPendingJoinLog",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var log []string
			SetDoWorkStepRecorderForTest(t, func(step string) {
				log = append(log, step)
			})
			a := NewAgent(AgentConfig{Election: tt.election, Commit: new(atomic.Int64)})
			require.Equal(t, tt.wantWork, a.DoWork(1))
			require.Equal(t, tt.wantLog, log)
		})
	}
}
