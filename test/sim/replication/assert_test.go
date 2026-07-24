//go:build replicationsim && testhooks

package replication

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/test/sim"
)

// Closed with a non-nil Leader is not soft liveness under replication;
// IsSelfLeaderState is. Locks the divergence from the election catalog.
func TestSoftLiveness_closedNotSuccess(t *testing.T) {
	t.Parallel()

	cfg := baseConfig(801)
	cfg.ElectionConfig.AppointedLeaderID = 0

	sim.Run(t, cfg, func(s *sim.ElectionSim) {
		require.NoError(t, sim.PumpUntilLeaderReady(s, 0, sim.DefaultLivenessDeadlineSteps))
		el := s.Node(0).Election()
		require.Equal(t, election.ElectionStateLeaderReady, el.State())
		require.True(t, hasSoftLiveness(s), "LEADER_READY must satisfy soft liveness")

		// Force Closed on every node with an election. SetStateForTest keeps
		// Leader; replication soft liveness still rejects Closed (unlike the
		// election catalog, which treats Closed+Leader as success).
		for _, n := range s.Nodes() {
			if e := n.Election(); e != nil {
				election.SetStateForTest(e, election.ElectionStateClosed)
			}
		}
		require.Equal(t, election.ElectionStateClosed, el.State())
		require.NotNil(t, el.Leader(), "Closed after LEADER_READY keeps Leader")
		require.False(t, hasSoftLiveness(s), "Closed must not satisfy soft liveness")

		// Restore self-leader: soft liveness returns.
		election.SetStateForTest(el, election.ElectionStateLeaderReady)
		require.True(t, hasSoftLiveness(s), "LEADER_READY again must satisfy soft liveness")
	})
}
