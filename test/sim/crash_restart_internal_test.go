//go:build testhooks

package sim

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
)

func TestRestart_candidateTermAtLeastDurableProposeMax(t *testing.T) {
	t.Parallel()

	Run(t, Config{Seed: 24}, func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(5))

		const (
			id         int32 = 2
			proposedID int64 = 42
		)
		node := s.Node(id)
		require.NotNil(t, node.nodeState)

		got, err := node.nodeState.ProposeMaxCandidateTermID(proposedID, 0, 1)
		require.NoError(t, err)
		require.Equal(t, proposedID, got)
		ct, err := node.nodeState.CandidateTerm()
		require.NoError(t, err)
		require.Equal(t, proposedID, ct.CandidateTermID())

		require.NoError(t, s.CrashNode(id))
		require.False(t, s.IsLive(id))
		ct, err = node.nodeState.CandidateTerm()
		require.NoError(t, err)
		require.Equal(t, proposedID, ct.CandidateTermID())

		require.NoError(t, s.RestartNode(id))
		restored := s.Node(id)
		require.True(t, s.IsLive(id))
		require.NotNil(t, restored.Election())

		after := election.CandidateTermIDForTest(restored.Election())
		require.GreaterOrEqual(t, after, proposedID)
		require.GreaterOrEqual(t, after, restored.Snapshot().CandidateTermID)
		require.Equal(t, after, restored.MaxCandidateSeenForTest())
		require.NoError(t, CheckCandidateTermMonotonicity(s))
	})
}

func TestRestart_stuckInitRollback(t *testing.T) {
	t.Parallel()

	Run(t, Config{Seed: 71}, func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(3))

		const id int32 = 1
		node := s.Node(id)
		require.NoError(t, s.CrashNode(id))
		require.False(t, s.IsLive(id))
		require.NotNil(t, node.crashSnap)

		// Leave election in Init so rollback runs.
		//
		// Per-sim hook avoids racing other parallel restart tests.
		s.restartLeaveInitHook = func(*Node, int64) {}

		err := s.RestartNode(id)
		require.ErrorContains(t, err, "stuck in Init")
		require.False(t, s.IsLive(id))
		require.Nil(t, node.Election())
		require.NotNil(t, node.crashSnap, "crashSnap kept for retry")

		// Still crashed: second RestartNode must not report "restart live".
		err = s.RestartNode(id)
		require.ErrorContains(t, err, "stuck in Init")
		require.NotContains(t, err.Error(), "restart live")

		// Clear hook; retry succeeds with kept snap.
		s.restartLeaveInitHook = nil
		require.NoError(t, s.RestartNode(id))
		require.True(t, s.IsLive(id))
		require.NotNil(t, node.Election())
		require.Nil(t, node.crashSnap)
	})
}

// TestRestart_scrubPeerDeliveredKeysFromSource proves attachLiveElection
// drops peer bookkeeping for the restarted source and bumps publishGen so
// hasDelivered cannot match pre-crash {from, index} identity.
func TestRestart_scrubPeerDeliveredKeysFromSource(t *testing.T) {
	t.Parallel()

	Run(t, Config{Seed: 73}, func(s *ElectionSim) {
		require.NoError(t, s.RunSteps(2))

		const from, to int32 = 0, 1
		source := s.Node(from)
		peer := s.Node(to)
		genBefore := source.publishGen
		preIndex := len(source.Publisher().Sent)

		require.NoError(t, InjectCanvassPosition(s, from, to, election.NullValue, 0))
		require.True(t, peer.hasDelivered(deliveredKey{
			from:  from,
			index: preIndex,
			gen:   genBefore,
		}))

		require.NoError(t, s.CrashNode(from))
		require.NoError(t, s.RestartNode(from))

		require.Equal(t, genBefore+1, source.publishGen)
		require.False(t, peer.hasDelivered(deliveredKey{
			from:  from,
			index: preIndex,
			gen:   genBefore,
		}))
		require.False(t, peer.hasDelivered(deliveredKey{
			from:  from,
			index: 0,
			gen:   source.publishGen,
		}))
		for _, k := range peer.receivedMsgKeys {
			require.NotEqual(t, from, k.from)
		}

		require.NoError(t, InjectCanvassPosition(s, from, to, election.NullValue, 0))
		require.True(t, peer.hasDelivered(deliveredKey{
			from:  from,
			index: 0,
			gen:   source.publishGen,
		}))
	})
}
