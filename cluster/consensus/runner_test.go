package consensus_test

import (
	"luas.fast/cluster/codecs"
	"luas.fast/cluster/consensus"
)

type mockElectionRunner struct{}

func (m *mockElectionRunner) DoWork(nowNs int64) int { return 0 }

func (m *mockElectionRunner) HandleConsensusMessage(buf []byte, offset int) bool {
	return false
}

func (m *mockElectionRunner) HandleReplayNewLeadershipTermEvent(
	codecs.NewLeadershipTermEventFields,
) error {
	return nil
}

var _ consensus.ElectionRunner = (*mockElectionRunner)(nil)
