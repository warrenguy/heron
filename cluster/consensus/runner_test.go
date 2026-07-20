package consensus_test

import "github.com/warrenguy/heron/cluster/consensus"

type mockElectionRunner struct{}

func (m *mockElectionRunner) DoWork(nowNs int64) int { return 0 }

func (m *mockElectionRunner) HandleConsensusMessage(buf []byte, offset int) bool {
	return false
}

var _ consensus.ElectionRunner = (*mockElectionRunner)(nil)
