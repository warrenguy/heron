package consensus_test

import "github.com/warrenguy/heron/cluster/consensus"

type mockElectionRunner struct{}

func (m *mockElectionRunner) DoWork(nowNs int64) int { return 0 }

var _ consensus.ElectionRunner = (*mockElectionRunner)(nil)
