package consensus_test

import "github.com/warrenguy/heron/cluster/consensus"

type mockPositionTable struct{}

func (m *mockPositionTable) UpdateMemberPosition(
	memberID int32,
	leadershipTermID int64,
	logPosition int64,
) {
}

func (m *mockPositionTable) QuorumPositionBoundedByLeaderLog(appendPosition, nowNs int64) int64 {
	return 0
}

type mockConsensusEgress struct{}

func (m *mockConsensusEgress) PublishCommitPosition(commitPosition, leadershipTermID int64) {}

func (m *mockConsensusEgress) PublishAppendPosition(
	leadershipTermID int64,
	logPosition int64,
	flags uint8,
) bool {
	return false
}

var (
	_ consensus.PositionTable   = (*mockPositionTable)(nil)
	_ consensus.ConsensusEgress = (*mockConsensusEgress)(nil)
)
