package consensus_test

import "github.com/warrenguy/heron/cluster/consensus"

type mockPositionTable struct{}

func (*mockPositionTable) UpdateMemberPosition(int32, int64, int64) {}

func (*mockPositionTable) QuorumPositionBoundedByLeaderLog(int64, int64) int64 { return 0 }

func (*mockPositionTable) HasActiveQuorum(int64, int64) bool { return true }

type mockConsensusEgress struct{}

func (*mockConsensusEgress) PublishCommitPosition(int64, int64) {}

func (*mockConsensusEgress) PublishAppendPosition(int64, int64, uint8) bool { return false }

func (*mockConsensusEgress) PublishNewLeadershipTerm(consensus.CanvassAnswer) bool { return false }

var (
	_ consensus.PositionTable   = (*mockPositionTable)(nil)
	_ consensus.ConsensusEgress = (*mockConsensusEgress)(nil)
)
