package consensus_test

import (
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/internal/electiontest"
)

var _ consensus.AgentHandler = (*electiontest.NoopAgentHandler)(nil)

// Discarded literal asserts FollowerLogJoinParams field names at compile time.
var _ = consensus.FollowerLogJoinParams{
	LogChannel:      "aeron:udp?endpoint=localhost:0",
	LogStreamID:     101,
	LogSessionID:    7,
	LogJoinPosition: 100,
	IsLeaderStartup: true,
}
