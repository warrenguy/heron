package consensus_test

import (
	"luas.fast/cluster/consensus"
	"luas.fast/internal/electiontest"
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
