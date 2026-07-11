package consensus_test

import "github.com/warrenguy/heron/cluster/consensus"

type mockAgentHandler struct{}

func (m *mockAgentHandler) PrepareForNewLeadership(logPosition, nowNs int64) int64 {
	return 0
}

func (m *mockAgentHandler) JoinLogAsLeader(
	leadershipTermID int64,
	logPosition int64,
	logSessionID int32,
	isStartup bool,
) {
}

func (m *mockAgentHandler) JoinLogAsFollower(
	params consensus.FollowerLogJoinParams,
	nowNs int64,
) bool {
	return false
}

func (m *mockAgentHandler) ElectionComplete(nowNs int64) {}

var _ consensus.AgentHandler = (*mockAgentHandler)(nil)

// Discarded literal pins FollowerLogJoinParams field names at compile time.
var _ = consensus.FollowerLogJoinParams{
	LogChannel:      "aeron:udp?endpoint=localhost:0",
	LogStreamID:     101,
	LogSessionID:    7,
	LogJoinPosition: 100,
	IsLeaderStartup: true,
}
