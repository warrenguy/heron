package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/election/electiontest"
)

type FakeClock struct {
	nowNs int64
}

func (c *FakeClock) TimeNs() int64 { return c.nowNs }

type FakeEpochClock struct {
	nowMs int64
}

func (c *FakeEpochClock) TimeMs() int64 { return c.nowMs }

type recordingNodeState struct {
	proposeCalls []struct {
		candidateTermID int64
		logPosition     int64
		timestampMs     int64
	}
}

func (r *recordingNodeState) CandidateTerm() (election.CandidateTerm, error) {
	return election.CandidateTerm{}, nil
}

func (r *recordingNodeState) UpdateCandidateTermID(
	candidateTermID, logPosition, timestampMs int64,
) error {
	return nil
}

func (r *recordingNodeState) ProposeMaxCandidateTermID(
	candidateTermID, logPosition, timestampMs int64,
) (int64, error) {
	r.proposeCalls = append(r.proposeCalls, struct {
		candidateTermID int64
		logPosition     int64
		timestampMs     int64
	}{candidateTermID, logPosition, timestampMs})
	return candidateTermID, nil
}

func (r *recordingNodeState) Close() error { return nil }

type recordingStateObserver struct {
	states []election.ElectionState
}

func (o *recordingStateObserver) OnElectionStateChange(s election.ElectionState) {
	o.states = append(o.states, s)
}

func testElectionDeps(t *testing.T) (
	election.ElectionConfig,
	election.ElectionDeps,
	*FakeClock,
	*FakeEpochClock,
	*recordingNodeState,
) {
	t.Helper()

	clock := &FakeClock{nowNs: 1_000_000_000}
	epochClock := &FakeEpochClock{nowMs: 42_000}
	nodeState := &recordingNodeState{}
	members := []election.Member{
		{ID: 0},
		{ID: 1},
		{ID: 2},
	}
	memberPtrs := election.MembersPtrs(members)

	cfg := election.ElectionConfig{
		LeadershipTermID: 5,
		LogPosition:      100,
		AppendPosition:   200,
		MemberID:         0,
	}
	deps := election.ElectionDeps{
		Handler:    electiontest.NoopAgentHandler{},
		Host:       electiontest.NoopElectionHost{},
		Publisher:  &election.SbePublisher{},
		NanoClock:  clock.TimeNs,
		EpochClock: epochClock.TimeMs,
		NodeState:  nodeState,
		Members:    memberPtrs,
		ThisMember: memberPtrs[0],
	}
	return cfg, deps, clock, epochClock, nodeState
}

func requireNodeStatePrefixHex(t *testing.T, buf []byte, n int) {
	t.Helper()
	require.GreaterOrEqual(t, len(buf), n)
	t.Logf("node-state prefix: %x", buf[:n])
}

func requireNodeStatePrefixEqual(t *testing.T, got, want []byte, n int) {
	t.Helper()
	require.GreaterOrEqual(t, len(got), n)
	require.GreaterOrEqual(t, len(want), n)
	want = want[:n]
	requireNodeStatePrefixHex(t, got, n)
	require.Equal(t, want, got[:n])
}
