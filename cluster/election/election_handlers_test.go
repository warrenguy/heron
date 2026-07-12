package election_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/election"
)

func TestHandleConsensusMessage_roundtrip(t *testing.T) {
	t.Parallel()

	cfg, deps, _, epochClock, nodeState := testElectionDeps(t)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCanvass)

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{ActingVersion: clusterwire.CanvassPositionSbeSchemaVersion}

	cases := []struct {
		name    string
		publish func()
		spy     func()
	}{
		{
			name: "CanvassPosition",
			publish: func() {
				pub.CanvassPosition(dst, 1, 2, 3, 4)
			},
		},
		{
			name: "RequestVote",
			publish: func() {
				pub.RequestVote(dst, 10, 20, 30, 40)
			},
			spy: func() {
				require.Len(t, nodeState.proposeCalls, 1)
				require.Equal(t, int64(30), nodeState.proposeCalls[0].candidateTermID)
				require.Equal(t, int64(20), nodeState.proposeCalls[0].logPosition)
				require.Equal(t, epochClock.nowMs, nodeState.proposeCalls[0].timestampMs)
			},
		},
		{
			name: "Vote",
			publish: func() {
				pub.PlaceVote(dst, 1, 2, 3, 4, 5, true)
			},
		},
		{
			name: "NewLeadershipTerm",
			publish: func() {
				pub.NewLeadershipTerm(dst, codecs.NewLeadershipTermFields{
					LogLeadershipTermID:  1,
					NextLeadershipTermID: 2,
					LeaderMemberID:       3,
				})
			},
		},
		{
			name: "AppendPosition",
			publish: func() {
				pub.AppendPosition(dst, 7, 8, 9, 0x01)
			},
		},
		{
			name: "CommitPosition",
			publish: func() {
				pub.CommitPosition(dst, 11, 12, 13)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nodeState.proposeCalls = nil
			pub.Reset()
			tc.publish()
			require.Len(t, pub.Sent, 1)
			require.True(t, e.HandleConsensusMessage(pub.Sent[0].Wire, 0))
			if tc.spy != nil {
				tc.spy()
			}
		})
	}
}

func TestHandleConsensusMessage_rejectsBadInput(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCanvass)

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{ActingVersion: clusterwire.CanvassPositionSbeSchemaVersion}
	pub.CanvassPosition(dst, 1, 2, 3, 4)
	wire := pub.Sent[0].Wire

	t.Run("bad schema id", func(t *testing.T) {
		bad := append([]byte(nil), wire...)
		bad[4] = 0
		bad[5] = 0
		require.False(t, e.HandleConsensusMessage(bad, 0))
	})

	t.Run("truncated buffer", func(t *testing.T) {
		require.False(t, e.HandleConsensusMessage(wire[:4], 0))
	})

	t.Run("truncated body", func(t *testing.T) {
		headerLen := codecs.MessageHeaderEncodedLen()
		require.Greater(t, len(wire), headerLen+4)
		require.False(t, e.HandleConsensusMessage(wire[:headerLen+4], 0))
	})

	t.Run("unknown template", func(t *testing.T) {
		bad := append([]byte(nil), wire...)
		bad[2] = 0xFF
		bad[3] = 0xFF
		require.False(t, e.HandleConsensusMessage(bad, 0))
	})
}

func TestHandleConsensusMessage_gatedInInit(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, nodeState := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{ActingVersion: clusterwire.CanvassPositionSbeSchemaVersion}
	pub.RequestVote(dst, 10, 20, 30, 40)
	require.Len(t, pub.Sent, 1)
	require.True(t, e.HandleConsensusMessage(pub.Sent[0].Wire, 0))
	require.Empty(t, nodeState.proposeCalls)
}

func TestElection_HandleError_resetsToInit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"non-nil error", errors.New("test error")},
		{"nil error", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, deps, _, _, _ := testElectionDeps(t)
			observer := &recordingStateObserver{}
			deps.StateObserver = observer
			e := election.NewElection(cfg, deps)
			election.SetStateForTest(e, election.ElectionStateCanvass)

			e.HandleError(5_000, tc.err)

			require.Equal(t, election.ElectionStateInit, e.State())
			require.Equal(t, []election.ElectionState{
				election.ElectionStateInit,
				election.ElectionStateInit,
			}, observer.states)
		})
	}
}

func TestOnRequestVote_proposesMaxCandidateTermID(t *testing.T) {
	t.Parallel()

	cfg, deps, _, epochClock, nodeState := testElectionDeps(t)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateCanvass)

	e.OnRequestVote(codecs.RequestVoteFields{
		CandidateTermID: 99,
		LogPosition:     100,
	})
	require.Len(t, nodeState.proposeCalls, 1)
	require.Equal(t, epochClock.nowMs, nodeState.proposeCalls[0].timestampMs)
}

func TestOnRecordingSignal_initNoOp(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)

	lr := &noopLogReplicationForTest{}
	election.SetLogReplicationForTest(e, lr)

	e.OnRecordingSignal(1, 2, 3, election.RecordingSignal(4))
	require.False(t, lr.called)
}

func TestOnRecordingSignal_withLogReplication(t *testing.T) {
	t.Parallel()

	cfg, deps, _, _, _ := testElectionDeps(t)
	e := election.NewElection(cfg, deps)
	election.SetStateForTest(e, election.ElectionStateFollowerLogReplication)

	lr := &noopLogReplicationForTest{}
	election.SetLogReplicationForTest(e, lr)

	e.OnRecordingSignal(1, 2, 3, election.RecordingSignal(4))
	require.True(t, lr.called)
}

type noopLogReplicationForTest struct {
	called bool
}

func (n *noopLogReplicationForTest) DoWork(int64) int { return 0 }

func (n *noopLogReplicationForTest) RecordingID() int64 { return 42 }

func (n *noopLogReplicationForTest) IsDone() bool { return false }

func (n *noopLogReplicationForTest) OnSignal(int64, int64, int64, election.RecordingSignal) {
	n.called = true
}
