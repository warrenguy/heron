//go:build testhooks

package sim

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/election"
)

// parityElection is a standalone Election wired like a sim node, used to compare
// the wire dispatch path against direct handler calls.
type parityElection struct {
	election  *election.Election
	host      *simElectionHost
	publisher *election.SbePublisher
}

func newParityElection(t *testing.T) *parityElection {
	t.Helper()

	parsed, err := election.ParseMembers(defaultThreeNodeMemberSpec)
	require.NoError(t, err)
	memberPtrs := election.MembersPtrs(parsed)

	nodeState, err := openNodeState(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, nodeState.Close()) })

	clock := NewNodeClock(simStartTimeNs, defaultEpochMs)
	publisher := &election.SbePublisher{ActingVersion: clusterwire.CanvassPositionSbeSchemaVersion}

	host := &simElectionHost{now: clock.TimeNs}

	cfg := DefaultElectionConfig()
	cfg.LeadershipTermID = 5
	cfg.MemberID = 0

	deps := election.ElectionDeps{
		Handler:    simAgentHandler{},
		Host:       host,
		Publisher:  publisher,
		NanoClock:  clock.TimeNs,
		EpochClock: clock.TimeMs,
		NodeState:  nodeState,
		Members:    memberPtrs,
		ThisMember: memberPtrs[0],
	}

	return &parityElection{
		election:  election.NewElection(cfg, deps),
		host:      host,
		publisher: publisher,
	}
}

func (p *parityElection) snapshot() election.ElectionSnapshot {
	return election.SnapshotForTest(p.election, p.host.Role(), p.publisher)
}

// TestDispatchParity_wireMatchesDirect proves the sim wire dispatch path
// (HandleConsensusMessage decode plus template routing) applies every consensus
// message with an observable election side effect identically to a direct
// On-handler call, so the SBE-wire sim cannot silently drift from production
// dispatch. CommitPosition is excluded while OnCommitPosition is a no-op: the
// baseline guard requires each delivered message to change the snapshot.
func TestDispatchParity_wireMatchesDirect(t *testing.T) {
	t.Parallel()

	target := election.PublicationTarget{MemberID: 0}

	cases := []struct {
		name       string
		templateID uint16
		state      election.ElectionState
		encode     func(enc *election.SbePublisher)
		direct     func(t *testing.T, e *election.Election, wire []byte)
	}{
		{
			name:       "CanvassPosition",
			templateID: uint16(clusterwire.CanvassPositionSbeTemplateID),
			state:      election.ElectionStateCanvass,
			encode: func(enc *election.SbePublisher) {
				enc.CanvassPosition(target, 5, 42, 5, 1)
			},
			direct: func(t *testing.T, e *election.Election, wire []byte) {
				f, ok := codecs.TryDecodeCanvassPositionAt(wire, 0)
				require.True(t, ok)
				e.OnCanvassPosition(f)
			},
		},
		{
			name:       "RequestVote",
			templateID: uint16(clusterwire.RequestVoteSbeTemplateID),
			state:      election.ElectionStateCanvass,
			encode: func(enc *election.SbePublisher) {
				enc.RequestVote(target, 5, 0, 6, 1)
			},
			direct: func(t *testing.T, e *election.Election, wire []byte) {
				f, ok := codecs.TryDecodeRequestVoteAt(wire, 0)
				require.True(t, ok)
				e.OnRequestVote(f)
			},
		},
		{
			name:       "Vote",
			templateID: uint16(clusterwire.VoteSbeTemplateID),
			state:      election.ElectionStateCandidateBallot,
			encode: func(enc *election.SbePublisher) {
				enc.PlaceVote(target, 5, 5, 30, 0, 1, true)
			},
			direct: func(t *testing.T, e *election.Election, wire []byte) {
				f, ok := codecs.TryDecodeVoteAt(wire, 0)
				require.True(t, ok)
				e.OnVote(f)
			},
		},
		{
			name:       "NewLeadershipTerm",
			templateID: uint16(clusterwire.NewLeadershipTermSbeTemplateID),
			state:      election.ElectionStateFollowerBallot,
			encode: func(enc *election.SbePublisher) {
				enc.NewLeadershipTerm(target, codecs.NewLeadershipTermFields{
					LeadershipTermID: 6,
					LeaderMemberID:   1,
					LogSessionID:     simLogSessionID,
				})
			},
			direct: func(t *testing.T, e *election.Election, wire []byte) {
				f, ok := codecs.TryDecodeNewLeadershipTermAt(wire, 0)
				require.True(t, ok)
				e.OnNewLeadershipTerm(f)
			},
		},
		{
			name:       "AppendPosition",
			templateID: uint16(clusterwire.AppendPositionSbeTemplateID),
			state:      election.ElectionStateLeaderReady,
			encode: func(enc *election.SbePublisher) {
				enc.AppendPosition(target, 5, 30, 1, 0)
			},
			direct: func(t *testing.T, e *election.Election, wire []byte) {
				f, ok := codecs.TryDecodeAppendPositionAt(wire, 0)
				require.True(t, ok)
				e.OnAppendPosition(f)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			enc := &election.SbePublisher{ActingVersion: clusterwire.CanvassPositionSbeSchemaVersion}
			tc.encode(enc)
			require.Len(t, enc.Sent, 1)
			require.Equal(t, tc.templateID, enc.Sent[0].TemplateID)
			wire := enc.Sent[0].Wire

			wireNode := newParityElection(t)
			election.SetStateForTest(wireNode.election, tc.state)
			require.True(t, wireNode.election.HandleConsensusMessage(wire, 0))
			wireNode.election.DoWork(simStartTimeNs)

			directNode := newParityElection(t)
			election.SetStateForTest(directNode.election, tc.state)
			tc.direct(t, directNode.election, wire)
			directNode.election.DoWork(simStartTimeNs)

			// Baseline receives no message: the injected message must change the
			// snapshot, or the parity assertion would pass vacuously.
			baseline := newParityElection(t)
			election.SetStateForTest(baseline.election, tc.state)
			baseline.election.DoWork(simStartTimeNs)

			require.Equal(t, directNode.snapshot(), wireNode.snapshot())
			require.NotEqual(t, baseline.snapshot(), wireNode.snapshot())
		})
	}
}
