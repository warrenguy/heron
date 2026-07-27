package sim

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/election"
)

type dropFromSourcePolicy struct{ from int32 }

func (p dropFromSourcePolicy) Allow(
	from, _ int32,
	_ election.SentMessage,
	_ *rand.Rand,
) (bool, int64) {
	return from == p.from, 0
}

func newFocusSim(t *testing.T, seed uint64) *ElectionSim {
	t.Helper()

	focus := int32(1)
	s, err := NewElectionSim(t, Config{
		Seed:        seed,
		MaxSteps:    50,
		FocusNodeID: &focus,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func TestInject_unknownNode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		inject  func(s *ElectionSim) error
		wantErr string
	}{
		{
			name:    "unknown source",
			inject:  func(s *ElectionSim) error { return InjectCanvassPosition(s, 99, 1, 0, 0) },
			wantErr: "unknown inject source node 99",
		},
		{
			name:    "unknown destination",
			inject:  func(s *ElectionSim) error { return InjectAppendPosition(s, 0, 99, 0, 0) },
			wantErr: "unknown inject destination node 99",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newFocusSim(t, 1)
			require.ErrorContains(t, tc.inject(s), tc.wantErr)
		})
	}
}

func TestInject_focusNodeIDDropsDelivery(t *testing.T) {
	t.Parallel()

	s := newFocusSim(t, 3)

	require.NoError(t, s.RunSteps(1))
	before := s.Node(1).Snapshot().InboundCount

	err := InjectCanvassPosition(s, 0, 2, 0, 0)
	require.ErrorContains(t, err, "did not deliver")
	require.Equal(t, before, s.Node(1).Snapshot().InboundCount)
}

func TestInject_publishFailure(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		publish func(election.PublicationTarget) bool
	}{
		{
			name:    "callback reports failure",
			publish: func(election.PublicationTarget) bool { return false },
		},
		{
			// No-return publishers (canvass, vote) discard publish's bool, so the
			// callback returns true even when nothing was appended. inject must
			// still surface encode failure from the unchanged Sent log.
			name:    "no-return publisher leaves Sent unchanged",
			publish: func(election.PublicationTarget) bool { return true },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newFocusSim(t, 4)
			err := inject(s, 0, 1, "append position", tc.publish)
			require.ErrorContains(t, err, "append position encode failed from 0 to 1")
		})
	}
}

func TestElectionSim_advanceToPhase_unknownNode(t *testing.T) {
	t.Parallel()

	s, err := NewElectionSim(t, Config{Seed: 1, MaxSteps: 10})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	err = s.AdvanceToPhase(99, s.Now()+1)
	require.ErrorContains(t, err, "unknown phase node 99")
}

// TestInject_ignoresUnrelatedDelivery proves inject waits for its own published
// message: an unrelated inbound delivery to the target must not report a dropped
// inject as delivered.
func TestInject_ignoresUnrelatedDelivery(t *testing.T) {
	t.Parallel()

	focus := int32(1)
	s, err := NewElectionSim(t, Config{
		Seed:          5,
		MaxSteps:      50,
		FocusNodeID:   &focus,
		NetworkPolicy: dropFromSourcePolicy{from: 2},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.RunSteps(1))

	// Queue an unrelated canvass from node 0 to the focus node, then inject a
	// canvass from node 2 that the policy drops. The node-0 delivery lands during
	// the inject drain, so success must still hinge on node 2's dropped message.
	s.Node(0).Publisher().CanvassPosition(
		election.PublicationTarget{MemberID: focus},
		codecs.CanvassPositionFields{
			LogLeadershipTermID: election.NullValue,
			LeadershipTermID:    election.NullValue,
		},
	)
	before := s.Node(focus).Snapshot().InboundCount

	err = InjectCanvassPosition(s, 2, focus, election.NullValue, 0)
	require.ErrorContains(t, err, "did not deliver")
	require.Greater(t, s.Node(focus).Snapshot().InboundCount, before)
}

// TestElectionSim_advanceToPhase_pastTargetKeepsClock proves a target before the
// current clock does not regress master time.
func TestElectionSim_advanceToPhase_pastTargetKeepsClock(t *testing.T) {
	t.Parallel()

	focus := int32(1)
	s, err := NewElectionSim(t, Config{Seed: 1, MaxSteps: 50, FocusNodeID: &focus})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.AdvanceToPhase(1, s.Now()+1_000_000))
	now := s.Now()
	require.NoError(t, s.AdvanceToPhase(1, now-500_000))
	require.GreaterOrEqual(t, s.Now(), now)
}
