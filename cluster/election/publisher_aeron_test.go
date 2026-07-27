//go:build testhooks

package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/election"
)

func boundAeronPublisher(
	t testing.TB,
	peerIDs ...int32,
) (*election.AeronPublisher, map[int32]*aeron.ExclusivePublication) {
	t.Helper()
	p := election.NewAeronPublisher()
	pubs := make(map[int32]*aeron.ExclusivePublication, len(peerIDs))
	for _, id := range peerIDs {
		pub := aeron.NewTestExclusivePublicationForHooks(t)
		p.SetPublication(id, pub)
		pubs[id] = pub
	}
	return p, pubs
}

func termStartWire(t *testing.T, pub *aeron.ExclusivePublication) []byte {
	t.Helper()
	termBuf := pub.TermBuffer(0)
	frameLength := int(logbuffer.FrameLengthVolatile(termBuf, 0))
	require.Positive(t, frameLength)

	// Aborted claims leave a PAD frame with intact payload; only a committed
	// data frame counts as sent.
	require.EqualValues(t, logbuffer.HdrTypeData, logbuffer.FrameType(termBuf, 0))
	return termBuf.Bytes()[logbuffer.OffsetFramePayload:frameLength]
}

func sampleRequestVoteFields() codecs.RequestVoteFields {
	return codecs.RequestVoteFields{
		LogLeadershipTermID: 1,
		LogPosition:         2,
		CandidateTermID:     3,
		CandidateMemberID:   4,
	}
}

func sampleAppendPositionFields() codecs.AppendPositionFields {
	return codecs.AppendPositionFields{
		LeadershipTermID: 1,
		LogPosition:      2,
		FollowerMemberID: 3,
	}
}

func sampleCanvassPositionFields() codecs.CanvassPositionFields {
	return codecs.CanvassPositionFields{
		LogLeadershipTermID: 1,
		LogPosition:         2,
		LeadershipTermID:    3,
		FollowerMemberID:    4,
	}
}

func TestAeronPublisher_missingPub(t *testing.T) {
	t.Parallel()

	p := election.NewAeronPublisher()
	dst := election.PublicationTarget{MemberID: 1}

	require.False(t, p.RequestVote(dst, sampleRequestVoteFields()))
	require.False(t, p.AppendPosition(dst, sampleAppendPositionFields()))

	// No-return methods: missing pub is silent no-op.
	p.CanvassPosition(dst, sampleCanvassPositionFields())
	p.PlaceVote(
		dst,
		codecs.VoteFields{
			CandidateTermID:     1,
			LogLeadershipTermID: 2,
			LogPosition:         3,
			CandidateMemberID:   4,
			FollowerMemberID:    5,
			Vote:                clusterwire.BooleanType_TRUE,
		},
	)
	p.NewLeadershipTerm(
		dst,
		codecs.NewLeadershipTermFields{
			LogLeadershipTermID: 1,
			LeadershipTermID:    1,
		},
	)
	p.CommitPosition(
		dst,
		codecs.CommitPositionFields{
			LeadershipTermID: 1,
			LogPosition:      2,
			LeaderMemberID:   3,
		},
	)
}

// TestAeronPublisher_roundtrip_templates runs the shared publisher case table
// (test_helpers_test.go) against the claim path and decodes the term buffer.
func TestAeronPublisher_roundtrip_templates(t *testing.T) {
	t.Parallel()

	const peerID int32 = 2
	dst := election.PublicationTarget{MemberID: peerID}
	actingVersion := uint16(clusterwire.CanvassPositionSbeSchemaVersion)

	for _, tc := range publisherRoundtripCases(dst) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, pubs := boundAeronPublisher(t, peerID)
			p.ActingVersion = actingVersion
			tc.publish(p)
			got, ok := tc.decode(termStartWire(t, pubs[peerID]))
			require.True(t, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestAeronPublisher_NewLeadershipTerm_dirtyClaimRegion proves zero-valued
// fields encode as zero on the claim path: TryClaim does not zero the term
// buffer, so every block byte must be written, never skipped.
func TestAeronPublisher_NewLeadershipTerm_dirtyClaimRegion(t *testing.T) {
	t.Parallel()

	const peerID int32 = 1
	p, pubs := boundAeronPublisher(t, peerID)

	termBytes := pubs[peerID].TermBuffer(0).Bytes()
	for i := range termBytes {
		termBytes[i] = 0xFF
	}

	fields := codecs.NewLeadershipTermFields{
		LogLeadershipTermID: 1,
		LeadershipTermID:    2,
		LogPosition:         3,
	}
	p.NewLeadershipTerm(election.PublicationTarget{MemberID: peerID}, fields)

	decoded, ok := codecs.TryDecodeNewLeadershipTermAt(termStartWire(t, pubs[peerID]), 0)
	require.True(t, ok)
	require.Equal(t, fields, decoded)
}

func TestAeronPublisher_backPressureExhaust(t *testing.T) {
	t.Parallel()

	const peerID int32 = 1
	p := election.NewAeronPublisher()
	pub := aeron.NewTestExclusivePublicationForHooksWithLimit(t, 0, true)
	p.SetPublication(peerID, pub)
	dst := election.PublicationTarget{MemberID: peerID}

	require.False(t, p.RequestVote(dst, sampleRequestVoteFields()))
	require.False(t, p.AppendPosition(dst, sampleAppendPositionFields()))
}

func TestAeronPublisher_closed(t *testing.T) {
	t.Parallel()

	const peerID int32 = 1
	p, pubs := boundAeronPublisher(t, peerID)
	require.NoError(t, pubs[peerID].Close())
	dst := election.PublicationTarget{MemberID: peerID}

	require.False(t, p.RequestVote(dst, sampleRequestVoteFields()))
	require.False(t, p.AppendPosition(dst, sampleAppendPositionFields()))
	p.CanvassPosition(dst, sampleCanvassPositionFields())
	p.CommitPosition(
		dst,
		codecs.CommitPositionFields{
			LeadershipTermID: 1,
			LogPosition:      2,
			LeaderMemberID:   3,
		},
	)
}

func TestAeronPublisher_zeroActingVersion_defaultsToSchemaVersion(t *testing.T) {
	t.Parallel()

	const peerID int32 = 1
	p, pubs := boundAeronPublisher(t, peerID)
	// ActingVersion left 0: actingVersion() maps to schema version.
	p.CanvassPosition(
		election.PublicationTarget{MemberID: peerID},
		sampleCanvassPositionFields(),
	)

	decoded, ok := codecs.TryDecodeCanvassPositionAt(termStartWire(t, pubs[peerID]), 0)
	require.True(t, ok)
	require.Equal(t, election.ProtocolSemanticVersion(), decoded.ProtocolVersion)
}

func TestAeronPublisher_protocolVersion_absent_pre_v9(t *testing.T) {
	t.Parallel()

	const peerID int32 = 1
	p, pubs := boundAeronPublisher(t, peerID)
	p.ActingVersion = 8
	p.CanvassPosition(
		election.PublicationTarget{MemberID: peerID},
		sampleCanvassPositionFields(),
	)

	decoded, ok := codecs.TryDecodeCanvassPositionAt(termStartWire(t, pubs[peerID]), 0)
	require.True(t, ok)
	require.Equal(t, int32(0), decoded.ProtocolVersion)
}

func TestAeronPublisher_SetPublication_replaceAndUnbind(t *testing.T) {
	t.Parallel()

	const peerID int32 = 1
	p := election.NewAeronPublisher()
	first := aeron.NewTestExclusivePublicationForHooks(t)
	second := aeron.NewTestExclusivePublicationForHooks(t)

	p.SetPublication(peerID, first)
	require.Same(t, first, p.Publication(peerID))

	p.SetPublication(peerID, second)
	require.Same(t, second, p.Publication(peerID))
	// Replace does not Close the previous handle.
	require.False(t, first.IsClosed())

	p.SetPublication(peerID, nil)
	require.Nil(t, p.Publication(peerID))
	require.False(t, second.IsClosed())

	dst := election.PublicationTarget{MemberID: peerID}
	require.False(t, p.RequestVote(dst, sampleRequestVoteFields()))
	require.False(t, p.AppendPosition(dst, sampleAppendPositionFields()))
}

func TestAeronPublisher_thisMemberUnbound(t *testing.T) {
	t.Parallel()

	// Peers only: local id 0 never bound.
	const (
		thisMemberID int32 = 0
		peerA        int32 = 1
		peerB        int32 = 2
	)
	p, pubs := boundAeronPublisher(t, peerA, peerB)
	require.Nil(t, p.Publication(thisMemberID))

	// Peer publish succeeds without self bind.
	ok := p.RequestVote(
		election.PublicationTarget{MemberID: peerA},
		sampleRequestVoteFields(),
	)
	require.True(t, ok)
	_, decodeOK := codecs.TryDecodeRequestVoteAt(termStartWire(t, pubs[peerA]), 0)
	require.True(t, decodeOK)

	p.CommitPosition(
		election.PublicationTarget{MemberID: peerB},
		codecs.CommitPositionFields{
			LeadershipTermID: 5,
			LogPosition:      6,
			LeaderMemberID:   thisMemberID,
		},
	)
	decoded, decodeOK := codecs.TryDecodeCommitPositionAt(termStartWire(t, pubs[peerB]), 0)
	require.True(t, decodeOK)
	require.Equal(t, thisMemberID, decoded.LeaderMemberID)
}

func TestAeronPublisher_HostAdapter_swapSmoke(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	thisIdx := 0
	ap, pubs := boundAeronPublisher(t, 1, 2)
	host, _, _, _ := newTestHostAdapter(
		t,
		members,
		thisIdx,
		func(cfg *election.HostAdapterConfig) {
			cfg.Publisher = ap
			cfg.LeaderMemberID = 1
		},
	)

	// Commit fan-out first so each peer term starts with CommitPosition.
	host.PublishCommitPosition(321, 7)
	for _, peerID := range []int32{1, 2} {
		fields, ok := codecs.TryDecodeCommitPositionAt(termStartWire(t, pubs[peerID]), 0)
		require.True(t, ok, "peer %d", peerID)
		require.Equal(t, int64(7), fields.LeadershipTermID)
		require.Equal(t, int64(321), fields.LogPosition)
		require.Equal(t, members[thisIdx].ID, fields.LeaderMemberID)
	}

	// Fresh leader pub so AppendPosition is the first frame for decode.
	appendPub := aeron.NewTestExclusivePublicationForHooks(t)
	ap.SetPublication(1, appendPub)
	require.True(t, host.PublishAppendPosition(5, 100, 3))
	appendFields, ok := codecs.TryDecodeAppendPositionAt(termStartWire(t, appendPub), 0)
	require.True(t, ok)
	require.Equal(t, int64(5), appendFields.LeadershipTermID)
	require.Equal(t, int64(100), appendFields.LogPosition)
	require.Equal(t, members[thisIdx].ID, appendFields.FollowerMemberID)
	require.Equal(t, uint8(3), appendFields.Flags)

	// Unbound peer: bool respects missing bind.
	ap.SetPublication(1, nil)
	require.False(t, host.PublishAppendPosition(5, 101, 0))
}
