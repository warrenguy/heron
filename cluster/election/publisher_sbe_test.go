package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/election"
)

func TestSbePublisher_roundtrip_templates(t *testing.T) {
	t.Parallel()

	dst := election.PublicationTarget{MemberID: 2}
	actingVersion := uint16(clusterwire.CanvassPositionSbeSchemaVersion)

	for _, tc := range publisherRoundtripCases(dst) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pub := &election.SbePublisher{ActingVersion: actingVersion}
			tc.publish(pub)
			require.Len(t, pub.Sent, 1)
			require.Equal(t, tc.templateID, pub.Sent[0].TemplateID)
			require.Equal(t, dst, pub.Sent[0].Dst)
			require.Equal(t, tc.want, pub.Sent[0].Fields)
			require.NotEmpty(t, pub.Sent[0].Wire)
			got, ok := tc.decode(pub.Sent[0].Wire)
			require.True(t, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSbePublisher_resetAndSentHandler(t *testing.T) {
	t.Parallel()

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{}
	var routed []election.SentMessage
	pub.SentHandler = func(msg election.SentMessage) { routed = append(routed, msg) }

	pub.CanvassPosition(
		dst,
		codecs.CanvassPositionFields{
			LogLeadershipTermID: 1,
			LogPosition:         2,
			LeadershipTermID:    3,
			FollowerMemberID:    4,
		},
	)
	ok := pub.RequestVote(
		dst,
		codecs.RequestVoteFields{
			LogLeadershipTermID: 10,
			LogPosition:         20,
			CandidateTermID:     30,
			CandidateMemberID:   40,
		},
	)
	require.True(t, ok)
	require.Len(t, pub.Sent, 2)
	require.Equal(t, pub.Sent, routed)

	pub.Reset()
	require.Empty(t, pub.Sent)
}

func TestSbePublisher_NewLeadershipTerm_v14(t *testing.T) {
	t.Parallel()

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{ActingVersion: 14}
	fields := codecs.NewLeadershipTermFields{
		LogLeadershipTermID:     1,
		NextLeadershipTermID:    2,
		NextTermBaseLogPosition: 3,
		NextLogPosition:         4,
		LeadershipTermID:        5,
		TermBaseLogPosition:     6,
		LogPosition:             7,
		LeaderRecordingID:       8,
		Timestamp:               9,
		LeaderMemberID:          10,
		LogSessionID:            11,
		CommitPosition:          99,
	}
	pub.NewLeadershipTerm(dst, fields)
	require.Len(t, pub.Sent, 1)

	decoded, ok := codecs.TryDecodeNewLeadershipTermAt(pub.Sent[0].Wire, 0)
	require.True(t, ok)
	want := fields
	want.CommitPosition = 0
	require.Equal(t, want, decoded)
}

func TestSbePublisher_zeroActingVersion_defaultsToSchemaVersion(t *testing.T) {
	t.Parallel()

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{}

	pub.CanvassPosition(
		dst,
		codecs.CanvassPositionFields{
			LogLeadershipTermID: 1,
			LogPosition:         2,
			LeadershipTermID:    3,
			FollowerMemberID:    4,
		},
	)
	require.Len(t, pub.Sent, 1)

	decoded, ok := codecs.TryDecodeCanvassPositionAt(pub.Sent[0].Wire, 0)
	require.True(t, ok)
	require.Equal(t, election.ProtocolSemanticVersion(), decoded.ProtocolVersion)
}

func TestSbePublisher_protocolVersion_absent_pre_v9(t *testing.T) {
	t.Parallel()

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{ActingVersion: 8}
	pub.CanvassPosition(
		dst,
		codecs.CanvassPositionFields{
			LogLeadershipTermID: 1,
			LogPosition:         2,
			LeadershipTermID:    3,
			FollowerMemberID:    4,
		},
	)
	require.Len(t, pub.Sent, 1)

	decoded, ok := codecs.TryDecodeCanvassPositionAt(pub.Sent[0].Wire, 0)
	require.True(t, ok)
	require.Equal(t, int32(0), decoded.ProtocolVersion)
}
