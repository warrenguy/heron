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

	cases := []struct {
		name       string
		publish    func(pub *election.SbePublisher)
		templateID uint16
		decode     func(wire []byte) (any, bool)
		want       any
	}{
		{
			name: "CanvassPosition",
			publish: func(pub *election.SbePublisher) {
				pub.CanvassPosition(dst, 1, 2, 3, 4)
			},
			templateID: clusterwire.CanvassPositionSbeTemplateID,
			decode: func(wire []byte) (any, bool) {
				f, ok := codecs.TryDecodeCanvassPositionAt(wire, 0)
				return f, ok
			},
			want: codecs.CanvassPositionFields{
				LogLeadershipTermID: 1,
				LogPosition:         2,
				LeadershipTermID:    3,
				FollowerMemberID:    4,
				ProtocolVersion:     election.ProtocolSemanticVersion(),
			},
		},
		{
			name: "RequestVote",
			publish: func(pub *election.SbePublisher) {
				pub.RequestVote(dst, 10, 20, 30, 40)
			},
			templateID: clusterwire.RequestVoteSbeTemplateID,
			decode: func(wire []byte) (any, bool) {
				f, ok := codecs.TryDecodeRequestVoteAt(wire, 0)
				return f, ok
			},
			want: codecs.RequestVoteFields{
				LogLeadershipTermID: 10,
				LogPosition:         20,
				CandidateTermID:     30,
				CandidateMemberID:   40,
				ProtocolVersion:     election.ProtocolSemanticVersion(),
			},
		},
		{
			name: "Vote",
			publish: func(pub *election.SbePublisher) {
				pub.PlaceVote(dst, 1, 2, 3, 4, 5, true)
			},
			templateID: clusterwire.VoteSbeTemplateID,
			decode: func(wire []byte) (any, bool) {
				f, ok := codecs.TryDecodeVoteAt(wire, 0)
				return f, ok
			},
			want: codecs.VoteFields{
				CandidateTermID:     1,
				LogLeadershipTermID: 2,
				LogPosition:         3,
				CandidateMemberID:   4,
				FollowerMemberID:    5,
				Vote:                clusterwire.BooleanType_TRUE,
			},
		},
		{
			name: "NewLeadershipTerm_v16",
			publish: func(pub *election.SbePublisher) {
				pub.NewLeadershipTerm(dst, codecs.NewLeadershipTermFields{
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
					AppVersion:              12,
					IsStartup:               clusterwire.BooleanType_FALSE,
					CommitPosition:          99,
				})
			},
			templateID: clusterwire.NewLeadershipTermSbeTemplateID,
			decode: func(wire []byte) (any, bool) {
				f, ok := codecs.TryDecodeNewLeadershipTermAt(wire, 0)
				return f, ok
			},
			want: codecs.NewLeadershipTermFields{
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
				AppVersion:              12,
				IsStartup:               clusterwire.BooleanType_FALSE,
				CommitPosition:          99,
			},
		},
		{
			name: "AppendPosition",
			publish: func(pub *election.SbePublisher) {
				pub.AppendPosition(dst, 7, 8, 9, 0x02)
			},
			templateID: clusterwire.AppendPositionSbeTemplateID,
			decode: func(wire []byte) (any, bool) {
				f, ok := codecs.TryDecodeAppendPositionAt(wire, 0)
				return f, ok
			},
			want: codecs.AppendPositionFields{
				LeadershipTermID: 7,
				LogPosition:      8,
				FollowerMemberID: 9,
				Flags:            0x02,
			},
		},
		{
			name: "CommitPosition",
			publish: func(pub *election.SbePublisher) {
				pub.CommitPosition(dst, 11, 12, 13)
			},
			templateID: clusterwire.CommitPositionSbeTemplateID,
			decode: func(wire []byte) (any, bool) {
				f, ok := codecs.TryDecodeCommitPositionAt(wire, 0)
				return f, ok
			},
			want: codecs.CommitPositionFields{
				LeadershipTermID: 11,
				LogPosition:      12,
				LeaderMemberID:   13,
			},
		},
	}

	for _, tc := range cases {
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

	pub.CanvassPosition(dst, 1, 2, 3, 4)
	require.True(t, pub.RequestVote(dst, 10, 20, 30, 40))
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

	pub.CanvassPosition(dst, 1, 2, 3, 4)
	require.Len(t, pub.Sent, 1)

	decoded, ok := codecs.TryDecodeCanvassPositionAt(pub.Sent[0].Wire, 0)
	require.True(t, ok)
	require.Equal(t, election.ProtocolSemanticVersion(), decoded.ProtocolVersion)
}

func TestSbePublisher_protocolVersion_absent_pre_v9(t *testing.T) {
	t.Parallel()

	dst := election.PublicationTarget{MemberID: 1}
	pub := &election.SbePublisher{ActingVersion: 8}
	pub.CanvassPosition(dst, 1, 2, 3, 4)
	require.Len(t, pub.Sent, 1)

	decoded, ok := codecs.TryDecodeCanvassPositionAt(pub.Sent[0].Wire, 0)
	require.True(t, ok)
	require.Equal(t, int32(0), decoded.ProtocolVersion)
}
