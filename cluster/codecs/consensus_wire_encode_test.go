package codecs_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestConsensusFlyweightBlockLengths(t *testing.T) {
	require.Equal(t, uint16(96), clusterwire.NewLeadershipTermSbeBlockLength)
	require.Equal(t, uint16(32), clusterwire.CanvassPositionSbeBlockLength)
	require.Equal(t, uint16(32), clusterwire.RequestVoteSbeBlockLength)
	require.Equal(t, uint16(36), clusterwire.VoteSbeBlockLength)
	require.Equal(t, uint16(21), clusterwire.AppendPositionSbeBlockLength)
	require.Equal(t, uint16(20), clusterwire.CommitPositionSbeBlockLength)
}

func TestConsensusBlockLength_versionGates(t *testing.T) {
	cases := []struct {
		name      string
		actingVer uint16
		want      uint16
		blockLen  func(uint16) uint16
	}{
		{"CanvassPosition_v8", 8, 28, codecs.CanvassPositionBlockLength},
		{"CanvassPosition_v16", 16, 32, codecs.CanvassPositionBlockLength},
		{"RequestVote_v8", 8, 28, codecs.RequestVoteBlockLength},
		{"RequestVote_v16", 16, 32, codecs.RequestVoteBlockLength},
		{"NewLeadershipTerm_v14", 14, 88, codecs.NewLeadershipTermBlockLength},
		{"NewLeadershipTerm_v16", 16, 96, codecs.NewLeadershipTermBlockLength},
		{"AppendPosition_v7", 7, 20, codecs.AppendPositionBlockLength},
		{"AppendPosition_v16", 16, 21, codecs.AppendPositionBlockLength},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.blockLen(tc.actingVer))
		})
	}
}

func TestCanvassPosition_roundtrip(t *testing.T) {
	defaultVer := clusterwire.CanvassPositionSbeSchemaVersion
	fieldCases := []codecFieldsActingCase[codecs.CanvassPositionFields]{
		{
			name: "zeros",
			fields: codecs.CanvassPositionFields{
				LogLeadershipTermID: 1,
				LogPosition:         2,
				LeadershipTermID:    3,
				FollowerMemberID:    4,
			},
		},
		{
			name:      "protocol_version_v16",
			actingVer: clusterwire.CanvassPositionSbeSchemaVersion,
			fields: codecs.CanvassPositionFields{
				LogLeadershipTermID: math.MaxInt64,
				LogPosition:         math.MaxInt64,
				LeadershipTermID:    math.MaxInt64,
				FollowerMemberID:    math.MaxInt32,
				ProtocolVersion:     7,
			},
		},
		{
			name:      "no_protocol_version_v8",
			actingVer: 8,
			fields: codecs.CanvassPositionFields{
				LogLeadershipTermID: 10,
				LogPosition:         11,
				LeadershipTermID:    12,
				FollowerMemberID:    13,
				ProtocolVersion:     7,
			},
		},
	}
	runCodecRoundtripCases(
		t,
		clusterwire.CanvassPositionSbeTemplateID,
		defaultVer,
		buildActingFieldRoundtripCases(
			defaultVer,
			fieldCases,
			func(buf []byte, offset int, actingVer uint16, fields codecs.CanvassPositionFields) (int, bool) {
				return codecs.EncodeCanvassPosition(buf, offset, actingVer, fields)
			},
			codecs.CanvassPositionOracleFields,
		),
	)
}

func TestRequestVote_roundtrip(t *testing.T) {
	defaultVer := clusterwire.RequestVoteSbeSchemaVersion
	fieldCases := []codecFieldsActingCase[codecs.RequestVoteFields]{
		{
			name: "zeros",
			fields: codecs.RequestVoteFields{
				LogLeadershipTermID: 1,
				LogPosition:         2,
				CandidateTermID:     3,
				CandidateMemberID:   4,
			},
		},
		{
			name:      "max_int_v16",
			actingVer: clusterwire.RequestVoteSbeSchemaVersion,
			fields: codecs.RequestVoteFields{
				LogLeadershipTermID: math.MaxInt64,
				LogPosition:         math.MaxInt64,
				CandidateTermID:     math.MaxInt64,
				CandidateMemberID:   math.MaxInt32,
				ProtocolVersion:     3,
			},
		},
		{
			name:      "no_protocol_version_v8",
			actingVer: 8,
			fields: codecs.RequestVoteFields{
				LogLeadershipTermID: 9,
				LogPosition:         10,
				CandidateTermID:     11,
				CandidateMemberID:   12,
				ProtocolVersion:     3,
			},
		},
	}
	runCodecRoundtripCases(
		t,
		clusterwire.RequestVoteSbeTemplateID,
		defaultVer,
		buildActingFieldRoundtripCases(
			defaultVer,
			fieldCases,
			func(buf []byte, offset int, actingVer uint16, fields codecs.RequestVoteFields) (int, bool) {
				return codecs.EncodeRequestVote(buf, offset, actingVer, fields)
			},
			codecs.RequestVoteOracleFields,
		),
	)
}

func TestVote_roundtrip(t *testing.T) {
	runCodecRoundtripCases(
		t,
		clusterwire.VoteSbeTemplateID,
		clusterwire.VoteSbeSchemaVersion,
		[]codecRoundtripCase{
			{
				name: "vote_true",
				encode: func(buf []byte, offset int) (int, bool) {
					return codecs.EncodeVote(buf, offset, clusterwire.VoteSbeSchemaVersion, codecs.VoteFields{
						CandidateTermID: 1, LogLeadershipTermID: 2, LogPosition: 3,
						CandidateMemberID: 4, FollowerMemberID: 5, Vote: clusterwire.BooleanType_TRUE,
					})
				},
				wantFields: codecs.VoteFields{
					CandidateTermID: 1, LogLeadershipTermID: 2, LogPosition: 3,
					CandidateMemberID: 4, FollowerMemberID: 5, Vote: clusterwire.BooleanType_TRUE,
				},
			},
			{
				name: "vote_false",
				encode: func(buf []byte, offset int) (int, bool) {
					return codecs.EncodeVote(buf, offset, clusterwire.VoteSbeSchemaVersion, codecs.VoteFields{
						CandidateTermID: 6, LogLeadershipTermID: 7, LogPosition: 8,
						CandidateMemberID: 9, FollowerMemberID: 10, Vote: clusterwire.BooleanType_FALSE,
					})
				},
				wantFields: codecs.VoteFields{
					CandidateTermID: 6, LogLeadershipTermID: 7, LogPosition: 8,
					CandidateMemberID: 9, FollowerMemberID: 10, Vote: clusterwire.BooleanType_FALSE,
				},
			},
			{
				name: "max_int",
				encode: func(buf []byte, offset int) (int, bool) {
					return codecs.EncodeVote(buf, offset, clusterwire.VoteSbeSchemaVersion, codecs.VoteFields{
						CandidateTermID: math.MaxInt64, LogLeadershipTermID: math.MaxInt64,
						LogPosition: math.MaxInt64, CandidateMemberID: math.MaxInt32,
						FollowerMemberID: math.MaxInt32, Vote: clusterwire.BooleanType_TRUE,
					})
				},
				wantFields: codecs.VoteFields{
					CandidateTermID: math.MaxInt64, LogLeadershipTermID: math.MaxInt64,
					LogPosition: math.MaxInt64, CandidateMemberID: math.MaxInt32,
					FollowerMemberID: math.MaxInt32, Vote: clusterwire.BooleanType_TRUE,
				},
			},
		},
	)
}

func TestNewLeadershipTerm_roundtrip(t *testing.T) {
	defaultVer := clusterwire.NewLeadershipTermSbeSchemaVersion
	fieldCases := []codecFieldsActingCase[codecs.NewLeadershipTermFields]{
		{
			name: "full_v16",
			fields: codecs.NewLeadershipTermFields{
				LogLeadershipTermID: 1, NextLeadershipTermID: 2, NextTermBaseLogPosition: 3,
				NextLogPosition: 4, LeadershipTermID: 5, TermBaseLogPosition: 6, LogPosition: 7,
				LeaderRecordingID: 8, Timestamp: 9, LeaderMemberID: 10, LogSessionID: 11,
				AppVersion: 12, IsStartup: clusterwire.BooleanType_TRUE, CommitPosition: 99,
			},
		},
		{
			name:      "no_commit_position_v14",
			actingVer: 14,
			fields: codecs.NewLeadershipTermFields{
				LogLeadershipTermID: 20, NextLeadershipTermID: 21, NextTermBaseLogPosition: 22,
				NextLogPosition: 23, LeadershipTermID: 24, TermBaseLogPosition: 25, LogPosition: 26,
				LeaderRecordingID: 27, Timestamp: 28, LeaderMemberID: 29, LogSessionID: 30,
				IsStartup: clusterwire.BooleanType_FALSE, CommitPosition: 100,
			},
		},
		{
			name: "app_version_omit_v16",
			fields: codecs.NewLeadershipTermFields{
				LogLeadershipTermID: 40, NextLeadershipTermID: 41, NextTermBaseLogPosition: 42,
				NextLogPosition: 43, LeadershipTermID: 44, TermBaseLogPosition: 45, LogPosition: 46,
				LeaderRecordingID: 47, Timestamp: 48, LeaderMemberID: 49, LogSessionID: 50,
				AppVersion: 0, IsStartup: clusterwire.BooleanType_TRUE, CommitPosition: 51,
			},
		},
		{
			name: "max_int_v16",
			fields: codecs.NewLeadershipTermFields{
				LogLeadershipTermID: math.MaxInt64, NextLeadershipTermID: math.MaxInt64,
				NextTermBaseLogPosition: math.MaxInt64, NextLogPosition: math.MaxInt64,
				LeadershipTermID: math.MaxInt64, TermBaseLogPosition: math.MaxInt64,
				LogPosition: math.MaxInt64, LeaderRecordingID: math.MaxInt64, Timestamp: math.MaxInt64,
				LeaderMemberID: math.MaxInt32, LogSessionID: math.MaxInt32, AppVersion: 7,
				IsStartup: clusterwire.BooleanType_TRUE, CommitPosition: math.MaxInt64,
			},
		},
	}
	runCodecRoundtripCases(
		t,
		clusterwire.NewLeadershipTermSbeTemplateID,
		defaultVer,
		buildActingFieldRoundtripCases(
			defaultVer,
			fieldCases,
			func(
				buf []byte,
				offset int,
				actingVer uint16,
				fields codecs.NewLeadershipTermFields,
			) (int, bool) {
				return codecs.EncodeNewLeadershipTerm(buf, offset, actingVer, fields)
			},
			codecs.NewLeadershipTermOracleFields,
		),
	)
}

func TestAppendPosition_roundtrip(t *testing.T) {
	defaultVer := clusterwire.AppendPositionSbeSchemaVersion
	fieldCases := []codecFieldsActingCase[codecs.AppendPositionFields]{
		{
			name: "flags_v16",
			fields: codecs.AppendPositionFields{
				LeadershipTermID: 1, LogPosition: 2, FollowerMemberID: 3, Flags: 0xAB,
			},
		},
		{
			name:      "no_flags_v7",
			actingVer: 7,
			fields: codecs.AppendPositionFields{
				LeadershipTermID: 4, LogPosition: 5, FollowerMemberID: 6, Flags: 0xCD,
			},
		},
		{
			name: "max_int_v16",
			fields: codecs.AppendPositionFields{
				LeadershipTermID: math.MaxInt64, LogPosition: math.MaxInt64,
				FollowerMemberID: math.MaxInt32, Flags: 0xFF,
			},
		},
	}
	runCodecRoundtripCases(
		t,
		clusterwire.AppendPositionSbeTemplateID,
		defaultVer,
		buildActingFieldRoundtripCases(
			defaultVer,
			fieldCases,
			func(buf []byte, offset int, actingVer uint16, fields codecs.AppendPositionFields) (int, bool) {
				return codecs.EncodeAppendPosition(buf, offset, actingVer, fields)
			},
			codecs.AppendPositionOracleFields,
		),
	)
}

func TestCommitPosition_roundtrip(t *testing.T) {
	runCodecRoundtripCases(
		t,
		clusterwire.CommitPositionSbeTemplateID,
		clusterwire.CommitPositionSbeSchemaVersion,
		[]codecRoundtripCase{
			{
				name: "zeros",
				encode: func(buf []byte, offset int) (int, bool) {
					return codecs.EncodeCommitPosition(
						buf, offset, clusterwire.CommitPositionSbeSchemaVersion,
						codecs.CommitPositionFields{LeadershipTermID: 1, LogPosition: 2, LeaderMemberID: 3},
					)
				},
				wantFields: codecs.CommitPositionFields{
					LeadershipTermID: 1, LogPosition: 2, LeaderMemberID: 3,
				},
			},
			{
				name: "max_int",
				encode: func(buf []byte, offset int) (int, bool) {
					return codecs.EncodeCommitPosition(
						buf, offset, clusterwire.CommitPositionSbeSchemaVersion,
						codecs.CommitPositionFields{
							LeadershipTermID: math.MaxInt64, LogPosition: math.MaxInt64,
							LeaderMemberID: math.MaxInt32,
						},
					)
				},
				wantFields: codecs.CommitPositionFields{
					LeadershipTermID: math.MaxInt64, LogPosition: math.MaxInt64,
					LeaderMemberID: math.MaxInt32,
				},
			},
		},
	)
}

func TestEncodeConsensusWire_BufferBounds(t *testing.T) {
	actingVersion := uint16(clusterwire.CanvassPositionSbeSchemaVersion)
	cases := []struct {
		name    string
		wireLen int
		encode  func([]byte) (int, bool)
	}{
		{
			name:    "CanvassPosition",
			wireLen: mustClusterWireMessageLen(t, codecs.CanvassPositionBlockLength(actingVersion)),
			encode: func(buf []byte) (int, bool) {
				return codecs.EncodeCanvassPosition(buf, 0, actingVersion, codecs.CanvassPositionFields{
					LogLeadershipTermID: 1, LogPosition: 2, LeadershipTermID: 3,
					FollowerMemberID: 4, ProtocolVersion: 5,
				})
			},
		},
		{
			name:    "RequestVote",
			wireLen: mustClusterWireMessageLen(t, codecs.RequestVoteBlockLength(actingVersion)),
			encode: func(buf []byte) (int, bool) {
				return codecs.EncodeRequestVote(buf, 0, actingVersion, codecs.RequestVoteFields{
					LogLeadershipTermID: 1, LogPosition: 2, CandidateTermID: 3,
					CandidateMemberID: 4, ProtocolVersion: 5,
				})
			},
		},
		{
			name:    "Vote",
			wireLen: mustClusterWireMessageLen(t, clusterwire.VoteSbeBlockLength),
			encode: func(buf []byte) (int, bool) {
				return codecs.EncodeVote(buf, 0, actingVersion, codecs.VoteFields{
					CandidateTermID: 1, LogLeadershipTermID: 2, LogPosition: 3,
					CandidateMemberID: 4, FollowerMemberID: 5, Vote: clusterwire.BooleanType_TRUE,
				})
			},
		},
		{
			name:    "NewLeadershipTerm",
			wireLen: mustClusterWireMessageLen(t, codecs.NewLeadershipTermBlockLength(actingVersion)),
			encode: func(buf []byte) (int, bool) {
				return codecs.EncodeNewLeadershipTerm(buf, 0, actingVersion, codecs.NewLeadershipTermFields{
					LogLeadershipTermID: 1, NextLeadershipTermID: 2, NextTermBaseLogPosition: 3,
					NextLogPosition: 4, LeadershipTermID: 5, TermBaseLogPosition: 6, LogPosition: 7,
					LeaderRecordingID: 8, Timestamp: 9, LeaderMemberID: 10, LogSessionID: 11,
					IsStartup: clusterwire.BooleanType_TRUE, CommitPosition: 12,
				})
			},
		},
		{
			name:    "AppendPosition",
			wireLen: mustClusterWireMessageLen(t, codecs.AppendPositionBlockLength(actingVersion)),
			encode: func(buf []byte) (int, bool) {
				return codecs.EncodeAppendPosition(buf, 0, actingVersion, codecs.AppendPositionFields{
					LeadershipTermID: 1, LogPosition: 2, FollowerMemberID: 3, Flags: 4,
				})
			},
		},
		{
			name:    "CommitPosition",
			wireLen: mustClusterWireMessageLen(t, clusterwire.CommitPositionSbeBlockLength),
			encode: func(buf []byte) (int, bool) {
				return codecs.EncodeCommitPosition(buf, 0, actingVersion, codecs.CommitPositionFields{
					LeadershipTermID: 1, LogPosition: 2, LeaderMemberID: 3,
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.wireLen-1)
			_, ok := tc.encode(buf)
			require.False(t, ok)
		})
	}
}

func mustClusterWireMessageLen(t *testing.T, blockLength uint16) int {
	t.Helper()
	wireLen, ok := codecs.ClusterWireMessageLen(blockLength)
	require.True(t, ok)
	return wireLen
}

func TestEncodeConsensusTemplates_ZeroAllocs(t *testing.T) {
	actingVersion := uint16(clusterwire.CanvassPositionSbeSchemaVersion)
	cases := []struct {
		name   string
		bufLen int
		encode func([]byte)
	}{
		{
			name:   "EncodeCanvassPosition",
			bufLen: messageHeaderLen + int(clusterwire.CanvassPositionSbeBlockLength),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeCanvassPosition(buf, 0, actingVersion, codecs.CanvassPositionFields{
					LogLeadershipTermID: 1, LogPosition: 2, LeadershipTermID: 3,
					FollowerMemberID: 4, ProtocolVersion: 5,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeRequestVote",
			bufLen: messageHeaderLen + int(clusterwire.RequestVoteSbeBlockLength),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeRequestVote(buf, 0, actingVersion, codecs.RequestVoteFields{
					LogLeadershipTermID: 1, LogPosition: 2, CandidateTermID: 3,
					CandidateMemberID: 4, ProtocolVersion: 5,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeVote",
			bufLen: messageHeaderLen + int(clusterwire.VoteSbeBlockLength),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeVote(buf, 0, actingVersion, codecs.VoteFields{
					CandidateTermID: 1, LogLeadershipTermID: 2, LogPosition: 3,
					CandidateMemberID: 4, FollowerMemberID: 5, Vote: clusterwire.BooleanType_TRUE,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeNewLeadershipTerm",
			bufLen: messageHeaderLen + int(clusterwire.NewLeadershipTermSbeBlockLength),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeNewLeadershipTerm(buf, 0, actingVersion, codecs.NewLeadershipTermFields{
					LogLeadershipTermID: 1, NextLeadershipTermID: 2, NextTermBaseLogPosition: 3,
					NextLogPosition: 4, LeadershipTermID: 5, TermBaseLogPosition: 6, LogPosition: 7,
					LeaderRecordingID: 8, Timestamp: 9, LeaderMemberID: 10, LogSessionID: 11,
					IsStartup: clusterwire.BooleanType_TRUE, CommitPosition: 12,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeAppendPosition",
			bufLen: messageHeaderLen + int(clusterwire.AppendPositionSbeBlockLength),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeAppendPosition(buf, 0, actingVersion, codecs.AppendPositionFields{
					LeadershipTermID: 1, LogPosition: 2, FollowerMemberID: 3, Flags: 4,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
		{
			name:   "EncodeCommitPosition",
			bufLen: messageHeaderLen + int(clusterwire.CommitPositionSbeBlockLength),
			encode: func(buf []byte) {
				_, ok := codecs.EncodeCommitPosition(buf, 0, actingVersion, codecs.CommitPositionFields{
					LeadershipTermID: 1, LogPosition: 2, LeaderMemberID: 3,
				})
				if !ok {
					panic("encode failed")
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.bufLen)
			testutil.AssertZeroAllocs(t, tc.name, 1000, func() {
				tc.encode(buf)
			})
		})
	}
}
