package codecs_test

import (
	"testing"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestTryDecodeConsensusAt_ZeroAllocs(t *testing.T) {
	actingVersion := uint16(clusterwire.CanvassPositionSbeSchemaVersion)
	cases := []struct {
		name   string
		wire   func(t *testing.T) []byte
		decode func([]byte, int, int)
	}{
		{
			name: "CanvassPosition",
			wire: func(t *testing.T) []byte {
				return encodeClusterWire(t, func(buf []byte) (int, bool) {
					return codecs.EncodeCanvassPosition(buf, 0, actingVersion, codecs.CanvassPositionFields{
						LogLeadershipTermID: 1, LogPosition: 2, LeadershipTermID: 3,
						FollowerMemberID: 4, ProtocolVersion: 5,
					})
				})
			},
			decode: func(buf []byte, offset, _ int) {
				_, _ = codecs.TryDecodeCanvassPositionAt(buf, offset)
			},
		},
		{
			name: "RequestVote",
			wire: func(t *testing.T) []byte {
				return encodeClusterWire(t, func(buf []byte) (int, bool) {
					return codecs.EncodeRequestVote(buf, 0, actingVersion, codecs.RequestVoteFields{
						LogLeadershipTermID: 1, LogPosition: 2, CandidateTermID: 3,
						CandidateMemberID: 4, ProtocolVersion: 5,
					})
				})
			},
			decode: func(buf []byte, offset, _ int) {
				_, _ = codecs.TryDecodeRequestVoteAt(buf, offset)
			},
		},
		{
			name: "Vote",
			wire: func(t *testing.T) []byte {
				return encodeClusterWire(t, func(buf []byte) (int, bool) {
					return codecs.EncodeVote(buf, 0, actingVersion, codecs.VoteFields{
						CandidateTermID: 1, LogLeadershipTermID: 2, LogPosition: 3,
						CandidateMemberID: 4, FollowerMemberID: 5, Vote: clusterwire.BooleanType_TRUE,
					})
				})
			},
			decode: func(buf []byte, offset, _ int) {
				_, _ = codecs.TryDecodeVoteAt(buf, offset)
			},
		},
		{
			name: "NewLeadershipTerm",
			wire: func(t *testing.T) []byte {
				return encodeClusterWire(t, func(buf []byte) (int, bool) {
					return codecs.EncodeNewLeadershipTerm(buf, 0, actingVersion, codecs.NewLeadershipTermFields{
						LogLeadershipTermID: 1, NextLeadershipTermID: 2, NextTermBaseLogPosition: 3,
						NextLogPosition: 4, LeadershipTermID: 5, TermBaseLogPosition: 6, LogPosition: 7,
						LeaderRecordingID: 8, Timestamp: 9, LeaderMemberID: 10, LogSessionID: 11,
						AppVersion: 12, IsStartup: clusterwire.BooleanType_TRUE, CommitPosition: 13,
					})
				})
			},
			decode: func(buf []byte, offset, _ int) {
				_, _ = codecs.TryDecodeNewLeadershipTermAt(buf, offset)
			},
		},
		{
			name: "AppendPosition",
			wire: func(t *testing.T) []byte {
				return encodeClusterWire(t, func(buf []byte) (int, bool) {
					return codecs.EncodeAppendPosition(buf, 0, actingVersion, codecs.AppendPositionFields{
						LeadershipTermID: 1, LogPosition: 2, FollowerMemberID: 3, Flags: 4,
					})
				})
			},
			decode: func(buf []byte, offset, _ int) {
				_, _ = codecs.TryDecodeAppendPositionAt(buf, offset)
			},
		},
		{
			name: "CommitPosition",
			wire: func(t *testing.T) []byte {
				return encodeClusterWire(t, func(buf []byte) (int, bool) {
					return codecs.EncodeCommitPosition(buf, 0, actingVersion, codecs.CommitPositionFields{
						LeadershipTermID: 1, LogPosition: 2, LeaderMemberID: 3,
					})
				})
			},
			decode: func(buf []byte, offset, _ int) {
				_, _ = codecs.TryDecodeCommitPositionAt(buf, offset)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runTryDecodeAtZeroAllocs(t, "TryDecode"+tc.name+"At", tc.wire(t), tc.decode)
		})
	}
}

func runTryDecodeAtZeroAllocs(
	t *testing.T,
	name string,
	wire []byte,
	decode func([]byte, int, int),
) {
	t.Helper()
	testutil.AssertZeroAllocs(t, name, 1000, func() {
		decode(wire, 0, codecTestMaxVarLen)
	})
}
