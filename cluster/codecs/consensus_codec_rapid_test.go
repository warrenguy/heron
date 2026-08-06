//go:build rapid

package codecs_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func TestCanvassPosition_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.CanvassPositionFields{
			LogLeadershipTermID: rapid.Int64().Draw(rt, "logLeadershipTermID"),
			LogPosition:         rapid.Int64().Draw(rt, "logPosition"),
			LeadershipTermID:    rapid.Int64().Draw(rt, "leadershipTermID"),
			FollowerMemberID:    rapid.Int32().Draw(rt, "followerMemberID"),
			ProtocolVersion:     rapidVersionIf(rt, actingVersion, codecs.CanvassProtocolVersionMinVersion),
		}
		wantFields := codecs.CanvassPositionOracleFields(fields, actingVersion)
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.CanvassPositionSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeCanvassPosition(actingVersion, fields),
			wantFields,
		)
	})
}

func TestRequestVote_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.RequestVoteFields{
			LogLeadershipTermID: rapid.Int64().Draw(rt, "logLeadershipTermID"),
			LogPosition:         rapid.Int64().Draw(rt, "logPosition"),
			CandidateTermID:     rapid.Int64().Draw(rt, "candidateTermID"),
			CandidateMemberID:   rapid.Int32().Draw(rt, "candidateMemberID"),
			ProtocolVersion: rapidVersionIf(
				rt, actingVersion, codecs.RequestVoteProtocolVersionMinVersion,
			),
		}
		wantFields := codecs.RequestVoteOracleFields(fields, actingVersion)
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.RequestVoteSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeRequestVote(actingVersion, fields),
			wantFields,
		)
	})
}

func TestVote_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.VoteFields{
			CandidateTermID:     rapid.Int64().Draw(rt, "candidateTermID"),
			LogLeadershipTermID: rapid.Int64().Draw(rt, "logLeadershipTermID"),
			LogPosition:         rapid.Int64().Draw(rt, "logPosition"),
			CandidateMemberID:   rapid.Int32().Draw(rt, "candidateMemberID"),
			FollowerMemberID:    rapid.Int32().Draw(rt, "followerMemberID"),
			Vote:                rapidBooleanType(rt, "vote"),
		}
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.VoteSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeVote(actingVersion, fields),
			fields,
		)
	})
}

func TestNewLeadershipTerm_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.NewLeadershipTermFields{
			LogLeadershipTermID:     rapid.Int64().Draw(rt, "logLeadershipTermID"),
			NextLeadershipTermID:    rapid.Int64().Draw(rt, "nextLeadershipTermID"),
			NextTermBaseLogPosition: rapid.Int64().Draw(rt, "nextTermBaseLogPosition"),
			NextLogPosition:         rapid.Int64().Draw(rt, "nextLogPosition"),
			LeadershipTermID:        rapid.Int64().Draw(rt, "leadershipTermID"),
			TermBaseLogPosition:     rapid.Int64().Draw(rt, "termBaseLogPosition"),
			LogPosition:             rapid.Int64().Draw(rt, "logPosition"),
			LeaderRecordingID:       rapid.Int64().Draw(rt, "leaderRecordingID"),
			Timestamp:               rapid.Int64().Draw(rt, "timestamp"),
			LeaderMemberID:          rapid.Int32().Draw(rt, "leaderMemberID"),
			LogSessionID:            rapid.Int32().Draw(rt, "logSessionID"),
			AppVersion:              rapidVersionIf(rt, actingVersion, 1),
			IsStartup:               rapidBooleanType(rt, "isStartup"),
			CommitPosition:          rapid.Int64().Draw(rt, "commitPosition"),
		}
		wantFields := codecs.NewLeadershipTermOracleFields(fields, actingVersion)
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.NewLeadershipTermSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeNewLeadershipTerm(actingVersion, fields),
			wantFields,
		)
	})
}

func TestAppendPosition_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.AppendPositionFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
			LogPosition:      rapid.Int64().Draw(rt, "logPosition"),
			FollowerMemberID: rapid.Int32().Draw(rt, "followerMemberID"),
			Flags:            uint8(rapid.IntRange(0, 255).Draw(rt, "flags")),
		}
		wantFields := codecs.AppendPositionOracleFields(fields, actingVersion)
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.AppendPositionSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeAppendPosition(actingVersion, fields),
			wantFields,
		)
	})
}

func TestCommitPosition_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.CommitPositionFields{
			LeadershipTermID: rapid.Int64().Draw(rt, "leadershipTermID"),
			LogPosition:      rapid.Int64().Draw(rt, "logPosition"),
			LeaderMemberID:   rapid.Int32().Draw(rt, "leaderMemberID"),
		}
		runCodecRoundtripAtOffset(
			rt,
			clusterwire.CommitPositionSbeTemplateID,
			actingVersion,
			offset,
			rapidEncodeCommitPosition(actingVersion, fields),
			fields,
		)
	})
}
