//go:build rapid

package codecs_test

import (
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/internal/testutil"
)

const (
	rapidChecksSBEDefault        = 1000
	rapidChecksSessionEvent      = 500
	rapidChecksSessionConnectReq = 200
)

var rapidCheck = testutil.RapidCheck

func rapidEncodeCanvassPosition(
	actingVersion uint16,
	fields codecs.CanvassPositionFields,
) func([]byte, int) (int, bool) {
	return rapidEncodeAt(actingVersion, fields, codecs.EncodeCanvassPosition)
}

func rapidEncodeRequestVote(
	actingVersion uint16,
	fields codecs.RequestVoteFields,
) func([]byte, int) (int, bool) {
	return rapidEncodeAt(actingVersion, fields, codecs.EncodeRequestVote)
}

func rapidEncodeVote(
	actingVersion uint16,
	fields codecs.VoteFields,
) func([]byte, int) (int, bool) {
	return rapidEncodeAt(actingVersion, fields, codecs.EncodeVote)
}

func rapidEncodeNewLeadershipTerm(
	actingVersion uint16,
	fields codecs.NewLeadershipTermFields,
) func([]byte, int) (int, bool) {
	return rapidEncodeAt(actingVersion, fields, codecs.EncodeNewLeadershipTerm)
}

func rapidEncodeAppendPosition(
	actingVersion uint16,
	fields codecs.AppendPositionFields,
) func([]byte, int) (int, bool) {
	return rapidEncodeAt(actingVersion, fields, codecs.EncodeAppendPosition)
}

func rapidEncodeCommitPosition(
	actingVersion uint16,
	fields codecs.CommitPositionFields,
) func([]byte, int) (int, bool) {
	return rapidEncodeAt(actingVersion, fields, codecs.EncodeCommitPosition)
}

func rapidBooleanType(rt *rapid.T, name string) clusterwire.BooleanType {
	rt.Helper()
	return rapid.SampledFrom([]clusterwire.BooleanType{
		clusterwire.BooleanType_FALSE,
		clusterwire.BooleanType_TRUE,
	}).Draw(rt, name)
}
