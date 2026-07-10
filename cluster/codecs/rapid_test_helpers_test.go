//go:build rapid

package codecs_test

import (
	"flag"
	"strconv"
	"sync"
	"testing"

	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
)

const (
	rapidChecksSBEDefault        = 1000
	rapidChecksSessionEvent      = 500
	rapidChecksSessionConnectReq = 200
)

var rapidChecksFlagMu sync.Mutex

func rapidCheck(tb testing.TB, checks int, prop func(*rapid.T)) {
	tb.Helper()

	f := flag.Lookup("rapid.checks")
	if f == nil {
		rapid.Check(tb, prop)
		return
	}

	rapidChecksFlagMu.Lock()
	defer rapidChecksFlagMu.Unlock()

	previous := f.Value.String()
	defer func() { _ = flag.Set("rapid.checks", previous) }()

	if err := flag.Set("rapid.checks", strconv.Itoa(checks)); err != nil {
		tb.Fatalf("rapid.checks: %v", err)
	}

	rapid.Check(tb, prop)
}

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
