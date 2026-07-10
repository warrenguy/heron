//go:build rapid

package election_test

import (
	"fmt"
	"os"
	"testing"

	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func TestProposeMaxCandidateTermID_monotonic_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		dir, err := os.MkdirTemp("", "node-state-rapid-*")
		require.NoError(rt, err)
		rt.Cleanup(func() { require.NoError(rt, os.RemoveAll(dir)) })

		f, err := election.OpenNodeStateFile(dir, true, recordinglog.FileSyncNone)
		require.NoError(rt, err)
		rt.Cleanup(func() { require.NoError(rt, f.Close()) })

		proposals := rapid.SliceOfN(rapid.Int64(), 1, 32).Draw(rt, "proposals")
		maxID := election.NullValue
		maxLogPos := election.NullValue
		maxTS := election.NullValue
		for i, id := range proposals {
			logPos := rapid.Int64().Draw(rt, fmt.Sprintf("logPos%d", i))
			ts := rapid.Int64().Draw(rt, fmt.Sprintf("ts%d", i))
			got, err := f.ProposeMaxCandidateTermID(id, logPos, ts)
			require.NoError(rt, err)
			if id > maxID {
				maxID = id
				maxLogPos = logPos
				maxTS = ts
			}
			require.Equal(rt, maxID, got)

			ct, err := f.CandidateTerm()
			require.NoError(rt, err)
			require.Equal(rt, maxID, ct.CandidateTermID())
			require.Equal(rt, maxLogPos, ct.LogPosition())
			require.Equal(rt, maxTS, ct.TimestampMs())
			require.GreaterOrEqual(rt, ct.CandidateTermID(), election.NullValue)
		}
	})
}
