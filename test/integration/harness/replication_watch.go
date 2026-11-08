//go:build integration

package harness

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/election"
)

// ReplicationWatch samples a returning member's election once per tick.
//
// Concurrency: call from the goroutine that ticks the member. Not safe from
// other goroutines.
type ReplicationWatch struct {
	t            testing.TB
	module       *cluster.ConsensusModule
	moduleErrors *ModuleErrors
	state        func() string
	memberID     int32
	timeoutBase  int

	// entryStopPosition is the log recording stop position on entry to
	// FOLLOWER_LOG_REPLICATION.
	entryStopPosition int64

	// Replicated reports whether the member entered FOLLOWER_LOG_REPLICATION.
	Replicated bool

	// TimeoutErr holds the first commit timeout after the watch started.
	TimeoutErr error
}

// NewReplicationWatch returns a watch on the open module m. The watch ignores
// commit timeouts that moduleErrors kept before the call. state returns the
// dump for log lines and failures.
func NewReplicationWatch(
	t testing.TB,
	m *cluster.ConsensusModule,
	moduleErrors *ModuleErrors,
	state func() string,
) *ReplicationWatch {
	memberID := m.ThisMemberID()
	return &ReplicationWatch{
		t:            t,
		module:       m,
		moduleErrors: moduleErrors,
		state:        state,
		memberID:     memberID,
		timeoutBase:  moduleErrors.Count(memberID, election.ErrCommitPositionTimeout),
	}
}

// Observe samples the member's election once.
func (w *ReplicationWatch) Observe() {
	if w.TimeoutErr == nil {
		w.TimeoutErr = w.moduleErrors.MatchAt(
			w.memberID,
			election.ErrCommitPositionTimeout,
			w.timeoutBase,
		)
		if w.TimeoutErr != nil {
			w.t.Logf("member %d reported %v:\n%s", w.memberID, w.TimeoutErr, w.state())
		}
	}
	if !w.Replicated && w.module.ElectionState() == election.ElectionStateFollowerLogReplication {
		w.Replicated = true

		// The entry tick at most installs the LogReplication helper. The helper
		// first offers on a later tick. This read therefore returns the stored
		// stop. It does not block on the catalog.
		w.entryStopPosition = w.module.LogRecordingStopPosition()
		w.t.Logf("member %d entered FOLLOWER_LOG_REPLICATION:\n%s", w.memberID, w.state())
	}
}

// RequireReplicated fails the test unless the member entered
// FOLLOWER_LOG_REPLICATION without a commit timeout. Returns the log recording
// stop position on entry to FOLLOWER_LOG_REPLICATION.
func (w *ReplicationWatch) RequireReplicated() int64 {
	w.t.Helper()

	require.True(
		w.t,
		w.Replicated,
		"member %d never entered FOLLOWER_LOG_REPLICATION:\n%s",
		w.memberID,
		w.state(),
	)
	require.NoError(w.t, w.TimeoutErr, "member %d commit timeout:\n%s", w.memberID, w.state())
	return w.entryStopPosition
}
