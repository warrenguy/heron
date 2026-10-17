//go:build testhooks

package consensus

import (
	"math"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

// logStopEndOfStream is the end-of-stream word the log stop rows write.
const logStopEndOfStream int64 = 256

// logStopAdapter builds the log adapter for one row.
type logStopAdapter func(tb testing.TB) *LogAdapter

// openLogImage binds an open image whose end-of-stream word is
// endOfStreamPosition.
func openLogImage(endOfStreamPosition int64) logStopAdapter {
	return func(tb testing.TB) *LogAdapter {
		tb.Helper()
		adapter, image := boundLogAdapter(tb)
		aeron.ImageSetEndOfStreamPositionForHooks(image, endOfStreamPosition)
		return adapter
	}
}

// closedLogImage binds a closed image. Its subscriber position is
// logStopEndOfStream. Its end-of-stream word is endOfStreamPosition.
func closedLogImage(endOfStreamPosition int64) logStopAdapter {
	return func(tb testing.TB) *LogAdapter {
		tb.Helper()
		adapter, image := boundLogAdapter(tb)
		aeron.ImageSetSubscriberPositionForHooks(image, logStopEndOfStream)
		aeron.ImageSetEndOfStreamPositionForHooks(image, endOfStreamPosition)
		aeron.ImageInternalCloseForHooks(image)
		return adapter
	}
}

// unboundLogImage returns an adapter that holds no image.
func unboundLogImage(testing.TB) *LogAdapter {
	return NewLogAdapter(0)
}

// newLogStopAgent builds a settled agent whose heartbeat and quorum triggers
// stay quiet at nowNs.
func newLogStopAgent(
	role Role,
	leaderMemberID int32,
	adapter *LogAdapter,
	control LogRecordingControl,
	nowNs int64,
) *Agent {
	a := NewAgent(AgentConfig{
		Role:                     role,
		Commit:                   new(atomic.Int64),
		Positions:                &scriptedActiveQuorumTable{active: true},
		LogAdapter:               adapter,
		LeadershipTermID:         3,
		LeaderMemberID:           leaderMemberID,
		LeaderHeartbeatTimeoutNs: DefaultLeaderHeartbeatTimeoutNs,
	})
	a.SetLogRecordingControl(control)
	a.timeOfLastLogUpdateNs = nowNs
	return a
}

// TestElectionReinstall_logStopTriggers covers the log recording STOP and
// closed log image triggers through DoWork.
func TestElectionReinstall_logStopTriggers(t *testing.T) {
	const nowNs int64 = 1_000_000_000_000
	const followedLeader int32 = 1

	tests := []struct {
		name           string
		role           Role
		leaderMemberID int32
		adapter        logStopAdapter
		stopSignalled  bool
		stopPosition   int64
		wantTrigger    ElectionTrigger
		wantGraceful   int32
	}{
		{
			name:           "follower_stop_at_end_of_stream",
			role:           RoleFollower,
			leaderMemberID: followedLeader,
			adapter:        openLogImage(logStopEndOfStream),
			stopSignalled:  true,
			stopPosition:   logStopEndOfStream,
			wantTrigger:    ElectionTriggerLogRecordingStopped,
			wantGraceful:   followedLeader,
		},
		{
			name:           "follower_stop_below_end_of_stream",
			role:           RoleFollower,
			leaderMemberID: followedLeader,
			adapter:        openLogImage(logStopEndOfStream),
			stopSignalled:  true,
			stopPosition:   logStopEndOfStream - 32,
			wantTrigger:    ElectionTriggerLogRecordingStopped,
			wantGraceful:   nullMemberID,
		},
		{
			name:           "leader_stop_with_active_quorum",
			role:           RoleLeader,
			leaderMemberID: nullMemberID,
			adapter:        openLogImage(logStopEndOfStream),
			stopSignalled:  true,
			stopPosition:   logStopEndOfStream,
			wantTrigger:    ElectionTriggerLogRecordingStopped,
			wantGraceful:   nullMemberID,
		},
		{
			name:           "follower_closed_image_end_of_stream",
			role:           RoleFollower,
			leaderMemberID: followedLeader,
			adapter:        closedLogImage(logStopEndOfStream),
			wantTrigger:    ElectionTriggerLogImageClosed,
			wantGraceful:   followedLeader,
		},
		{
			name:           "follower_closed_image_no_end_of_stream",
			role:           RoleFollower,
			leaderMemberID: followedLeader,
			adapter:        closedLogImage(math.MaxInt64),
			wantTrigger:    ElectionTriggerLogImageClosed,
			wantGraceful:   nullMemberID,
		},
		{
			name:           "follower_adapter_without_image",
			role:           RoleFollower,
			leaderMemberID: followedLeader,
			adapter:        unboundLogImage,
			wantTrigger:    ElectionTriggerLogImageClosed,
			wantGraceful:   nullMemberID,
		},
		{
			name:           "leader_holding_closed_image",
			role:           RoleLeader,
			leaderMemberID: nullMemberID,
			adapter:        closedLogImage(logStopEndOfStream),
			wantTrigger:    ElectionTriggerNone,
		},
		{
			name:           "follower_nil_adapter",
			role:           RoleFollower,
			leaderMemberID: followedLeader,
			wantTrigger:    ElectionTriggerNone,
		},
		{
			name:           "stop_and_closed_image_one_tick",
			role:           RoleFollower,
			leaderMemberID: followedLeader,
			adapter:        closedLogImage(logStopEndOfStream),
			stopSignalled:  true,
			stopPosition:   logStopEndOfStream,
			wantTrigger:    ElectionTriggerLogRecordingStopped,
			wantGraceful:   followedLeader,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var adapter *LogAdapter
			if tc.adapter != nil {
				adapter = tc.adapter(t)
			}
			control := &fakeLogRecordingControl{
				stopSignalled:      tc.stopSignalled,
				stopSignalPosition: tc.stopPosition,
			}
			fac := &recordingElectionFactory{}
			a := newLogStopAgent(tc.role, tc.leaderMemberID, adapter, control, nowNs)
			a.SetElectionFactory(fac.factory())

			work := a.DoWork(nowNs)
			require.False(t, control.stopSignalled, "mark consumed")

			if tc.wantTrigger == ElectionTriggerNone {
				require.Zero(t, work)
				require.Nil(t, a.Election())
				require.Zero(t, a.ElectionCount())
				require.Equal(t, tc.role, a.Role())
				require.Empty(t, fac.seeds)
				return
			}
			// Install (1) plus same-cycle runner.DoWork (1).
			require.Equal(t, 2, work)
			require.NotNil(t, a.Election())
			require.Equal(t, tc.wantTrigger, a.LastElectionTrigger())
			require.Equal(t, int64(1), a.ElectionCount())
			require.Equal(t, RoleFollower, a.Role())
			require.Len(t, fac.seeds, 1)
			require.Equal(t, tc.wantGraceful, fac.seeds[0].GracefulClosedLeaderID)
			require.Equal(t, int64(3), fac.seeds[0].LeadershipTermID)
			require.Equal(t, 1, fac.runners[0].calls)
		})
	}

	t.Run("stop_while_election_live", func(t *testing.T) {
		control := &fakeLogRecordingControl{
			stopSignalled:      true,
			stopSignalPosition: logStopEndOfStream,
		}
		fac := &recordingElectionFactory{}
		a := newLogStopAgent(
			RoleFollower,
			followedLeader,
			openLogImage(logStopEndOfStream)(t),
			control,
			nowNs,
		)
		a.SetElection(&orderRunner{n: 1})
		a.SetElectionFactory(fac.factory())

		// Live tick: the runner works, the mark drops.
		require.Equal(t, 1, a.DoWork(nowNs))
		require.False(t, control.stopSignalled)
		require.Equal(t, int64(1), a.ElectionCount())
		require.Equal(t, ElectionTriggerBootstrap, a.LastElectionTrigger())
		require.Empty(t, fac.seeds)

		// Settled tick: the dropped mark installs nothing.
		a.ElectionComplete(nowNs)
		require.Zero(t, a.DoWork(nowNs+1))
		require.Nil(t, a.Election())
		require.Equal(t, int64(1), a.ElectionCount())
		require.Empty(t, fac.seeds)
	})

	t.Run("nil_factory", func(t *testing.T) {
		control := &fakeLogRecordingControl{
			stopSignalled:      true,
			stopSignalPosition: logStopEndOfStream,
		}
		a := newLogStopAgent(
			RoleFollower,
			followedLeader,
			openLogImage(logStopEndOfStream)(t),
			control,
			nowNs,
		)

		require.Zero(t, a.DoWork(nowNs))
		require.False(t, control.stopSignalled)
		require.Nil(t, a.Election())
		require.Zero(t, a.ElectionCount())
	})
}
