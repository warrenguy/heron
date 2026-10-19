//go:build testhooks

package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

// seedUncommittedEntries tracks one timer and one close at each position, in
// order. The close at positions[i] holds a session with id i+1.
func seedUncommittedEntries(a *Agent, positions []int64) {
	for i, position := range positions {
		a.uncommittedTimers = append(a.uncommittedTimers, uncommittedTimer{
			appendPosition: position,
			correlationID:  int64(i + 1),
		})
		a.sessions.trackUncommittedClose(position, &clientSession{id: int64(i + 1)})
	}
}

// requireUncommittedPositions asserts both lists hold exactly want, in order.
func requireUncommittedPositions(t *testing.T, a *Agent, want []int64) {
	t.Helper()
	timerPositions := make([]int64, 0, len(a.uncommittedTimers))
	for _, timer := range a.uncommittedTimers {
		timerPositions = append(timerPositions, timer.appendPosition)
	}
	closePositions := make([]int64, 0, len(a.sessions.uncommittedCloses))
	for _, entry := range a.sessions.uncommittedCloses {
		require.NotNil(t, entry.session)
		closePositions = append(closePositions, entry.logPosition)
	}
	require.Equal(t, want, timerPositions)
	require.Equal(t, want, closePositions)
}

func TestSweepUncommittedEntriesTo(t *testing.T) {
	t.Parallel()

	const intervalNs int64 = 1000
	tests := []struct {
		name       string
		commit     int64
		nowNs      int64
		quorum     int64
		tracked    []int64
		wantWork   int
		wantCommit int64
		wantKept   []int64
	}{
		{
			name:       "gate pass sweeps at or below quorum",
			nowNs:      1,
			quorum:     200,
			tracked:    []int64{100, 200, 300},
			wantWork:   1,
			wantCommit: 200,
			wantKept:   []int64{300},
		},
		{
			name:       "gate pass past every entry empties both lists",
			nowNs:      1,
			quorum:     400,
			tracked:    []int64{100, 200, 300},
			wantWork:   1,
			wantCommit: 400,
			wantKept:   []int64{},
		},
		{
			// The entry at 100 sits at or below quorum, so a sweep would drop it.
			name:       "gate miss sweeps nothing",
			commit:     200,
			nowNs:      1,
			quorum:     150,
			tracked:    []int64{100, 300},
			wantWork:   0,
			wantCommit: 200,
			wantKept:   []int64{100, 300},
		},
		{
			name:       "interval pass with quorum below commit sweeps nothing new",
			commit:     200,
			nowNs:      intervalNs,
			quorum:     150,
			tracked:    []int64{300},
			wantWork:   1,
			wantCommit: 200,
			wantKept:   []int64{300},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			commit := new(atomic.Int64)
			commit.Store(tt.commit)
			a := NewAgent(AgentConfig{
				Role:                      RoleLeader,
				Commit:                    commit,
				LeaderHeartbeatIntervalNs: intervalNs,
			})
			seedUncommittedEntries(a, tt.tracked)
			timerCap := cap(a.uncommittedTimers)
			closeCap := cap(a.sessions.uncommittedCloses)

			require.Equal(t, tt.wantWork, a.UpdateLeaderPosition(tt.nowNs, tt.quorum))
			require.Equal(t, tt.wantCommit, a.CommitPosition())
			requireUncommittedPositions(t, a, tt.wantKept)
			require.Equal(t, timerCap, cap(a.uncommittedTimers))
			require.Equal(t, closeCap, cap(a.sessions.uncommittedCloses))

			// Swept close slots hold nil, so the swept sessions can be freed.
			vacated := a.sessions.uncommittedCloses[len(tt.wantKept):len(tt.tracked)]
			for _, entry := range vacated {
				require.Nil(t, entry.session)
			}
		})
	}
}

func TestPrepareForNewLeadership_clearsUncommittedEntries(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{
		Role:   RoleLeader,
		Commit: new(atomic.Int64),
	})
	seedUncommittedEntries(a, []int64{100, 200})
	timerCap := cap(a.uncommittedTimers)
	closeCap := cap(a.sessions.uncommittedCloses)

	_, prepared, err := a.PrepareForNewLeadership(50, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	requireUncommittedPositions(t, a, []int64{})
	require.Equal(t, timerCap, cap(a.uncommittedTimers))
	require.Equal(t, closeCap, cap(a.sessions.uncommittedCloses))
	for _, entry := range a.sessions.uncommittedCloses[:closeCap] {
		require.Nil(t, entry.session)
	}

	// A new term tracks at positions below the cleared entries.
	seedUncommittedEntries(a, []int64{60})
	requireUncommittedPositions(t, a, []int64{60})
}

func TestUncommittedEntries_trackAndSweepAllocs(t *testing.T) {
	p, _ := boundLogPublisher(t)
	a := NewAgent(AgentConfig{
		Role:         RoleLeader,
		Commit:       new(atomic.Int64),
		LogPublisher: p,
	})
	session := &clientSession{id: 1}
	cycle := func() {
		require.True(t, a.handleTimerExpiry(1, 0))
		position := a.uncommittedTimers[len(a.uncommittedTimers)-1].appendPosition
		a.sessions.trackUncommittedClose(position, session)
		a.sweepUncommittedEntriesTo(position)
	}

	// Warm-up: the first track grows each list to its working capacity.
	cycle()
	require.Empty(t, a.uncommittedTimers)
	require.Empty(t, a.sessions.uncommittedCloses)

	require.Zero(t, testing.AllocsPerRun(100, cycle))
	require.Empty(t, a.uncommittedTimers)
	require.Empty(t, a.sessions.uncommittedCloses)
}

func TestUncommittedEntries_trackOnlyOnAppend(t *testing.T) {
	const (
		termID        int64 = 3
		acceptNs      int64 = 1
		nowNs         int64 = 100
		correlationID int64 = 7
	)
	a, p, _, s := openLeaderSession(t, termID, acceptNs)

	// Phase: blocked log. Both appends fail and track nothing.
	blockLogPublication(t, p)
	require.False(t, a.handleTimerExpiry(correlationID, nowNs))
	q := attachQueueClientIngress(a)
	q.Enqueue(encodeSessionCloseRequestWire(t, termID, s.id))
	require.Equal(t, 1, a.pollClientIngress(nowNs))
	require.Equal(t, sessionClosing, s.state)
	require.Zero(t, a.sessionCloseWork(nowNs))
	require.Empty(t, a.uncommittedTimers)
	require.Empty(t, a.sessions.uncommittedCloses)

	// Phase: ready log. Each append tracks one entry at its claim end.
	p.SetPublication(aeron.NewTestExclusivePublicationForHooks(t))
	require.True(t, a.handleTimerExpiry(correlationID, nowNs+1))
	require.Equal(t, []uncommittedTimer{{
		appendPosition: p.Position(),
		correlationID:  correlationID,
	}}, a.uncommittedTimers)
	require.Equal(t, 1, a.sessionCloseWork(nowNs+2))
	require.Equal(t, []uncommittedClose{{
		logPosition: p.Position(),
		session:     s,
	}}, a.sessions.uncommittedCloses)
	_, ok := a.sessions.sessionByID(s.id)
	require.False(t, ok)
}
