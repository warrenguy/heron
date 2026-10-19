//go:build testhooks

package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// logEntryFrame is one TimerEvent, SessionOpenEvent or SessionCloseEvent on
// the log, with its end position.
type logEntryFrame struct {
	templateID  uint16
	endPosition int64
	// id is the correlation id of a TimerEvent, or the session id of a
	// session event.
	id          int64
	closeReason clusterwire.CloseReason
}

// scanLogEntryFrames returns every TimerEvent, SessionOpenEvent and
// SessionCloseEvent in term 0 below end, in log order.
func scanLogEntryFrames(
	t fragmentRequireT,
	pub *aeron.ExclusivePublication,
	end int64,
) []logEntryFrame {
	t.Helper()
	termBuf := pub.TermBuffer(0)
	require.Less(t, end, int64(termBuf.Capacity()))
	frames := make([]logEntryFrame, 0)
	for offset := 0; int64(offset) < end; {
		frameLength := int(logbuffer.FrameLengthVolatile(termBuf, offset))
		require.Positive(t, frameLength)
		wire := termBuf.Bytes()[offset+logbuffer.OffsetFramePayload : offset+frameLength]
		offset += logbuffer.AlignFrameLength(frameLength)
		frameEnd := int64(offset)
		if fields, ok := codecs.TryDecodeTimerEventAt(wire, 0); ok {
			frames = append(frames, logEntryFrame{
				templateID:  clusterwire.TimerEventSbeTemplateID,
				endPosition: frameEnd,
				id:          fields.CorrelationID,
			})
			continue
		}
		if fields, ok := codecs.TryDecodeSessionOpenEventAt(wire, 0); ok {
			frames = append(frames, logEntryFrame{
				templateID:  clusterwire.SessionOpenEventSbeTemplateID,
				endPosition: frameEnd,
				id:          fields.ClusterSessionID,
			})
			continue
		}
		if fields, ok := codecs.TryDecodeSessionCloseEventAt(wire, 0); ok {
			frames = append(frames, logEntryFrame{
				templateID:  clusterwire.SessionCloseEventSbeTemplateID,
				endPosition: frameEnd,
				id:          fields.ClusterSessionID,
				closeReason: fields.CloseReason,
			})
		}
	}
	return frames
}

// seedUncommittedEntries tracks one timer and one close at each position, in
// order. The close at positions[i] holds a session with id i+1.
func seedUncommittedEntries(a *Agent, positions []int64) {
	for i, position := range positions {
		trackUncommittedTimer(a, position, int64(i+1))
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

// trackUncommittedTimer records one fired timer at appendPosition.
func trackUncommittedTimer(a *Agent, appendPosition, correlationID int64) {
	a.uncommittedTimers = append(a.uncommittedTimers, uncommittedTimer{
		appendPosition: appendPosition,
		correlationID:  correlationID,
	})
}

// trackClosedSession learns session id open at openedAt, closes it with
// reason, removes it, and records the close at closeAt.
func trackClosedSession(
	t *testing.T,
	a *Agent,
	id, openedAt, closeAt int64,
	reason clusterwire.CloseReason,
) *clientSession {
	t.Helper()
	s := seedLearnedOpen(t, a, id, 1, openedAt)
	require.True(t, a.sessions.markClosing(id, reason))
	a.removeSessionWithResources(s)
	a.sessions.trackUncommittedClose(closeAt, s)
	return s
}

func TestRestoreUncommittedEntries(t *testing.T) {
	t.Parallel()

	const prepareAt int64 = 1000
	liveDeadline := 4 * timerWheelTickResolutionNs

	tests := []struct {
		name string
		run  func(t *testing.T, a *Agent)
	}{
		{
			name: "timers above the position restore at deadline 0",
			run: func(t *testing.T, a *Agent) {
				trackUncommittedTimer(a, prepareAt-1, 1)
				trackUncommittedTimer(a, prepareAt, 2)
				trackUncommittedTimer(a, prepareAt+1, 3)
				trackUncommittedTimer(a, prepareAt+2, 4)

				a.restoreUncommittedEntries(prepareAt)
				requireAgentTimers(t, a, map[int64]int64{3: 0, 4: 0}, nil)
				require.Empty(t, a.uncommittedTimers)
				require.NoError(t, a.TimerFailure())
			},
		},
		{
			name: "restore replaces a live timer for the id",
			run: func(t *testing.T, a *Agent) {
				scheduleTimer(a, 1, liveDeadline)
				trackUncommittedTimer(a, prepareAt+1, 1)

				a.restoreUncommittedEntries(prepareAt)
				requireAgentTimers(t, a, map[int64]int64{1: 0}, nil)
			},
		},
		{
			name: "wheel refusal marks timer failure once",
			run: func(t *testing.T, a *Agent) {
				a.timers = newSmallTimerService(testWheelCapacity)
				for id := int64(1); id <= 4; id++ {
					trackUncommittedTimer(a, prepareAt+id, id)
				}

				a.restoreUncommittedEntries(prepareAt)
				require.ErrorIs(t, a.TimerFailure(), errTimerCapacity)
				requireAgentTimers(t, a, map[int64]int64{1: 0, 2: 0}, nil)
				require.Empty(t, a.uncommittedTimers)
			},
		},
		{
			name: "restore leaves the expired timer counts",
			run: func(t *testing.T, a *Agent) {
				a.expiredTimerCountByCorrelationID[1] = 2
				a.expiredTimerCountByCorrelationID[5] = 1
				trackUncommittedTimer(a, prepareAt+1, 1)

				a.restoreUncommittedEntries(prepareAt)
				requireAgentTimers(t, a, map[int64]int64{1: 0}, map[int64]int64{1: 2, 5: 1})
			},
		},
		{
			name: "closes above the position return closing with their reasons",
			run: func(t *testing.T, a *Agent) {
				committed := trackClosedSession(t, a, 1, 8, prepareAt,
					clusterwire.CloseReason_TIMEOUT)
				timedOut := trackClosedSession(t, a, 2, 8, prepareAt+1,
					clusterwire.CloseReason_TIMEOUT)
				clientClosed := trackClosedSession(t, a, 3, 8, prepareAt+2,
					clusterwire.CloseReason_CLIENT_ACTION)
				require.Empty(t, a.sessions.all)

				a.restoreUncommittedEntries(prepareAt)
				requireSessionIDs(t, a, 2, 3)
				_, ok := a.sessions.sessionByID(committed.id)
				require.False(t, ok)
				require.Equal(t, 2, a.ClosingSessionCount())
				require.Equal(t, 0, a.OpenSessionCount())
				require.Empty(t, a.sessions.pending)
				require.Empty(t, a.sessions.okPending)
				for _, s := range []*clientSession{timedOut, clientClosed} {
					require.Equal(t, sessionClosing, s.state)
					require.Equal(t, int64(8), s.openedLogPosition)
					require.False(t, s.newLeaderEventPending)
					require.Nil(t, s.egressPublication)
					require.Equal(t, int64(nullValue), s.egressAddCorrelationID)
				}
				require.Equal(t, clusterwire.CloseReason_TIMEOUT, timedOut.closeReason)
				require.Equal(t, clusterwire.CloseReason_CLIENT_ACTION, clientClosed.closeReason)
				require.Empty(t, a.sessions.uncommittedCloses)

				// Vacated slots hold nil, so the dropped sessions can be freed.
				for _, entry := range a.sessions.uncommittedCloses[:3] {
					require.Nil(t, entry.session)
				}
			},
		},
		{
			name: "restored session re-enters the all list in accept order",
			run: func(t *testing.T, a *Agent) {
				seedLearnedOpen(t, a, 1, 1, 8)
				trackClosedSession(t, a, 2, 8, prepareAt+1,
					clusterwire.CloseReason_TIMEOUT)
				seedLearnedOpen(t, a, 3, 1, 9)
				requireSessionIDs(t, a, 1, 3)

				a.restoreUncommittedEntries(prepareAt)
				requireSessionIDs(t, a, 1, 2, 3)
				require.Equal(t, 1, a.ClosingSessionCount())
			},
		},
		{
			name: "prepare drops the close of a session opened above the position",
			run: func(t *testing.T, a *Agent) {
				trackClosedSession(t, a, 1, prepareAt+1, prepareAt+2,
					clusterwire.CloseReason_TIMEOUT)

				_, prepared, err := a.PrepareForNewLeadership(prepareAt, 0)
				require.NoError(t, err)
				require.True(t, prepared)
				requireSessionIDs(t, a)
				require.Equal(t, 0, a.ClosingSessionCount())
				require.Empty(t, a.sessions.uncommittedCloses)
			},
		},
		{
			name: "id already in the registry keeps its entry",
			run: func(t *testing.T, a *Agent) {
				closed := trackClosedSession(t, a, 1, 8, prepareAt+1,
					clusterwire.CloseReason_TIMEOUT)
				live := seedLearnedOpen(t, a, 1, 1, 8)
				require.NotSame(t, closed, live)

				a.restoreUncommittedEntries(prepareAt)
				requireSessionIDs(t, a, 1)
				got, ok := a.sessions.sessionByID(1)
				require.True(t, ok)
				require.Same(t, live, got)
				require.Equal(t, sessionOpen, live.state)
				require.Equal(t, 0, a.ClosingSessionCount())
				require.Empty(t, a.sessions.uncommittedCloses)
			},
		},
		{
			name: "second teardown at the same position changes nothing",
			run: func(t *testing.T, a *Agent) {
				trackUncommittedTimer(a, prepareAt+1, 1)
				closed := trackClosedSession(t, a, 2, 8, prepareAt+1,
					clusterwire.CloseReason_TIMEOUT)

				for range 2 {
					_, prepared, err := a.PrepareForNewLeadership(prepareAt, 0)
					require.NoError(t, err)
					require.True(t, prepared)
					requireAgentTimers(t, a, map[int64]int64{1: 0}, nil)
					requireSessionIDs(t, a, closed.id)
					require.Equal(t, 1, a.ClosingSessionCount())
					require.Empty(t, a.uncommittedTimers)
					require.Empty(t, a.sessions.uncommittedCloses)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a := NewAgent(AgentConfig{
				Role:   RoleLeader,
				Commit: new(atomic.Int64),
			})
			tt.run(t, a)
		})
	}
}
