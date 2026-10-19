//go:build rapid && testhooks

package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

type uncommittedEntriesOpKind int

const (
	uncommittedOpSchedule uncommittedEntriesOpKind = iota
	uncommittedOpTimerWork
	uncommittedOpOpen
	uncommittedOpClientClose
	uncommittedOpTimeoutClose
	uncommittedOpUpdateLeader
	uncommittedOpPrepare
)

// openSessionIDs returns the ids of every open session, in accept order.
func openSessionIDs(a *Agent) []int64 {
	ids := make([]int64, 0)
	for _, s := range a.sessions.all {
		if s.state == sessionOpen {
			ids = append(ids, s.id)
		}
	}
	return ids
}

// scheduledTimerDeadlines returns the deadline of each timer on the wheel, by
// correlation id.
func scheduledTimerDeadlines(rt *rapid.T, a *Agent) map[int64]int64 {
	rt.Helper()
	deadlines := make(map[int64]int64, len(a.timers.timerIDByCorrelationID))
	for correlationID := range a.timers.timerIDByCorrelationID {
		deadlines[correlationID] = scheduledDeadline(rt, a.timers, correlationID)
	}
	return deadlines
}

// TestUncommittedEntries_rollback_rapid drives a leader through random timer
// fires, session connects and opens, client and timeout closes, commit
// raises, and prepares. A scan of the log publication is the oracle. The
// expected tracked lists hold only the frames above both commit and the
// publication position at the last prepare.
//
// After each op:
//   - uncommittedTimers equals those log TimerEvents, in order.
//   - uncommittedCloses equals those log SessionCloseEvents, in order. Each
//     entry holds the closing session outside the registry.
//   - Each live session holds the end position of its SessionOpenEvent.
//
// A prepare runs at a drawn P at or below the commit C. After the prepare:
//   - The wheel holds the earlier timers, plus each TimerEvent id above C at
//     deadline 0.
//   - The closing sessions are the earlier ones plus each close above C,
//     less each session opened above P.
//   - No session opened above P remains. No pre-open session remains.
//   - Both lists are empty.
//
// The member then leads again through JoinLogAsLeader and ElectionComplete.
func TestUncommittedEntries_rollback_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		const leadershipTermID int64 = 3
		commit := new(atomic.Int64)
		a, _, pub := leaderWithTimerIngress(t, AgentConfig{
			LeadershipTermID: leadershipTermID,
			Commit:           commit,
		})
		wireImmediateEgress(t, a)
		clientIngress := attachQueueClientIngress(a)

		nowNs := int64(0)
		nextCorrelationID := int64(1)
		termID := leadershipTermID
		// preparedEnd is the publication position at the last prepare. That
		// prepare emptied both lists, so no frame at or below it is tracked.
		preparedEnd := int64(0)

		assertInvariants := func() {
			rt.Helper()
			commitPosition := a.CommitPosition()
			publicationPosition := a.logPublisher.Position()
			require.LessOrEqual(rt, commitPosition, publicationPosition)
			trackedFloor := max(commitPosition, preparedEnd)

			wantTimers := make([]uncommittedTimer, 0)
			wantCloses := make([]logEntryFrame, 0)
			openEnds := make(map[int64]int64)
			for _, frame := range scanLogEntryFrames(rt, pub, publicationPosition) {
				switch frame.templateID {
				case clusterwire.SessionOpenEventSbeTemplateID:
					openEnds[frame.id] = frame.endPosition
					continue
				case clusterwire.TimerEventSbeTemplateID:
					if frame.endPosition > trackedFloor {
						wantTimers = append(wantTimers, uncommittedTimer{
							appendPosition: frame.endPosition,
							correlationID:  frame.id,
						})
					}
				case clusterwire.SessionCloseEventSbeTemplateID:
					if frame.endPosition > trackedFloor {
						wantCloses = append(wantCloses, frame)
					}
				}
			}

			require.Equal(rt, wantTimers, append([]uncommittedTimer{}, a.uncommittedTimers...))
			require.Len(rt, a.sessions.uncommittedCloses, len(wantCloses))
			for i, want := range wantCloses {
				entry := a.sessions.uncommittedCloses[i]
				require.Equal(rt, want.endPosition, entry.logPosition)
				require.NotNil(rt, entry.session)
				require.Equal(rt, want.id, entry.session.id)
				require.Equal(rt, sessionClosing, entry.session.state)
				require.Equal(rt, want.closeReason, entry.session.closeReason)
				_, inRegistry := a.sessions.sessionByID(want.id)
				require.False(rt, inRegistry)
			}
			for _, s := range a.sessions.all {
				if s.state != sessionOpen && s.state != sessionClosing {
					continue
				}
				openEnd, ok := openEnds[s.id]
				require.True(rt, ok)
				require.Equal(rt, openEnd, s.openedLogPosition)
			}
		}

		ops := []uncommittedEntriesOpKind{
			uncommittedOpSchedule,
			uncommittedOpSchedule,
			uncommittedOpTimerWork,
			uncommittedOpTimerWork,
			uncommittedOpOpen,
			uncommittedOpClientClose,
			uncommittedOpTimeoutClose,
			uncommittedOpUpdateLeader,
			uncommittedOpUpdateLeader,
			uncommittedOpPrepare,
		}
		stepCount := rapid.IntRange(1, 60).Draw(rt, "stepCount")
		for range stepCount {
			switch rapid.SampledFrom(ops).Draw(rt, "op") {
			case uncommittedOpSchedule:
				correlationID := rapid.Int64Range(1, 8).Draw(rt, "timerID")
				deadline := nowNs + rapid.Int64Range(0, 6*timerWheelTickResolutionNs).Draw(rt, "deadlineDelta")
				scheduleTimer(a, correlationID, deadline)

			case uncommittedOpTimerWork:
				nowNs += rapid.Int64Range(0, 3*timerWheelTickResolutionNs).Draw(rt, "timeDelta")
				a.timerWork(nowNs)

			case uncommittedOpOpen:
				a.acceptSessionConnect(validConnectFields(nextCorrelationID), nowNs)
				nextCorrelationID++
				// A connect left pending reaches the next prepare as pre-open.
				if rapid.Bool().Draw(rt, "openNow") {
					a.sessionWork(nowNs)
				}

			case uncommittedOpClientClose:
				ids := openSessionIDs(a)
				if len(ids) == 0 {
					break
				}
				id := rapid.SampledFrom(ids).Draw(rt, "clientCloseID")
				clientIngress.Enqueue(encodeSessionCloseRequestWire(t, termID, id))
				require.Equal(rt, 1, a.pollClientIngress(nowNs))
				_, ok := a.sessions.sessionByID(id)
				require.False(rt, ok)

			case uncommittedOpTimeoutClose:
				ids := openSessionIDs(a)
				if len(ids) == 0 {
					break
				}
				id := rapid.SampledFrom(ids).Draw(rt, "timeoutCloseID")
				s, ok := a.sessions.sessionByID(id)
				require.True(rt, ok)
				s.lastActivityNs = nowNs - a.sessionTimeoutNs - 1
				// Mark and remove: two work units. Each restored close also
				// appends and removes: one work unit.
				restoredCloses := a.ClosingSessionCount()
				require.Equal(rt, 2+restoredCloses, a.sessionCloseWork(nowNs))
				_, ok = a.sessions.sessionByID(id)
				require.False(rt, ok)

			case uncommittedOpUpdateLeader:
				quorum := rapid.Int64Range(a.CommitPosition(), a.logPublisher.Position()).Draw(rt, "quorum")
				a.UpdateLeaderPosition(nowNs, quorum)

			case uncommittedOpPrepare:
				commitPosition := a.CommitPosition()
				prepareAt := rapid.Int64Range(0, commitPosition).Draw(rt, "prepareAt")
				wantTimers := scheduledTimerDeadlines(rt, a)
				for _, timer := range a.uncommittedTimers {
					wantTimers[timer.correlationID] = 0
				}
				wantClosing := make([]int64, 0)
				wantOpen := make([]int64, 0)
				for _, s := range a.sessions.all {
					if s.openedLogPosition > prepareAt {
						continue
					}
					switch s.state {
					case sessionOpen:
						wantOpen = append(wantOpen, s.id)
					case sessionClosing:
						wantClosing = append(wantClosing, s.id)
					}
				}
				for _, entry := range a.sessions.uncommittedCloses {
					if entry.session.openedLogPosition <= prepareAt {
						wantClosing = append(wantClosing, entry.session.id)
					}
				}

				_, prepared, err := a.PrepareForNewLeadership(prepareAt, nowNs)
				require.NoError(rt, err)
				require.True(rt, prepared)
				require.Equal(rt, commitPosition, a.CommitPosition())
				preparedEnd = a.logPublisher.Position()

				requireAgentTimers(rt, a, wantTimers, nil)
				require.Empty(rt, a.uncommittedTimers)
				require.Empty(rt, a.sessions.uncommittedCloses)
				require.Empty(rt, a.sessions.pending)
				gotClosing := make([]int64, 0)
				gotOpen := make([]int64, 0)
				for _, s := range a.sessions.all {
					require.LessOrEqual(rt, s.openedLogPosition, prepareAt)
					switch s.state {
					case sessionOpen:
						gotOpen = append(gotOpen, s.id)
					case sessionClosing:
						gotClosing = append(gotClosing, s.id)
						require.Nil(rt, s.egressPublication)
					}
				}
				require.Equal(rt, wantOpen, gotOpen)
				require.ElementsMatch(rt, wantClosing, gotClosing)

				termID++
				require.NoError(rt, a.JoinLogAsLeader(termID, preparedEnd, 0, false))
				a.ElectionComplete(nowNs)
			}
			assertInvariants()
		}
	})
}
