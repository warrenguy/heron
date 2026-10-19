//go:build rapid && testhooks

package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
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
	rt *rapid.T,
	pub *aeron.ExclusivePublication,
	end int64,
) []logEntryFrame {
	rt.Helper()
	termBuf := pub.TermBuffer(0)
	require.Less(rt, end, int64(termBuf.Capacity()))
	frames := make([]logEntryFrame, 0)
	for offset := 0; int64(offset) < end; {
		frameLength := int(logbuffer.FrameLengthVolatile(termBuf, offset))
		require.Positive(rt, frameLength)
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

// TestUncommittedEntries_rollback_rapid drives a leader through random timer
// fires, session opens, client and timeout closes, and commit raises. A scan of
// the log publication is the oracle.
//
// After each op:
//   - uncommittedTimers equals the log TimerEvents above commit, in order.
//   - uncommittedCloses equals the log SessionCloseEvents above commit, in
//     order. Each entry holds the closing session outside the registry.
//   - Each live session holds the end position of its SessionOpenEvent.
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

		assertInvariants := func() {
			rt.Helper()
			commitPosition := a.CommitPosition()
			publicationPosition := a.logPublisher.Position()
			require.LessOrEqual(rt, commitPosition, publicationPosition)

			wantTimers := make([]uncommittedTimer, 0)
			wantCloses := make([]logEntryFrame, 0)
			openEnds := make(map[int64]int64)
			for _, frame := range scanLogEntryFrames(rt, pub, publicationPosition) {
				switch frame.templateID {
				case clusterwire.SessionOpenEventSbeTemplateID:
					openEnds[frame.id] = frame.endPosition
					continue
				case clusterwire.TimerEventSbeTemplateID:
					if frame.endPosition > commitPosition {
						wantTimers = append(wantTimers, uncommittedTimer{
							appendPosition: frame.endPosition,
							correlationID:  frame.id,
						})
					}
				case clusterwire.SessionCloseEventSbeTemplateID:
					if frame.endPosition > commitPosition {
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
				a.sessionWork(nowNs)

			case uncommittedOpClientClose:
				ids := openSessionIDs(a)
				if len(ids) == 0 {
					break
				}
				id := rapid.SampledFrom(ids).Draw(rt, "clientCloseID")
				clientIngress.Enqueue(encodeSessionCloseRequestWire(t, leadershipTermID, id))
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
				// Mark and remove: two work units.
				require.Equal(rt, 2, a.sessionCloseWork(nowNs))
				_, ok = a.sessions.sessionByID(id)
				require.False(rt, ok)

			case uncommittedOpUpdateLeader:
				quorum := rapid.Int64Range(a.CommitPosition(), a.logPublisher.Position()).Draw(rt, "quorum")
				a.UpdateLeaderPosition(nowNs, quorum)
			}
			assertInvariants()
		}
	})
}
