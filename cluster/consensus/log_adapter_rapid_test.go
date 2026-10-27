//go:build rapid && testhooks

package consensus

import (
	"math"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// TestLogAdapter_Poll_boundIsolation_rapid checks Poll against a reference
// model over random frame layouts and a non-decreasing bound. A frame delivers
// once, in log order, when its start is below the bound. Each Poll delivers at
// most fragmentLimit frames. Position is the end of the last delivered frame,
// so it passes the bound only by finishing a frame that straddles it.
func TestLogAdapter_Poll_boundIsolation_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		limit := rapid.IntRange(1, 6).Draw(rt, "fragmentLimit")
		a, image := boundLogAdapterWithLimit(t, limit)
		spy := &logAdapterHandlerSpy{}
		a.SetHandler(spy)

		// Contiguous unfragmented frames from position 0. Session id is the
		// 1-based frame index. Trailing filler varies span alignment.
		frameCount := rapid.IntRange(1, 10).Draw(rt, "frameCount")
		starts := make([]int64, frameCount)
		ends := make([]int64, frameCount)
		offset := 0
		for i := range frameCount {
			payload := encodeLogAdapterSessionMessagePayload(t, codecs.SessionMessageHeaderFields{
				ClusterSessionID: int64(i + 1),
				Timestamp:        int64(i),
			})
			filler := rapid.IntRange(0, 96).Draw(rt, "filler")
			payload = append(payload, make([]byte, filler)...)
			writeLogAdapterUnfragmentedFrame(t, image, offset, payload)
			starts[i] = int64(offset)
			offset += logbuffer.AlignFragmentChunkLength(len(payload))
			ends[i] = int64(offset)
		}
		totalEnd := int64(offset)

		// Reference model: next undelivered frame, expected position, totals.
		next := 0
		pos := int64(0)
		delivered := 0

		polls := rapid.IntRange(1, 12).Draw(rt, "polls")
		bound := int64(0)
		for range polls {
			// Non-decreasing bound: an exact frame start, a free position, or
			// the last bound.
			var proposal int64
			switch rapid.IntRange(0, 2).Draw(rt, "boundMode") {
			case 0:
				proposal = starts[rapid.IntRange(0, frameCount-1).Draw(rt, "boundFrame")]
			case 1:
				proposal = rapid.Int64Range(0, totalEnd+64).Draw(rt, "boundFree")
			default:
				proposal = bound
			}
			bound = max(bound, proposal)

			want := 0
			wantLast := int64(0)
			for next < frameCount && want < limit && starts[next] < bound {
				pos = ends[next]
				wantLast = int64(next + 1)
				next++
				want++
			}

			require.Equal(rt, want, a.Poll(bound), "bound=%d", bound)
			require.Equal(rt, pos, a.Position(), "bound=%d", bound)
			delivered += want
			require.Equal(rt, delivered, spy.sessionMessages)
			if want > 0 {
				require.Equal(rt, wantLast, spy.lastClusterSessionID)
			}
			require.Zero(rt, spy.nltEvents)
		}

		// Drain past the end: every frame delivers exactly once, in order.
		for next < frameCount {
			want := min(limit, frameCount-next)
			require.Equal(rt, want, a.Poll(totalEnd+1))
			next += want
			delivered += want
		}
		require.Equal(rt, totalEnd, a.Position())
		require.Equal(rt, frameCount, spy.sessionMessages)
		require.Equal(rt, int64(frameCount), spy.lastClusterSessionID)
	})
}

// snapshotHoldFrame is one frame kind in the snapshot hold property log.
type snapshotHoldFrame int

const (
	snapshotHoldMessage snapshotHoldFrame = iota
	snapshotHoldSnapshot
	snapshotHoldSuspend
)

// snapshotHoldModel is the reference model for TestAgent_snapshotActionHold_rapid.
type snapshotHoldModel struct {
	kinds        []snapshotHoldFrame
	ends         []int64
	starts       []int64
	serviceCount int

	next         int   // next undelivered frame
	position     int64 // adapter position
	commit       int64
	snapshotWait int64
	take         int64 // module take position, or 0
	waits        []int64
	retired      []int64
	abandoned    []int64 // retired waits and cancelled takes, in order
	popped       int64
	maxPopped    int64 // highest popped wait position

	// snapshotEnds lists every SNAPSHOT action end in log order. acked[s]
	// counts the snapshot actions service s has acked.
	snapshotEnds []int64
	acked        []int
}

// heldBound lowers boundPosition to a pending snapshot wait, else to the
// module take position.
func (m *snapshotHoldModel) heldBound(boundPosition int64) int64 {
	if m.snapshotWait != 0 {
		return min(boundPosition, m.snapshotWait)
	}
	if m.take != 0 {
		return min(boundPosition, m.take)
	}
	return boundPosition
}

// poll mirrors one LogAdapter.Poll under the held bound.
func (m *snapshotHoldModel) poll(boundPosition int64) {
	limit := m.heldBound(boundPosition)
	for m.next < len(m.kinds) && m.starts[m.next] < limit {
		kind := m.kinds[m.next]
		end := m.ends[m.next]
		m.position = end
		m.next++
		if kind == snapshotHoldMessage {
			continue
		}
		if kind == snapshotHoldSnapshot && m.snapshotWait == 0 && !slices.Contains(m.abandoned, end) {
			m.waits = append(m.waits, end)
			m.snapshotWait = end
		}
		return
	}
}

// replay mirrors a replay from the adapter position to boundPosition. Each
// poll raises commit to the adapter position.
func (m *snapshotHoldModel) replay(boundPosition int64) {
	for {
		limit := m.heldBound(boundPosition)
		before := m.position
		if before >= limit {
			return
		}
		m.poll(limit)
		m.commit = max(m.commit, m.position)
		if m.position == before {
			return
		}
	}
}

// canAck reports whether service serviceID may ack its next snapshot action.
// The action end must sit at or below commit.
func (m *snapshotHoldModel) canAck(serviceID int) bool {
	k := m.acked[serviceID]
	return k < len(m.snapshotEnds) && m.snapshotEnds[k] <= m.commit
}

func (m *snapshotHoldModel) everyServiceAcked(index int) bool {
	for _, count := range m.acked {
		if count <= index {
			return false
		}
	}
	return true
}

func (m *snapshotHoldModel) controlWork() {
	if len(m.waits) == 0 {
		return
	}
	head := m.waits[0]
	if !m.everyServiceAcked(slices.Index(m.snapshotEnds, head)) {
		return
	}
	m.waits = m.waits[1:]
	m.maxPopped = max(m.maxPopped, head)
	if head == m.snapshotWait {
		m.snapshotWait = 0
		m.take = head
	}
	m.popped++
}

// takeWork resolves the module take. The fixture does not wire a recording
// control, so the take resolves on its first step.
func (m *snapshotHoldModel) takeWork() {
	m.take = 0
}

// retire retires a pending wait, then cancels a take. A cancelled take counts
// as a pop: the pop already consumed its acks.
func (m *snapshotHoldModel) retire() {
	if m.snapshotWait != 0 {
		m.retired = append(m.retired, m.snapshotWait)
		m.abandoned = append(m.abandoned, m.snapshotWait)
		m.waits = slices.DeleteFunc(m.waits, func(wait int64) bool { return wait == m.snapshotWait })
		m.snapshotWait = 0
	}
	if m.take != 0 {
		m.abandoned = append(m.abandoned, m.take)
		m.take = 0
	}
}

// expectedAckID returns the lowest expected ack id across services. The
// expected id of a service is its acked count minus its acks for waits that
// have not popped. Every other acked action popped or dropped at a retired
// position. With zero services, returns the popped count.
func (m *snapshotHoldModel) expectedAckID() int64 {
	if m.serviceCount == 0 {
		return m.popped
	}
	lowest := int64(math.MaxInt64)
	for _, count := range m.acked {
		next := int64(count)
		for _, end := range m.snapshotEnds[:count] {
			if slices.Contains(m.waits, end) {
				next--
			}
		}
		lowest = min(lowest, next)
	}
	return lowest
}

// writeSnapshotHoldFrames writes one frame per kind from position 0. Returns
// the end of each frame.
func writeSnapshotHoldFrames(
	t *testing.T,
	image *aeron.Image,
	kinds []snapshotHoldFrame,
) []int64 {
	t.Helper()
	ends := make([]int64, 0, len(kinds))
	offset := 0
	for i, kind := range kinds {
		var end int64
		switch kind {
		case snapshotHoldMessage:
			end = writeSessionMessageFrame(t, image, offset, int64(i+1))
		case snapshotHoldSnapshot:
			end = writeClusterActionFrame(t, image, offset, clusterwire.ClusterAction_SNAPSHOT, 0)
		default:
			end = writeClusterActionFrame(t, image, offset, clusterwire.ClusterAction_SUSPEND, 0)
		}
		ends = append(ends, end)
		offset = int(end)
	}
	return ends
}

// TestAgent_snapshotActionHold_rapid checks the snapshot hold against a
// reference model over random logs and random poll, replay, ack, control,
// module take and prepare steps. A replay binds a replay image at commit,
// replays to a drawn stop, unbinds, then rebinds the live image at the replay
// end. A log poll never passes a pending snapshot wait or an unresolved
// module take. The adapter never moves below a popped action. The wait FIFO,
// commit and expected ack id match the model. A final drain pops every
// remaining wait and resolves every take, so every snapshot action ends popped
// or retired.
func TestAgent_snapshotActionHold_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		serviceCount := rapid.IntRange(0, 3).Draw(rt, "serviceCount")
		adapter, image := boundLogAdapter(t)
		replayImage := aeron.NewTestImageForHooks(t, aeron.NewTestSubscriptionForHooks(t), 0)

		frameCount := rapid.IntRange(1, 12).Draw(rt, "frameCount")
		m := &snapshotHoldModel{serviceCount: serviceCount, acked: make([]int, serviceCount)}
		for range frameCount {
			m.kinds = append(m.kinds, snapshotHoldFrame(rapid.IntRange(0, 2).Draw(rt, "frameKind")))
		}
		m.ends = writeSnapshotHoldFrames(t, image, m.kinds)
		require.Equal(rt, m.ends, writeSnapshotHoldFrames(t, replayImage, m.kinds))
		start := int64(0)
		for i, kind := range m.kinds {
			m.starts = append(m.starts, start)
			start = m.ends[i]
			if kind == snapshotHoldSnapshot {
				m.snapshotEnds = append(m.snapshotEnds, m.ends[i])
			}
		}
		totalEnd := m.ends[frameCount-1]

		a, q := newFollowerActionAgent(totalEnd+100, adapter, serviceCount)
		const nowNs int64 = 5

		drawPosition := func(label string) int64 {
			if rapid.Bool().Draw(rt, label+"Free") {
				return rapid.Int64Range(0, totalEnd+64).Draw(rt, label)
			}
			return m.ends[rapid.IntRange(0, frameCount-1).Draw(rt, label+"Frame")]
		}
		ack := func(serviceID int) {
			if !m.canAck(serviceID) {
				return
			}
			k := m.acked[serviceID]
			enqueueServiceAck(t, q, m.snapshotEnds[k], int64(k), int32(serviceID))
			require.Equal(rt, 1, a.pollServiceControl())
			m.acked[serviceID]++
		}
		followerWork := func() {
			a.followerConsensusWork(nowNs)
			m.poll(min(a.notifiedCommitPosition, totalEnd+100))
			m.commit = max(m.commit, m.position)
		}
		requireModel := func(step string) {
			rt.Helper()
			wait := a.snapshotWaitPosition
			if hold := a.snapshotHoldPosition(); hold != 0 {
				require.LessOrEqual(rt, adapter.Position(), hold, step)
			}
			if wait != 0 {
				count := 0
				for _, w := range a.serviceAckWaits {
					if w == wait {
						count++
					}
				}
				require.Equal(rt, 1, count, step)
			}
			require.Equal(rt, m.take != 0, a.snapshotTake != nil, step)
			if m.take != 0 {
				require.Equal(rt, m.take, a.snapshotTake.logPosition, step)
			}
			require.Equal(rt, m.position, adapter.Position(), step)
			require.GreaterOrEqual(rt, adapter.Position(), m.maxPopped, step)
			require.Equal(rt, m.commit, a.CommitPosition(), step)
			require.Equal(rt, m.snapshotWait, wait, step)
			require.Len(rt, a.serviceAckWaits, len(m.waits), step)
			if len(m.waits) > 0 {
				require.Equal(rt, m.waits, a.serviceAckWaits, step)
			}
			var abandoned []int64
			for _, wait := range a.abandonedSnapshotWaits {
				require.NotContains(rt, a.serviceAckWaits, wait.position, step)
				abandoned = append(abandoned, wait.position)
			}
			require.Equal(rt, m.abandoned, abandoned, step)
			require.Equal(rt, m.expectedAckID(), a.ExpectedServiceAckID(), step)
			require.NoError(rt, a.ServiceAckFailure(), step)
		}

		replay := func() {
			stop := drawPosition("replayStop")
			commit := a.CommitPosition()
			require.Equal(rt, commit, adapter.Position())
			aeron.ImageSetSubscriberPositionForHooks(replayImage, commit)
			a.BindReplayLog(replayImage)
			for {
				before := adapter.Position()
				a.ReplayLogPoll(stop, nowNs)
				if adapter.Position() == before {
					break
				}
			}
			a.UnbindReplayLog(replayImage)
			m.replay(stop)

			// The live join binds a fresh image at the replay end.
			aeron.ImageSetSubscriberPositionForHooks(image, adapter.Position())
			adapter.SetImage(image)
		}

		opNames := []string{
			"notify", "follower work", "catchup", "replay", "ack", "control", "take", "prepare",
		}
		steps := rapid.IntRange(1, 40).Draw(rt, "steps")
		for range steps {
			op := rapid.IntRange(0, len(opNames)-1).Draw(rt, "op")
			switch op {
			case 0:
				a.notifiedCommitPosition = max(a.notifiedCommitPosition, drawPosition("notify"))
			case 1:
				followerWork()
			case 2:
				limit := drawPosition("catchupLimit")
				_, err := a.CatchupPoll(limit, nowNs)
				require.NoError(rt, err)
				m.poll(limit)
				m.commit = max(m.commit, m.position)
			case 3:
				replay()
			case 4:
				if serviceCount > 0 {
					ack(rapid.IntRange(0, serviceCount-1).Draw(rt, "serviceID"))
				}
			case 5:
				a.serviceControlWork()
				m.controlWork()
			case 6:
				a.snapshotTakeWork(nowNs)
				m.takeWork()
			default:
				_, prepared, err := a.PrepareForNewLeadership(0, nowNs)
				require.NoError(rt, err)
				require.True(rt, prepared)
				m.retire()
			}
			requireModel(opNames[op])
		}

		// Final drain: every service acks every action, every remaining wait
		// pops and every take resolves.
		a.notifiedCommitPosition = totalEnd
		done := func() bool {
			if adapter.Position() != totalEnd || len(a.serviceAckWaits) != 0 {
				return false
			}
			if a.snapshotTake != nil {
				return false
			}
			for _, count := range m.acked {
				if count < len(m.snapshotEnds) {
					return false
				}
			}
			return true
		}
		for round := 0; round < 64 && !done(); round++ {
			for serviceID := range serviceCount {
				ack(serviceID)
			}
			a.serviceControlWork()
			m.controlWork()
			a.snapshotTakeWork(nowNs)
			m.takeWork()
			followerWork()
			requireModel("final drain")
		}
		require.True(rt, done())
		require.Equal(rt, totalEnd, adapter.Position())
		require.Zero(rt, a.PendingServiceAckWaits())
		require.Equal(rt, int64(len(m.snapshotEnds)), m.popped+int64(len(m.retired)))
		wantID := m.popped
		if serviceCount > 0 {
			wantID += int64(len(m.retired))
		}
		require.Equal(rt, wantID, a.ExpectedServiceAckID())
	})
}

// replayTimerFrame is one frame in the replay range timer property log. A
// schedule point is a session message the service model answers with
// ScheduleTimer.
type replayTimerFrame struct {
	correlationID int64
	isEvent       bool
	end           int64
}

// replayTimerRequest is one ScheduleTimer the service model sends after
// delayPolls more polls.
type replayTimerRequest struct {
	correlationID int64
	delayPolls    int
}

// TestTimerEventConsume_replayRange_rapid checks that a promoted follower
// fires no timer whose TimerEvent sat in its log, steady range or election
// replay range alike. A service model schedules one timer per schedule point
// it reads under commit, with a drawn lag. Every id fires once in total: a
// TimerEvent in the log counts as the fire. No service cancel or reschedule
// runs.
func TestTimerEventConsume_replayRange_rapid(t *testing.T) {
	t.Parallel()

	const (
		logTermID int64 = 3
		nowNs     int64 = 5
	)
	rapid.Check(t, func(rt *rapid.T) {
		limit := rapid.IntRange(1, 3).Draw(rt, "fragmentLimit")
		adapter, image := boundLogAdapterWithLimit(t, limit)
		replayImage := aeron.NewTestImageForHooks(t, aeron.NewTestSubscriptionForHooks(t), 0)

		// Log: each id has one schedule point. An id with an event has one
		// TimerEvent after its schedule point.
		idCount := rapid.IntRange(1, 12).Draw(rt, "idCount")
		deadlines := make(map[int64]int64, idCount)
		hasEvent := make(map[int64]bool, idCount)
		var awaiting []int64
		var frames []replayTimerFrame
		next := int64(1)
		offset := 0
		writeFrame := func(frame replayTimerFrame) {
			var end int64
			if frame.isEvent {
				end = writeTimerEventFrameInTerm(t, image, offset, frame.correlationID, logTermID)
				require.Equal(rt, end, writeTimerEventFrameInTerm(
					t, replayImage, offset, frame.correlationID, logTermID,
				))
			} else {
				end = writeSessionMessageFrame(t, image, offset, frame.correlationID)
				require.Equal(rt, end, writeSessionMessageFrame(
					t, replayImage, offset, frame.correlationID,
				))
			}
			frame.end = end
			frames = append(frames, frame)
			offset = int(end)
		}
		for next <= int64(idCount) || len(awaiting) > 0 {
			addEvent := len(awaiting) > 0 &&
				(next > int64(idCount) || rapid.Bool().Draw(rt, "eventNext"))
			if addEvent {
				i := rapid.IntRange(0, len(awaiting)-1).Draw(rt, "eventID")
				id := awaiting[i]
				awaiting = slices.Delete(awaiting, i, i+1)
				writeFrame(replayTimerFrame{correlationID: id, isEvent: true})
				continue
			}
			id := next
			next++
			deadlines[id] = rapid.Int64Range(1, 8).Draw(rt, "deadlineTicks") *
				timerWheelTickResolutionNs
			writeFrame(replayTimerFrame{correlationID: id})
			if rapid.Bool().Draw(rt, "hasEvent") {
				hasEvent[id] = true
				awaiting = append(awaiting, id)
			}
		}
		replayStop := frames[len(frames)-1].end
		commitIndex := rapid.IntRange(0, len(frames)).Draw(rt, "commitFrames")
		electionCommit := int64(0)
		if commitIndex > 0 {
			electionCommit = frames[commitIndex-1].end
		}

		a, q, _ := leaderWithTimerIngress(t, AgentConfig{
			LeadershipTermID: logTermID,
			Commit:           new(atomic.Int64),
			LogAdapter:       adapter,
		})
		a.RoleSet(RoleFollower)
		appended := make(map[int64]int)
		a.timerExpiryHandler = func(correlationID, now int64) bool {
			appended[correlationID]++
			return a.handleTimerExpiry(correlationID, now)
		}

		// Service model: reads frames under commit and sends ScheduleTimer for
		// each schedule point after a drawn lag of polls.
		read := 0
		var pending []replayTimerRequest
		serviceStep := func() {
			for read < len(frames) && frames[read].end <= a.CommitPosition() {
				frame := frames[read]
				read++
				if frame.isEvent {
					continue
				}
				pending = append(pending, replayTimerRequest{
					correlationID: frame.correlationID,
					delayPolls:    rapid.IntRange(0, 3).Draw(rt, "lag"),
				})
			}
			kept := pending[:0]
			for _, request := range pending {
				if request.delayPolls > 0 {
					request.delayPolls--
					kept = append(kept, request)
					continue
				}
				enqueueScheduleTimer(t, q, request.correlationID, deadlines[request.correlationID])
				require.Equal(rt, 1, a.pollServiceControl())
			}
			pending = kept
		}

		// Phase 1: steady polls to the election commit.
		a.notifiedCommitPosition = electionCommit
		for adapter.Position() < electionCommit {
			a.followerConsensusWork(nowNs)
			serviceStep()
		}
		require.Equal(rt, electionCommit, a.CommitPosition())

		// Phase 2: election replay of the range from the commit.
		aeron.ImageSetSubscriberPositionForHooks(replayImage, electionCommit)
		adapter.SetImage(nil)
		a.BindReplayLog(replayImage)
		for adapter.Position() < replayStop {
			a.ReplayLogPoll(replayStop, nowNs)
			serviceStep()
		}
		a.UnbindReplayLog(replayImage)
		require.Equal(rt, replayStop, a.CommitPosition())

		// Phase 3: promote, then run the service and the timers past every
		// deadline.
		require.NoError(rt, a.JoinLogAsLeader(logTermID+1, replayStop, 0, false))
		a.ElectionComplete(0)
		tick := int64(1)
		for ; tick <= 12 || len(pending) > 0 || read < len(frames); tick++ {
			serviceStep()
			a.timerWork(tick * timerWheelTickResolutionNs)
		}
		a.timerWork((tick + 8) * timerWheelTickResolutionNs)

		for id := int64(1); id <= int64(idCount); id++ {
			want := 1
			if hasEvent[id] {
				want = 0
			}
			require.Equal(rt, want, appended[id], "id %d", id)
		}
		require.Zero(rt, a.ExpiredTimerCount())
		require.Zero(rt, a.ScheduledTimerCount())
	})
}

// TestTimerEventConsume_formerLeaderReplayRange_rapid drives a leader that
// fires up to 12 ids, some above a drawn commit C. A drawn T at or above C
// truncates the log. The leader prepares at C and replays [C, T]. Then it
// leads again and runs timer work past every deadline, or follows and reads
// a new leader's TimerEvent for each truncated id.
//
// A re-led member fires each truncated id exactly once. Either way no timer
// and no count remains.
func TestTimerEventConsume_formerLeaderReplayRange_rapid(t *testing.T) {
	t.Parallel()

	const logTermID int64 = 3
	rapid.Check(t, func(rt *rapid.T) {
		a, q, pub := leaderWithTimerIngress(t, AgentConfig{
			LeadershipTermID: logTermID,
			Commit:           new(atomic.Int64),
			LogAdapter:       NewLogAdapter(0),
		})
		newTermEvents := make(map[int64]int)
		a.timerExpiryHandler = func(correlationID, now int64) bool {
			ok := a.handleTimerExpiry(correlationID, now)
			if ok {
				newTermEvents[correlationID]++
			}
			return ok
		}

		// Phase 1: fire every id as leader.
		idCount := rapid.IntRange(1, 12).Draw(rt, "idCount")
		for id := int64(1); id <= int64(idCount); id++ {
			deadline := rapid.Int64Range(1, 8).Draw(rt, "deadlineTicks") * timerWheelTickResolutionNs
			enqueueScheduleTimer(t, q, id, deadline)
			require.Equal(rt, 1, a.pollServiceControl())
		}
		tick := int64(1)
		for ; tick <= 9; tick++ {
			a.timerWork(tick * timerWheelTickResolutionNs)
		}
		require.Zero(rt, a.ScheduledTimerCount())
		fired := slices.Clone(a.uncommittedTimers)
		require.Len(rt, fired, idCount)
		clear(newTermEvents)

		// Phase 2: commit a drawn prefix. Truncate at or above it.
		frameEnd := func(n int) int64 {
			if n == 0 {
				return 0
			}
			return fired[n-1].appendPosition
		}
		commitCount := rapid.IntRange(0, idCount).Draw(rt, "commitCount")
		keptCount := rapid.IntRange(commitCount, idCount).Draw(rt, "keptCount")
		commitPosition := frameEnd(commitCount)
		keptEnd := frameEnd(keptCount)
		tickNs := tick * timerWheelTickResolutionNs
		if commitPosition > 0 {
			require.Equal(rt, 1, a.UpdateLeaderPosition(tickNs, commitPosition))
		}

		// Phase 3: prepare at C, then replay the kept range from C.
		_, prepared, err := a.PrepareForNewLeadership(commitPosition, tickNs)
		require.NoError(rt, err)
		require.True(rt, prepared)
		require.Equal(rt, int64(idCount-commitCount), a.ScheduledTimerCount())
		replayImage := aeron.NewTestImageForHooks(t, aeron.NewTestSubscriptionForHooks(t), 0)
		copyLogFrames(t, pub, replayImage, keptEnd)
		replayLogFrom(rt, a, replayImage, commitPosition, keptEnd, tickNs)
		require.Equal(rt, int64(idCount-keptCount), a.ScheduledTimerCount())

		if rapid.Bool().Draw(rt, "leadAgain") {
			// Phase 4a: lead again. The wheel fires each truncated id once.
			require.NoError(rt, a.JoinLogAsLeader(logTermID+1, keptEnd, 0, false))
			a.ElectionComplete(tickNs)
			for range timerWheelTicksPerWheel + 1 {
				tickNs += timerWheelTickResolutionNs
				a.timerWork(tickNs)
			}
			for i, timer := range fired {
				want := 0
				if i >= keptCount {
					want = 1
				}
				require.Equal(rt, want, newTermEvents[timer.correlationID],
					"id %d", timer.correlationID)
			}
		} else {
			// Phase 4b: follow. The new leader appends a TimerEvent for each
			// truncated id. The poll past the replay stop reads them.
			offset := int(keptEnd)
			for _, timer := range fired[keptCount:] {
				offset = int(writeTimerEventFrameInTerm(
					t, replayImage, offset, timer.correlationID, logTermID+1,
				))
			}
			replayLogFrom(rt, a, replayImage, keptEnd, int64(offset), tickNs)
		}
		require.Zero(rt, a.ExpiredTimerCount())
		require.Zero(rt, a.ScheduledTimerCount())
	})
}
