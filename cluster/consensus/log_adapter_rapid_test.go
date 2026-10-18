//go:build rapid && testhooks

package consensus

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

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
	waits        []int64
	retired      []int64
	popped       int64

	// snapshotEnds lists every SNAPSHOT action end in log order. acked[s]
	// counts the snapshot actions service s has acked.
	snapshotEnds []int64
	acked        []int
}

func (m *snapshotHoldModel) isRetired(position int64) bool {
	return slices.Contains(m.retired, position)
}

// heldBound lowers boundPosition to a pending snapshot wait.
func (m *snapshotHoldModel) heldBound(boundPosition int64) int64 {
	if m.snapshotWait != 0 {
		return min(boundPosition, m.snapshotWait)
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
		if kind == snapshotHoldSnapshot && m.snapshotWait == 0 && !m.isRetired(end) {
			m.waits = append(m.waits, end)
			m.snapshotWait = end
		}
		return
	}
}

func (m *snapshotHoldModel) drain(boundPosition int64) {
	for {
		limit := m.heldBound(boundPosition)
		before := m.position
		if before >= limit {
			return
		}
		m.poll(limit)
		if m.position == before {
			return
		}
	}
}

// canAck reports whether service serviceID may ack its next snapshot action.
// The action end must sit at or below commit. Every service must have acked
// each earlier retired action.
func (m *snapshotHoldModel) canAck(serviceID int) bool {
	k := m.acked[serviceID]
	if k >= len(m.snapshotEnds) || m.snapshotEnds[k] > m.commit {
		return false
	}
	for r := range k {
		if m.isRetired(m.snapshotEnds[r]) && !m.everyServiceAcked(r) {
			return false
		}
	}
	return true
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
	if head == m.snapshotWait {
		m.snapshotWait = 0
	}
	m.popped++
}

func (m *snapshotHoldModel) retire() {
	if m.snapshotWait == 0 {
		return
	}
	m.retired = append(m.retired, m.snapshotWait)
	m.waits = slices.DeleteFunc(m.waits, func(wait int64) bool { return wait == m.snapshotWait })
	m.snapshotWait = 0
}

// expectedAckID returns the popped count plus one per retired action that
// every service has acked. With zero services, retired actions do not advance
// the id.
func (m *snapshotHoldModel) expectedAckID() int64 {
	id := m.popped
	if m.serviceCount == 0 {
		return id
	}
	for _, position := range m.retired {
		if m.everyServiceAcked(slices.Index(m.snapshotEnds, position)) {
			id++
		}
	}
	return id
}

// TestAgent_snapshotActionHold_rapid checks the follower snapshot hold against
// a reference model over random logs and random poll, ack, control and
// prepare steps. No log poll passes a pending snapshot wait. The wait FIFO,
// commit and expected ack id match the model. A final drain pops every
// remaining wait, so every snapshot action ends popped or retired.
func TestAgent_snapshotActionHold_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		serviceCount := rapid.IntRange(0, 3).Draw(rt, "serviceCount")
		adapter, image := boundLogAdapter(t)

		frameCount := rapid.IntRange(1, 12).Draw(rt, "frameCount")
		m := &snapshotHoldModel{serviceCount: serviceCount, acked: make([]int, serviceCount)}
		offset := 0
		for i := range frameCount {
			kind := snapshotHoldFrame(rapid.IntRange(0, 2).Draw(rt, "frameKind"))
			var end int64
			switch kind {
			case snapshotHoldMessage:
				end = writeSessionMessageFrame(t, image, offset, int64(i+1))
			case snapshotHoldSnapshot:
				end = writeClusterActionFrame(t, image, offset, clusterwire.ClusterAction_SNAPSHOT, 0)
				m.snapshotEnds = append(m.snapshotEnds, end)
			default:
				end = writeClusterActionFrame(t, image, offset, clusterwire.ClusterAction_SUSPEND, 0)
			}
			m.kinds = append(m.kinds, kind)
			m.starts = append(m.starts, int64(offset))
			m.ends = append(m.ends, end)
			offset = int(end)
		}
		totalEnd := int64(offset)

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
			if wait != 0 {
				require.LessOrEqual(rt, adapter.Position(), wait, step)
				count := 0
				for _, w := range a.serviceAckWaits {
					if w == wait {
						count++
					}
				}
				require.Equal(rt, 1, count, step)
			}
			require.Equal(rt, m.position, adapter.Position(), step)
			require.Equal(rt, m.commit, a.CommitPosition(), step)
			require.Equal(rt, m.snapshotWait, wait, step)
			require.Len(rt, a.serviceAckWaits, len(m.waits), step)
			if len(m.waits) > 0 {
				require.Equal(rt, m.waits, a.serviceAckWaits, step)
			}
			for _, position := range a.abandonedSnapshotPositions {
				require.NotContains(rt, a.serviceAckWaits, position, step)
			}
			require.Equal(rt, m.retired, a.abandonedSnapshotPositions, step)
			require.Equal(rt, m.expectedAckID(), a.ExpectedServiceAckID(), step)
			require.NoError(rt, a.ServiceAckFailure(), step)
		}

		opNames := []string{"notify", "follower work", "catchup", "drain", "ack", "control", "prepare"}
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
				a.CatchupPoll(limit, nowNs)
				m.poll(limit)
				m.commit = max(m.commit, m.position)
			case 3:
				bound := drawPosition("drainBound")
				a.logPollNowNs = nowNs
				a.drainLogBacklog(bound)
				m.drain(bound)
			case 4:
				if serviceCount > 0 {
					ack(rapid.IntRange(0, serviceCount-1).Draw(rt, "serviceID"))
				}
			case 5:
				a.serviceControlWork()
				m.controlWork()
			default:
				_, prepared, err := a.PrepareForNewLeadership(0, nowNs)
				require.NoError(rt, err)
				require.True(rt, prepared)
				m.retire()
			}
			requireModel(opNames[op])
		}

		// Final drain: every service acks every action and every remaining wait
		// pops.
		a.notifiedCommitPosition = totalEnd
		done := func() bool {
			if adapter.Position() != totalEnd || len(a.serviceAckWaits) != 0 {
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
