package sim

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScheduler_sameAtNsSeqOrdering(t *testing.T) {
	t.Parallel()

	sch := NewScheduler(1_000)
	var order []uint64

	sch.Schedule(2_000, ScheduledEvent{Kind: EventNodeTick, NodeID: 1})
	sch.Schedule(1_500, ScheduledEvent{Kind: EventNodeTick, NodeID: 0})
	sch.Schedule(2_000, ScheduledEvent{Kind: EventDeliver, NodeID: 2})

	for {
		ev, ok := sch.PopNext()
		if !ok {
			break
		}
		order = append(order, ev.Seq)
		sch.SetNow(ev.AtNs)
	}

	require.Equal(t, []uint64{2, 1, 3}, order)
}

func TestScheduler_coalescedRetickBeforeSameInstantDelivers(t *testing.T) {
	t.Parallel()

	sch := NewScheduler(1_000)
	sch.Schedule(1_000, ScheduledEvent{Kind: EventNodeTick, NodeID: 0})

	coalesceSeq := sch.ReserveSeq()
	sch.Schedule(1_000, ScheduledEvent{Kind: EventDeliver, NodeID: 1})
	sch.Schedule(1_000, ScheduledEvent{Kind: EventDeliver, NodeID: 2})
	sch.ScheduleReserved(1_000, coalesceSeq, ScheduledEvent{Kind: EventNodeTick, NodeID: 0})

	var order []uint64
	for {
		ev, ok := sch.PopNext()
		if !ok {
			break
		}
		order = append(order, ev.Seq)
	}

	require.Equal(t, []uint64{1, 2, 3, 4}, order)
}

func TestScheduler_PeekNext(t *testing.T) {
	t.Parallel()

	sch := NewScheduler(500)
	sch.Schedule(1_000, ScheduledEvent{Kind: EventNodeTick, NodeID: 0})

	ev, ok := sch.PeekNext()
	require.True(t, ok)
	require.Equal(t, EventNodeTick, ev.Kind)
	require.Equal(t, int32(0), ev.NodeID)

	_, ok = sch.PopNext()
	require.True(t, ok)
	_, ok = sch.PeekNext()
	require.False(t, ok)
}
