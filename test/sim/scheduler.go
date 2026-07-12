package sim

import "container/heap"

// EventKind identifies discrete simulation events.
type EventKind uint8

const (
	EventNodeTick EventKind = iota
	EventDeliver
	EventNemesis
)

// ScheduledEvent is one heap-ordered simulation event.
type ScheduledEvent struct {
	AtNs    int64
	Seq     uint64
	Kind    EventKind
	NodeID  int32
	Payload any
}

// Scheduler drives discrete-event simulation with total (AtNs, Seq) ordering.
type Scheduler struct {
	nowNs   int64
	nextSeq uint64
	heap    eventHeap
}

type eventHeap []ScheduledEvent

func (h eventHeap) Len() int { return len(h) }

func (h eventHeap) Less(i, j int) bool {
	if h[i].AtNs != h[j].AtNs {
		return h[i].AtNs < h[j].AtNs
	}
	return h[i].Seq < h[j].Seq
}

func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) {
	*h = append(*h, x.(ScheduledEvent))
}

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// NewScheduler creates a scheduler with master clock at startNs.
func NewScheduler(startNs int64) *Scheduler {
	return &Scheduler{nowNs: startNs}
}

// Now returns the current master simulated time in nanoseconds.
func (s *Scheduler) Now() int64 { return s.nowNs }

// SetNow sets master time during event dispatch.
func (s *Scheduler) SetNow(nowNs int64) { s.nowNs = nowNs }

// ReserveSeq allocates the next sequence number without scheduling an event.
//
// Use with ScheduleReserved so coalesced re-ticks precede same-instant delivers.
func (s *Scheduler) ReserveSeq() uint64 {
	s.nextSeq++
	return s.nextSeq
}

// Schedule inserts an event; Seq is assigned here for deterministic ordering.
func (s *Scheduler) Schedule(atNs int64, ev ScheduledEvent) {
	ev.Seq = s.ReserveSeq()
	ev.AtNs = atNs
	heap.Push(&s.heap, ev)
}

// ScheduleReserved inserts an event with a previously reserved Seq.
func (s *Scheduler) ScheduleReserved(atNs int64, seq uint64, ev ScheduledEvent) {
	ev.AtNs = atNs
	ev.Seq = seq
	heap.Push(&s.heap, ev)
}

// PeekNext returns the next event without removing it.
func (s *Scheduler) PeekNext() (ScheduledEvent, bool) {
	if s.heap.Len() == 0 {
		return ScheduledEvent{}, false
	}
	return s.heap[0], true
}

// PopNext removes and returns the earliest event.
func (s *Scheduler) PopNext() (ScheduledEvent, bool) {
	if s.heap.Len() == 0 {
		return ScheduledEvent{}, false
	}
	return heap.Pop(&s.heap).(ScheduledEvent), true
}
