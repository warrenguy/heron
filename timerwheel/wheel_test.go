package timerwheel

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testWheelResolution int64 = 8
	testWheelSpokes     int   = 4
	testWheelAlloc      int   = 2
	testWheelMaxSlots   int   = 1024
	testPollBudget      int   = 8
)

func newTestWheel(alloc, maxSlots int) *Wheel {
	return New(
		0,
		testWheelResolution,
		testWheelSpokes,
		alloc,
		maxSlots,
	)
}

func mustSchedule(t *testing.T, w *Wheel, deadline int64) int64 {
	t.Helper()
	id, ok := w.Schedule(deadline)
	require.True(t, ok)
	return id
}

func collectTimerIDs(dst *[]int64) func(now, timerID int64) bool {
	return func(_, timerID int64) bool {
		*dst = append(*dst, timerID)
		return true
	}
}

// pollWheel walks one wheel tick per call until the wheel has no timers or
// the budget is spent. Each call may expire the current spoke and advance once.
func pollWheel(w *Wheel, now int64, budget int) (int, []int64) {
	var got []int64
	total := 0
	for range budget {
		if w.Count() == 0 {
			break
		}
		total += w.Poll(now, collectTimerIDs(&got), budget-total)
	}
	return total, got
}

func TestWheel_happyExpiry(t *testing.T) {
	t.Parallel()

	w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
	id := mustSchedule(t, w, 24)
	require.Equal(t, int64(1), w.Count())

	var got []int64
	n := w.Poll(16, collectTimerIDs(&got), testPollBudget)
	require.Equal(t, 0, n)
	require.Empty(t, got)
	require.Equal(t, int64(1), w.Count())

	// Now is one wheel tick past the deadline's wheel tick. Catch-up scans
	// the deadline's spoke.
	n, got = pollWheel(w, 32, testPollBudget)
	require.Equal(t, 1, n)
	require.Equal(t, []int64{id}, got)
	require.Equal(t, int64(0), w.Count())
}

func TestWheel_alreadyDue(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		windTo   int64
		deadline int64
		pollNow  int64
	}{
		{
			name:     "deadline at now lands in current wheel tick",
			deadline: 0,
			pollNow:  0,
		},
		{
			name:     "deadline before now lands in current wheel tick",
			deadline: -5,
			pollNow:  3,
		},
		{
			name:     "after CurrentTime clamp uses current wheel tick",
			windTo:   100,
			deadline: 50,
			pollNow:  100,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
			w.CurrentTime(tc.windTo)
			id := mustSchedule(t, w, tc.deadline)
			require.Equal(t, int64(1), w.Count())

			var got []int64
			n := w.Poll(tc.pollNow, collectTimerIDs(&got), testPollBudget)
			require.Equal(t, 1, n)
			require.Equal(t, []int64{id}, got)
			require.Equal(t, int64(0), w.Count())
		})
	}
}

func TestWheel_neverEarly(t *testing.T) {
	t.Parallel()

	t.Run("now below deadline expires nothing", func(t *testing.T) {
		t.Parallel()
		w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
		mustSchedule(t, w, 16)
		var got []int64
		n := w.Poll(15, collectTimerIDs(&got), testPollBudget)
		require.Equal(t, 0, n)
		require.Empty(t, got)
		require.Equal(t, int64(1), w.Count())
	})

	t.Run("across wheel tick boundary", func(t *testing.T) {
		t.Parallel()
		w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
		// deadline 10 is in wheel tick 1 ([8, 16)).
		id := mustSchedule(t, w, 10)

		// Exhaust wheel tick 0. now=8 is CurrentTickTime of wheel tick 0.
		var got []int64
		n := w.Poll(8, collectTimerIDs(&got), testPollBudget)
		require.Equal(t, 0, n)
		require.Empty(t, got)
		require.Equal(t, int64(1), w.Count())

		// On the deadline's spoke, now is still below the deadline.
		n = w.Poll(9, collectTimerIDs(&got), testPollBudget)
		require.Equal(t, 0, n)
		require.Empty(t, got)
		require.Equal(t, int64(1), w.Count())

		n = w.Poll(10, collectTimerIDs(&got), testPollBudget)
		require.Equal(t, 1, n)
		require.Equal(t, []int64{id}, got)
		require.Equal(t, int64(0), w.Count())
	})

	t.Run("mixed deadlines on same spoke", func(t *testing.T) {
		t.Parallel()
		w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
		early := mustSchedule(t, w, 1)
		late := mustSchedule(t, w, 5)
		require.Equal(t, 0, spokeForTimerID(early))
		require.Equal(t, 0, spokeForTimerID(late))

		var got []int64
		n := w.Poll(3, collectTimerIDs(&got), testPollBudget)
		require.Equal(t, 1, n)
		require.Equal(t, []int64{early}, got)
		require.Equal(t, int64(1), w.Count())

		n = w.Poll(5, collectTimerIDs(&got), testPollBudget)
		require.Equal(t, 1, n)
		require.Equal(t, []int64{early, late}, got)
		require.Equal(t, int64(0), w.Count())
	})

	t.Run("later revolution occupant stays", func(t *testing.T) {
		t.Parallel()
		w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
		early := mustSchedule(t, w, 1)
		wrap := mustSchedule(t, w, 33)
		require.Equal(t, 0, spokeForTimerID(early))
		require.Equal(t, 0, spokeForTimerID(wrap))

		var got []int64
		n := w.Poll(7, collectTimerIDs(&got), testPollBudget)
		require.Equal(t, 1, n)
		require.Equal(t, []int64{early}, got)
		require.Equal(t, int64(1), w.Count())
		deadline, ok := w.Deadline(wrap)
		require.True(t, ok)
		require.Equal(t, int64(33), deadline)
	})
}

func TestWheel_cancel(t *testing.T) {
	t.Parallel()

	t.Run("spoke zero reuse and unknown id", func(t *testing.T) {
		t.Parallel()
		w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
		id := mustSchedule(t, w, 0)
		require.True(t, w.Cancel(id))
		require.Equal(t, int64(0), w.Count())

		require.False(t, w.Cancel(id))
		require.False(t, w.Cancel(0))
		require.False(t, w.Cancel(1<<40))

		// Cancelled slot is reusable on the same spoke.
		id2 := mustSchedule(t, w, 0)
		require.Equal(t, id, id2)
		require.Equal(t, int64(1), w.Count())

		other := mustSchedule(t, w, 0)
		require.NotEqual(t, id2, other)
		require.True(t, w.Cancel(id2))
		require.Equal(t, int64(1), w.Count())

		var got []int64
		n := w.Poll(0, collectTimerIDs(&got), testPollBudget)
		require.Equal(t, 1, n)
		require.Equal(t, []int64{other}, got)
		require.Equal(t, int64(0), w.Count())
	})

	t.Run("later spoke id", func(t *testing.T) {
		t.Parallel()
		w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
		// deadline 16 is wheel tick 2, spoke 2. Low 32 bits of the id must
		// decode the slot. int(timerID) would miss on a non-zero spoke.
		id := mustSchedule(t, w, 16)
		require.Equal(t, 2, spokeForTimerID(id))
		require.Equal(t, 0, slotForTimerID(id))
		require.True(t, w.Cancel(id))
		require.Equal(t, int64(0), w.Count())
		require.False(t, w.Cancel(id))

		n, got := pollWheel(w, 32, testPollBudget)
		require.Equal(t, 0, n)
		require.Empty(t, got)
	})
}

func TestWheel_deadline(t *testing.T) {
	t.Parallel()

	w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
	id := mustSchedule(t, w, 24)
	deadline, ok := w.Deadline(id)
	require.True(t, ok)
	require.Equal(t, int64(24), deadline)

	require.True(t, w.Cancel(id))
	_, ok = w.Deadline(id)
	require.False(t, ok)

	_, ok = w.Deadline(0)
	require.False(t, ok)
	_, ok = w.Deadline(1 << 40)
	require.False(t, ok)
}

func TestWheel_sameWheelTick(t *testing.T) {
	t.Parallel()

	w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
	var want []int64
	for _, deadline := range []int64{1, 2, 3} {
		want = append(want, mustSchedule(t, w, deadline))
	}
	require.Equal(t, int64(3), w.Count())

	var got []int64
	n := w.Poll(7, collectTimerIDs(&got), testPollBudget)
	require.Equal(t, 3, n)
	require.Equal(t, int64(0), w.Count())
	require.ElementsMatch(t, want, got)
}

func TestWheel_expiryLimit(t *testing.T) {
	t.Parallel()

	w := newTestWheel(8, testWheelMaxSlots)
	for range 5 {
		mustSchedule(t, w, 0)
	}
	require.Equal(t, int64(5), w.Count())

	var got []int64
	n := w.Poll(0, collectTimerIDs(&got), 2)
	require.Equal(t, 2, n)
	require.Equal(t, int64(3), w.Count())

	n = w.Poll(0, collectTimerIDs(&got), 2)
	require.Equal(t, 2, n)
	require.Equal(t, int64(1), w.Count())

	n = w.Poll(0, collectTimerIDs(&got), 2)
	require.Equal(t, 1, n)
	require.Equal(t, int64(0), w.Count())
	require.Len(t, got, 5)
}

func TestWheel_abortRestores(t *testing.T) {
	t.Parallel()

	w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
	id := mustSchedule(t, w, 0)
	mustSchedule(t, w, 0)
	require.Equal(t, int64(2), w.Count())

	var first int64
	n := w.Poll(
		0,
		func(_, timerID int64) bool {
			first = timerID
			return false
		},
		testPollBudget,
	)
	require.Equal(t, 0, n)
	require.Equal(t, int64(2), w.Count())
	require.Equal(t, id, first)

	var got []int64
	n = w.Poll(0, collectTimerIDs(&got), testPollBudget)
	require.Equal(t, 2, n)
	require.Equal(t, first, got[0])
	require.Equal(t, int64(0), w.Count())
}

func TestWheel_growth(t *testing.T) {
	t.Parallel()

	w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
	// Later spoke must survive the per-spoke rebuild, not a linear copy.
	later := mustSchedule(t, w, 16)
	require.Equal(t, 2, spokeForTimerID(later))

	var ids []int64
	for range 5 {
		ids = append(ids, mustSchedule(t, w, 0))
	}
	require.Equal(t, int64(6), w.Count())
	require.Greater(t, w.tickAllocation, testWheelAlloc)

	require.True(t, w.Cancel(later))
	require.Equal(t, int64(5), w.Count())

	// Timer ids issued before growth still name their slots.
	require.True(t, w.Cancel(ids[0]))
	require.Equal(t, int64(4), w.Count())

	var got []int64
	n := w.Poll(0, collectTimerIDs(&got), 16)
	require.Equal(t, 4, n)
	require.Equal(t, int64(0), w.Count())
	require.ElementsMatch(t, ids[1:], got)
}

func TestWheel_refusal(t *testing.T) {
	t.Parallel()

	// 4 spokes * 2 slots = 8. Doubling a spoke would need 16 slots.
	w := newTestWheel(testWheelAlloc, 8)
	mustSchedule(t, w, 0)
	mustSchedule(t, w, 0)
	require.Equal(t, int64(2), w.Count())
	allocBefore := w.tickAllocation
	slotsBefore := len(w.wheel)

	id, ok := w.Schedule(0)
	require.False(t, ok)
	require.Equal(t, int64(0), id)
	require.Equal(t, int64(2), w.Count())
	require.Equal(t, allocBefore, w.tickAllocation)
	require.Len(t, w.wheel, slotsBefore)

	var got []int64
	n := w.Poll(0, collectTimerIDs(&got), testPollBudget)
	require.Equal(t, 2, n)
	require.Equal(t, int64(0), w.Count())
}

func TestWheel_emptyFastAdvance(t *testing.T) {
	t.Parallel()

	t.Run("past CurrentTickTime advances without a scan", func(t *testing.T) {
		t.Parallel()
		w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
		require.Equal(t, int64(0), w.currentTick)
		var got []int64
		n := w.Poll(w.CurrentTickTime(), collectTimerIDs(&got), testPollBudget)
		require.Equal(t, 0, n)
		require.Empty(t, got)
		require.Equal(t, int64(1), w.currentTick)

		// Past deadline lands on the advanced currentTick, not wheel tick 0.
		id := mustSchedule(t, w, 0)
		require.Equal(t, 1, spokeForTimerID(id))
		require.Equal(t, int64(1), w.Count())
	})

	t.Run("empty poll at MaxInt64 keeps count 0", func(t *testing.T) {
		t.Parallel()
		w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
		var got []int64
		n := w.Poll(math.MaxInt64, collectTimerIDs(&got), testPollBudget)
		require.Equal(t, 0, n)
		require.Empty(t, got)
		require.Equal(t, int64(0), w.Count())
	})
}

func TestWheel_windForward(t *testing.T) {
	t.Parallel()

	w := newTestWheel(testWheelAlloc, testWheelMaxSlots)
	bySpoke := make([]int64, testWheelSpokes)
	for spoke := range testWheelSpokes {
		id := mustSchedule(t, w, int64(spoke)*testWheelResolution)
		require.Equal(t, spoke, spokeForTimerID(id))
		bySpoke[spoke] = id
	}

	windNow := int64(1000)
	w.CurrentTime(windNow)
	require.Equal(t, windNow/testWheelResolution, w.currentTick)
	require.Equal(t, int64(testWheelSpokes), w.Count())

	landed := int(w.currentTick) & w.tickMask
	wrapSpoke := (landed - 1) & w.tickMask
	require.Equal(t, 1, landed)
	require.Equal(t, 0, wrapSpoke)

	// First poll at CurrentTickTime expires only the landed spoke.
	var first []int64
	n := w.Poll(w.CurrentTickTime(), collectTimerIDs(&first), testPollBudget)
	require.Equal(t, 1, n)
	require.Equal(t, []int64{bySpoke[landed]}, first)
	require.NotContains(t, first, bySpoke[wrapSpoke])
	require.Equal(t, int64(testWheelSpokes-1), w.Count())

	var later []int64
	now := w.CurrentTickTime()
	for range testWheelSpokes {
		if w.Count() == 0 {
			break
		}
		now += testWheelResolution
		w.Poll(now, collectTimerIDs(&later), testPollBudget)
	}
	require.Equal(t, int64(0), w.Count())
	require.Contains(t, later, bySpoke[wrapSpoke])
	require.Len(t, later, testWheelSpokes-1)
}

func TestWheel_newPanics(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		resolution int64
		spokes     int
		alloc      int
	}{
		{"tick resolution", 3, testWheelSpokes, testWheelAlloc},
		{"ticks per wheel", testWheelResolution, 3, testWheelAlloc},
		{"tick allocation", testWheelResolution, testWheelSpokes, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Panics(t, func() {
				New(0, tc.resolution, tc.spokes, tc.alloc, testWheelMaxSlots)
			})
		})
	}
}
