package consensus

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/timerwheel"
)

const (
	testWheelResolution int64 = 8
	testWheelSpokes     int   = 4
	testWheelAlloc      int   = 2
	testWheelCapacity   int   = testWheelSpokes * testWheelAlloc
)

func newSmallTimerService(maxSlots int) *timerService {
	return newTimerServiceForWheel(timerwheel.New(
		0,
		testWheelResolution,
		testWheelSpokes,
		testWheelAlloc,
		maxSlots,
	))
}

func requireTimerCount(t fragmentRequireT, svc *timerService, n int) {
	t.Helper()
	require.Equal(t, int64(n), svc.count())
	requireTimerMapsLen(t, svc, n)
}

func requireTimerMapsLen(t fragmentRequireT, svc *timerService, n int) {
	t.Helper()
	require.Len(t, svc.timerIDByCorrelationID, n)
	require.Len(t, svc.correlationIDByTimerID, n)
	for corr, timerID := range svc.timerIDByCorrelationID {
		require.Equal(t, corr, svc.correlationIDByTimerID[timerID])
	}
}

func collectCorrelationIDs(dst *[]int64) func(int64, int64) bool {
	return func(correlationID, _ int64) bool {
		*dst = append(*dst, correlationID)
		return true
	}
}

func collectCorrelationSet(dst map[int64]struct{}) func(int64, int64) bool {
	return func(correlationID, _ int64) bool {
		dst[correlationID] = struct{}{}
		return true
	}
}

func requireNoExpiry(t *testing.T, svc *timerService, now int64, msg string) {
	t.Helper()
	n := svc.poll(now, func(correlationID, _ int64) bool {
		t.Fatalf("%s for %d", msg, correlationID)
		return true
	})
	require.Equal(t, 0, n)
}

func TestTimerService_scheduleExpire(t *testing.T) {
	t.Parallel()

	svc := newTimerService()
	const corr int64 = math.MaxInt64
	require.True(t, svc.schedule(corr, 1))
	requireTimerCount(t, svc, 1)

	var got []int64
	n := svc.poll(1, collectCorrelationIDs(&got))
	require.Equal(t, 1, n)
	require.Equal(t, []int64{corr}, got)
	requireTimerCount(t, svc, 0)
}

func TestTimerService_reschedule(t *testing.T) {
	t.Parallel()

	svc := newTimerService()
	const corr int64 = 7
	oldDeadline := timerWheelTickResolutionNs
	newDeadline := 8 * timerWheelTickResolutionNs
	require.True(t, svc.schedule(corr, oldDeadline))
	requireTimerCount(t, svc, 1)
	require.True(t, svc.schedule(corr, newDeadline))
	requireTimerCount(t, svc, 1)

	requireNoExpiry(t, svc, 4*timerWheelTickResolutionNs, "old deadline fired")
	requireTimerCount(t, svc, 1)

	var got []int64
	// Two wheel ticks past the new deadline: catch-up scans that spoke.
	n := svc.poll(newDeadline+2*timerWheelTickResolutionNs, collectCorrelationIDs(&got))
	require.Equal(t, 1, n)
	require.Equal(t, []int64{corr}, got)
	requireTimerCount(t, svc, 0)
}

func TestTimerService_currentTimeExpiresNothing(t *testing.T) {
	t.Parallel()

	svc := newTimerService()
	const wind int64 = 1 << 40
	require.True(t, svc.schedule(1, timerWheelTickResolutionNs))
	svc.currentTime(wind)
	requireTimerCount(t, svc, 1)
	require.Equal(t, wind+timerWheelTickResolutionNs, svc.wheel.CurrentTickTime())

	requireNoExpiry(t, svc, wind, "wind-forward poll fired")
	requireTimerCount(t, svc, 1)
}

func TestTimerService_cancel(t *testing.T) {
	t.Parallel()

	svc := newTimerService()
	require.False(t, svc.cancel(99))
	require.True(t, svc.schedule(3, 10))
	require.True(t, svc.cancel(3))
	requireTimerCount(t, svc, 0)
	require.False(t, svc.cancel(3))

	requireNoExpiry(t, svc, 10, "cancelled timer fired")
}

func TestTimerService_abortRedelivery(t *testing.T) {
	t.Parallel()

	svc := newTimerService()
	const corr int64 = 11
	require.True(t, svc.schedule(corr, 0))

	n := svc.poll(0, func(correlationID, _ int64) bool {
		require.Equal(t, corr, correlationID)
		return false
	})
	require.Equal(t, 0, n)
	requireTimerCount(t, svc, 1)

	var got []int64
	n = svc.poll(0, collectCorrelationIDs(&got))
	require.Equal(t, 1, n)
	require.Equal(t, []int64{corr}, got)
	requireTimerCount(t, svc, 0)
}

func TestTimerService_catchUp(t *testing.T) {
	t.Parallel()

	t.Run("spread deadlines expire in one poll", func(t *testing.T) {
		t.Parallel()
		svc := newTimerService()
		want := make(map[int64]struct{})
		for i := range 5 {
			corr := int64(i + 1)
			deadline := int64(i) * timerWheelTickResolutionNs
			require.True(t, svc.schedule(corr, deadline))
			want[corr] = struct{}{}
		}
		now := 10 * timerWheelTickResolutionNs
		got := make(map[int64]struct{})
		n := svc.poll(now, collectCorrelationSet(got))
		require.Equal(t, 5, n)
		require.Equal(t, want, got)
		requireTimerCount(t, svc, 0)
	})

	t.Run("expires at most timerPollLimit", func(t *testing.T) {
		t.Parallel()
		svc := newTimerService()
		for i := range timerPollLimit + 5 {
			require.True(t, svc.schedule(int64(i+1), int64(i)*timerWheelTickResolutionNs))
		}
		now := int64(timerPollLimit+10) * timerWheelTickResolutionNs
		n := svc.poll(now, func(int64, int64) bool { return true })
		require.Equal(t, timerPollLimit, n)
		requireTimerCount(t, svc, 5)

		n = svc.poll(now, func(int64, int64) bool { return true })
		require.Equal(t, 5, n)
		requireTimerCount(t, svc, 0)
	})

	t.Run("clustered spokes cap at timerPollLimit", func(t *testing.T) {
		t.Parallel()
		svc := newTimerService()
		const perSpoke = 15
		for i := range perSpoke {
			require.True(t, svc.schedule(int64(i+1), 0))
		}
		for i := range perSpoke {
			require.True(t, svc.schedule(int64(100+i), timerWheelTickResolutionNs))
		}
		requireTimerCount(t, svc, 2*perSpoke)
		now := 3 * timerWheelTickResolutionNs
		n := svc.poll(now, func(int64, int64) bool { return true })
		require.Equal(t, timerPollLimit, n)
		requireTimerCount(t, svc, 2*perSpoke-timerPollLimit)
	})

	t.Run("deadline equal to now on a tick boundary needs three polls", func(t *testing.T) {
		t.Parallel()
		svc := newTimerServiceForWheel(timerwheel.New(0, 1, 4, 1, 4))
		const corr int64 = 1
		const now int64 = 2
		require.True(t, svc.schedule(corr, now))
		requireTimerCount(t, svc, 1)

		requireNoExpiry(t, svc, now, "first catch-up poll fired")
		requireTimerCount(t, svc, 1)

		requireNoExpiry(t, svc, now, "landing poll fired")
		requireTimerCount(t, svc, 1)

		var got []int64
		n := svc.poll(now, collectCorrelationIDs(&got))
		require.Equal(t, 1, n)
		require.Equal(t, []int64{corr}, got)
		requireTimerCount(t, svc, 0)
	})
}

func TestTimerService_clampDeadline(t *testing.T) {
	t.Parallel()

	svc := newTimerService()
	const corr int64 = 42
	require.True(t, svc.schedule(corr, math.MaxInt64))
	requireTimerCount(t, svc, 1)

	requireNoExpiry(t, svc, 0, "clamped deadline fired")
	requireTimerCount(t, svc, 1)

	require.True(t, svc.cancel(corr))
	requireTimerCount(t, svc, 0)
}

func TestTimerService_freshScheduleRefusal(t *testing.T) {
	t.Parallel()

	svc := newSmallTimerService(testWheelCapacity)
	require.True(t, svc.schedule(1, 0))
	require.True(t, svc.schedule(2, 0))
	requireTimerCount(t, svc, 2)

	require.False(t, svc.schedule(3, 0))
	requireTimerCount(t, svc, 2)
	require.NotContains(t, svc.timerIDByCorrelationID, int64(3))

	got := make(map[int64]struct{})
	n := svc.poll(0, collectCorrelationSet(got))
	require.Equal(t, 2, n)
	require.Equal(t, map[int64]struct{}{1: {}, 2: {}}, got)
	requireTimerCount(t, svc, 0)
}

func TestTimerService_rescheduleRefusal(t *testing.T) {
	t.Parallel()

	svc := newSmallTimerService(testWheelCapacity)
	// Future deadline lands on a later spoke so cancel-first cannot free the
	// filled current spoke.
	future := testWheelResolution * 2
	require.True(t, svc.schedule(1, future))
	require.True(t, svc.schedule(2, 0))
	require.True(t, svc.schedule(3, 0))
	requireTimerCount(t, svc, 3)

	require.False(t, svc.schedule(1, 0))
	requireTimerCount(t, svc, 2)
	require.NotContains(t, svc.timerIDByCorrelationID, int64(1))
	require.False(t, svc.cancel(1))

	fired := make(map[int64]struct{})
	handler := collectCorrelationSet(fired)
	// First poll at future+one wheel tick expires spoke 0 (corr 2, 3) and
	// lands on spoke 2. Second poll scans spoke 2, the old deadline spoke.
	now := future + testWheelResolution
	n := svc.poll(now, handler)
	n += svc.poll(now, handler)
	require.Equal(t, 2, n)
	require.Equal(t, map[int64]struct{}{2: {}, 3: {}}, fired)
	require.NotContains(t, fired, int64(1))
	requireTimerCount(t, svc, 0)
}

func TestTimerService_snapshotTimerRows(t *testing.T) {
	t.Parallel()

	svc := newTimerService()
	require.Empty(t, svc.snapshotTimerRows())
	require.True(t, svc.schedule(30, 300))
	require.True(t, svc.schedule(-5, 500))
	require.True(t, svc.schedule(10, timerwheel.NullDeadline))
	require.True(t, svc.schedule(20, 200))
	require.True(t, svc.cancel(20))

	require.Equal(t, []codecs.TimerFields{
		{CorrelationID: -5, Deadline: 500},
		{CorrelationID: 10, Deadline: timerwheel.NullDeadline - 1},
		{CorrelationID: 30, Deadline: 300},
	}, svc.snapshotTimerRows())
}
