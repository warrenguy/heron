package conductor

import (
	"fmt"
	"maps"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

const (
	pendingRegistryTestCorrelationID int64 = 7
	pendingRegistryTestDeadlineNs          = int64(9_000_000_000)
)

func pendingRegistryTestRegistration(correlationID, deadlineNs int64) PendingRegistration {
	return PendingRegistration{
		CorrelationID: correlationID,
		Kind:          PendingKindNextSessionID,
		DeadlineNs:    deadlineNs,
	}
}

func TestPendingRegistry_contains(t *testing.T) {
	registry := newPendingRegistry()
	registration := pendingRegistryTestRegistration(
		pendingRegistryTestCorrelationID,
		pendingRegistryTestDeadlineNs,
	)

	require.False(t, registry.Contains(pendingRegistryTestCorrelationID))
	require.NoError(t, registry.Register(registration))
	require.True(t, registry.Contains(pendingRegistryTestCorrelationID))
	registry.Remove(pendingRegistryTestCorrelationID)
	require.False(t, registry.Contains(pendingRegistryTestCorrelationID))
}

func TestPendingRegistry_registerGetRemove(t *testing.T) {
	registry := newPendingRegistry()
	registration := pendingRegistryTestRegistration(
		pendingRegistryTestCorrelationID,
		pendingRegistryTestDeadlineNs,
	)

	require.NoError(t, registry.Register(registration))
	require.False(t, registry.IsEmpty())

	got, ok := registry.Get(pendingRegistryTestCorrelationID)
	require.True(t, ok)
	require.Equal(t, registration, *got)

	registry.Remove(pendingRegistryTestCorrelationID)
	require.True(t, registry.IsEmpty())
	_, ok = registry.Get(pendingRegistryTestCorrelationID)
	require.False(t, ok)
}

func TestPendingRegistry_duplicateRegister(t *testing.T) {
	registry := newPendingRegistry()
	registration := pendingRegistryTestRegistration(
		pendingRegistryTestCorrelationID,
		pendingRegistryTestDeadlineNs,
	)

	require.NoError(t, registry.Register(registration))
	err := registry.Register(registration)
	require.ErrorIs(t, err, ErrPendingAlreadyRegistered)
	require.EqualError(t, err, "conductor: correlation already pending")
}

func TestPendingRegistry_ExpireBefore(t *testing.T) {
	const (
		activeCorrelationID   int64 = 7
		otherCorrelationID    int64 = 8
		receivedCorrelationID int64 = 9
		deadlineNs                  = int64(1_000)
	)

	tests := []struct {
		name                  string
		nowNs                 int64
		receivedCorrelationID int64
		activeCorrelationID   int64
		wantWorkCount         int
		wantTimedOutActiveID  int64
		wantRemaining         []int64
		wantRemoved           []int64
		wantActiveErr         error
	}{
		{
			name:                 "beforeDeadline",
			nowNs:                deadlineNs - 1,
			activeCorrelationID:  activeCorrelationID,
			wantRemaining:        []int64{activeCorrelationID, otherCorrelationID, receivedCorrelationID},
			wantTimedOutActiveID: NullCorrelationID,
		},
		{
			name:                 "atDeadlineExpiresInactiveOnly",
			nowNs:                deadlineNs,
			activeCorrelationID:  NullCorrelationID,
			wantWorkCount:        3,
			wantRemaining:        []int64{},
			wantRemoved:          []int64{activeCorrelationID, otherCorrelationID, receivedCorrelationID},
			wantTimedOutActiveID: NullCorrelationID,
		},
		{
			name:                 "atDeadlineTimesOutActive",
			nowNs:                deadlineNs,
			activeCorrelationID:  activeCorrelationID,
			wantWorkCount:        3,
			wantRemaining:        []int64{activeCorrelationID},
			wantRemoved:          []int64{otherCorrelationID, receivedCorrelationID},
			wantTimedOutActiveID: activeCorrelationID,
			wantActiveErr:        ErrAwaitTimeout,
		},
		{
			name:                  "skipsReceivedCorrelationID",
			nowNs:                 deadlineNs,
			receivedCorrelationID: receivedCorrelationID,
			activeCorrelationID:   activeCorrelationID,
			wantWorkCount:         2,
			wantRemaining:         []int64{activeCorrelationID, receivedCorrelationID},
			wantRemoved:           []int64{otherCorrelationID},
			wantTimedOutActiveID:  activeCorrelationID,
			wantActiveErr:         ErrAwaitTimeout,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			registry := newPendingRegistry()
			for _, correlationID := range []int64{
				activeCorrelationID,
				otherCorrelationID,
				receivedCorrelationID,
			} {
				require.NoError(t, registry.Register(
					pendingRegistryTestRegistration(correlationID, deadlineNs),
				))
			}

			workCount, timedOutActiveID := registry.ExpireBefore(
				testCase.nowNs,
				testCase.receivedCorrelationID,
				testCase.activeCorrelationID,
				nil,
			)
			require.Equal(t, testCase.wantWorkCount, workCount)
			require.Equal(t, testCase.wantTimedOutActiveID, timedOutActiveID)

			for _, correlationID := range testCase.wantRemaining {
				_, ok := registry.Get(correlationID)
				require.True(t, ok, "correlation %d should remain", correlationID)
			}
			for _, correlationID := range testCase.wantRemoved {
				_, ok := registry.Get(correlationID)
				require.False(t, ok, "correlation %d should be removed", correlationID)
			}
			if testCase.wantActiveErr != nil {
				activeRegistration, ok := registry.Get(activeCorrelationID)
				require.True(t, ok)
				require.ErrorIs(t, activeRegistration.Err, testCase.wantActiveErr)
			}
		})
	}
}

func TestPendingRegistry_ExpireBefore_activeTimeoutOnExpireOnce(t *testing.T) {
	const (
		activeCorrelationID int64 = 7
		deadlineNs                = int64(1_000)
		nowNs                     = deadlineNs
	)

	registry := newPendingRegistry()
	require.NoError(t, registry.Register(
		pendingRegistryTestRegistration(activeCorrelationID, deadlineNs),
	))

	var expireHandlerCalls int
	expireHandler := func(registration PendingRegistration, removed bool) {
		expireHandlerCalls++
		require.Equal(t, activeCorrelationID, registration.CorrelationID)
		require.False(t, removed)
		require.ErrorIs(t, registration.Err, ErrAwaitTimeout)
	}

	workCount, timedOutActiveID := registry.ExpireBefore(
		nowNs,
		NullCorrelationID,
		activeCorrelationID,
		expireHandler,
	)
	require.Equal(t, 1, workCount)
	require.Equal(t, activeCorrelationID, timedOutActiveID)
	require.Equal(t, 1, expireHandlerCalls)

	workCount, timedOutActiveID = registry.ExpireBefore(
		nowNs+1,
		NullCorrelationID,
		activeCorrelationID,
		expireHandler,
	)
	require.Zero(t, workCount)
	require.Equal(t, activeCorrelationID, timedOutActiveID)
	require.Equal(t, 1, expireHandlerCalls)

	activeRegistration, ok := registry.Get(activeCorrelationID)
	require.True(t, ok)
	require.ErrorIs(t, activeRegistration.Err, ErrAwaitTimeout)
}

func TestPendingRegistry_concurrentRegisterExpire(t *testing.T) {
	const deadlineNs = int64(1_000)

	registry := newPendingRegistry()
	stop := make(chan struct{})
	var registerWG sync.WaitGroup
	registerWG.Go(func() {
		var correlationID int64
		for {
			select {
			case <-stop:
				return
			default:
				correlationID++
				_ = registry.Register(
					pendingRegistryTestRegistration(correlationID, deadlineNs),
				)
			}
		}
	})

	var nowNs int64
	for range 500 {
		nowNs += 2
		registry.ExpireBefore(nowNs, NullCorrelationID, NullCorrelationID, nil)
	}

	close(stop)
	registerWG.Wait()
}

const pendingRegistryRapidBaseNowNs int64 = 1_000_000_000

type pendingExpireRapidEntry struct {
	deadlineNs int64
	present    bool
	timedOut   bool
}

func drawPendingExpireRapidCorrelationID(
	rt *rapid.T,
	seen map[int64]struct{},
	label string,
) int64 {
	rt.Helper()

	for {
		correlationID := rapid.Int64().Draw(rt, label)
		if correlationID == NullCorrelationID {
			continue
		}
		if _, exists := seen[correlationID]; exists {
			continue
		}
		seen[correlationID] = struct{}{}
		return correlationID
	}
}

func drawPendingExpireRapidCorrelationPick(
	rt *rapid.T,
	correlationIDs []int64,
	label string,
) int64 {
	rt.Helper()

	idx := rapid.IntRange(0, len(correlationIDs)-1).Draw(rt, label)
	return correlationIDs[idx]
}

func drawPendingExpireRapidOptionalCorrelationID(
	rt *rapid.T,
	correlationIDs []int64,
	nullLabel string,
	pickLabel string,
) int64 {
	rt.Helper()

	if rapid.Bool().Draw(rt, nullLabel) {
		return NullCorrelationID
	}
	return drawPendingExpireRapidCorrelationPick(rt, correlationIDs, pickLabel)
}

func applyPendingExpireRapidExpire(
	model map[int64]pendingExpireRapidEntry,
	nowNs int64,
	receivedCorrelationID int64,
	activeCorrelationID int64,
) (
	wantWorkCount int,
	wantTimedOutActiveID int64,
	newlyTimedOut map[int64]struct{},
	newlyRemoved map[int64]struct{},
) {
	wantTimedOutActiveID = NullCorrelationID
	newlyTimedOut = make(map[int64]struct{})
	newlyRemoved = make(map[int64]struct{})
	for correlationID, entry := range model {
		if !entry.present {
			continue
		}
		if correlationID == receivedCorrelationID {
			continue
		}
		if nowNs < entry.deadlineNs {
			continue
		}
		if correlationID == activeCorrelationID {
			wantTimedOutActiveID = correlationID
			if !entry.timedOut {
				wantWorkCount++
				entry.timedOut = true
				model[correlationID] = entry
				newlyTimedOut[correlationID] = struct{}{}
			}
			continue
		}
		wantWorkCount++
		entry.present = false
		model[correlationID] = entry
		newlyRemoved[correlationID] = struct{}{}
	}
	return wantWorkCount, wantTimedOutActiveID, newlyTimedOut, newlyRemoved
}

func assertPendingExpireRapidRegistry(
	rt *rapid.T,
	registry *pendingRegistry,
	model map[int64]pendingExpireRapidEntry,
) {
	rt.Helper()

	for correlationID, entry := range model {
		got, ok := registry.Get(correlationID)
		if entry.present {
			require.True(rt, ok, "correlation %d should remain", correlationID)
			if entry.timedOut {
				require.ErrorIs(rt, got.Err, ErrAwaitTimeout)
			} else {
				require.NoError(rt, got.Err)
			}
			continue
		}
		require.False(rt, ok, "correlation %d should be removed", correlationID)
	}
}

func assertPendingExpireRapidHandlerStep(
	rt *rapid.T,
	beforeCalls map[int64]int,
	handlerCalls map[int64]int,
	handlerRemoved map[int64]bool,
	correlationIDs []int64,
	newlyTimedOut map[int64]struct{},
	newlyRemoved map[int64]struct{},
) {
	rt.Helper()

	for _, correlationID := range correlationIDs {
		newCalls := handlerCalls[correlationID] - beforeCalls[correlationID]
		_, timedOutNow := newlyTimedOut[correlationID]
		_, removedNow := newlyRemoved[correlationID]
		switch {
		case timedOutNow:
			require.Equal(
				rt,
				1,
				newCalls,
				"correlation %d active timeout should invoke handler once",
				correlationID,
			)
			require.False(rt, handlerRemoved[correlationID])
		case removedNow:
			require.Equal(
				rt,
				1,
				newCalls,
				"correlation %d inactive expiry should invoke handler once",
				correlationID,
			)
			require.True(rt, handlerRemoved[correlationID])
		default:
			require.Zero(rt, newCalls, "correlation %d should not invoke handler", correlationID)
		}
	}
}

func TestPendingRegistry_ExpireBefore_opSequences_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		registry := newPendingRegistry()
		count := rapid.IntRange(1, 16).Draw(rt, "count")
		seen := make(map[int64]struct{}, count)
		correlationIDs := make([]int64, 0, count)
		model := make(map[int64]pendingExpireRapidEntry, count)

		for len(correlationIDs) < count {
			correlationID := drawPendingExpireRapidCorrelationID(rt, seen, "seedCorrelationID")
			deadlineNs := rapid.Int64Range(0, 10_000_000_000).Draw(rt, "seedDeadlineNs")
			require.NoError(
				rt,
				registry.Register(pendingRegistryTestRegistration(correlationID, deadlineNs)),
			)
			correlationIDs = append(correlationIDs, correlationID)
			model[correlationID] = pendingExpireRapidEntry{
				deadlineNs: deadlineNs,
				present:    true,
			}
		}

		var receivedCorrelationID int64
		var activeCorrelationID int64
		handlerCalls := make(map[int64]int, count)
		handlerRemoved := make(map[int64]bool, count)
		expireHandler := func(registration PendingRegistration, removed bool) {
			handlerCalls[registration.CorrelationID]++
			handlerRemoved[registration.CorrelationID] = removed
			if !removed {
				require.ErrorIs(rt, registration.Err, ErrAwaitTimeout)
			}
		}

		steps := rapid.IntRange(1, 24).Draw(rt, "steps")
		for step := range steps {
			receivedCorrelationID = drawPendingExpireRapidOptionalCorrelationID(
				rt,
				correlationIDs,
				fmt.Sprintf("receivedNull%d", step),
				fmt.Sprintf("receivedIdx%d", step),
			)
			activeCorrelationID = drawPendingExpireRapidOptionalCorrelationID(
				rt,
				correlationIDs,
				fmt.Sprintf("activeNull%d", step),
				fmt.Sprintf("activeIdx%d", step),
			)
			nowNs := pendingRegistryRapidBaseNowNs +
				rapid.Int64Range(-1_000_000_000, 20_000_000_000).Draw(rt, fmt.Sprintf("nowOffset%d", step))

			beforeCalls := make(map[int64]int, len(handlerCalls))
			maps.Copy(beforeCalls, handlerCalls)

			var (
				wantWorkCount        int
				wantTimedOutActiveID int64
				newlyTimedOut        map[int64]struct{}
				newlyRemoved         map[int64]struct{}
			)
			wantWorkCount, wantTimedOutActiveID, newlyTimedOut, newlyRemoved = applyPendingExpireRapidExpire(
				model,
				nowNs,
				receivedCorrelationID,
				activeCorrelationID,
			)
			workCount, timedOutActiveID := registry.ExpireBefore(
				nowNs,
				receivedCorrelationID,
				activeCorrelationID,
				expireHandler,
			)
			require.Equal(rt, wantWorkCount, workCount)
			require.Equal(rt, wantTimedOutActiveID, timedOutActiveID)

			assertPendingExpireRapidRegistry(rt, &registry, model)
			assertPendingExpireRapidHandlerStep(
				rt,
				beforeCalls,
				handlerCalls,
				handlerRemoved,
				correlationIDs,
				newlyTimedOut,
				newlyRemoved,
			)
		}
	})
}
