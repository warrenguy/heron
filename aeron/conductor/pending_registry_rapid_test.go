//go:build rapid

package conductor

import (
	"fmt"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

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
	idx := rapid.IntRange(0, len(correlationIDs)-1).Draw(rt, pickLabel)
	return correlationIDs[idx]
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
		case timedOutNow || removedNow:
			require.Equal(
				rt,
				1,
				newCalls,
				"correlation %d should invoke handler once",
				correlationID,
			)
			require.Equal(rt, removedNow, handlerRemoved[correlationID])
		default:
			require.Zero(rt, newCalls, "correlation %d should not invoke handler", correlationID)
		}
	}
}

func TestPendingRegistry_ExpireBefore_opSequences_rapid(t *testing.T) {
	t.Parallel()
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
