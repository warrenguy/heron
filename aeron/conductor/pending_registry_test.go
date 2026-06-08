package conductor

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	pendingRegistryTestCorrelationID int64 = 7
	pendingRegistryTestDeadlineNs          = int64(9_000_000_000)
)

func pendingRegistryTestRegistration(correlationID int64) PendingRegistration {
	return PendingRegistration{
		CorrelationID: correlationID,
		Kind:          PendingKindNextSessionID,
		DeadlineNs:    pendingRegistryTestDeadlineNs,
	}
}

func TestPendingRegistry_contains(t *testing.T) {
	registry := newPendingRegistry()
	registration := pendingRegistryTestRegistration(pendingRegistryTestCorrelationID)

	require.False(t, registry.Contains(pendingRegistryTestCorrelationID))
	require.NoError(t, registry.Register(registration))
	require.True(t, registry.Contains(pendingRegistryTestCorrelationID))
	registry.Remove(pendingRegistryTestCorrelationID)
	require.False(t, registry.Contains(pendingRegistryTestCorrelationID))
}

func TestPendingRegistry_registerGetRemove(t *testing.T) {
	registry := newPendingRegistry()
	registration := pendingRegistryTestRegistration(pendingRegistryTestCorrelationID)

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
	registration := pendingRegistryTestRegistration(pendingRegistryTestCorrelationID)

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
			require.NoError(t, registry.Register(PendingRegistration{
				CorrelationID: activeCorrelationID,
				Kind:          PendingKindNextSessionID,
				DeadlineNs:    deadlineNs,
			}))
			require.NoError(t, registry.Register(PendingRegistration{
				CorrelationID: otherCorrelationID,
				Kind:          PendingKindNextSessionID,
				DeadlineNs:    deadlineNs,
			}))
			require.NoError(t, registry.Register(PendingRegistration{
				CorrelationID: receivedCorrelationID,
				Kind:          PendingKindNextSessionID,
				DeadlineNs:    deadlineNs,
			}))

			workCount, timedOutActiveID := registry.ExpireBefore(
				testCase.nowNs,
				testCase.receivedCorrelationID,
				testCase.activeCorrelationID,
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
				_ = registry.Register(PendingRegistration{
					CorrelationID: correlationID,
					Kind:          PendingKindNextSessionID,
					DeadlineNs:    deadlineNs,
				})
			}
		}
	})

	var nowNs int64
	for range 500 {
		nowNs += 2
		registry.ExpireBefore(nowNs, NullCorrelationID, NullCorrelationID)
	}

	close(stop)
	registerWG.Wait()
}
