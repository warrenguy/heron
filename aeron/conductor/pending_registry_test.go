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

func TestPendingRegistry_ExpireBefore_addCounterKeepsReady(t *testing.T) {
	const (
		readyCorrelationID   int64 = 7
		waitingCorrelationID int64 = 8
		deadlineNs                 = int64(1_000)
	)

	registry := newPendingRegistry()
	for _, seed := range []struct {
		correlationID int64
		ready         bool
	}{
		{correlationID: readyCorrelationID, ready: true},
		{correlationID: waitingCorrelationID, ready: false},
	} {
		registration := pendingRegistryTestRegistration(seed.correlationID, deadlineNs)
		registration.Kind = PendingKindAddCounter
		registration.Ready = seed.ready
		require.NoError(t, registry.Register(registration))
	}

	var expired []int64
	workCount, timedOutActiveID := registry.ExpireBefore(
		deadlineNs,
		NullCorrelationID,
		NullCorrelationID,
		func(registration PendingRegistration, removed bool) {
			require.False(t, removed)
			expired = append(expired, registration.CorrelationID)
		},
	)
	require.Equal(t, 1, workCount)
	require.Equal(t, NullCorrelationID, timedOutActiveID)
	require.Equal(t, []int64{waitingCorrelationID}, expired)

	readyRegistration, ok := registry.Get(readyCorrelationID)
	require.True(t, ok)
	require.NoError(t, readyRegistration.Err)
	require.True(t, readyRegistration.Ready)

	waitingRegistration, ok := registry.Get(waitingCorrelationID)
	require.True(t, ok)
	require.ErrorIs(t, waitingRegistration.Err, ErrAwaitTimeout)
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
