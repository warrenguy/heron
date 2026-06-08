package conductor

import (
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
