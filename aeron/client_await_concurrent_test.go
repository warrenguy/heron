//go:build !race

// Hammer-during-register races Conductor await fields; excluded from -race until
// clientLock serializes DoWork with RegisterPending on one client.

package aeron

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func TestClient_GetNextAvailableSessionID_concurrentDoWorkBeforeRegister(t *testing.T) {
	var nanoNs int64
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Microsecond,
	)

	const wantSessionID int32 = 88

	stopHammer := make(chan struct{})
	var stopOnce sync.Once
	stopHammerGoroutine := func() {
		stopOnce.Do(func() { close(stopHammer) })
	}
	t.Cleanup(stopHammerGoroutine)

	go func() {
		for {
			select {
			case <-stopHammer:
				return
			default:
				client.DoWork()
			}
		}
	}()

	type result struct {
		sessionID int32
		err       error
	}
	resultCh := make(chan result, 1)
	go func() {
		sessionID, err := client.GetNextAvailableSessionID(clientAwaitTestStreamID)
		resultCh <- result{sessionID: sessionID, err: err}
	}()

	deadline := time.Now().Add(testutil.TestDriverTimeout)
	require.True(
		t,
		pollAndRespondToNextAvailableSessionIDCommand(
			t,
			fixture,
			wantSessionID,
			deadline,
			stopHammerGoroutine,
		),
		"timed out waiting for GetNextAvailableSessionID command on ring",
	)

	select {
	case got := <-resultCh:
		require.NoError(t, got.err)
		require.Equal(t, wantSessionID, got.sessionID)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for GetNextAvailableSessionID client result")
	}
	requireActiveCorrelationCleared(t, fixture)
}
