package aeron

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func TestClient_awaitPollSleepingHook_clearRemovesSideTableEntry(t *testing.T) {
	var nanoNs int64
	client, _ := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		testutil.TestDriverTimeout,
		testutil.TestIdleSleepDuration,
	)

	var calls atomic.Int32
	setAwaitPollSleepingHook(client, func() {
		calls.Add(1)
	})
	t.Cleanup(func() { setAwaitPollSleepingHook(client, nil) })

	invokeAwaitPollSleepingHook(client)
	require.Equal(t, int32(1), calls.Load())

	setAwaitPollSleepingHook(client, nil)
	invokeAwaitPollSleepingHook(client)
	require.Equal(t, int32(1), calls.Load())
}

func TestClient_awaitPollSleepingHook_clearedWhenTestEnds(t *testing.T) {
	var nanoNs int64
	client, _ := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		testutil.TestDriverTimeout,
		testutil.TestIdleSleepDuration,
	)

	calls := 0
	t.Run("install", func(t *testing.T) {
		client.SetAwaitPollSleepingHookForTest(t, func() {
			calls++
		})
		invokeAwaitPollSleepingHook(client)
		require.Equal(t, 1, calls)
	})

	// Phase: the subtest cleanup removed the hook.
	invokeAwaitPollSleepingHook(client)
	require.Equal(t, 1, calls)
}

func TestClient_awaitPollSleepingHook_nilReceiver(t *testing.T) {
	cases := []struct {
		name string
		run  func()
	}{
		{
			name: "set hook",
			run:  func() { setAwaitPollSleepingHook(nil, func() {}) },
		},
		{
			name: "clear hook",
			run:  func() { setAwaitPollSleepingHook(nil, nil) },
		},
		{
			name: "invoke",
			run:  func() { invokeAwaitPollSleepingHook(nil) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, tc.run)
		})
	}
}
