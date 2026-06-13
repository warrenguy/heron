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
	client.SetAwaitPollSleepingHookForTest(func() {
		calls.Add(1)
	})
	t.Cleanup(func() { client.SetAwaitPollSleepingHookForTest(nil) })

	invokeAwaitPollSleepingHook(client)
	require.Equal(t, int32(1), calls.Load())

	client.SetAwaitPollSleepingHookForTest(nil)
	invokeAwaitPollSleepingHook(client)
	require.Equal(t, int32(1), calls.Load())
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
