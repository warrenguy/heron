//go:build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/test/integration/harness"
)

const clientStayAliveDuration = 10 * time.Second

func TestClientStayAlive(t *testing.T) {
	md := harness.Start(t)

	client, err := aeron.Connect(aeron.Context{
		AeronDir: md.Dir,
		ErrorHandler: func(err error) {
			require.FailNow(t, "unexpected client error during stay-alive", "%v", err)
		},
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, client.ClientId(), int64(0))
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	deadline := time.Now().Add(clientStayAliveDuration)
	sawProgress := false
	for time.Now().Before(deadline) {
		if client.DoWork() > 0 {
			sawProgress = true
		}
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}
	require.True(t, sawProgress, "client never made progress during stay-alive")
}

func TestClientDetectsDriverDeath(t *testing.T) {
	md := harness.Start(t)

	driverTimeout := 2 * time.Second
	var gotErr error
	client, err := aeron.Connect(aeron.Context{
		AeronDir:      md.Dir,
		DriverTimeout: driverTimeout,
		ErrorHandler: func(err error) {
			gotErr = err
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	for range 20 {
		client.DoWork()
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}

	require.NoError(t, md.Kill())

	deadline := time.Now().Add(driverTimeout + time.Second)
	for time.Now().Before(deadline) {
		client.DoWork()
		if gotErr != nil {
			var timeoutErr *aeron.DriverTimeoutError
			require.ErrorAs(t, gotErr, &timeoutErr)
			var staleErr *conductor.DriverKeepaliveStaleError
			require.ErrorAs(t, timeoutErr.Unwrap(), &staleErr)
			require.Greater(t, staleErr.AgeMs, staleErr.TimeoutMs)
			require.True(t, client.IsTerminating())
			return
		}
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}

	require.Fail(t, "expected DriverTimeoutError after driver kill")
}
