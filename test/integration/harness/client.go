//go:build integration

package harness

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

const (
	// IPCChannel is the default channel for live-driver integration tests.
	IPCChannel = "aeron:ipc"

	// ConnectedPollTimeout matches default zero Context DriverTimeout (10s).
	ConnectedPollTimeout = 10 * time.Second
)

// ConnectClient connects to md and registers client.Close on t.Cleanup.
func ConnectClient(t testing.TB, md *MediaDriver, ctx aeron.Context) *aeron.Client {
	t.Helper()

	if ctx.AeronDir == "" {
		ctx.AeronDir = md.Dir
	}
	client, err := aeron.Connect(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return client
}

// ConnectIPCClient connects to md and records client errors in the returned flag.
func ConnectIPCClient(t testing.TB, md *MediaDriver) (*aeron.Client, *atomic.Bool) {
	t.Helper()

	return ConnectIPCClientContext(t, md, aeron.Context{})
}

// ConnectIPCClientContext connects with ctx and records client errors in the returned flag.
func ConnectIPCClientContext(
	t testing.TB,
	md *MediaDriver,
	ctx aeron.Context,
) (*aeron.Client, *atomic.Bool) {
	t.Helper()

	var hadClientError atomic.Bool
	callerHandler := ctx.ErrorHandler
	ctx.ErrorHandler = func(err error) {
		hadClientError.Store(true)
		if callerHandler != nil {
			callerHandler(err)
		} else {
			t.Errorf("client error: %v", err)
		}
	}

	return ConnectClient(t, md, ctx), &hadClientError
}

// CleanupClose registers closeFn on t.Cleanup; records errors without masking test failure.
func CleanupClose(t testing.TB, label string, closeFn func() error) {
	t.Helper()
	t.Cleanup(func() {
		if err := closeFn(); err != nil {
			t.Errorf("%s: %v", label, err)
		}
	})
}

func pollClientUntilDynamic(
	t testing.TB,
	client *aeron.Client,
	timeout time.Duration,
	ok func() bool,
	failMsg func() string,
) {
	t.Helper()

	if pollUntilBool(
		timeout,
		aeron.DefaultIdleSleepDuration,
		func() bool {
			client.DoWork()
			return ok()
		},
	) {
		return
	}
	t.Errorf("%s within %s", failMsg(), timeout)
	t.FailNow()
}

func connectedPollTimeoutOr(timeout time.Duration) time.Duration {
	if timeout == 0 {
		return ConnectedPollTimeout
	}
	return timeout
}

// WaitForSubscriptionConnected polls DoWork until the subscription reports connected.
func WaitForSubscriptionConnected(
	t testing.TB,
	client *aeron.Client,
	sub *aeron.Subscription,
	timeout time.Duration,
) {
	t.Helper()

	pollClientUntilDynamic(
		t,
		client,
		connectedPollTimeoutOr(timeout),
		sub.IsConnected,
		func() string {
			return fmt.Sprintf("subscription %d never connected", sub.RegistrationID())
		},
	)
}

// WaitForIPCLinkConnected polls subscription image attach, then publication metadata.
// Live IPC: image arrives via ON_AVAILABLE_IMAGE before pub log is_connected flips.
func WaitForIPCLinkConnected(
	t testing.TB,
	client *aeron.Client,
	pub *aeron.Publication,
	sub *aeron.Subscription,
	timeout time.Duration,
) {
	t.Helper()

	timeout = connectedPollTimeoutOr(timeout)
	deadline := time.Now().Add(timeout)
	WaitForSubscriptionConnected(t, client, sub, time.Until(deadline))
	PollUntilConnected(t, client, pub, time.Until(deadline))
}

func requireLookupMiss[T any](
	t testing.TB,
	client *aeron.Client,
	timeout time.Duration,
	lookup func() (T, bool),
	resourceLabel string,
) {
	t.Helper()

	pollClientUntilDynamic(
		t,
		client,
		timeout,
		func() bool {
			_, ok := lookup()
			return !ok
		},
		func() string {
			return fmt.Sprintf("%s still registered after close", resourceLabel)
		},
	)
}

// RequirePublicationLookupMiss polls DoWork until Publication(regID) misses.
func RequirePublicationLookupMiss(
	t testing.TB,
	client *aeron.Client,
	regID int64,
) {
	t.Helper()

	requireLookupMiss(
		t,
		client,
		commandPollDeadline,
		func() (*aeron.Publication, bool) {
			return client.Publication(regID)
		},
		fmt.Sprintf("publication %d", regID),
	)
}

// RequireSubscriptionLookupMiss polls DoWork until Subscription(regID) misses.
func RequireSubscriptionLookupMiss(
	t testing.TB,
	client *aeron.Client,
	regID int64,
) {
	t.Helper()

	requireLookupMiss(
		t,
		client,
		commandPollDeadline,
		func() (*aeron.Subscription, bool) {
			return client.Subscription(regID)
		},
		fmt.Sprintf("subscription %d", regID),
	)
}
