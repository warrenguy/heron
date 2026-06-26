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

// PollClientUntilTransportClean polls client.DoWork until ok returns true, failing
// immediately on transport errors recorded in hadClientError.
func PollClientUntilTransportClean(
	t testing.TB,
	client *aeron.Client,
	hadClientError *atomic.Bool,
	timeout time.Duration,
	transportErrLabel string,
	ok func() bool,
	timeoutMsg func() string,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		require.False(t, hadClientError.Load(), transportErrLabel)
		client.DoWork()
		if ok() {
			return
		}
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}
	require.FailNow(t, timeoutMsg())
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
	pub ipcPublisher,
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
	regID int64,
	resourceKind string,
	lookup func(int64) (T, bool),
) {
	t.Helper()

	pollClientUntilDynamic(
		t,
		client,
		timeout,
		func() bool {
			_, ok := lookup(regID)
			return !ok
		},
		func() string {
			return fmt.Sprintf("%s %d still registered after close", resourceKind, regID)
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

	requireLookupMiss(t, client, CommandPollDeadline, regID, "publication", client.Publication)
}

// RequireExclusivePublicationLookupMiss polls DoWork until ExclusivePublication(regID) misses.
func RequireExclusivePublicationLookupMiss(
	t testing.TB,
	client *aeron.Client,
	regID int64,
) {
	t.Helper()

	requireLookupMiss(
		t,
		client,
		CommandPollDeadline,
		regID,
		"exclusive publication",
		client.ExclusivePublication,
	)
}

// RequireSubscriptionLookupMiss polls DoWork until Subscription(regID) misses.
func RequireSubscriptionLookupMiss(
	t testing.TB,
	client *aeron.Client,
	regID int64,
) {
	t.Helper()

	requireLookupMiss(t, client, CommandPollDeadline, regID, "subscription", client.Subscription)
}
