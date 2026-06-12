//go:build integration

package harness

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

const (
	// IPCChannel is the default channel for live-driver integration tests.
	IPCChannel = "aeron:ipc"

	// Matches default zero Context DriverTimeout (10s).
	connectedPollTimeout = 10 * time.Second
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

// CleanupClose registers closeFn on t.Cleanup; records errors without masking test failure.
func CleanupClose(t testing.TB, label string, closeFn func() error) {
	t.Helper()
	t.Cleanup(func() {
		if err := closeFn(); err != nil {
			t.Errorf("%s: %v", label, err)
		}
	})
}

func pollClientUntil(
	t testing.TB,
	client *aeron.Client,
	timeout time.Duration,
	ok func() bool,
	failMsg string,
) {
	t.Helper()

	pollUntil(
		t,
		timeout,
		aeron.DefaultIdleSleepDuration,
		func() bool {
			client.DoWork()
			return ok()
		},
		failMsg,
	)
}

// WaitForPublicationConnected polls DoWork until pub.IsConnected on live IPC.
// Requires a matching subscriber; publication metadata alone does not flip true.
func WaitForPublicationConnected(
	t testing.TB,
	client *aeron.Client,
	pub *aeron.Publication,
	timeout time.Duration,
) {
	t.Helper()
	if timeout == 0 {
		timeout = connectedPollTimeout
	}

	pollClientUntil(
		t,
		client,
		timeout,
		pub.IsConnected,
		fmt.Sprintf("publication %d never connected", pub.RegistrationID()),
	)
}

// WaitForSubscriptionConnected polls DoWork until the subscription reports connected.
func WaitForSubscriptionConnected(
	t testing.TB,
	client *aeron.Client,
	sub *aeron.Subscription,
	timeout time.Duration,
) {
	t.Helper()
	if timeout == 0 {
		timeout = connectedPollTimeout
	}

	pollClientUntil(
		t,
		client,
		timeout,
		sub.IsConnected,
		fmt.Sprintf("subscription %d never connected", sub.RegistrationID()),
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
	if timeout == 0 {
		timeout = connectedPollTimeout
	}

	deadline := time.Now().Add(timeout)
	WaitForSubscriptionConnected(t, client, sub, time.Until(deadline))
	WaitForPublicationConnected(t, client, pub, time.Until(deadline))
}

func requireLookupMiss[T any](
	t testing.TB,
	client *aeron.Client,
	timeout time.Duration,
	lookup func() (T, bool),
	resourceLabel string,
) {
	t.Helper()

	pollClientUntil(
		t,
		client,
		timeout,
		func() bool {
			_, ok := lookup()
			return !ok
		},
		fmt.Sprintf("%s still registered after close", resourceLabel),
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
