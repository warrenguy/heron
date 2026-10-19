//go:build integration

package integration_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/test/integration/harness"
)

// manualSendChannel has no tag, so the driver gives each publication its own
// send endpoint.
const manualSendChannel = "aeron:udp?control-mode=manual"

type sendDestinationPublication interface {
	payloadOfferer
	AddDestination(channel string) error
	RemoveDestination(channel string) error
	Close() error
}

// TestMultiDestinationPublication asserts destinations receive until removed, re-remove passes.
func TestMultiDestinationPublication(t *testing.T) {
	md := harness.SharedParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	const streamID = harness.MultiDestinationPublicationStreamID

	tests := []struct {
		name string
		add  func(*testing.T) sendDestinationPublication
	}{
		{
			name: "publication",
			add: func(t *testing.T) sendDestinationPublication {
				pub, err := client.AddPublication(streamID, manualSendChannel)
				require.NoError(t, err)
				return pub
			},
		},
		{
			name: "exclusive publication",
			add: func(t *testing.T) sendDestinationPublication {
				pub, err := client.AddExclusivePublication(streamID, manualSendChannel)
				require.NoError(t, err)
				return pub
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoints := newSendDestinationEndpoints(t, client, streamID, 2)
			destA, destB := endpoints[0], endpoints[1]
			pub := test.add(t)
			harness.CleanupClose(t, "close manual publication", pub.Close)

			// Both destinations receive.
			require.NoError(t, pub.AddDestination(destA.channel))
			require.NoError(t, pub.AddDestination(destB.channel))
			harness.WaitUntilConnected(t, pub)
			harness.WaitUntilImageCount(t, destA.sub, 1)
			harness.WaitUntilImageCount(t, destB.sub, 1)
			requireDestinationsReceive(t, client, pub, 0xE0, destA, destB)

			// Remove A. Only B receives.
			require.NoError(t, pub.RemoveDestination(destA.channel))
			requireDestinationsReceive(t, client, pub, 0xE1, destB)
			requireNoFragments(t, client, destA.sub, 250*time.Millisecond)

			// The driver acknowledges a remove that matches no destination.
			require.NoError(t, pub.RemoveDestination(destA.channel))
		})
	}

	finishPubSubIPCTest(t, md, hadClientError)
}

// TestMultiDestinationPublication_driverErrors asserts the driver rejects a non-manual destination.
func TestMultiDestinationPublication_driverErrors(t *testing.T) {
	// A rejected command lands in the driver error log, which
	// TestClientErrorLogReadable expects empty on the shared driver.
	md := harness.StartParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	const streamID = harness.MultiDestinationPublicationErrorsStreamID

	ports := harness.FreeUDPPorts(t, 2)
	dest := "aeron:udp?endpoint=" + harness.LoopbackUDPEndpoint(ports[1])

	tests := []struct {
		name          string
		channel       string
		wantErrorCode int32
	}{
		{
			name:    "udp without manual control",
			channel: "aeron:udp?endpoint=" + harness.LoopbackUDPEndpoint(ports[0]),
		},
		{
			name:          "ipc",
			channel:       "aeron:ipc",
			wantErrorCode: conductor.ErrorCodeUnknownPublication,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pub, err := client.AddPublication(streamID, test.channel)
			require.NoError(t, err)
			harness.CleanupClose(t, "close rejected publication", pub.Close)

			err = pub.AddDestination(dest)
			var registrationErr *aeron.RegistrationError
			require.ErrorAs(t, err, &registrationErr)
			if test.wantErrorCode != 0 {
				require.Equal(t, test.wantErrorCode, registrationErr.ErrorCode)
			}

			// The rejected add leaves the client usable.
			other, err := client.AddPublication(streamID, "aeron:ipc")
			require.NoError(t, err)
			require.NoError(t, other.Close())
		})
	}

	finishPubSubIPCTest(t, md, hadClientError)
}

type sendDestinationEndpoint struct {
	channel string
	sub     *aeron.Subscription
}

func newSendDestinationEndpoints(
	t *testing.T,
	client *aeron.Client,
	streamID int32,
	count int,
) []sendDestinationEndpoint {
	t.Helper()

	endpoints := make([]sendDestinationEndpoint, count)
	for index, port := range harness.FreeUDPPorts(t, count) {
		channel := "aeron:udp?endpoint=" + harness.LoopbackUDPEndpoint(port)
		sub, err := client.AddSubscription(streamID, channel)
		require.NoError(t, err)
		harness.CleanupClose(t, "close destination subscription", sub.Close)
		endpoints[index] = sendDestinationEndpoint{channel: channel, sub: sub}
	}
	return endpoints
}

// awaitAsyncDestination runs call, then drives DoWork and PollCommand until
// the driver acknowledges the command. It returns the correlation id.
func awaitAsyncDestination(
	t *testing.T,
	client *aeron.Client,
	hadClientError *atomic.Bool,
	call func() (int64, error),
) int64 {
	t.Helper()

	correlationID, err := call()
	require.NoError(t, err)

	var done bool
	var pollErr error
	harness.PollClientUntilTransportClean(
		t,
		client,
		hadClientError,
		harness.ConnectedPollTimeout,
		"transport error during async destination command",
		func() bool {
			done, pollErr = client.PollCommand(correlationID)
			return done || pollErr != nil
		},
		func() string {
			return fmt.Sprintf("async destination command %d did not complete", correlationID)
		},
	)
	require.NoError(t, pollErr)
	require.True(t, done)
	return correlationID
}

// requireDestinationsReceive offers distinct payloads and asserts each
// endpoint receives all of them.
func requireDestinationsReceive(
	t *testing.T,
	client *aeron.Client,
	pub payloadOfferer,
	headerBase byte,
	endpoints ...sendDestinationEndpoint,
) {
	t.Helper()

	const (
		messageCount = 4
		payloadSize  = 64
	)
	payloads := harness.DistinctIPCPayloads(messageCount, payloadSize, headerBase)
	offerPayloads(t, client, pub, payloads)
	for _, endpoint := range endpoints {
		require.Equal(t, payloads, harness.PollDelivered(t, client, endpoint.sub, messageCount))
	}
}

// requireDestinationRemoved offers distinct payloads and asserts the
// remaining endpoints receive them while the removed endpoint receives
// nothing. A live remaining endpoint keeps the publication connected, so
// each offer sends.
func requireDestinationRemoved(
	t *testing.T,
	client *aeron.Client,
	pub payloadOfferer,
	headerBase byte,
	removed sendDestinationEndpoint,
	remaining ...sendDestinationEndpoint,
) {
	t.Helper()

	requireDestinationsReceive(t, client, pub, headerBase, remaining...)
	requireNoFragments(t, client, removed.sub, 250*time.Millisecond)
}

type asyncSendDestinationPublication interface {
	sendDestinationPublication
	AsyncAddDestination(channel string) (int64, error)
	AsyncRemoveDestination(channel string) (int64, error)
	AsyncRemoveDestinationByID(destinationRegistrationID int64) (int64, error)
	RemoveDestinationByID(destinationRegistrationID int64) error
}

// TestMultiDestinationPublication_async asserts async adds and removes on live aeronmd.
func TestMultiDestinationPublication_async(t *testing.T) {
	md := harness.SharedParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	const streamID = harness.MultiDestinationPublicationAsyncStreamID

	tests := []struct {
		name string
		add  func(*testing.T) asyncSendDestinationPublication
	}{
		{
			name: "publication",
			add: func(t *testing.T) asyncSendDestinationPublication {
				pub, err := client.AddPublication(streamID, manualSendChannel)
				require.NoError(t, err)
				return pub
			},
		},
		{
			name: "exclusive publication",
			add: func(t *testing.T) asyncSendDestinationPublication {
				pub, err := client.AddExclusivePublication(streamID, manualSendChannel)
				require.NoError(t, err)
				return pub
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoints := newSendDestinationEndpoints(t, client, streamID, 3)
			destA, destB, destC := endpoints[0], endpoints[1], endpoints[2]
			pub := test.add(t)
			harness.CleanupClose(t, "close manual publication", pub.Close)

			// Async add A, B and C. All receive. C stays attached so every
			// later offer sends.
			idA := awaitAsyncDestination(t, client, hadClientError, func() (int64, error) {
				return pub.AsyncAddDestination(destA.channel)
			})
			for _, dest := range []sendDestinationEndpoint{destB, destC} {
				awaitAsyncDestination(t, client, hadClientError, func() (int64, error) {
					return pub.AsyncAddDestination(dest.channel)
				})
			}
			harness.WaitUntilConnected(t, pub)
			for _, dest := range endpoints {
				harness.WaitUntilImageCount(t, dest.sub, 1)
			}
			requireDestinationsReceive(t, client, pub, 0xE2, destA, destB, destC)

			// Async remove B by channel. A and C receive.
			awaitAsyncDestination(t, client, hadClientError, func() (int64, error) {
				return pub.AsyncRemoveDestination(destB.channel)
			})
			requireDestinationRemoved(t, client, pub, 0xE3, destB, destA, destC)

			// Blocking remove A by the async add id. C receives.
			require.NoError(t, pub.RemoveDestinationByID(idA))
			requireDestinationRemoved(t, client, pub, 0xE5, destA, destC)
		})
	}

	// A concurrent publication that joins an existing driver publication has
	// a RegistrationID that differs from its OriginalRegistrationID. The
	// driver keys the send endpoint on the original id.
	t.Run("joined publication", func(t *testing.T) {
		endpoints := newSendDestinationEndpoints(t, client, streamID, 2)
		dest, kept := endpoints[0], endpoints[1]

		// The driver issues each correlation id once, so the tag is unique on
		// the shared driver.
		channel := fmt.Sprintf("aeron:udp?control-mode=manual|tags=%d", client.NextCorrelationID())
		first, err := client.AddPublication(streamID, channel)
		require.NoError(t, err)
		harness.CleanupClose(t, "close first tagged publication", first.Close)
		joined, err := client.AddPublication(streamID, channel)
		require.NoError(t, err)
		harness.CleanupClose(t, "close joined tagged publication", joined.Close)
		require.NotEqual(t, joined.RegistrationID(), joined.OriginalRegistrationID())
		require.Equal(t, first.OriginalRegistrationID(), joined.OriginalRegistrationID())

		destID := awaitAsyncDestination(t, client, hadClientError, func() (int64, error) {
			return joined.AsyncAddDestination(dest.channel)
		})
		awaitAsyncDestination(t, client, hadClientError, func() (int64, error) {
			return joined.AsyncAddDestination(kept.channel)
		})
		harness.WaitUntilConnected(t, joined)
		harness.WaitUntilImageCount(t, dest.sub, 1)
		harness.WaitUntilImageCount(t, kept.sub, 1)
		requireDestinationsReceive(t, client, joined, 0xE4, dest, kept)

		awaitAsyncDestination(t, client, hadClientError, func() (int64, error) {
			return joined.AsyncRemoveDestinationByID(destID)
		})
		requireDestinationRemoved(t, client, joined, 0xE6, dest, kept)
	})

	finishPubSubIPCTest(t, md, hadClientError)
}
