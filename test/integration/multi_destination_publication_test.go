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

// manualSendChannel has no tag, so the driver gives each publication its own
// send endpoint.
const manualSendChannel = "aeron:udp?control-mode=manual"

type sendDestinationPublication interface {
	payloadOfferer
	AddDestination(channel string) error
	RemoveDestination(channel string) error
	Close() error
}

// TestMultiDestinationPublication asserts destinations receive until removed and a repeat remove is acknowledged.
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
			const (
				messageCount = 4
				payloadSize  = 64
			)
			ports := harness.FreeUDPPorts(t, 2)
			destA := "aeron:udp?endpoint=" + harness.LoopbackUDPEndpoint(ports[0])
			destB := "aeron:udp?endpoint=" + harness.LoopbackUDPEndpoint(ports[1])

			subA, err := client.AddSubscription(streamID, destA)
			require.NoError(t, err)
			harness.CleanupClose(t, "close destination A subscription", subA.Close)
			subB, err := client.AddSubscription(streamID, destB)
			require.NoError(t, err)
			harness.CleanupClose(t, "close destination B subscription", subB.Close)

			pub := test.add(t)
			harness.CleanupClose(t, "close manual publication", pub.Close)

			// Both destinations receive.
			require.NoError(t, pub.AddDestination(destA))
			require.NoError(t, pub.AddDestination(destB))
			harness.WaitUntilConnected(t, pub)
			harness.WaitUntilImageCount(t, subA, 1)
			harness.WaitUntilImageCount(t, subB, 1)

			payloads := harness.DistinctIPCPayloads(messageCount, payloadSize, 0xE0)
			offerPayloads(t, client, pub, payloads)
			require.Equal(t, payloads, harness.PollDelivered(t, client, subA, messageCount))
			require.Equal(t, payloads, harness.PollDelivered(t, client, subB, messageCount))

			// Remove A. Only B receives.
			require.NoError(t, pub.RemoveDestination(destA))

			afterRemove := harness.DistinctIPCPayloads(messageCount, payloadSize, 0xE1)
			offerPayloads(t, client, pub, afterRemove)
			require.Equal(t, afterRemove, harness.PollDelivered(t, client, subB, messageCount))
			requireNoFragments(t, client, subA, 250*time.Millisecond)

			// The driver acknowledges a remove that matches no destination.
			require.NoError(t, pub.RemoveDestination(destA))
		})
	}

	finishPubSubIPCTest(t, md, hadClientError)
}

// TestMultiDestinationPublication_driverErrors asserts the driver rejects AddDestination on non-manual publications.
func TestMultiDestinationPublication_driverErrors(t *testing.T) {
	md := harness.SharedParallel(t)
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
