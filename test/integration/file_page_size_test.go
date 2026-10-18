//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/test/integration/harness"
)

const (
	largeFilePageSize          = 65536
	largePageIPCLogLength      = 262144
	largePageUDPTermLength     = 131072
	largePageUDPLogLength      = 458752
	largePageMessageCount      = 4
	largePagePayloadSize       = 64
	largePageIPCStreamID       = int32(9201)
	largePageExclusiveStreamID = int32(9202)
	largePageUDPStreamID       = int32(9203)
)

// TestClientMapsLogsWithLargeFilePageSize maps logs the driver aligns to 64 KiB pages.
func TestClientMapsLogsWithLargeFilePageSize(t *testing.T) {
	t.Parallel()
	md := harness.StartWithConfig(t, harness.Config{
		Env: harness.FilePageSizeDriverEnv(largeFilePageSize),
	})

	meta, err := cnc.ReadMetadataFile(md.CncPath())
	require.NoError(t, err)
	require.Equal(t, int32(largeFilePageSize), meta.FilePageSize)

	t.Run("ipc publication", func(t *testing.T) {
		pubClient, hadPubClientError := harness.ConnectIPCClient(t, md)
		subClient, hadSubClientError := harness.ConnectIPCClient(t, md)
		pub, sub := harness.ConnectIPCPubSubClients(t, pubClient, subClient, largePageIPCStreamID)

		offer := func(payload []byte) { harness.OfferWithRetry(t, pub, payload) }
		requireLargePageDelivery(t, pubClient, subClient, sub, offer, 0xD0)
		requireImageLogLength(t, sub, largePageIPCLogLength)
		finishPubSubIPCTest(t, md, hadPubClientError, hadSubClientError)
	})

	t.Run("ipc exclusive publication", func(t *testing.T) {
		pubClient, hadPubClientError := harness.ConnectIPCClient(t, md)
		subClient, hadSubClientError := harness.ConnectIPCClient(t, md)
		pub, sub := harness.ConnectExclusiveIPCPubSubClients(
			t,
			pubClient,
			subClient,
			largePageExclusiveStreamID,
		)

		offer := func(payload []byte) { harness.OfferWithRetry(t, pub, payload) }
		requireLargePageDelivery(t, pubClient, subClient, sub, offer, 0xD8)
		requireImageLogLength(t, sub, largePageIPCLogLength)
		finishPubSubIPCTest(t, md, hadPubClientError, hadSubClientError)
	})

	t.Run("udp publication", func(t *testing.T) {
		client, hadClientError := harness.ConnectIPCClient(t, md)
		channel := "aeron:udp?endpoint=" + harness.LoopbackUDPEndpoint(harness.FreeUDPPort(t)) +
			"|term-length=" + strconv.Itoa(largePageUDPTermLength)

		pub, err := client.AddPublication(largePageUDPStreamID, channel)
		require.NoError(t, err)
		harness.CleanupClose(t, "close publication", pub.Close)
		sub, err := client.AddSubscription(largePageUDPStreamID, channel)
		require.NoError(t, err)
		harness.CleanupClose(t, "close subscription", sub.Close)
		harness.WaitForIPCLinkConnected(t, client, pub, sub)

		offer := func(payload []byte) { harness.OfferWithRetry(t, pub, payload) }
		requireLargePageDelivery(t, client, client, sub, offer, 0xE0)
		requireImageLogLength(t, sub, largePageUDPLogLength)
		requirePublicationLogCount(t, md, largePageUDPLogLength, 1)
		finishPubSubIPCTest(t, md, hadClientError)
	})
}

// requireLargePageDelivery offers distinct payloads. Asserts that sub delivers
// them in order.
func requireLargePageDelivery(
	t *testing.T,
	pubClient *aeron.Client,
	subClient *aeron.Client,
	sub *aeron.Subscription,
	offer func(payload []byte),
	headerBase byte,
) {
	t.Helper()

	payloads := harness.DistinctIPCPayloads(largePageMessageCount, largePagePayloadSize, headerBase)
	for _, payload := range payloads {
		offer(payload)
	}
	var received [][]byte
	if pubClient == subClient {
		received = harness.PollDelivered(t, subClient, sub, largePageMessageCount)
	} else {
		received = harness.PollDeliveredSplit(t, pubClient, subClient, sub, largePageMessageCount)
	}
	require.Equal(t, payloads, received)
}

func requireImageLogLength(t *testing.T, sub *aeron.Subscription, want int64) {
	t.Helper()

	image, ok := sub.ImageAt(0)
	require.True(t, ok)
	info, err := os.Stat(image.LogFileName())
	require.NoError(t, err)
	require.Equal(t, want, info.Size())
}

// requirePublicationLogCount asserts that exactly want publication logs under
// md.Dir have length bytes.
func requirePublicationLogCount(t *testing.T, md *harness.MediaDriver, length int64, want int) {
	t.Helper()

	logs, err := filepath.Glob(filepath.Join(md.Dir, "publications", "*.logbuffer"))
	require.NoError(t, err)
	var count int
	for _, path := range logs {
		info, err := os.Stat(path)
		require.NoError(t, err)
		if info.Size() == length {
			count++
		}
	}
	require.Equal(t, want, count, "publication logs %v", logs)
}
