//go:build integration

package harness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
)

const driverConsumeWindow = 200 * time.Millisecond

func TestCncWiring(t *testing.T) {
	md, attach := StartAttachedCnc(t)
	toDriver := attach.Rings.ToDriver
	toClients := attach.Rings.ToClients

	t.Run("PassiveAttach", func(t *testing.T) {
		t.Run("MetadataMatchesMmap", func(t *testing.T) {
			assertMetadataMatchesMmap(t, md, attach)
		})

		t.Run("RingConstruct", func(t *testing.T) {
			assertRingConstruct(t, attach)
		})

		t.Run("DriverConsumerHeartbeatFresh", func(t *testing.T) {
			assertConsumerHeartbeatFresh(t, attach)
		})
	})

	t.Run("DriverProcessAlive", func(t *testing.T) {
		assertDriverAliveAndPIDMatches(t, md, attach)
	})

	t.Run("RingOfferNoCorruption", func(t *testing.T) {
		err := conductor.NewDriverProxy(toDriver, 1).ClientKeepalive()
		require.NoError(t, err)

		time.Sleep(driverConsumeWindow)
		md.AssertProcessAlive(t)
	})

	t.Run("SoakAttach", func(t *testing.T) {
		meta := attach.Rings.Meta
		timeoutMs := meta.ClientLivenessTimeoutNs / 1_000_000
		const marginMs = int64(1_000)
		require.Greater(t, timeoutMs, marginMs)

		runSoak(t, soakDuration, soakPollInterval, func() {
			assertDriverAliveAndPIDMatches(t, md, attach)
			require.True(t, consumerHeartbeatWithin(toDriver, timeoutMs-marginMs, true))
			require.False(t, toClients.ReceiveNext())
		})
	})

	t.Run("GetNextSessionIDRoundtrip", func(t *testing.T) {
		const sessionIDStreamID int32 = 100

		client, err := aeron.Connect(aeron.Context{AeronDir: md.Dir})
		require.NoError(t, err)
		require.GreaterOrEqual(t, client.ClientId(), int64(0))
		t.Cleanup(func() {
			require.NoError(t, client.Close())
		})

		firstSessionID, err := client.NextAvailableSessionID(sessionIDStreamID)
		require.NoError(t, err)
		secondSessionID, err := client.NextAvailableSessionID(sessionIDStreamID)
		require.NoError(t, err)
		require.NotEqual(t, firstSessionID, secondSessionID)

		// Live-driver roundtrip via Client.NextAvailableSessionID await path
		// (offer -> conductor -> broadcast -> Client return). Broadcast wire decode and correlation
		// matching are covered in unit tests (aeron/client_await_test.go). Failure modes
		// (timeout, ON_ERROR, driver death) stay in unit tests by design. Aeron 1.51.0 seeds
		// next_session_id via aeron_randomised_int32(); values may be negative.
		t.Logf("next session ids: %d, %d", firstSessionID, secondSessionID)

		// Client advances its own broadcast cursor; drain harness receiver so later subtests
		// polling toClients do not see stale ON_NEXT_AVAILABLE_SESSION_ID events.
		for toClients.ReceiveNext() {
			require.True(t, toClients.Validate())
		}

		md.AssertProcessAlive(t)
	})

	t.Run("AddPublicationOfferedOnLiveRing", func(t *testing.T) {
		const (
			clientID      int64 = 1
			correlationID int64 = 2001
			streamID      int32 = 1001
			channel             = "aeron:ipc"
		)

		WaitForToDriverRingDrained(t, toDriver)

		consumerBefore := toDriver.ConsumerPosition()
		producerBefore := toDriver.ProducerPosition()

		want := conductor.PublicationMessage{
			ClientID:      clientID,
			CorrelationID: correlationID,
			StreamID:      streamID,
			Channel:       channel,
		}

		proxy := conductor.NewDriverProxy(toDriver, clientID)
		err := proxy.AddPublication(correlationID, streamID, channel)
		require.NoError(t, err)

		assertAddPublicationOfferedOnRing(
			t,
			toDriver,
			want,
			consumerBefore,
			producerBefore,
		)

		WaitForToDriverRingDrained(t, toDriver)
		require.Greater(t, toDriver.ConsumerPosition(), consumerBefore)
		require.Equal(t, toDriver.ProducerPosition(), toDriver.ConsumerPosition())

		md.AssertProcessAlive(t)
	})
}
