//go:build integration

package harness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
		const correlationID int64 = 1001
		err := conductor.NewDriverProxy(toDriver, 1).
			GetNextAvailableSessionID(correlationID, 100)
		require.NoError(t, err)

		event := waitForNextSessionIDEvent(t, toClients, correlationID)
		// Aeron 1.51.0 seeds next_session_id via aeron_randomised_int32(); values may be
		// negative. Roundtrip is proven by type, length, decode, and correlation match.
		t.Logf("next session id: %d", event.Int32Value)
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
