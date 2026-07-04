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
	md, attach := SharedAttachedCnc(t)
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
			AssertConsumerHeartbeatFresh(t, attach)
		})
	})

	t.Run("DriverProcessAlive", func(t *testing.T) {
		assertDriverAliveAndPIDMatches(t, md, attach)
	})

	t.Run("RingOfferNoCorruption", func(t *testing.T) {
		err := conductor.NewDriverProxy(toDriver, 1).ClientKeepalive()
		require.NoError(t, err)

		time.Sleep(driverConsumeWindow)

		DrainToClients(t, toClients, 0)

		md.AssertProcessAlive(t)
	})

	t.Run("SoakAttach", func(t *testing.T) {
		timeoutMs := attach.Rings.Meta.ClientLivenessTimeoutNs / 1_000_000
		const marginMs = int64(1_000)
		require.Greater(t, timeoutMs, marginMs)

		// Shared driver may still deliver straggler broadcast from earlier package tests.
		// Settle and drain before soak; assert quiescence once at soak entry.
		time.Sleep(driverConsumeWindow)
		DrainToClients(t, toClients, 0)
		require.False(t, toClients.ReceiveNext(), "to-clients must be quiescent at soak entry")

		// Soak ticks tolerate bounded stragglers only; continuous broadcast fails the test.
		const maxStragglerDrainsPerTick = 4
		RunSoak(t, soakDuration, soakPollInterval, func() {
			assertDriverAliveAndPIDMatches(t, md, attach)
			require.True(t, consumerHeartbeatWithin(toDriver, timeoutMs-marginMs, true))
			require.LessOrEqual(
				t,
				DrainToClients(t, toClients, maxStragglerDrainsPerTick+1),
				maxStragglerDrainsPerTick,
				"unexpected continuous to-clients broadcast during soak",
			)
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
		// (timeout, ON_ERROR, driver death) stay in unit tests by design.
		// next_session_id is seeded via aeron_randomised_int32(); values may be negative.
		t.Logf("next session ids: %d, %d", firstSessionID, secondSessionID)

		// Client advances its own broadcast cursor; drain harness receiver so later subtests
		// polling toClients do not see stale ON_NEXT_AVAILABLE_SESSION_ID events.
		DrainToClients(t, toClients, 0)

		md.AssertProcessAlive(t)
	})

	t.Run("AddPublicationOfferedOnLiveRing", func(t *testing.T) {
		want := conductor.PublicationMessage{
			ClientID:      1,
			CorrelationID: 2001,
			StreamID:      1001,
			Channel:       IPCChannel,
		}

		consumerBefore, producerBefore := QuiescentToDriverSnapshot(t, toDriver)

		proxy := conductor.NewDriverProxy(toDriver, want.ClientID)
		require.NoError(t, proxy.AddPublication(want.CorrelationID, want.StreamID, want.Channel))

		assertAddPublicationOfferedOnRing(
			t,
			toDriver,
			want,
			consumerBefore,
			producerBefore,
		)

		consumerAfter, _ := QuiescentToDriverSnapshot(t, toDriver)
		require.Greater(t, consumerAfter, consumerBefore)

		md.AssertProcessAlive(t)
	})
}
