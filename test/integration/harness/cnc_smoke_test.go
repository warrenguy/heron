//go:build integration

package harness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
		err := offerClientKeepalive(toDriver, 1, -1)
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
			require.False(t, freshToClientsReceiver(t, attach).ReceiveNext())
		})
	})

	t.Run("GetNextSessionIDRoundtrip", func(t *testing.T) {
		const correlationID int64 = 1001
		err := offerGetNextSessionID(toDriver, 1, correlationID, 100)
		require.NoError(t, err)

		event := waitForNextSessionIDEvent(t, toClients, correlationID)
		// Aeron 1.51.0 seeds next_session_id via aeron_randomised_int32(); values may be
		// negative. Roundtrip is proven by type, length, decode, and correlation match.
		t.Logf("next session id: %d", event.NextSessionID)
		md.AssertProcessAlive(t)
	})
}
