//go:build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/test/integration/harness"
)

func TestClientCloseLifecycle(t *testing.T) {
	md := harness.Start(t)

	client, err := aeron.Connect(aeron.Context{AeronDir: md.Dir})
	require.NoError(t, err)

	require.NoError(t, client.Close())
	require.Zero(t, client.DoWork())
	require.NoError(t, client.Close())

	md.AssertProcessAlive(t)
}

func TestClientCloseOffersCLIENT_CLOSEOnLiveRing(t *testing.T) {
	md, attach := harness.StartAttachedCnc(t)
	toDriver := attach.Rings.ToDriver

	client, err := aeron.Connect(aeron.Context{AeronDir: md.Dir})
	require.NoError(t, err)
	clientID := client.ClientId()
	require.GreaterOrEqual(t, clientID, int64(0))

	for range 3 {
		client.DoWork()
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}

	harness.WaitForToDriverRingDrained(t, toDriver)

	consumerBefore := toDriver.ConsumerPosition()
	producerBefore := toDriver.ProducerPosition()

	harness.AssertClientCloseOfferedOnRingAfterClose(
		t,
		toDriver,
		clientID,
		consumerBefore,
		producerBefore,
		client.Close,
	)
	md.AssertProcessAlive(t)
}
