//go:build integration

package integration_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/test/integration/harness"
)

const publicationReadyPollTimeout = 10 * time.Second

func TestChannelStatusCounterIsCounterActive(t *testing.T) {
	md, attach := harness.StartAttachedCnc(t)
	toDriver := attach.Rings.ToDriver
	toClients := attach.Rings.ToClients

	client, err := aeron.Connect(aeron.Context{AeronDir: md.Dir})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	countersReader, err := client.CountersReader()
	require.NoError(t, err)

	const (
		streamID int32 = 1001
		// UDP publications receive a send channel-status counter from the driver (IPC may omit it).
		channel = "aeron:udp?endpoint=127.0.0.1:24325"
	)

	harness.WaitForToDriverRingDrained(t, toDriver)

	correlationID := client.NextCorrelationID()
	proxy := conductor.NewDriverProxy(toDriver, client.ClientId())
	require.NoError(t, proxy.AddPublication(correlationID, streamID, channel))

	ready := waitForPublicationReady(t, client, toClients, correlationID)

	require.NotEqual(t, cnc.NullCounterID, ready.ChannelStatusCounterID)
	require.Greater(t, ready.RegistrationID, int64(0))
	require.True(
		t,
		countersReader.IsCounterActive(
			ready.ChannelStatusCounterID,
			ready.RegistrationID,
			cnc.DriverSendChannelStatusTypeID,
		),
		"channel status counter %d should be active for registration %d",
		ready.ChannelStatusCounterID,
		ready.RegistrationID,
	)
	require.False(
		t,
		countersReader.IsCounterActive(
			ready.ChannelStatusCounterID,
			ready.RegistrationID+1,
			cnc.DriverSendChannelStatusTypeID,
		),
	)

	statusValue, err := countersReader.GetCounterValue(ready.ChannelStatusCounterID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, statusValue, int64(0))

	harness.WaitForToDriverRingDrained(t, toDriver)
	md.AssertProcessAlive(t)
}

func waitForPublicationReady(
	t *testing.T,
	client *aeron.Client,
	toClients *broadcast.BroadcastReceiver,
	correlationID int64,
) conductor.PublicationBuffersReady {
	t.Helper()

	deadline := time.Now().Add(publicationReadyPollTimeout)
	for time.Now().Before(deadline) {
		client.DoWork()

		if toClients.ReceiveNext() && toClients.Validate() {
			switch toClients.TypeID() {
			case conductor.OnPublicationReady, conductor.OnExclusivePublicationReady:
				decoded, err := conductor.DecodePublicationBuffersReady(
					toClients.Buffer(),
					toClients.Offset(),
					toClients.Length(),
				)
				require.NoError(t, err)
				if decoded.CorrelationID == correlationID {
					return decoded
				}
			case conductor.OnError:
				require.GreaterOrEqual(
					t,
					toClients.Length(),
					conductor.ErrorResponseMinBodyLength,
					"ON_ERROR correlation %d",
					correlationID,
				)
				onError, err := conductor.DecodeErrorResponse(
					toClients.Buffer(),
					toClients.Offset(),
					toClients.Length(),
				)
				require.NoError(t, err)
				if onError.OffendingCommandCorrelationID == correlationID {
					require.FailNow(
						t,
						fmt.Sprintf(
							"ADD_PUBLICATION failed: code=%d message=%q",
							onError.ErrorCode,
							onError.ErrorMessage,
						),
					)
				}
			}
		}

		time.Sleep(aeron.DefaultIdleSleepDuration)
	}

	require.FailNow(
		t,
		fmt.Sprintf("ON_PUBLICATION_READY not received within %s", publicationReadyPollTimeout),
	)
	return conductor.PublicationBuffersReady{}
}
