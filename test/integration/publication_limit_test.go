//go:build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/test/integration/harness"
)

// TestPublicationLimitCounterAliasesDriver proves Publication.PositionLimit
// matches the driver publisher-limit counter on live aeronmd.
func TestPublicationLimitCounterAliasesDriver(t *testing.T) {
	md := harness.SharedParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	pub, _ := harness.ConnectIPCPubSubClients(
		t,
		client,
		client,
		harness.PublicationLimitStreamID,
	)

	countersReader, err := client.CountersReader()
	require.NoError(t, err)

	limitID, driverLimit := harness.PublicationLimitCounterValue(
		t,
		countersReader,
		pub.RegistrationID(),
	)

	require.Equal(t, driverLimit, pub.PositionLimit())
	require.True(
		t,
		countersReader.IsCounterActive(
			limitID,
			pub.RegistrationID(),
			cnc.DriverPublisherLimitTypeID,
		),
	)

	finishPubSubIPCTest(t, md, hadClientError)
}

// TestPublicationLimitAdvancesAfterSubscriberDrain proves publication limit
// advances after subscriber drain on live aeronmd.
func TestPublicationLimitAdvancesAfterSubscriberDrain(t *testing.T) {
	md := harness.SharedParallel(t)
	client, hadClientError := harness.ConnectIPCClient(t, md)

	pub, sub := harness.ConnectIPCPubSubClients(
		t,
		client,
		client,
		harness.PublicationLimitAfterDrainStreamID,
	)

	termDataLength, err := harness.TermDataLengthFromCmdEnv(md)
	require.NoError(t, err)

	payload := []byte{1, 2, 3, 4}
	maxOffers := harness.ComputeRolloverMessageCount(termDataLength, len(payload))
	require.Positive(t, maxOffers)

	var offered int
	sawBackPressured := false
	for range maxOffers {
		result := pub.Offer(payload)
		switch {
		case result > 0:
			offered++
			client.DoWork()
		case result == aeron.BackPressured:
			sawBackPressured = true
		case result == aeron.AdminAction:
			client.DoWork()
		default:
			require.FailNowf(t, "unexpected offer status", "status=%d", result)
		}
		if sawBackPressured {
			break
		}
	}
	require.True(
		t,
		sawBackPressured,
		"expected BackPressured without subscriber drain within %d offers (termLen=%d payload=%d)",
		maxOffers,
		termDataLength,
		len(payload),
	)
	require.Positive(t, offered, "expected successful offers before BackPressured")
	require.Equal(t, int64(0), pub.AvailableWindow())

	limitBeforeDrain := pub.PositionLimit()
	windowBeforeDrain := pub.AvailableWindow()

	// Drain every offered message; partial drain left window at 0 after multi-message burst.
	harness.PollDelivered(t, client, sub, offered)

	pollStart := time.Now()
	deadline := pollStart.Add(harness.ConnectedPollTimeout)
	advanced := false
	for time.Now().Before(deadline) {
		client.DoWork()
		advanced = pub.AvailableWindow() > 0 || pub.PositionLimit() > limitBeforeDrain
		if advanced {
			break
		}
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}
	finalLimit := pub.PositionLimit()
	finalWindow := pub.AvailableWindow()
	require.True(
		t,
		advanced,
		"limit/window did not advance within %s (elapsed %s): limit %d->%d window %d->%d",
		harness.ConnectedPollTimeout,
		time.Since(pollStart).Round(time.Millisecond),
		limitBeforeDrain,
		finalLimit,
		windowBeforeDrain,
		finalWindow,
	)

	countersReader, err := client.CountersReader()
	require.NoError(t, err)
	_, driverLimit := harness.PublicationLimitCounterValue(
		t,
		countersReader,
		pub.RegistrationID(),
	)
	require.Equal(t, driverLimit, finalLimit)

	require.Positive(t, pub.Offer(payload))

	finishPubSubIPCTest(t, md, hadClientError)
}
