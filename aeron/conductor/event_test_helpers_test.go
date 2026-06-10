package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/conductor"
)

// Golden bytes generated from Aeron 1.51.0 Java flyweight layouts via conductor encoders.
const (
	goldenPublicationReadyLogFileName    = "/dev/shm/aeron-publication.log"
	goldenPublicationReadyCorrelationID  = int64(1001)
	goldenSubscriptionReadyCorrelationID = int64(3003)
	goldenAvailableImageCorrelationID    = int64(4004)
	goldenUnavailableImageCorrelationID  = int64(6006)
)

var goldenPublicationBuffersReadyEvent = conductor.PublicationBuffersReady{
	CorrelationID:             goldenPublicationReadyCorrelationID,
	RegistrationID:            2002,
	SessionID:                 42,
	StreamID:                  7,
	PublicationLimitCounterID: 10,
	ChannelStatusCounterID:    11,
	LogFileName:               goldenPublicationReadyLogFileName,
}

var goldenSubscriptionReadyEvent = conductor.SubscriptionReady{
	CorrelationID:          goldenSubscriptionReadyCorrelationID,
	ChannelStatusCounterID: 12,
}

var goldenAvailableImageEvent = conductor.AvailableImage{
	CorrelationID:              goldenAvailableImageCorrelationID,
	SessionID:                  42,
	StreamID:                   7,
	SubscriptionRegistrationID: 5005,
	SubscriberPositionID:       13,
	LogFileName:                goldenPublicationReadyLogFileName,
	SourceIdentity:             "ipc",
}

var goldenUnavailableImageEvent = conductor.UnavailableImage{
	CorrelationID:              goldenUnavailableImageCorrelationID,
	SubscriptionRegistrationID: 5005,
	StreamID:                   7,
	Channel:                    "aeron:ipc",
}

var goldenPublicationErrorEvent = conductor.PublicationError{
	RegistrationID: 2002,
	SessionID:      42,
	StreamID:       7,
	ErrorCode:      1,
	ErrorMessage:   "simulated",
}

func goldenPublicationBuffersReadyWithCorrelation(
	correlationID int64,
) conductor.PublicationBuffersReady {
	event := goldenPublicationBuffersReadyEvent
	event.CorrelationID = correlationID
	return event
}

func goldenSubscriptionReadyWithCorrelation(
	correlationID int64,
) conductor.SubscriptionReady {
	event := goldenSubscriptionReadyEvent
	event.CorrelationID = correlationID
	return event
}

func transmitConductorEvent(
	t *testing.T,
	adapter *conductor.DriverEventsAdapter,
	msgTypeID int,
	payload []byte,
) {
	t.Helper()

	copyReceiver := adapter.CopyReceiverForTest()
	buf := copyReceiver.BroadcastReceiver().Buffer()
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	require.NoError(t, err)
	err = tx.Transmit(msgTypeID, payload)
	require.NoError(t, err)
}
