package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/broadcast"
)

const (
	goldenPublicationReadyLogFileName            = "/dev/shm/aeron-publication.log"
	goldenExclusivePublicationReadyLogFileName   = "/dev/shm/aeron-exclusive-publication.log"
	goldenPublicationReadyCorrelationID          = int64(1001)
	goldenExclusivePublicationReadyCorrelationID = int64(0x0F06_0000_0002)
	goldenSubscriptionReadyCorrelationID         = int64(3003)
	goldenAvailableImageCorrelationID            = int64(4004)
	goldenUnavailableImageCorrelationID          = int64(6006)
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

var goldenExclusivePublicationBuffersReadyEvent = conductor.PublicationBuffersReady{
	CorrelationID:             goldenExclusivePublicationReadyCorrelationID,
	RegistrationID:            2003,
	SessionID:                 43,
	StreamID:                  8,
	PublicationLimitCounterID: 10,
	ChannelStatusCounterID:    11,
	LogFileName:               goldenExclusivePublicationReadyLogFileName,
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

func publicationBuffersReadyWithCorrelation(
	base conductor.PublicationBuffersReady,
	correlationID int64,
) conductor.PublicationBuffersReady {
	event := base
	event.CorrelationID = correlationID
	return event
}

func goldenPublicationBuffersReadyWithCorrelation(
	correlationID int64,
) conductor.PublicationBuffersReady {
	return publicationBuffersReadyWithCorrelation(
		goldenPublicationBuffersReadyEvent,
		correlationID,
	)
}

func goldenExclusivePublicationBuffersReadyWithCorrelation(
	correlationID int64,
) conductor.PublicationBuffersReady {
	return publicationBuffersReadyWithCorrelation(
		goldenExclusivePublicationBuffersReadyEvent,
		correlationID,
	)
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
