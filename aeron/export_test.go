package aeron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/cnc/cnctest"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

func MapConductorErrorForTest(err error) error {
	return mapConductorError(err)
}

func MapAwaitErrorForTest(err error) error {
	return mapAwaitError(err)
}

func MapPollPendingErrorForTest(
	correlationID int64,
	timeout time.Duration,
	err error,
) error {
	return mapPollPendingError(correlationID, timeout, err)
}

func MapRegistrationErrorForTest(err error) error {
	return mapRegistrationError(err)
}

func (client *Client) AwaitResponseForTest(
	correlationID int64,
	kind conductor.PendingKind,
	timeout time.Duration,
	offer func() error,
) (conductor.PendingRegistration, error) {
	return client.awaitResponse(correlationID, kind, timeout, offer)
}

func (client *Client) PollAwaitForTest(correlationID int64) error {
	if err := client.requireReadyWithConductor(); err != nil {
		return err
	}
	done, _, err := client.conductor.PollPending(correlationID)
	if done {
		return mapPollPendingError(correlationID, client.driverTimeout, err)
	}
	client.DoWork()
	done, _, err = client.conductor.PollPending(correlationID)
	if !done {
		return ErrClientNotReady
	}
	return mapPollPendingError(correlationID, client.driverTimeout, err)
}

func NormalizeContextForTest(ctx Context) Context {
	return normalizeContext(ctx)
}

func (ctx Context) UseConductorInvokerEnabledForTest() bool {
	return ctx.useConductorInvokerEnabled()
}

func WaitForDriverHeartbeatForTest(
	toDriver *ringbuffer.ManyToOneRingBuffer,
	timeout time.Duration,
	epochClock conductor.EpochClock,
) error {
	return waitForDriverHeartbeat(toDriver, timeout, epochClock)
}

func ValidateControlProtocolVersionForTest(reader cnc.CountersView) error {
	return validateControlProtocolVersion(reader)
}

func ValidateClientIDForTest(clientID int64) error {
	return validateClientID(clientID)
}

func NewAccessorTestClient(tb testing.TB) *Client {
	tb.Helper()

	mapped := cnctest.OpenAccessorTestMappedCncFile(tb)

	countersReader, err := cnc.NewCountersReader(
		mapped.CountersMetaDataBuffer(),
		mapped.CountersValuesBuffer(),
	)
	require.NoError(tb, err)

	return &Client{
		mapped:         mapped,
		countersReader: countersReader,
	}
}

func NewClientForTest(
	conductorInstance *conductor.Conductor,
	nanoClock conductor.NanoClock,
	clientID ...int64,
) *Client {
	var id int64
	if len(clientID) > 0 {
		id = clientID[0]
	}

	return NewClientForTestWithDriverProxy(
		conductorInstance,
		nil,
		nanoClock,
		id,
		defaultDriverTimeout,
		defaultIdleSleepDuration,
	)
}

func NewClientForTestWithDriverProxy(
	conductorInstance *conductor.Conductor,
	driverProxy *conductor.DriverProxy,
	nanoClock conductor.NanoClock,
	clientID int64,
	driverTimeout time.Duration,
	idleSleepDuration time.Duration,
) *Client {
	return &Client{
		clientID:          clientID,
		conductor:         conductorInstance,
		driverProxy:       driverProxy,
		nanoClock:         nanoClock,
		driverTimeout:     driverTimeout,
		idleSleepDuration: idleSleepDuration,
	}
}
