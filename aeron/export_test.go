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

	return &Client{
		clientID:  id,
		conductor: conductorInstance,
		nanoClock: nanoClock,
	}
}
