package testutil

import (
	"time"

	"github.com/stretchr/testify/require"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const (
	TestClientID            int64 = 42
	TestEpochMs             int64 = 1_700_000_000_123
	TestRingDataCapacity          = 4096
	TestDriverTimeout             = 10 * time.Second
	TestKeepAliveInterval         = 500 * time.Millisecond
	TestIdleSleepDuration         = time.Millisecond
	TestInterServiceTimeout       = time.Second
)

var (
	TestStartNanoNs = (2 * time.Second).Nanoseconds()
	TestWorkNanoNs  = (600 * time.Millisecond).Nanoseconds()
)

type TB interface {
	Helper()
	require.TestingT
}

type ConductorConfig struct {
	ClientID            int64
	DriverTimeout       time.Duration
	KeepAliveInterval   time.Duration
	IdleSleepDuration   time.Duration
	InterServiceTimeout time.Duration
	EpochMs             int64
	NanoNs              *int64
	NanoClock           func() int64
	ErrorHandler        func(error)
}

func DefaultConductorConfig(nanoNs *int64) ConductorConfig {
	return ConductorConfig{
		ClientID:            TestClientID,
		DriverTimeout:       TestDriverTimeout,
		KeepAliveInterval:   TestKeepAliveInterval,
		IdleSleepDuration:   TestIdleSleepDuration,
		InterServiceTimeout: TestInterServiceTimeout,
		EpochMs:             TestEpochMs,
		NanoNs:              nanoNs,
	}
}

type ConductorFixture struct {
	Conductor   *conductor.Conductor
	DriverProxy *conductor.DriverProxy
	Transmitter *broadcast.BroadcastTransmitter
	Ring        *ringbuffer.ManyToOneRingBuffer
	NanoClock   func() int64
}

func NewTestRing(tb TB) *ringbuffer.ManyToOneRingBuffer {
	tb.Helper()

	ringBuffer := aeronatomic.Wrap(
		make([]byte, ringbuffer.TotalBufferLength(TestRingDataCapacity)),
	)
	ring, err := ringbuffer.NewManyToOneRingBuffer(ringBuffer)
	require.NoError(tb, err)
	return ring
}

func newTestTransport(epochMs int64) (
	*ringbuffer.ManyToOneRingBuffer,
	*broadcast.BroadcastReceiver,
	*broadcast.BroadcastTransmitter,
	error,
) {
	ringBuffer := aeronatomic.Wrap(
		make([]byte, ringbuffer.TotalBufferLength(TestRingDataCapacity)),
	)
	ring, err := ringbuffer.NewManyToOneRingBuffer(ringBuffer)
	if err != nil {
		return nil, nil, nil, err
	}
	ring.SetConsumerHeartbeatTime(epochMs)

	broadcastBuffer := aeronatomic.Wrap(
		make([]byte, broadcast.TotalBufferLength(TestRingDataCapacity)),
	)
	receiver, err := broadcast.NewBroadcastReceiver(broadcastBuffer)
	if err != nil {
		return nil, nil, nil, err
	}
	transmitter, err := broadcast.NewBroadcastTransmitter(broadcastBuffer)
	if err != nil {
		return nil, nil, nil, err
	}
	return ring, receiver, transmitter, nil
}

func NewConductorFixture(tb TB, config ConductorConfig) *ConductorFixture {
	tb.Helper()

	if config.NanoNs == nil {
		var nanoNs int64
		config.NanoNs = &nanoNs
	}

	nanoClock := config.NanoClock
	if nanoClock == nil {
		nanoClock = func() int64 {
			return *config.NanoNs
		}
	}

	ring, receiver, transmitter, err := newTestTransport(config.EpochMs)
	require.NoError(tb, err)

	meta := make([]byte, cnc.CounterMetadataLength)
	values := make([]byte, cnc.CounterValueLength)
	countersReader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)

	conductorConfig := conductor.Config{
		ClientID:            config.ClientID,
		DriverTimeout:       config.DriverTimeout,
		KeepAliveInterval:   config.KeepAliveInterval,
		IdleSleepDuration:   config.IdleSleepDuration,
		InterServiceTimeout: config.InterServiceTimeout,
		EpochClock: func() int64 {
			return config.EpochMs
		},
		NanoClock:    nanoClock,
		ErrorHandler: config.ErrorHandler,
	}

	driverProxy := conductor.NewDriverProxy(ring, config.ClientID)
	return &ConductorFixture{
		Conductor: conductor.NewConductor(
			driverProxy,
			receiver,
			countersReader,
			conductorConfig,
		),
		DriverProxy: driverProxy,
		Transmitter: transmitter,
		Ring:        ring,
		NanoClock:   nanoClock,
	}
}

func (fixture *ConductorFixture) Transmit(msgTypeID int, payload []byte) error {
	return fixture.Transmitter.Transmit(msgTypeID, payload)
}
