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
	ImageLifecycle      conductor.ImageLifecycle
	ResourceLifecycle   conductor.ResourceLifecycle
	MediaDriverDir      string
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

func newTestRingBuffer() (*ringbuffer.ManyToOneRingBuffer, error) {
	ringBuffer := aeronatomic.Wrap(
		make([]byte, ringbuffer.TotalBufferLength(TestRingDataCapacity)),
	)
	return ringbuffer.NewManyToOneRingBuffer(ringBuffer)
}

func NewTestRing(tb TB) *ringbuffer.ManyToOneRingBuffer {
	tb.Helper()

	ring, err := newTestRingBuffer()
	require.NoError(tb, err)
	return ring
}

func newTestTransport(epochMs int64) (
	*ringbuffer.ManyToOneRingBuffer,
	*broadcast.BroadcastReceiver,
	*broadcast.BroadcastTransmitter,
	error,
) {
	ring, err := newTestRingBuffer()
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

	cfg := config
	nanoNs := cfg.NanoNs
	if nanoNs == nil {
		var defaultNanoNs int64
		nanoNs = &defaultNanoNs
		cfg.NanoNs = nanoNs
	}

	nanoClock := cfg.NanoClock
	if nanoClock == nil {
		nanoClock = func() int64 {
			return *nanoNs
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
		NanoClock:         nanoClock,
		ErrorHandler:      config.ErrorHandler,
		ImageLifecycle:    config.ImageLifecycle,
		ResourceLifecycle: config.ResourceLifecycle,
		MediaDriverDir:    config.MediaDriverDir,
	}

	driverProxy := conductor.NewDriverProxy(ring, config.ClientID)
	conductorInstance, err := conductor.NewConductor(
		driverProxy,
		receiver,
		countersReader,
		conductorConfig,
	)
	require.NoError(tb, err)
	return &ConductorFixture{
		Conductor:   conductorInstance,
		DriverProxy: driverProxy,
		Transmitter: transmitter,
		Ring:        ring,
		NanoClock:   nanoClock,
	}
}

func (fixture *ConductorFixture) Transmit(msgTypeID int, payload []byte) error {
	return fixture.Transmitter.Transmit(msgTypeID, payload)
}
