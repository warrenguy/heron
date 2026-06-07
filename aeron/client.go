package aeron

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/mmap"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const driverHeartbeatPollInterval = 50 * time.Millisecond

type Client struct {
	clientID                int64
	mapped                  *cnc.MappedCncFile
	conductor               *conductor.Conductor
	driverProxy             *conductor.DriverProxy
	nanoClock               conductor.NanoClock
	serviceTimestampsSeeded bool
	closed                  bool
}

func Connect(ctx Context) (*Client, error) {
	ctx = normalizeContext(ctx)
	if ctx.AeronDir == "" {
		return nil, errors.New("aeron: AeronDir is required")
	}
	if !ctx.useConductorInvokerEnabled() {
		return nil, errors.New("aeron: dedicated conductor invoker is not supported")
	}

	cncPath := filepath.Join(ctx.AeronDir, cnc.FileName)
	if err := cnc.WaitCncReady(cncPath, ctx.DriverTimeout); err != nil {
		return nil, fmt.Errorf("aeron: wait for cnc ready: %w", err)
	}

	mapped, err := cnc.MapCncFile(cncPath)
	if err != nil {
		return nil, fmt.Errorf("aeron: map cnc file: %w", err)
	}

	connected := false
	defer func() {
		if !connected {
			_ = mapped.Close()
		}
	}()

	rings, err := cnc.WireRings(mapped)
	if err != nil {
		return nil, fmt.Errorf("aeron: wire rings: %w", err)
	}

	if rings.Mapping != nil && rings.Mapping.Data != nil {
		mmap.PreTouch(rings.Mapping.Data, 0)
	}

	if err := waitForDriverHeartbeat(rings.ToDriver, ctx.DriverTimeout, ctx.EpochClock); err != nil {
		return nil, err
	}

	countersReader, err := cnc.NewCountersReader(
		mapped.CountersMetaDataBuffer(),
		mapped.CountersValuesBuffer(),
	)
	if err != nil {
		return nil, fmt.Errorf("aeron: counters reader: %w", err)
	}

	if err := validateControlProtocolVersion(countersReader); err != nil {
		return nil, err
	}

	clientID := rings.ToDriver.NextCorrelationID()
	if err := validateClientID(clientID); err != nil {
		return nil, err
	}

	driverProxy := conductor.NewDriverProxy(rings.ToDriver, clientID)
	config := conductor.Config{
		ClientID:            clientID,
		DriverTimeout:       ctx.DriverTimeout,
		KeepAliveInterval:   ctx.KeepAliveInterval,
		IdleSleepDuration:   ctx.IdleSleepDuration,
		InterServiceTimeout: ctx.InterServiceTimeout,
		EpochClock:          ctx.EpochClock,
		NanoClock:           ctx.NanoClock,
	}
	if ctx.ErrorHandler != nil {
		config.ErrorHandler = func(err error) {
			ctx.ErrorHandler(mapConductorError(err))
		}
	}
	conductorInstance := conductor.NewConductor(
		driverProxy,
		rings.ToClients,
		countersReader,
		config,
	)

	connected = true
	return &Client{
		clientID:    clientID,
		mapped:      mapped,
		conductor:   conductorInstance,
		driverProxy: driverProxy,
		nanoClock:   ctx.NanoClock,
	}, nil
}

func (client *Client) DoWork() int {
	if client == nil || client.closed || client.conductor == nil {
		return 0
	}
	if !client.serviceTimestampsSeeded {
		client.conductor.SeedServiceTimestamps(client.nanoClock())
		client.serviceTimestampsSeeded = true
	}
	return client.conductor.DoWork()
}

func (client *Client) IsTerminating() bool {
	if client == nil || client.closed || client.conductor == nil {
		return false
	}
	return client.conductor.IsTerminating()
}

func (client *Client) Close() error {
	if client == nil || client.closed {
		return nil
	}
	client.closed = true

	if client.driverProxy != nil {
		client.driverProxy.ClientClose()
	}

	var err error
	if client.mapped != nil {
		err = client.mapped.Close()
		client.mapped = nil
	}
	client.conductor = nil
	client.driverProxy = nil
	return err
}

//nolint:staticcheck // Aeron Java API uses ClientId.
func (client *Client) ClientId() int64 {
	if client == nil {
		return 0
	}
	return client.clientID
}

func waitForDriverHeartbeat(
	toDriver *ringbuffer.ManyToOneRingBuffer,
	timeout time.Duration,
	epochClock conductor.EpochClock,
) error {
	driverTimeoutMs := timeout.Milliseconds()
	startMs := epochClock()
	deadlineMs := startMs + driverTimeoutMs
	wallDeadline := time.Now().Add(timeout)
	sawPositiveHeartbeat := false

	for {
		nowMs := epochClock()
		if nowMs >= deadlineMs || !time.Now().Before(wallDeadline) {
			break
		}

		heartbeatMs := toDriver.ConsumerHeartbeatTime()
		if heartbeatMs == conductor.DriverShutdownHeartbeat {
			return &DriverTimeoutError{Cause: conductor.ErrDriverShutdown}
		}
		if heartbeatMs > 0 {
			sawPositiveHeartbeat = true
			ageMs := nowMs - heartbeatMs
			if ageMs < 0 {
				ageMs = 0
			}
			if ageMs <= driverTimeoutMs {
				return nil
			}
			break
		}
		time.Sleep(driverHeartbeatPollInterval)
	}

	cause := ErrDriverHeartbeatNotReady
	if sawPositiveHeartbeat {
		cause = ErrDriverHeartbeatStaleAtConnect
	}
	return &DriverTimeoutError{Cause: cause}
}

func validateClientID(clientID int64) error {
	if clientID < 0 {
		return errors.New("aeron: invalid client id from driver ring")
	}
	return nil
}

func validateControlProtocolVersion(reader *cnc.CountersReader) error {
	want := int64(conductor.ControlProtocolVersion)
	got, err := reader.GetCounterValue(conductor.SystemCounterControlProtocolVersion)
	if err != nil {
		return fmt.Errorf("aeron: read control protocol version counter: %w", err)
	}
	if got != want {
		return &ControlProtocolVersionError{Got: got, Want: want}
	}
	return nil
}
