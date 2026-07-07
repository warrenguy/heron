package aeron

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/mmap"
	"github.com/warrenguy/heron/agrona/ringbuffer"
)

const (
	driverHeartbeatPollInterval = 50 * time.Millisecond
	closeDrainMaxIdleTicks      = 3
)

// Client is the Aeron media driver client. The caller goroutine invokes DoWork
// to pump conductor service; there is no background conductor goroutine.
//
// One Client permits at most one blocking command (awaitResponse, awaitAdd,
// OfferAdd*, PollAdd*, Close teardown) at a time. clientLock enforces that
// contract.
//
// DoWork from another goroutine during a blocking await blocks until the await
// completes.
//
// Concurrent Close during blocking Add* or NextAvailableSessionID is
// unsupported. Close sets closed before waiting on clientLock; the blocking
// call returns ErrClientClosed when closed is observed (offer and poll loops).
// If a driver response latches on the same poll tick as Close, the await returns
// ErrClientClosed on a best-effort basis rather than success. Incomplete add
// pendings are cancelled; partial add resources are rolled back. Close teardown
// then proceeds once clientLock is released. Callers should still serialize Close
// with blocking client APIs.
//
// Poll loops under clientLock call doWorkLocked, not DoWork.
//
// Callbacks invoked from doWorkLocked (ErrorHandler, available/unavailable image
// handlers) must not call DoWork, Close, Add*, OfferAdd*, PollAdd*,
// NextAvailableSessionID, or IsTerminating.
//
// Publication Offer from other goroutines remains allowed when the caller does
// not overlap blocking client APIs. Subscription and Image Poll stay on the
// invoker goroutine; see Subscription and Image godoc.
type Client struct {
	clientID                int64
	mapped                  *cnc.MappedCncFile
	countersReader          *cnc.CountersReader
	conductor               *conductor.Conductor
	driverProxy             *conductor.DriverProxy
	nanoClock               conductor.NanoClock
	driverTimeout           time.Duration
	pollTimeout             time.Duration
	publicationOfferTimeout time.Duration
	idleSleepDuration       time.Duration
	serviceTimestampsSeeded bool
	clientLock              sync.Mutex
	closed                  atomic.Bool
}

func (client *Client) lockClient() {
	client.clientLock.Lock()
}

func (client *Client) unlockClient() {
	client.clientLock.Unlock()
}

func Connect(ctx Context) (*Client, error) {
	ctx = normalizeContext(ctx)
	if err := validateConnectContext(ctx); err != nil {
		return nil, err
	}

	mapped, rings, err := openCnc(ctx)
	if err != nil {
		return nil, err
	}

	connected := false
	defer func() {
		if !connected {
			_ = mapped.Close()
		}
	}()

	client, err := newConductorClient(ctx, mapped, rings)
	if err != nil {
		return nil, err
	}
	connected = true
	return client, nil
}

func validateConnectContext(ctx Context) error {
	if ctx.AeronDir == "" {
		return errors.New("aeron: AeronDir is required")
	}
	if ctx.CloseLingerDuration < 0 {
		return errors.New("aeron: CloseLingerDuration must not be negative")
	}
	if !ctx.useConductorInvokerEnabled() {
		return errors.New("aeron: dedicated conductor invoker is not supported")
	}
	if ctx.PollTimeout < 0 {
		return errors.New("aeron: PollTimeout must not be negative")
	}
	if ctx.PublicationOfferTimeout < 0 {
		return errors.New("aeron: PublicationOfferTimeout must not be negative")
	}
	return nil
}

func openCnc(ctx Context) (*cnc.MappedCncFile, *cnc.Rings, error) {
	cncPath := filepath.Join(ctx.AeronDir, cnc.FileName)
	if err := cnc.WaitCncReady(cncPath, ctx.DriverTimeout); err != nil {
		return nil, nil, fmt.Errorf("aeron: wait for cnc ready: %w", err)
	}

	mapped, err := cnc.MapCncFile(cncPath)
	if err != nil {
		return nil, nil, fmt.Errorf("aeron: map cnc file: %w", err)
	}

	opened := false
	defer func() {
		if !opened {
			_ = mapped.Close()
		}
	}()

	rings, err := cnc.WireRings(mapped)
	if err != nil {
		return nil, nil, fmt.Errorf("aeron: wire rings: %w", err)
	}

	if rings.Mapping != nil && rings.Mapping.Data != nil {
		mmap.PreTouch(rings.Mapping.Data, 0)
	}

	opened = true
	return mapped, rings, nil
}

func conductorConnectConfig(ctx Context, clientID int64) conductor.Config {
	config := conductor.Config{
		ClientID:               clientID,
		DriverTimeout:          ctx.DriverTimeout,
		KeepAliveInterval:      ctx.KeepAliveInterval,
		IdleSleepDuration:      ctx.IdleSleepDuration,
		InterServiceTimeout:    ctx.InterServiceTimeout,
		EpochClock:             ctx.EpochClock,
		NanoClock:              ctx.NanoClock,
		SubscriberErrorHandler: ctx.ErrorHandler,
		ImageLifecycle:         ConductorImageLifecycle,
		ResourceLifecycle:      ConductorResourceLifecycle,
		MediaDriverDir:         ctx.AeronDir,
		CloseLingerNs:          ctx.CloseLingerDuration.Nanoseconds(),
	}
	if handler := ctx.ErrorHandler; handler != nil {
		config.ErrorHandler = func(err error) {
			handler(mapConductorError(err))
		}
	}
	return config
}

func newConductorClient(
	ctx Context,
	mapped *cnc.MappedCncFile,
	rings *cnc.Rings,
) (*Client, error) {
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
	config := conductorConnectConfig(ctx, clientID)

	conductorInstance, err := conductor.NewConductor(
		driverProxy,
		rings.ToClients,
		countersReader,
		config,
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		clientID:                clientID,
		mapped:                  mapped,
		countersReader:          countersReader,
		conductor:               conductorInstance,
		driverProxy:             driverProxy,
		nanoClock:               ctx.NanoClock,
		driverTimeout:           ctx.DriverTimeout,
		pollTimeout:             ctx.PollTimeout,
		publicationOfferTimeout: ctx.PublicationOfferTimeout,
		idleSleepDuration:       ctx.IdleSleepDuration,
	}, nil
}

func (client *Client) doWorkLocked() int {
	if client.conductor == nil {
		return 0
	}
	if !client.serviceTimestampsSeeded {
		client.conductor.SeedServiceTimestamps(client.nanoClock())
		client.serviceTimestampsSeeded = true
	}
	return client.conductor.DoWork()
}

// DoWork runs one conductor service tick under clientLock. Returns work done
// count; zero when closed or the tick made no progress.
//
// See Client godoc for invoker and concurrency contract.
func (client *Client) DoWork() int {
	if client == nil || client.closed.Load() {
		return 0
	}
	client.lockClient()
	defer client.unlockClient()
	if client.closed.Load() {
		return 0
	}
	workDone := client.doWorkLocked()
	invokeDoWorkAfterHook(client)
	return workDone
}

// IsTerminating reports whether the conductor has entered termination (driver
// shutdown, keepalive stale, client or service timeout, etc.).
//
// Takes clientLock for a consistent read alongside Close and blocking awaits.
// Must not be called from ErrorHandler or available/unavailable image handlers
// invoked during doWorkLocked; see Client godoc.
func (client *Client) IsTerminating() bool {
	if client == nil || client.closed.Load() {
		return false
	}
	client.lockClient()
	defer client.unlockClient()
	if client.closed.Load() || client.conductor == nil {
		return false
	}
	return client.conductor.IsTerminating()
}

// Close tears down resources, drains close responses and log linger, sends
// CLIENT_CLOSE, and unmaps the CNC file.
//
// Idempotent. Close must not be invoked concurrently from multiple goroutines;
// concurrent Close is not part of the supported API contract. If it occurs anyway,
// only the first Close to win the closed CAS runs teardown under clientLock;
// other Close calls return nil immediately without waiting for teardown to
// finish, so nil does not imply the CNC file is unmapped.
//
// Linger drain is best-effort when NanoClock does not advance.
//
// Returns the first error from unmapping CNC; teardown errors go to
// ErrorHandler.
func (client *Client) Close() error {
	if client == nil {
		return nil
	}
	if !client.closed.CompareAndSwap(false, true) {
		return nil
	}
	client.lockClient()
	defer client.unlockClient()
	return client.closeTeardownLocked()
}

func (client *Client) closeTeardownLocked() error {
	if client.conductor != nil {
		client.conductor.ClearPendingEntriesForClose()
		client.conductor.CloseAllResources()
		client.drainCloseResponses()
		client.awaitCloseLingerDrain()
	}

	if client.driverProxy != nil {
		client.driverProxy.ClientClose()
	}

	var err error
	if client.mapped != nil {
		err = client.mapped.Close()
		client.mapped = nil
	}
	client.conductor = nil
	client.countersReader = nil
	client.driverProxy = nil
	return err
}

func (client *Client) drainCloseResponses() {
	// closeDrainMaxIdleTicks idle DoWork ticks exit early so unit tests with
	// synthetic drivers finish without waiting the full DriverTimeout cap.
	deadline := time.Now().Add(client.driverTimeout)
	idleTicks := 0
	for time.Now().Before(deadline) {
		if client.doWorkLocked() != 0 {
			idleTicks = 0
		} else if idleTicks++; idleTicks >= closeDrainMaxIdleTicks {
			return
		}
		time.Sleep(client.idleSleepDuration)
	}
}

func (client *Client) awaitCloseLingerDrain() {
	nanoClock := client.nanoClock
	if nanoClock == nil || !client.conductor.HasScheduledLinger() {
		return
	}

	lingerNs := client.conductor.CloseLingerNs()
	lingerDeadlineNs := nanoClock() + lingerNs
	wallDeadline := time.Now().Add(time.Duration(lingerNs))
	for nanoClock() < lingerDeadlineNs && time.Now().Before(wallDeadline) {
		client.conductor.DrainLingeringLogBuffers()
		client.doWorkLocked()
		if !client.conductor.HasScheduledLinger() {
			return
		}
		time.Sleep(client.idleSleepDuration)
	}
	client.conductor.DrainLingeringLogBuffers()
}

// CancelPending abandons an in-flight OfferAdd registration.
//
// Idempotent when the pending entry is absent. Not safe for concurrent use with
// OfferAdd* on the same correlation id.
func (client *Client) CancelPending(correlationID int64) {
	if client == nil || correlationID == conductor.NullCorrelationID {
		return
	}
	client.lockClient()
	defer client.unlockClient()
	if client.conductor != nil {
		client.conductor.CancelPending(correlationID)
	}
}

// NextCorrelationID allocates the next correlation id from the driver ring.
// Concurrent Close during allocation is unsupported; closed and nil proxy checks
// are best-effort without clientLock.
func (client *Client) NextCorrelationID() int64 {
	if client == nil || client.closed.Load() || client.driverProxy == nil {
		return 0
	}
	return client.driverProxy.NextCorrelationID()
}

// NextAvailableSessionID blocks until the driver returns the next session id or
// DriverTimeout elapses.
//
// Correlation id is allocated before clientLock is acquired. Blocking await
// serializes with other blocking client APIs.
//
// DoWork from another goroutine blocks until this call completes.
//
// Concurrent Close during this call is unsupported; see Client godoc.
func (client *Client) NextAvailableSessionID(streamID int32) (int32, error) {
	if err := client.requireReadyWithDriverProxy(); err != nil {
		return 0, err
	}
	correlationID := client.driverProxy.NextCorrelationID()
	registration, err := client.awaitResponse(
		correlationID,
		conductor.PendingKindNextSessionID,
		client.driverTimeout,
		func() error {
			return client.driverProxy.NextAvailableSessionID(correlationID, streamID)
		},
	)
	if err != nil {
		return 0, err
	}
	return registration.Int32Value, nil
}

func (client *Client) errIfClosed() error {
	if client == nil || client.closed.Load() {
		return ErrClientClosed
	}
	return nil
}

func (client *Client) requireReadyWithConductor() error {
	if client == nil {
		return ErrClientNotReady
	}
	if err := client.errIfClosed(); err != nil {
		return err
	}
	if client.conductor == nil || client.nanoClock == nil {
		return ErrClientNotReady
	}
	return nil
}

func (client *Client) requireReadyWithDriverProxy() error {
	if err := client.requireReadyWithConductor(); err != nil {
		return err
	}
	if client.driverProxy == nil {
		return ErrClientNotReady
	}
	return nil
}

// mapConductorTerminationError maps driver shutdown and keepalive-stale causes to
// DriverTimeoutError.
//
// Client and service timeouts pass through for errors.Is.
func mapConductorTerminationError(conductorInstance *conductor.Conductor) error {
	cause := conductorInstance.TerminationCause()
	if cause == nil {
		cause = conductor.ErrDriverShutdown
	}
	return mapConductorError(cause)
}

// awaitResponse blocks until the pending registration completes or times out.
// Poll ticks call doWorkLocked, not DoWork. See Client godoc for concurrency.
func (client *Client) awaitResponse(
	correlationID int64,
	kind conductor.PendingKind,
	timeout time.Duration,
	offer func() error,
) (conductor.PendingRegistration, error) {
	if client == nil {
		return conductor.PendingRegistration{}, ErrClientNotReady
	}
	client.lockClient()
	defer client.unlockClient()

	if err := client.requireReadyWithConductor(); err != nil {
		return conductor.PendingRegistration{}, err
	}
	conductorInstance := client.conductor
	defer conductorInstance.SetActiveCorrelationID(conductor.NullCorrelationID)
	startNs := client.nanoClock()
	deadlineNs := startNs + timeout.Nanoseconds()
	if err := conductorInstance.RegisterPendingWithDeadline(
		kind,
		correlationID,
		deadlineNs,
	); err != nil {
		return conductor.PendingRegistration{}, mapRegistrationError(err)
	}
	pendingCompleted := false
	defer func() {
		if !pendingCompleted {
			conductorInstance.CancelPending(correlationID)
		}
	}()

	conductorInstance.SetActiveCorrelationID(correlationID)
	if offer != nil {
		if err := offer(); err != nil {
			return conductor.PendingRegistration{}, err
		}
	}
	registration, completed, err := client.awaitPendingPoll(
		conductorInstance,
		correlationID,
		deadlineNs,
		func(pollErr error) error {
			return mapPollPendingError(correlationID, timeout, pollErr)
		},
	)
	if completed {
		pendingCompleted = true
		if err != nil {
			return conductor.PendingRegistration{}, err
		}
		return registration, nil
	}
	if err != nil {
		return conductor.PendingRegistration{}, err
	}
	return conductor.PendingRegistration{}, newAwaitTimeoutError(correlationID, timeout)
}

func (client *Client) awaitPendingPollTick(
	conductorInstance *conductor.Conductor,
	correlationID int64,
	mapPollErr func(error) error,
) (conductor.PendingRegistration, bool, error) {
	if err := client.requireReadyWithConductor(); err != nil {
		return conductor.PendingRegistration{}, false, err
	}
	if conductorInstance.IsTerminating() {
		return conductor.PendingRegistration{}, false, mapConductorTerminationError(conductorInstance)
	}
	client.doWorkLocked()
	done, registration, err := conductorInstance.PollPending(correlationID)
	if !done {
		return conductor.PendingRegistration{}, false, nil
	}
	return client.completeAwaitPendingPollTick(
		conductorInstance,
		registration,
		mapPollErr,
		err,
	)
}

func (client *Client) completeAwaitPendingPollTick(
	conductorInstance *conductor.Conductor,
	registration conductor.PendingRegistration,
	mapPollErr func(error) error,
	pollErr error,
) (conductor.PendingRegistration, bool, error) {
	if pollErr != nil {
		return conductor.PendingRegistration{}, true, mapPollErr(pollErr)
	}
	// Close CAS may publish closed after the iteration-start check; best-effort
	// ErrClientClosed rather than returning success on the same tick.
	if err := client.errIfClosed(); err != nil {
		conductorInstance.RollbackAddPendingResources(registration)
		return registration, true, err
	}
	return registration, true, nil
}

func (client *Client) awaitPendingPoll(
	conductorInstance *conductor.Conductor,
	correlationID int64,
	deadlineNs int64,
	mapPollErr func(error) error,
) (conductor.PendingRegistration, bool, error) {
	for client.nanoClock() < deadlineNs {
		registration, completed, err := client.awaitPendingPollTick(
			conductorInstance,
			correlationID,
			mapPollErr,
		)
		if err != nil {
			return registration, completed, err
		}
		if completed {
			return registration, true, nil
		}
		invokeAwaitPollSleepingHook(client)
		time.Sleep(client.idleSleepDuration)
	}
	return conductor.PendingRegistration{}, false, nil
}

//nolint:staticcheck // Aeron Java API uses ClientId.
func (client *Client) ClientId() int64 {
	if client == nil {
		return 0
	}
	return client.clientID
}

func (client *Client) CncMetadata() (cnc.Metadata, error) {
	mapped, err := client.mappedCncFile()
	if err != nil {
		return cnc.Metadata{}, err
	}
	return mapped.Metadata(), nil
}

func (client *Client) CountersReader() (*cnc.CountersReader, error) {
	if err := client.errIfClosed(); err != nil {
		return nil, err
	}
	if client.countersReader == nil {
		return nil, errors.New("aeron: client has no counters reader")
	}
	return client.countersReader, nil
}

func (client *Client) ErrorLogBuffer() ([]byte, error) {
	mapped, err := client.mappedCncFile()
	if err != nil {
		return nil, err
	}
	return mapped.ErrorLogBuffer(), nil
}

func (client *Client) mappedCncFile() (*cnc.MappedCncFile, error) {
	if err := client.errIfClosed(); err != nil {
		return nil, err
	}
	if client.mapped == nil {
		return nil, errors.New("aeron: client has no mapped cnc file")
	}
	return client.mapped, nil
}

func driverShutdownHeartbeatError(heartbeatMs int64) error {
	if heartbeatMs == conductor.DriverShutdownHeartbeat {
		return &DriverTimeoutError{Cause: conductor.ErrDriverShutdown}
	}
	return nil
}

// Epoch deadline is the connect timeout; wall clock and driverHeartbeatPollInterval
// backstop frozen-epoch tests and shutdown observed after the poll sleep only.
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
		heartbeatMs := toDriver.ConsumerHeartbeatTime()
		if err := driverShutdownHeartbeatError(heartbeatMs); err != nil {
			return err
		}

		nowMs := epochClock()
		if heartbeatMs > 0 {
			sawPositiveHeartbeat = true
			ageMs := max(nowMs-heartbeatMs, 0)
			if ageMs <= driverTimeoutMs {
				return nil
			}
			break
		}

		if nowMs >= deadlineMs || !time.Now().Before(wallDeadline) {
			break
		}
		time.Sleep(driverHeartbeatPollInterval)
	}

	if err := driverShutdownHeartbeatError(toDriver.ConsumerHeartbeatTime()); err != nil {
		return err
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
	got, err := reader.CounterValue(conductor.SystemCounterControlProtocolVersion)
	if err != nil {
		return fmt.Errorf("aeron: read control protocol version counter: %w", err)
	}
	if got != want {
		return &ControlProtocolVersionError{Got: got, Want: want}
	}
	return nil
}
