package aeron

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const (
	clientAddPublicationRegistrationID int64 = 2002
	clientAddStreamID                  int32 = 7
	clientAddChannel                         = "aeron:ipc"
)

func clientAddPublicationReadyEvent(
	correlationID int64,
	logPath string,
) conductor.PublicationBuffersReady {
	return conductor.PublicationBuffersReady{
		CorrelationID:             correlationID,
		RegistrationID:            clientAddPublicationRegistrationID,
		SessionID:                 42,
		StreamID:                  clientAddStreamID,
		PublicationLimitCounterID: 0,
		ChannelStatusCounterID:    1,
		LogFileName:               logPath,
	}
}

func clientAddSubscriptionReadyEvent(correlationID int64) conductor.SubscriptionReady {
	return conductor.SubscriptionReady{
		CorrelationID:          correlationID,
		ChannelStatusCounterID: 12,
	}
}

func newClientAddFixture(
	tb testutil.TB,
	nanoClock func() int64,
	driverTimeout time.Duration,
	idleSleepDuration time.Duration,
	mediaDriverDir string,
) (*Client, *testutil.ConductorFixture) {
	tb.Helper()

	config := testutil.DefaultConductorConfig(nil)
	config.NanoClock = nanoClock
	config.DriverTimeout = driverTimeout
	config.IdleSleepDuration = idleSleepDuration
	config.ImageLifecycle = ConductorImageLifecycle
	config.ResourceLifecycle = ConductorResourceLifecycle
	config.MediaDriverDir = mediaDriverDir
	fixture := testutil.NewConductorFixture(tb, config)
	client := NewClientForTestWithDriverProxy(
		fixture.Conductor,
		fixture.DriverProxy,
		fixture.NanoClock,
		config.ClientID,
		config.DriverTimeout,
		config.IdleSleepDuration,
	)
	return client, fixture
}

func countersReaderForClientAddTest(tb testutil.TB) *cnc.CountersReader {
	tb.Helper()
	return newTestCountersReaderFromRecords(3, func(meta, values []byte) {
		cnc.WriteCounterRecord(
			meta,
			values,
			0,
			cnc.CounterRecordAllocated,
			cnc.DriverPublisherLimitTypeID,
			clientAddPublicationRegistrationID,
			0,
		)
		err := cnc.WriteChannelStatusCounterRecord(
			meta,
			values,
			1,
			cnc.CounterRecordAllocated,
			cnc.DriverSendChannelStatusTypeID,
			clientAddChannel,
			clientAddPublicationRegistrationID,
			1,
		)
		if err != nil {
			panic(err)
		}
	})
}

func setupClientAddPublicationLog(tb testing.TB) (dir, logPath string) {
	tb.Helper()
	dir = tb.TempDir()
	logPath = filepath.Join(dir, "publication.log")
	WriteTestLogFileForTest(tb, logPath)
	return dir, logPath
}

func decodeAddPublicationCorrelationID(
	buffer aeronatomic.AtomicBuffer,
	index int,
	length int,
) (int64, error) {
	decoded, err := conductor.DecodePublicationMessage(buffer, index, length)
	if err != nil {
		return 0, err
	}
	return decoded.CorrelationID, nil
}

func decodeAddSubscriptionCorrelationID(
	buffer aeronatomic.AtomicBuffer,
	index int,
	length int,
) (int64, error) {
	decoded, err := conductor.DecodeSubscriptionMessage(buffer, index, length)
	if err != nil {
		return 0, err
	}
	return decoded.CorrelationID, nil
}

func decodeCorrelatedCorrelationID(
	buffer aeronatomic.AtomicBuffer,
	index int,
	_ int,
) (int64, error) {
	decoded, err := conductor.DecodeCorrelated(buffer, index)
	if err != nil {
		return 0, err
	}
	return decoded.CorrelationID, nil
}

const clientConcurrentResultTimeout = 2 * time.Second

func requireErrClientClosed(t *testing.T, err error) {
	t.Helper()

	require.ErrorIs(t, err, ErrClientClosed)
	require.EqualError(t, err, ErrClientClosed.Error())
}

func receiveAddResult[T any](
	t *testing.T,
	resultCh <-chan T,
	timeout time.Duration,
	timeoutMsg string,
) T {
	t.Helper()

	select {
	case got := <-resultCh:
		return got
	case <-time.After(timeout):
		require.FailNow(t, timeoutMsg)
		var zero T
		return zero
	}
}

func requireCloseBlockedDuringAwait(
	t *testing.T,
	client *Client,
	closeDone <-chan error,
) {
	t.Helper()

	require.Eventually(
		t,
		func() bool {
			if !errors.Is(client.errIfClosed(), ErrClientClosed) {
				return false
			}
			select {
			case <-closeDone:
				return false
			default:
				return true
			}
		},
		clientConcurrentResultTimeout,
		time.Microsecond,
		"closed state not observable before await released lock",
	)
	select {
	case err := <-closeDone:
		t.Fatal("Close returned before await released lock:", err)
	default:
	}
}

func requireAddCommandNotOnRing(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	msgTypeID int,
) {
	t.Helper()

	require.NotContains(
		t,
		testutil.CommittedRecordTypeIDs(fixture.Ring),
		msgTypeID,
		"add command reached ring before offer-phase synchronization",
	)
}

func requireAddPhaseTimeout(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	client *Client,
	err error,
	registrationKind string,
	countFn func(*Client) int,
	checkStreamChannel bool,
) {
	t.Helper()

	var timeoutErr *RegistrationTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.Equal(t, registrationKind, timeoutErr.Kind)
	correlationID := fixture.DriverProxy.NextCorrelationID() - 1
	require.Equal(t, correlationID, timeoutErr.CorrelationID)
	if checkStreamChannel {
		require.Equal(t, clientAddStreamID, timeoutErr.StreamID)
		require.Equal(t, clientAddChannel, timeoutErr.Channel)
	}
	require.EqualError(
		t,
		err,
		fmt.Sprintf(
			"aeron: %s registration timeout: correlation=%d stream=7 channel=aeron:ipc",
			registrationKind,
			correlationID,
		),
	)
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	require.Equal(t, 0, countFn(client))
	requireActiveCorrelationCleared(t, fixture)
}

func requireAddRingExhaustionRegistrationError(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	client *Client,
	err error,
	registrationKind string,
	countFn func(*Client) int,
) {
	t.Helper()

	var registrationErr *RegistrationError
	require.ErrorAs(t, err, &registrationErr)
	require.Equal(t, registrationKind, registrationErr.Kind)
	require.ErrorIs(t, err, conductor.ErrRingInsufficientCapacity)
	require.EqualError(
		t,
		err,
		fmt.Sprintf(
			"aeron: %s registration error: correlation=0: conductor: ring insufficient capacity",
			registrationKind,
		),
	)
	correlationID := fixture.DriverProxy.NextCorrelationID() - 1
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	requireActiveCorrelationCleared(t, fixture)
	require.Equal(t, 0, countFn(client))
}

func requireDriverShutdownTimeout(t *testing.T, err error) {
	t.Helper()

	var timeoutErr *DriverTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.ErrorIs(t, timeoutErr.Unwrap(), conductor.ErrDriverShutdown)
	require.EqualError(
		t,
		err,
		"aeron: media driver timeout: conductor: media driver has been shutdown",
	)
}

func requireClientTimeoutFromDriver(t *testing.T, err error) {
	t.Helper()

	require.ErrorIs(t, err, conductor.ErrClientTimeout)
	require.EqualError(t, err, "conductor: client timeout from driver")
}

func setupClientAddConductorTerminating(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	setNanoNs func(int64),
) {
	t.Helper()

	fixture.Conductor.DoWork()
	setNanoNs(testutil.TestWorkNanoNs)
	fixture.Ring.SetConsumerHeartbeatTime(conductor.NullCorrelationID)
	require.Equal(t, 1, fixture.Conductor.DoWork())
	require.True(t, fixture.Conductor.IsTerminating())
}

type clientAddTestClock struct {
	startNs            int64
	timeout            time.Duration
	pastDeadline       atomic.Bool
	reads              atomic.Int64
	deadlineAfterReads int64
	nanoNs             atomic.Int64
}

func newClientAddTestClock(startNs int64, timeout time.Duration) *clientAddTestClock {
	clock := &clientAddTestClock{
		startNs: startNs,
		timeout: timeout,
	}
	clock.nanoNs.Store(startNs)
	return clock
}

func (clock *clientAddTestClock) WithDeadlineAfterReads(minReads int64) *clientAddTestClock {
	clock.deadlineAfterReads = minReads
	return clock
}

func (clock *clientAddTestClock) NanoClock() func() int64 {
	deadlineNs := clock.startNs + clock.timeout.Nanoseconds() + 1
	return func() int64 {
		reads := clock.reads.Add(1)
		if clock.deadlineAfterReads > 0 && reads >= clock.deadlineAfterReads {
			clock.EnterDeadlinePhase()
		}
		if clock.pastDeadline.Load() {
			return deadlineNs
		}
		return clock.nanoNs.Load()
	}
}

func (clock *clientAddTestClock) ClockReads() int64 {
	return clock.reads.Load()
}

func (clock *clientAddTestClock) EnterDeadlinePhase() {
	clock.pastDeadline.Store(true)
}

func (clock *clientAddTestClock) SetNanoNs(ns int64) {
	clock.nanoNs.Store(ns)
}

func waitForClientAddClockReads(clock *clientAddTestClock, minReads int64) {
	for clock.ClockReads() < minReads {
		runtime.Gosched()
	}
}

func seedFullClientAddRing(tb testutil.TB, ring *ringbuffer.ManyToOneRingBuffer) {
	tb.Helper()

	buffer := ring.Buffer()
	capacity := testutil.TestRingDataCapacity
	head := int64(0)
	tail := int64(capacity)
	buffer.PutInt64Release(ringbuffer.HeadIndex(capacity), head)
	buffer.PutInt64Release(ringbuffer.TailIndex(capacity), tail)
	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(capacity), head)
}

func resetClientAddRingEmpty(ring *ringbuffer.ManyToOneRingBuffer) {
	buffer := ring.Buffer()
	capacity := testutil.TestRingDataCapacity
	tail := buffer.GetInt64Volatile(ringbuffer.TailIndex(capacity))
	buffer.PutInt64Release(ringbuffer.HeadIndex(capacity), tail)
	buffer.PutInt64Release(ringbuffer.HeadCacheIndex(capacity), tail)
}

func pollAndRespondToAddCommand(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	msgTypeID int,
	decode func(aeronatomic.AtomicBuffer, int, int) (int64, error),
	deadline time.Time,
	buildResponse func(correlationID int64) (eventType int, payload []byte),
	onCommandSeen func(correlationID int64),
) (bool, int64) {
	t.Helper()

	for time.Now().Before(deadline) {
		var responded bool
		var correlationID int64
		fixture.Ring.Read(func(ringMsgTypeID int, buffer aeronatomic.AtomicBuffer, index, length int) {
			if ringMsgTypeID != msgTypeID {
				return
			}
			decoded, err := decode(buffer, index, length)
			if err != nil {
				return
			}
			correlationID = decoded
			if onCommandSeen != nil {
				onCommandSeen(correlationID)
			}
			eventType, payload := buildResponse(correlationID)
			if transmitErr := fixture.Transmit(eventType, payload); transmitErr == nil {
				responded = true
			}
		})
		if responded {
			return true, correlationID
		}
		time.Sleep(time.Millisecond)
	}
	return false, 0
}

func pollAndRespondToAddPublicationCommand(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	logPath string,
	deadline time.Time,
	onCommandSeen func(correlationID int64),
) (bool, int64) {
	t.Helper()

	return pollAndRespondToAddCommand(
		t,
		fixture,
		conductor.AddPublication,
		decodeAddPublicationCorrelationID,
		deadline,
		func(correlationID int64) (int, []byte) {
			return conductor.OnPublicationReady, conductor.PublicationBuffersReadyBytes(
				clientAddPublicationReadyEvent(correlationID, logPath),
			)
		},
		onCommandSeen,
	)
}

func pollAndRespondToAddSubscriptionCommand(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	deadline time.Time,
	onCommandSeen func(correlationID int64),
) (bool, int64) {
	t.Helper()

	return pollAndRespondToAddCommand(
		t,
		fixture,
		conductor.AddSubscription,
		decodeAddSubscriptionCorrelationID,
		deadline,
		func(correlationID int64) (int, []byte) {
			return conductor.OnSubscriptionReady, conductor.SubscriptionReadyBytes(
				clientAddSubscriptionReadyEvent(correlationID),
			)
		},
		onCommandSeen,
	)
}

func pollAndRespondToAddCommandError(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	msgTypeID int,
	decode func(aeronatomic.AtomicBuffer, int, int) (int64, error),
	deadline time.Time,
	errorCode int32,
	message string,
) (bool, int64) {
	t.Helper()

	return pollAndRespondToAddCommand(
		t,
		fixture,
		msgTypeID,
		decode,
		deadline,
		func(correlationID int64) (int, []byte) {
			return conductor.OnError, conductor.ErrorResponseBytes(
				correlationID,
				errorCode,
				message,
			)
		},
		nil,
	)
}

func waitForAddCommand(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	msgTypeID int,
	decode func(aeronatomic.AtomicBuffer, int, int) (int64, error),
	commandName string,
) int64 {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var correlationID int64
		var sawCommand bool
		fixture.Ring.Read(func(ringMsgTypeID int, buffer aeronatomic.AtomicBuffer, index, length int) {
			if ringMsgTypeID != msgTypeID {
				return
			}
			decoded, err := decode(buffer, index, length)
			if err != nil {
				return
			}
			sawCommand = true
			correlationID = decoded
		})
		if sawCommand {
			return correlationID
		}
		time.Sleep(time.Millisecond)
	}
	require.FailNow(t, "timed out waiting for "+commandName+" command on ring")
	return 0
}

func waitForAddPublicationCommand(t *testing.T, fixture *testutil.ConductorFixture) int64 {
	t.Helper()

	return waitForAddCommand(
		t,
		fixture,
		conductor.AddPublication,
		decodeAddPublicationCorrelationID,
		"AddPublication",
	)
}

func waitForAddSubscriptionCommand(t *testing.T, fixture *testutil.ConductorFixture) int64 {
	t.Helper()

	return waitForAddCommand(
		t,
		fixture,
		conductor.AddSubscription,
		decodeAddSubscriptionCorrelationID,
		"AddSubscription",
	)
}

func TestClient_AddPublication_insertsMapOnSyntheticReady(t *testing.T) {
	var nanoNs int64
	dir, logPath := setupClientAddPublicationLog(t)
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))

	type result struct {
		pub *Publication
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
		resultCh <- result{pub: pub, err: err}
	}()

	pollDeadline := time.Now().Add(time.Second)
	responded, correlationID := pollAndRespondToAddPublicationCommand(
		t,
		fixture,
		logPath,
		pollDeadline,
		nil,
	)
	require.True(t, responded, "timed out waiting for AddPublication command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		2*time.Second,
		"timed out waiting for AddPublication client result",
	)
	require.NoError(t, got.err)
	require.NotNil(t, got.pub)
	require.Equal(t, correlationID, fixture.DriverProxy.NextCorrelationID()-1)
	require.Equal(t, clientAddPublicationRegistrationID, got.pub.RegistrationID())
	lookup, ok := client.Publication(clientAddPublicationRegistrationID)
	require.True(t, ok)
	require.Same(t, got.pub, lookup)
	require.Equal(t, 1, client.PublicationCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_AddPublication_driverRegistrationError_failsAwait(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)

	type result struct {
		pub *Publication
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
		resultCh <- result{pub: pub, err: err}
	}()

	pollDeadline := time.Now().Add(time.Second)
	responded, correlationID := pollAndRespondToAddCommandError(
		t,
		fixture,
		conductor.AddPublication,
		decodeAddPublicationCorrelationID,
		pollDeadline,
		11,
		"add publication rejected",
	)
	require.True(t, responded, "timed out waiting for AddPublication command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		2*time.Second,
		"timed out waiting for AddPublication client result",
	)
	require.Nil(t, got.pub)
	var registrationErr *RegistrationError
	require.ErrorAs(t, got.err, &registrationErr)
	require.Equal(t, registrationKindPublication, registrationErr.Kind)
	require.Equal(t, correlationID, registrationErr.CorrelationID)
	require.Equal(t, int32(11), registrationErr.ErrorCode)
	require.Equal(t, "add publication rejected", registrationErr.Message)
	var driverErr *conductor.DriverRegistrationError
	require.ErrorAs(t, registrationErr.Cause, &driverErr)
	require.EqualError(
		t,
		got.err,
		fmt.Sprintf(
			"aeron: driver registration error: correlation=%d code=11 message=add publication rejected",
			correlationID,
		),
	)
	require.Equal(t, 0, client.PublicationCountForTest())
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_AddPublication_offerPhaseTimeout(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		0,
		time.Microsecond,
		dir,
	)

	pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
	require.Nil(t, pub)
	requireAddPhaseTimeout(
		t,
		fixture,
		client,
		err,
		registrationKindPublication,
		(*Client).PublicationCountForTest,
		true,
	)
	requireClientAddRingNoCommittedCommands(t, fixture.Ring)
}

func TestClient_AddPublication_pollPhaseTimeout(t *testing.T) {
	const timeout = 5 * time.Millisecond
	startNs := testutil.TestStartNanoNs
	clock := newClientAddTestClock(startNs, timeout)
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		clock.NanoClock(),
		timeout,
		time.Microsecond,
		dir,
	)

	type result struct {
		pub *Publication
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
		resultCh <- result{pub: pub, err: err}
	}()

	waitForAddPublicationCommand(t, fixture)
	clock.EnterDeadlinePhase()

	got := receiveAddResult(
		t,
		resultCh,
		2*time.Second,
		"timed out waiting for AddPublication client result",
	)
	require.Nil(t, got.pub)
	requireAddPhaseTimeout(
		t,
		fixture,
		client,
		got.err,
		registrationKindPublication,
		(*Client).PublicationCountForTest,
		false,
	)
}

func TestClient_Add_ringExhaustion_returnsRegistrationError(t *testing.T) {
	const timeout = time.Millisecond
	startNs := testutil.TestStartNanoNs

	tests := []struct {
		name             string
		dir              func(t *testing.T) string
		addCall          func(*Client) (any, error)
		registrationKind string
		countFn          func(*Client) int
	}{
		{
			name: "publication",
			dir: func(t *testing.T) string {
				dir := t.TempDir()
				return dir
			},
			addCall: func(client *Client) (any, error) {
				return client.AddPublication(clientAddStreamID, clientAddChannel)
			},
			registrationKind: registrationKindPublication,
			countFn:          (*Client).PublicationCountForTest,
		},
		{
			name: "subscription",
			dir: func(t *testing.T) string {
				return t.TempDir()
			},
			addCall: func(client *Client) (any, error) {
				return client.AddSubscription(clientAddStreamID, clientAddChannel)
			},
			registrationKind: registrationKindSubscription,
			countFn:          (*Client).SubscriptionCountForTest,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			clock := newClientAddTestClock(startNs, timeout).WithDeadlineAfterReads(4)
			client, fixture := newClientAddFixture(
				t,
				clock.NanoClock(),
				timeout,
				time.Microsecond,
				testCase.dir(t),
			)
			seedFullClientAddRing(t, fixture.Ring)

			got, err := testCase.addCall(client)
			require.Nil(t, got)
			requireAddRingExhaustionRegistrationError(
				t,
				fixture,
				client,
				err,
				testCase.registrationKind,
				testCase.countFn,
			)
		})
	}
}

func TestClient_AddPublication_ringBackpressureRetryThenSuccess(t *testing.T) {
	clock := newClientAddTestClock(0, time.Second)
	dir, logPath := setupClientAddPublicationLog(t)
	client, fixture := newClientAddFixture(
		t,
		clock.NanoClock(),
		time.Second,
		time.Millisecond,
		dir,
	)
	fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))
	seedFullClientAddRing(t, fixture.Ring)

	type result struct {
		pub *Publication
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
		resultCh <- result{pub: pub, err: err}
	}()

	waitForClientAddClockReads(clock, 4)
	resetClientAddRingEmpty(fixture.Ring)

	pollDeadline := time.Now().Add(time.Second)
	responded, _ := pollAndRespondToAddPublicationCommand(
		t,
		fixture,
		logPath,
		pollDeadline,
		nil,
	)
	require.True(t, responded, "timed out waiting for AddPublication command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		2*time.Second,
		"timed out waiting for AddPublication client result",
	)
	require.NoError(t, got.err)
	require.NotNil(t, got.pub)
	require.Equal(t, 1, client.PublicationCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_AddSubscription_ringBackpressureRetryThenSuccess(t *testing.T) {
	clock := newClientAddTestClock(0, time.Second)
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		clock.NanoClock(),
		time.Second,
		time.Millisecond,
		dir,
	)
	seedFullClientAddRing(t, fixture.Ring)

	type result struct {
		sub *Subscription
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
		resultCh <- result{sub: sub, err: err}
	}()

	waitForClientAddClockReads(clock, 4)
	resetClientAddRingEmpty(fixture.Ring)

	pollDeadline := time.Now().Add(time.Second)
	responded, _ := pollAndRespondToAddSubscriptionCommand(t, fixture, pollDeadline, nil)
	require.True(t, responded, "timed out waiting for AddSubscription command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		2*time.Second,
		"timed out waiting for AddSubscription client result",
	)
	require.NoError(t, got.err)
	require.NotNil(t, got.sub)
	require.Equal(t, 1, client.SubscriptionCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_AddPublication_terminationDuringOfferRetry(t *testing.T) {
	clock := newClientAddTestClock(0, time.Second)
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		clock.NanoClock(),
		time.Second,
		time.Microsecond,
		dir,
	)
	seedFullClientAddRing(t, fixture.Ring)

	type result struct {
		pub *Publication
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
		resultCh <- result{pub: pub, err: err}
	}()

	waitForClientAddClockReads(clock, 4)
	requireAddCommandNotOnRing(t, fixture, conductor.AddPublication)

	correlationID := fixture.Conductor.ActiveCorrelationID()
	require.NotEqual(t, conductor.NullCorrelationID, correlationID)
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.True(t, ok, "expected pending registration during offer-phase backpressure")

	fixture.Ring.SetConsumerHeartbeatTime(conductor.NullCorrelationID)
	clock.SetNanoNs(testutil.TestWorkNanoNs)

	got := receiveAddResult(
		t,
		resultCh,
		2*time.Second,
		"timed out waiting for AddPublication client result",
	)
	require.Nil(t, got.pub)
	requireDriverShutdownTimeout(t, got.err)
	_, ok = fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	require.Equal(t, 0, client.PublicationCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_AddSubscription_terminationDuringOfferRetry(t *testing.T) {
	clock := newClientAddTestClock(0, time.Second)
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		clock.NanoClock(),
		time.Second,
		time.Microsecond,
		dir,
	)
	seedFullClientAddRing(t, fixture.Ring)

	type result struct {
		sub *Subscription
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
		resultCh <- result{sub: sub, err: err}
	}()

	waitForClientAddClockReads(clock, 4)
	requireAddCommandNotOnRing(t, fixture, conductor.AddSubscription)

	correlationID := fixture.Conductor.ActiveCorrelationID()
	require.NotEqual(t, conductor.NullCorrelationID, correlationID)
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.True(t, ok, "expected pending registration during offer-phase backpressure")

	fixture.Ring.SetConsumerHeartbeatTime(conductor.NullCorrelationID)
	clock.SetNanoNs(testutil.TestWorkNanoNs)

	got := receiveAddResult(
		t,
		resultCh,
		2*time.Second,
		"timed out waiting for AddSubscription client result",
	)
	require.Nil(t, got.sub)
	requireDriverShutdownTimeout(t, got.err)
	_, ok = fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	require.Equal(t, 0, client.SubscriptionCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_Add_terminationCause(t *testing.T) {
	tests := []struct {
		name    string
		dir     func(t *testing.T) string
		addCall func(*Client) (any, error)
	}{
		{
			name: "publication",
			dir: func(t *testing.T) string {
				dir := t.TempDir()
				return dir
			},
			addCall: func(client *Client) (any, error) {
				return client.AddPublication(clientAddStreamID, clientAddChannel)
			},
		},
		{
			name: "subscription",
			dir: func(t *testing.T) string {
				return t.TempDir()
			},
			addCall: func(client *Client) (any, error) {
				return client.AddSubscription(clientAddStreamID, clientAddChannel)
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				testCase.dir(t),
			)

			client.DoWork()
			nanoNs = testutil.TestWorkNanoNs
			fixture.Ring.SetConsumerHeartbeatTime(conductor.NullCorrelationID)

			got, err := testCase.addCall(client)
			require.Nil(t, got)
			requireDriverShutdownTimeout(t, err)
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_Add_clientTimeoutTerminationCause(t *testing.T) {
	tests := []struct {
		name    string
		dir     func(t *testing.T) string
		addCall func(*Client) (any, error)
	}{
		{
			name: "publication",
			dir: func(t *testing.T) string {
				dir := t.TempDir()
				return dir
			},
			addCall: func(client *Client) (any, error) {
				return client.AddPublication(clientAddStreamID, clientAddChannel)
			},
		},
		{
			name: "subscription",
			dir: func(t *testing.T) string {
				return t.TempDir()
			},
			addCall: func(client *Client) (any, error) {
				return client.AddSubscription(clientAddStreamID, clientAddChannel)
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				testCase.dir(t),
			)

			client.DoWork()
			require.NoError(
				t,
				fixture.Transmit(
					conductor.OnClientTimeout,
					testutil.ClientTimeoutPayload(testutil.TestClientID),
				),
			)
			nanoNs = testutil.TestWorkNanoNs

			got, err := testCase.addCall(client)
			require.Nil(t, got)
			requireClientTimeoutFromDriver(t, err)
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_AddSubscription_insertsMapOnSyntheticReady(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)

	type result struct {
		sub *Subscription
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
		resultCh <- result{sub: sub, err: err}
	}()

	pollDeadline := time.Now().Add(time.Second)
	responded, correlationID := pollAndRespondToAddSubscriptionCommand(t, fixture, pollDeadline, nil)
	require.True(t, responded, "timed out waiting for AddSubscription command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		2*time.Second,
		"timed out waiting for AddSubscription client result",
	)
	require.NoError(t, got.err)
	require.NotNil(t, got.sub)
	require.Equal(t, correlationID, got.sub.RegistrationID())
	lookup, ok := client.Subscription(correlationID)
	require.True(t, ok)
	require.Same(t, got.sub, lookup)
	require.Equal(t, 1, client.SubscriptionCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_AddSubscription_driverRegistrationError_failsAwait(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)

	type result struct {
		sub *Subscription
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
		resultCh <- result{sub: sub, err: err}
	}()

	pollDeadline := time.Now().Add(time.Second)
	responded, correlationID := pollAndRespondToAddCommandError(
		t,
		fixture,
		conductor.AddSubscription,
		decodeAddSubscriptionCorrelationID,
		pollDeadline,
		11,
		"add subscription rejected",
	)
	require.True(t, responded, "timed out waiting for AddSubscription command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		2*time.Second,
		"timed out waiting for AddSubscription client result",
	)
	require.Nil(t, got.sub)
	var registrationErr *RegistrationError
	require.ErrorAs(t, got.err, &registrationErr)
	require.Equal(t, registrationKindSubscription, registrationErr.Kind)
	require.Equal(t, correlationID, registrationErr.CorrelationID)
	require.Equal(t, int32(11), registrationErr.ErrorCode)
	require.Equal(t, "add subscription rejected", registrationErr.Message)
	var driverErr *conductor.DriverRegistrationError
	require.ErrorAs(t, registrationErr.Cause, &driverErr)
	require.EqualError(
		t,
		got.err,
		fmt.Sprintf(
			"aeron: driver registration error: correlation=%d code=11 message=add subscription rejected",
			correlationID,
		),
	)
	require.Equal(t, 0, client.SubscriptionCountForTest())
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_AddSubscription_offerPhaseTimeout(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		0,
		time.Microsecond,
		dir,
	)

	sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
	require.Nil(t, sub)
	requireAddPhaseTimeout(
		t,
		fixture,
		client,
		err,
		registrationKindSubscription,
		(*Client).SubscriptionCountForTest,
		true,
	)
	requireClientAddRingNoCommittedCommands(t, fixture.Ring)
}

func TestClient_AddSubscription_pollPhaseTimeout(t *testing.T) {
	const timeout = 5 * time.Millisecond
	startNs := testutil.TestStartNanoNs
	clock := newClientAddTestClock(startNs, timeout)
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		clock.NanoClock(),
		timeout,
		time.Microsecond,
		dir,
	)

	type result struct {
		sub *Subscription
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
		resultCh <- result{sub: sub, err: err}
	}()

	waitForAddSubscriptionCommand(t, fixture)
	clock.EnterDeadlinePhase()

	got := receiveAddResult(
		t,
		resultCh,
		2*time.Second,
		"timed out waiting for AddSubscription client result",
	)
	require.Nil(t, got.sub)
	requireAddPhaseTimeout(
		t,
		fixture,
		client,
		got.err,
		registrationKindSubscription,
		(*Client).SubscriptionCountForTest,
		false,
	)
}

func TestClient_Add_registerPendingFailures(t *testing.T) {
	const pendingCorrelationID int64 = 0

	tests := []struct {
		name    string
		setup   func(t *testing.T, fixture *testutil.ConductorFixture, setNanoNs func(int64))
		addCall func(client *Client) error
		wantErr error
		wantMsg string
	}{
		{
			name:  "publicationConductorTerminating",
			setup: setupClientAddConductorTerminating,
			addCall: func(client *Client) error {
				_, err := client.AddPublication(clientAddStreamID, clientAddChannel)
				return err
			},
			wantErr: ErrConductorTerminating,
			wantMsg: "aeron: conductor is terminating",
		},
		{
			name:  "subscriptionConductorTerminating",
			setup: setupClientAddConductorTerminating,
			addCall: func(client *Client) error {
				_, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
				return err
			},
			wantErr: ErrConductorTerminating,
			wantMsg: "aeron: conductor is terminating",
		},
		{
			name: "publicationDuplicatePending",
			setup: func(t *testing.T, fixture *testutil.ConductorFixture, _ func(int64)) {
				require.NoError(
					t,
					fixture.Conductor.RegisterPendingAdd(
						conductor.PendingKindAddPublication,
						pendingCorrelationID,
						clientAddStreamID,
						clientAddChannel,
						time.Second,
					),
				)
			},
			addCall: func(client *Client) error {
				_, err := client.AddPublication(clientAddStreamID, clientAddChannel)
				return err
			},
			wantErr: ErrPendingAlreadyRegistered,
			wantMsg: "aeron: correlation already pending",
		},
		{
			name: "subscriptionDuplicatePending",
			setup: func(t *testing.T, fixture *testutil.ConductorFixture, _ func(int64)) {
				require.NoError(
					t,
					fixture.Conductor.RegisterPendingAdd(
						conductor.PendingKindAddSubscription,
						pendingCorrelationID,
						clientAddStreamID,
						clientAddChannel,
						time.Second,
					),
				)
			},
			addCall: func(client *Client) error {
				_, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
				return err
			},
			wantErr: ErrPendingAlreadyRegistered,
			wantMsg: "aeron: correlation already pending",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			dir := t.TempDir()
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				dir,
			)
			testCase.setup(t, fixture, func(value int64) { nanoNs = value })

			err := testCase.addCall(client)
			require.ErrorIs(t, err, testCase.wantErr)
			require.EqualError(t, err, testCase.wantMsg)
		})
	}
}

func TestClient_AwaitDrainForTest_emptyClient(t *testing.T) {
	var nanoNs int64
	client, _ := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		t.TempDir(),
	)

	require.NoError(t, client.AwaitDrainForTest(time.Second))
}

func TestClient_Add_nilDriverProxy(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	dir := t.TempDir()
	_, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	client := NewClientForTestWithDriverProxy(
		fixture.Conductor,
		nil,
		fixture.NanoClock,
		testutil.TestClientID,
		time.Second,
		time.Millisecond,
	)

	pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
	require.Nil(t, pub)
	require.ErrorIs(t, err, ErrClientNotReady)

	sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
	require.Nil(t, sub)
	require.ErrorIs(t, err, ErrClientNotReady)
}

func TestClient_Add_registerPendingSharedDeadline(t *testing.T) {
	const timeout = 500 * time.Millisecond
	startNs := testutil.TestStartNanoNs
	deadlineNs := startNs + timeout.Nanoseconds()
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return startNs },
		timeout,
		time.Millisecond,
		dir,
	)

	type result struct {
		sub *Subscription
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
		resultCh <- result{sub: sub, err: err}
	}()

	correlationID := waitForAddSubscriptionCommand(t, fixture)
	registration, ok := fixture.Conductor.PendingResult(correlationID)
	require.True(t, ok)
	require.Equal(t, deadlineNs, registration.DeadlineNs)

	require.NoError(
		t,
		fixture.Transmit(
			conductor.OnSubscriptionReady,
			conductor.SubscriptionReadyBytes(
				clientAddSubscriptionReadyEvent(correlationID),
			),
		),
	)

	got := receiveAddResult(
		t,
		resultCh,
		2*time.Second,
		"timed out waiting for AddSubscription client result",
	)
	require.NoError(t, got.err)
	require.NotNil(t, got.sub)
}

func TestClient_AddPublication_pollPhaseLogMapFailure(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	badLogPath := filepath.Join(dir, "missing.log")
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))

	type result struct {
		pub *Publication
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
		resultCh <- result{pub: pub, err: err}
	}()

	pollDeadline := time.Now().Add(time.Second)
	responded, correlationID := pollAndRespondToAddPublicationCommand(
		t,
		fixture,
		badLogPath,
		pollDeadline,
		nil,
	)
	require.True(t, responded, "timed out waiting for AddPublication command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		2*time.Second,
		"timed out waiting for AddPublication client result",
	)
	require.Nil(t, got.pub)
	var registrationErr *RegistrationError
	require.ErrorAs(t, got.err, &registrationErr)
	require.Equal(t, registrationKindPublication, registrationErr.Kind)
	require.Equal(t, correlationID, registrationErr.CorrelationID)
	require.ErrorIs(t, got.err, conductor.ErrLogBufferMapFailed)
	require.EqualError(
		t,
		got.err,
		fmt.Sprintf(
			"aeron: publication registration error: correlation=%d: conductor: failed to map log buffer",
			correlationID,
		),
	)
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	require.Equal(t, 0, client.PublicationCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_awaitPendingPoll_closedGuardAfterPollPending_addPublication(t *testing.T) {
	var nanoNs int64
	dir, logPath := setupClientAddPublicationLog(t)
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))

	correlationID := client.NextCorrelationID()
	deadlineNs := client.nanoClock() + time.Second.Nanoseconds()
	require.NoError(
		t,
		fixture.Conductor.RegisterPendingAddWithDeadline(
			conductor.PendingKindAddPublication,
			correlationID,
			clientAddStreamID,
			clientAddChannel,
			deadlineNs,
		),
	)
	conductor.PublicationReadyForTest(
		fixture.Conductor,
		clientAddPublicationReadyEvent(correlationID, logPath),
	)
	require.Equal(t, 1, client.PublicationCountForTest())

	_, completed, err := client.AwaitPendingPollOneTickForTest(correlationID, true, func() {
		client.PublishClosedForTest()
	})
	require.True(t, completed)
	require.ErrorIs(t, err, ErrClientClosed)
	require.EqualError(t, err, ErrClientClosed.Error())
	require.Equal(t, 0, client.PublicationCountForTest())
}

func TestClient_awaitPendingPoll_closedGuardAfterPollPending_addSubscription(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)

	correlationID := client.NextCorrelationID()
	deadlineNs := client.nanoClock() + time.Second.Nanoseconds()
	require.NoError(
		t,
		fixture.Conductor.RegisterPendingAddWithDeadline(
			conductor.PendingKindAddSubscription,
			correlationID,
			clientAddStreamID,
			clientAddChannel,
			deadlineNs,
		),
	)
	conductor.SubscriptionReadyForTest(
		fixture.Conductor,
		clientAddSubscriptionReadyEvent(correlationID),
	)
	require.Equal(t, 1, client.SubscriptionCountForTest())

	_, completed, err := client.AwaitPendingPollOneTickForTest(correlationID, true, func() {
		client.PublishClosedForTest()
	})
	require.True(t, completed)
	require.ErrorIs(t, err, ErrClientClosed)
	require.EqualError(t, err, ErrClientClosed.Error())
	require.Equal(t, 0, client.SubscriptionCountForTest())
}

func newGuardClosedBeforeOfferClient(t *testing.T) (*Client, *testutil.ConductorFixture) {
	t.Helper()
	var nanoNs int64
	return newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		t.TempDir(),
	)
}

func TestClient_GuardClosedBeforeOfferForTest_proxyMismatch(t *testing.T) {
	client, fixture := newGuardClosedBeforeOfferClient(t)
	otherProxy := conductor.NewDriverProxy(fixture.Ring, testutil.TestClientID+1)

	err := client.GuardClosedBeforeOfferForTest(otherProxy, false)
	require.ErrorIs(t, err, ErrClientNotReady)
	require.EqualError(t, err, ErrClientNotReady.Error())
}

func TestClient_GuardClosedBeforeOfferForTest_closedFlag(t *testing.T) {
	client, fixture := newGuardClosedBeforeOfferClient(t)
	client.PublishClosedForTest()

	err := client.GuardClosedBeforeOfferForTest(fixture.DriverProxy, false)
	require.ErrorIs(t, err, ErrClientClosed)
	require.EqualError(t, err, ErrClientClosed.Error())
}

func TestClient_GuardClosedBeforeOfferForTest_staleMappedCncFlag(t *testing.T) {
	client, fixture := newGuardClosedBeforeOfferClient(t)

	err := client.GuardClosedBeforeOfferForTest(fixture.DriverProxy, true)
	require.ErrorIs(t, err, ErrClientClosed)
}

func TestClient_Publication_lookup_wrongTypeHandle(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	const registrationID int64 = 42
	conductor.RegisterPublicationForTest(
		fixture.Conductor,
		registrationID,
		NewSubscriptionForTest(registrationID, clientAddStreamID, clientAddChannel),
	)

	pub, ok := client.Publication(registrationID)
	require.Nil(t, pub)
	require.False(t, ok)
}

type clientAddWrongSubscriptionHandle struct {
	registrationID int64
	streamID       int32
}

func (handle clientAddWrongSubscriptionHandle) RegistrationID() int64 {
	return handle.registrationID
}

func (handle clientAddWrongSubscriptionHandle) StreamID() int32 {
	return handle.streamID
}

func TestClient_Subscription_lookup_wrongTypeHandle(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	const registrationID int64 = 42
	conductor.RegisterSubscriptionForTest(
		fixture.Conductor,
		registrationID,
		clientAddWrongSubscriptionHandle{
			registrationID: registrationID,
			streamID:       clientAddStreamID,
		},
	)

	sub, ok := client.Subscription(registrationID)
	require.Nil(t, sub)
	require.False(t, ok)
}

func TestClient_Add_lookup_miss(t *testing.T) {
	tests := []struct {
		name   string
		lookup func(*Client) (any, bool)
	}{
		{
			name: "publication",
			lookup: func(client *Client) (any, bool) {
				pub, ok := client.Publication(clientAddPublicationRegistrationID)
				return pub, ok
			},
		},
		{
			name: "subscription",
			lookup: func(client *Client) (any, bool) {
				sub, ok := client.Subscription(99)
				return sub, ok
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			client, _ := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				t.TempDir(),
			)

			got, ok := testCase.lookup(client)
			require.Nil(t, got)
			require.False(t, ok)
		})
	}
}

func requireClientAddRingNoCommittedCommands(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
) {
	t.Helper()
	require.Zero(t, testutil.CommittedNonPaddingRecordCount(ring))
}

func TestClient_Add_nonASCIIChannel(t *testing.T) {
	channels := []struct {
		name    string
		channel string
	}{
		{name: "high byte", channel: "aeron:\x80"},
		{name: "utf8 multibyte", channel: "ü"},
	}
	tests := []struct {
		name    string
		addCall func(*Client, string) (any, error)
	}{
		{
			name: "publication",
			addCall: func(client *Client, channel string) (any, error) {
				return client.AddPublication(clientAddStreamID, channel)
			},
		},
		{
			name: "subscription",
			addCall: func(client *Client, channel string) (any, error) {
				return client.AddSubscription(clientAddStreamID, channel)
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			for _, channelTest := range channels {
				t.Run(channelTest.name, func(t *testing.T) {
					var nanoNs int64
					client, fixture := newClientAddFixture(
						t,
						func() int64 { return nanoNs },
						time.Second,
						time.Millisecond,
						t.TempDir(),
					)

					got, err := testCase.addCall(client, channelTest.channel)
					require.Nil(t, got)
					require.EqualError(t, err, "aeron: channel must be ASCII")
					requireClientAddRingNoCommittedCommands(t, fixture.Ring)
					correlationID := fixture.DriverProxy.NextCorrelationID() - 1
					_, ok := fixture.Conductor.PendingResult(correlationID)
					require.False(t, ok)
				})
			}
		})
	}
}

func TestClient_Add_emptyChannel(t *testing.T) {
	tests := []struct {
		name    string
		addCall func(*Client) (any, error)
	}{
		{
			name: "publication",
			addCall: func(client *Client) (any, error) {
				return client.AddPublication(clientAddStreamID, "")
			},
		},
		{
			name: "subscription",
			addCall: func(client *Client) (any, error) {
				return client.AddSubscription(clientAddStreamID, "")
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			client, _ := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				t.TempDir(),
			)

			got, err := testCase.addCall(client)
			require.Nil(t, got)
			require.EqualError(t, err, "aeron: channel is required")
		})
	}
}

func TestClient_AddPublication_refcountsLog(t *testing.T) {
	var nanoNs int64
	dir, logPath := setupClientAddPublicationLog(t)
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))
	registry := conductor.LogBuffersRegistryForTest(fixture.Conductor)

	type result struct {
		pub *Publication
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
		resultCh <- result{pub: pub, err: err}
	}()

	pollDeadline := time.Now().Add(time.Second)
	responded, _ := pollAndRespondToAddPublicationCommand(
		t,
		fixture,
		logPath,
		pollDeadline,
		nil,
	)
	require.True(t, responded, "timed out waiting for AddPublication command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		2*time.Second,
		"timed out waiting for AddPublication client result",
	)
	require.NoError(t, got.err)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(registry, logPath))
}

func TestClient_closedAddAndLookupPaths(t *testing.T) {
	nanoNs := testutil.TestStartNanoNs
	dir := t.TempDir()
	client, _ := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)
	require.NoError(t, client.Close())

	pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
	require.Nil(t, pub)
	require.ErrorIs(t, err, ErrClientClosed)

	sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
	require.Nil(t, sub)
	require.ErrorIs(t, err, ErrClientClosed)

	lookupPub, ok := client.Publication(clientAddPublicationRegistrationID)
	require.Nil(t, lookupPub)
	require.False(t, ok)

	lookupSub, ok := client.Subscription(99)
	require.Nil(t, lookupSub)
	require.False(t, ok)
}

func TestHandleFromPending_wrongHandle(t *testing.T) {
	tests := []struct {
		name    string
		run     func(conductor.PendingRegistration) (any, error)
		wantMsg string
	}{
		{
			name: "publicationNil",
			run: func(pending conductor.PendingRegistration) (any, error) {
				return publicationFromPending(pending)
			},
			wantMsg: "aeron: publication handle is nil",
		},
		{
			name: "publicationWrongType",
			run: func(pending conductor.PendingRegistration) (any, error) {
				pending.Publication = "not-a-publication"
				return publicationFromPending(pending)
			},
			wantMsg: "aeron: publication handle string is not *Publication",
		},
		{
			name: "subscriptionNil",
			run: func(pending conductor.PendingRegistration) (any, error) {
				return subscriptionFromPending(pending)
			},
			wantMsg: "aeron: subscription handle is nil",
		},
		{
			name: "subscriptionWrongType",
			run: func(pending conductor.PendingRegistration) (any, error) {
				pending.Subscription = "not-a-subscription"
				return subscriptionFromPending(pending)
			},
			wantMsg: "aeron: subscription handle string is not *Subscription",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := testCase.run(conductor.PendingRegistration{})
			require.EqualError(t, err, testCase.wantMsg)
		})
	}
}

func TestClient_nilAddAndLookupPaths(t *testing.T) {
	var client *Client

	pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
	require.Nil(t, pub)
	require.ErrorIs(t, err, ErrClientNotReady)

	sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
	require.Nil(t, sub)
	require.ErrorIs(t, err, ErrClientNotReady)

	lookupPub, ok := client.Publication(clientAddPublicationRegistrationID)
	require.Nil(t, lookupPub)
	require.False(t, ok)

	lookupSub, ok := client.Subscription(99)
	require.Nil(t, lookupSub)
	require.False(t, ok)
}

func TestMapAddPollPendingError_ctorFailure(t *testing.T) {
	cause := errors.New("aeron: map log file failed")
	const correlationID int64 = 1001
	mapped := MapAddPollPendingErrorForTest(
		registrationKindPublication,
		correlationID,
		clientAddStreamID,
		clientAddChannel,
		cause,
	)
	var registrationErr *RegistrationError
	require.ErrorAs(t, mapped, &registrationErr)
	require.Equal(t, registrationKindPublication, registrationErr.Kind)
	require.Equal(t, correlationID, registrationErr.CorrelationID)
	require.ErrorIs(t, mapped, cause)
	require.EqualError(
		t,
		mapped,
		"aeron: publication registration error: correlation=1001: aeron: map log file failed",
	)
}
