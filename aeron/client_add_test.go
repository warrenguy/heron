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

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/ringbuffer"
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

func conductorFixtureConfigFromConnect(connectConfig conductor.Config) testutil.ConductorConfig {
	config := testutil.DefaultConductorConfig(nil)
	config.DriverTimeout = connectConfig.DriverTimeout
	config.KeepAliveInterval = connectConfig.KeepAliveInterval
	config.IdleSleepDuration = connectConfig.IdleSleepDuration
	config.InterServiceTimeout = connectConfig.InterServiceTimeout
	config.ImageLifecycle = connectConfig.ImageLifecycle
	config.ResourceLifecycle = connectConfig.ResourceLifecycle
	config.MediaDriverDir = connectConfig.MediaDriverDir
	config.ErrorHandler = connectConfig.ErrorHandler
	config.SubscriberErrorHandler = connectConfig.SubscriberErrorHandler
	config.CloseLingerNs = connectConfig.CloseLingerNs
	return config
}

func TestConductorFixtureConfigFromConnect_propagatesSubscriberErrorHandler(t *testing.T) {
	t.Parallel()

	var subscriberCalls int
	cfg := Config{
		ErrorHandler: func(error) { subscriberCalls++ },
	}
	connectConfig := ConductorConnectConfigForTest(cfg, testutil.TestClientID)
	fixtureConfig := conductorFixtureConfigFromConnect(connectConfig)
	require.NotNil(t, fixtureConfig.SubscriberErrorHandler)
	fixtureConfig.SubscriberErrorHandler(errors.New("subscriber-side fault"))
	require.Equal(t, 1, subscriberCalls)
	require.NotNil(t, fixtureConfig.ErrorHandler)
}

// newClientAddFixtureWithConfig builds a Connect-shaped fixture via
// ConductorConnectConfigForTest (client nil on ResourceLifecycle).
//
// Overrides test timing: NanoClock->0, DriverTimeout->1s, IdleSleepDuration->1ms.
func newClientAddFixtureWithConfig(
	tb testing.TB,
	cfg Config,
) (*Client, *testutil.ConductorFixture) {
	tb.Helper()

	connectCfg := cfg
	if connectCfg.AeronDir == "" {
		connectCfg.AeronDir = tb.TempDir()
	}

	connectConfig := ConductorConnectConfigForTest(connectCfg, testutil.TestClientID)
	config := conductorFixtureConfigFromConnect(connectConfig)
	config.NanoClock = func() int64 { return 0 }
	config.DriverTimeout = time.Second
	config.IdleSleepDuration = time.Millisecond
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

func countersReaderForClientAddPublications(
	tb testutil.TB,
	registrationIDs ...int64,
) *cnc.CountersReader {
	tb.Helper()
	if len(registrationIDs) == 0 {
		registrationIDs = []int64{clientAddPublicationRegistrationID}
	}
	recordCount := len(registrationIDs) * 2
	return newTestCountersReaderFromRecords(recordCount, func(meta, values []byte) {
		for i, registrationID := range registrationIDs {
			record := int32(i * 2)
			cnc.WriteCounterRecord(
				meta,
				values,
				record,
				cnc.CounterRecordAllocated,
				cnc.DriverPublisherLimitTypeID,
				registrationID,
				0,
			)
			err := cnc.WriteChannelStatusCounterRecord(
				meta,
				values,
				record+1,
				cnc.CounterRecordAllocated,
				cnc.DriverSendChannelStatusTypeID,
				clientAddChannel,
				registrationID,
				1,
			)
			if err != nil {
				panic(err)
			}
		}
	})
}

func countersReaderForClientAddTest(tb testutil.TB) *cnc.CountersReader {
	tb.Helper()
	return countersReaderForClientAddPublications(tb, clientAddPublicationRegistrationID)
}

func setupClientAddPublicationLog(tb testing.TB) (dir, logPath string) {
	tb.Helper()
	return setupClientAddPublicationKindLog(tb, false)
}

func setupClientAddExclusivePublicationLog(tb testing.TB) (dir, logPath string) {
	tb.Helper()
	return setupClientAddPublicationKindLog(tb, true)
}

func setupClientAddPublicationKindLog(tb testing.TB, exclusive bool) (dir, logPath string) {
	tb.Helper()
	dir = tb.TempDir()
	filename := "publication.log"
	if exclusive {
		filename = "exclusive-publication.log"
	}
	logPath = filepath.Join(dir, filename)
	WriteTestKindLogFileForTest(tb, logPath, exclusive)
	return dir, logPath
}

func clientAddMediaDir(t *testing.T) string {
	return t.TempDir()
}

func requireRegisterClientAddPending(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	kind conductor.PendingKind,
	correlationID int64,
) {
	t.Helper()

	require.NoError(
		t,
		fixture.Conductor.RegisterPendingAdd(
			kind,
			correlationID,
			clientAddStreamID,
			clientAddChannel,
			time.Second,
		),
	)
}

type clientAddPublicationKindCase struct {
	name                   string
	pendingKind            conductor.PendingKind
	addMsgType             int
	readyMsgType           int
	commandName            string
	decodeCorrelationID    func(atomicbuffer.AtomicBuffer, int, int) (int64, error)
	driverRejectMessage    string
	registrationErrorLabel string
	setupLog               func(testing.TB) (dir, logPath string)
	add                    func(*Client) (any, error)
	offerAdd               func(*Client) (int64, error)
	pollAdd                func(*Client, int64) (any, bool, error)
	pollOneTick            func(*Client, int64, bool, func()) (any, bool, error)
	lookup                 func(*Client, int64) (any, bool)
	oppositeLookup         func(*Client, int64) (any, bool)
	signalReady            func(*conductor.Conductor, conductor.PublicationBuffersReady)
	clientResultTimeoutMsg string
	offerResultTimeoutMsg  string
	pollTimeoutMsg         string
	ringRespondTimeoutMsg  string
}

func (testCase clientAddPublicationKindCase) pollRespond(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	logPath string,
	deadline time.Time,
	onCommandSeen func(correlationID int64),
) (bool, int64) {
	return pollAndRespondToAddPublicationKindCommand(
		t,
		fixture,
		logPath,
		deadline,
		onCommandSeen,
		testCase.addMsgType,
		testCase.readyMsgType,
	)
}

func (testCase clientAddPublicationKindCase) waitForCommand(
	t *testing.T,
	fixture *testutil.ConductorFixture,
) int64 {
	return waitForAddPublicationKindCommand(
		t,
		fixture,
		testCase.addMsgType,
		testCase.commandName,
	)
}

func clientAddPublicationKindCases() []clientAddPublicationKindCase {
	return []clientAddPublicationKindCase{
		{
			name:                   "publication",
			pendingKind:            conductor.PendingKindAddPublication,
			addMsgType:             conductor.AddPublication,
			readyMsgType:           conductor.OnPublicationReady,
			commandName:            "AddPublication",
			decodeCorrelationID:    decodeAddPublicationCorrelationID,
			driverRejectMessage:    "add publication rejected",
			registrationErrorLabel: "publication",
			setupLog:               setupClientAddPublicationLog,
			add: func(client *Client) (any, error) {
				return client.AddPublication(clientAddStreamID, clientAddChannel)
			},
			offerAdd: func(client *Client) (int64, error) {
				return client.OfferAddPublication(clientAddStreamID, clientAddChannel)
			},
			pollAdd: func(client *Client, correlationID int64) (any, bool, error) {
				return client.PollAddPublication(correlationID)
			},
			pollOneTick: func(
				client *Client,
				correlationID int64,
				skipDoWork bool,
				afterPollPending func(),
			) (any, bool, error) {
				return pollAddForTest(
					client,
					correlationID,
					publicationFromPending,
					skipDoWork,
					afterPollPending,
				)
			},
			lookup: func(client *Client, regID int64) (any, bool) {
				return client.Publication(regID)
			},
			oppositeLookup: func(client *Client, regID int64) (any, bool) {
				return client.ExclusivePublication(regID)
			},
			signalReady:            conductor.PublicationReadyForTest,
			clientResultTimeoutMsg: "timed out waiting for AddPublication client result",
			offerResultTimeoutMsg:  "timed out waiting for OfferAddPublication result",
			pollTimeoutMsg:         "timed out waiting for PollAddPublication",
			ringRespondTimeoutMsg:  "timed out waiting for AddPublication command on ring",
		},
		{
			name:                   "exclusivePublication",
			pendingKind:            conductor.PendingKindAddExclusivePublication,
			addMsgType:             conductor.AddExclusivePublication,
			readyMsgType:           conductor.OnExclusivePublicationReady,
			commandName:            "AddExclusivePublication",
			decodeCorrelationID:    decodeAddPublicationCorrelationID,
			driverRejectMessage:    "add exclusive publication rejected",
			registrationErrorLabel: "exclusive publication",
			setupLog:               setupClientAddExclusivePublicationLog,
			add: func(client *Client) (any, error) {
				return client.AddExclusivePublication(clientAddStreamID, clientAddChannel)
			},
			offerAdd: func(client *Client) (int64, error) {
				return client.OfferAddExclusivePublication(clientAddStreamID, clientAddChannel)
			},
			pollAdd: func(client *Client, correlationID int64) (any, bool, error) {
				return client.PollAddExclusivePublication(correlationID)
			},
			pollOneTick: func(
				client *Client,
				correlationID int64,
				skipDoWork bool,
				afterPollPending func(),
			) (any, bool, error) {
				return pollAddForTest(
					client,
					correlationID,
					exclusivePublicationFromPending,
					skipDoWork,
					afterPollPending,
				)
			},
			lookup: func(client *Client, regID int64) (any, bool) {
				return client.ExclusivePublication(regID)
			},
			oppositeLookup: func(client *Client, regID int64) (any, bool) {
				return client.Publication(regID)
			},
			signalReady:            conductor.ExclusivePublicationReadyForTest,
			clientResultTimeoutMsg: "timed out waiting for AddExclusivePublication client result",
			offerResultTimeoutMsg:  "timed out waiting for OfferAddExclusivePublication result",
			pollTimeoutMsg:         "timed out waiting for PollAddExclusivePublication",
			ringRespondTimeoutMsg:  "timed out waiting for AddExclusivePublication command on ring",
		},
	}
}

type clientOfferAddResult struct {
	correlationID int64
	err           error
}

type clientAddAsyncResult struct {
	value any
	err   error
}

func startClientAddAsync(
	client *Client,
	add func(*Client) (any, error),
) <-chan clientAddAsyncResult {
	resultCh := make(chan clientAddAsyncResult, 1)
	go func() {
		value, err := add(client)
		resultCh <- clientAddAsyncResult{value: value, err: err}
	}()
	return resultCh
}

type clientOfferAddAsyncKindCase struct {
	name                  string
	offerAdd              func(*Client) (int64, error)
	poll                  func(*Client, int64) (any, bool, error)
	registrationKind      string
	countFn               func(*Client) int
	dir                   func(t *testing.T) string
	addMsgType            int
	offerResultTimeoutMsg string
	pollOneTick           func(*Client, int64, bool, func()) (any, bool, error)
}

func clientOfferAddAsyncKindCases() []clientOfferAddAsyncKindCase {
	pubCases := clientAddPublicationKindCases()
	cases := make([]clientOfferAddAsyncKindCase, 0, len(pubCases)+1)
	for _, pubCase := range pubCases {
		cases = append(cases, clientOfferAddAsyncKindCase{
			name:                  pubCase.name,
			offerAdd:              pubCase.offerAdd,
			poll:                  pubCase.pollAdd,
			registrationKind:      registrationKindFor(pubCase.pendingKind),
			countFn:               (*Client).PublicationCountForTest,
			dir:                   clientAddMediaDir,
			addMsgType:            pubCase.addMsgType,
			offerResultTimeoutMsg: pubCase.offerResultTimeoutMsg,
			pollOneTick:           pubCase.pollOneTick,
		})
	}
	cases = append(cases, clientOfferAddAsyncKindCase{
		name: "subscription",
		offerAdd: func(client *Client) (int64, error) {
			return client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
		},
		poll: func(client *Client, correlationID int64) (any, bool, error) {
			return client.PollAddSubscription(correlationID)
		},
		registrationKind:      registrationKindSubscription,
		countFn:               (*Client).SubscriptionCountForTest,
		dir:                   clientAddMediaDir,
		addMsgType:            conductor.AddSubscription,
		offerResultTimeoutMsg: "timed out waiting for OfferAddSubscription result",
		pollOneTick: func(
			client *Client,
			correlationID int64,
			skipDoWork bool,
			afterPollPending func(),
		) (any, bool, error) {
			return pollAddForTest(
				client,
				correlationID,
				subscriptionFromPending,
				skipDoWork,
				afterPollPending,
			)
		},
	})
	return cases
}

func clientAddPublicationKindCaseFor(kind conductor.PendingKind) clientAddPublicationKindCase {
	for _, testCase := range clientAddPublicationKindCases() {
		if testCase.pendingKind == kind {
			return testCase
		}
	}
	panic(fmt.Sprintf("aeron: unsupported publication pending kind %v", kind))
}

func decodeAddPublicationCorrelationID(
	buffer atomicbuffer.AtomicBuffer,
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
	buffer atomicbuffer.AtomicBuffer,
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
	buffer atomicbuffer.AtomicBuffer,
	index int,
	_ int,
) (int64, error) {
	decoded, err := conductor.DecodeCorrelated(buffer, index)
	if err != nil {
		return 0, err
	}
	return decoded.CorrelationID, nil
}

const (
	clientConcurrentResultTimeout      = 2 * time.Second
	concurrentAwaitHammerCheckDuration = 20 * time.Millisecond
)

func requireErrClientClosed(t *testing.T, err error) {
	t.Helper()

	require.ErrorIs(t, err, ErrClientClosed)
	require.EqualError(t, err, ErrClientClosed.Error())
}

func pollAddUntilReady[T any](
	t *testing.T,
	client *Client,
	correlationID int64,
	respond func(t *testing.T, deadline time.Time),
	poll func(*Client, int64) (T, bool, error),
	failMsg string,
) T {
	t.Helper()

	pollDeadline := time.Now().Add(time.Second)
	go respond(t, pollDeadline)

	deadline := time.Now().Add(clientConcurrentResultTimeout)
	for time.Now().Before(deadline) {
		value, done, err := poll(client, correlationID)
		if done {
			require.NoError(t, err)
			return value
		}
		require.NoError(t, err)
		time.Sleep(time.Millisecond)
	}
	require.FailNow(t, failMsg)
	var zero T
	return zero
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

func requireRegistrationTimeoutError(
	t *testing.T,
	err error,
	registrationKind string,
	correlationID int64,
	checkStreamChannel bool,
) {
	t.Helper()

	var timeoutErr *RegistrationTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.Equal(t, registrationKind, timeoutErr.Kind)
	require.Equal(t, correlationID, timeoutErr.CorrelationID)
	if checkStreamChannel {
		require.Equal(t, clientAddStreamID, timeoutErr.StreamID)
		require.Equal(t, clientAddChannel, timeoutErr.Channel)
	}
	require.EqualError(
		t,
		err,
		fmt.Sprintf(
			"aeron: %s registration timeout: correlation=%d stream=%d channel=%s",
			registrationKind,
			correlationID,
			clientAddStreamID,
			clientAddChannel,
		),
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

	correlationID := fixture.DriverProxy.NextCorrelationID() - 1
	requireRegistrationTimeoutError(
		t,
		err,
		registrationKind,
		correlationID,
		checkStreamChannel,
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
	decode func(atomicbuffer.AtomicBuffer, int, int) (int64, error),
	deadline time.Time,
	buildResponse func(correlationID int64) (eventType int, payload []byte),
	onCommandSeen func(correlationID int64),
) (bool, int64) {
	t.Helper()

	for time.Now().Before(deadline) {
		var responded bool
		var correlationID int64
		fixture.Ring.Read(func(ringMsgTypeID int, buffer atomicbuffer.AtomicBuffer, index, length int) {
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

func pollAndRespondToAddPublicationKindCommand(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	logPath string,
	deadline time.Time,
	onCommandSeen func(correlationID int64),
	msgTypeID int,
	readyMsgType int,
) (bool, int64) {
	t.Helper()

	return pollAndRespondToAddCommand(
		t,
		fixture,
		msgTypeID,
		decodeAddPublicationCorrelationID,
		deadline,
		func(correlationID int64) (int, []byte) {
			return readyMsgType, conductor.PublicationBuffersReadyBytes(
				clientAddPublicationReadyEvent(correlationID, logPath),
			)
		},
		onCommandSeen,
	)
}

func pollAndRespondToAddPublicationCommand(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	logPath string,
	deadline time.Time,
	onCommandSeen func(correlationID int64),
) (bool, int64) {
	t.Helper()

	return pollAndRespondToAddPublicationKindCommand(
		t,
		fixture,
		logPath,
		deadline,
		onCommandSeen,
		conductor.AddPublication,
		conductor.OnPublicationReady,
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
	decode func(atomicbuffer.AtomicBuffer, int, int) (int64, error),
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
	decode func(atomicbuffer.AtomicBuffer, int, int) (int64, error),
	commandName string,
) int64 {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var correlationID int64
		var sawCommand bool
		fixture.Ring.Read(func(ringMsgTypeID int, buffer atomicbuffer.AtomicBuffer, index, length int) {
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

	return waitForAddPublicationKindCommand(
		t,
		fixture,
		conductor.AddPublication,
		"AddPublication",
	)
}

func waitForAddPublicationKindCommand(
	t *testing.T,
	fixture *testutil.ConductorFixture,
	msgTypeID int,
	commandName string,
) int64 {
	t.Helper()

	return waitForAddCommand(
		t,
		fixture,
		msgTypeID,
		decodeAddPublicationCorrelationID,
		commandName,
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

func addPublicationHandleRegistrationID(pub any) int64 {
	handle, ok := pub.(conductor.PublicationHandle)
	if !ok {
		panic(fmt.Sprintf("unexpected publication type %T", pub))
	}
	return handle.RegistrationID()
}

func TestClient_AddPublicationKind_insertsMapOnSyntheticReady(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			dir, logPath := testCase.setupLog(t)
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				dir,
			)
			fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))

			resultCh := startClientAddAsync(client, testCase.add)

			pollDeadline := time.Now().Add(time.Second)
			responded, correlationID := testCase.pollRespond(
				t,
				fixture,
				logPath,
				pollDeadline,
				nil,
			)
			require.True(t, responded, testCase.ringRespondTimeoutMsg)

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.clientResultTimeoutMsg,
			)
			require.NoError(t, got.err)
			require.NotNil(t, got.value)
			require.Equal(t, correlationID, fixture.DriverProxy.NextCorrelationID()-1)
			require.Equal(
				t,
				clientAddPublicationRegistrationID,
				addPublicationHandleRegistrationID(got.value),
			)
			lookup, ok := testCase.lookup(client, clientAddPublicationRegistrationID)
			require.True(t, ok)
			require.Same(t, got.value, lookup)
			require.Equal(t, 1, client.PublicationCountForTest())
			requireActiveCorrelationCleared(t, fixture)

			_, oppositeOK := testCase.oppositeLookup(client, clientAddPublicationRegistrationID)
			require.False(t, oppositeOK)
		})
	}
}

func TestClient_AddPublicationKind_driverRegistrationError_failsAwait(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
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

			resultCh := startClientAddAsync(client, testCase.add)

			pollDeadline := time.Now().Add(time.Second)
			responded, correlationID := pollAndRespondToAddCommandError(
				t,
				fixture,
				testCase.addMsgType,
				testCase.decodeCorrelationID,
				pollDeadline,
				11,
				testCase.driverRejectMessage,
			)
			require.True(t, responded, testCase.ringRespondTimeoutMsg)

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.clientResultTimeoutMsg,
			)
			require.Nil(t, got.value)
			var registrationErr *RegistrationError
			require.ErrorAs(t, got.err, &registrationErr)
			require.Equal(t, registrationKindFor(testCase.pendingKind), registrationErr.Kind)
			require.Equal(t, correlationID, registrationErr.CorrelationID)
			require.Equal(t, int32(11), registrationErr.ErrorCode)
			require.Equal(t, testCase.driverRejectMessage, registrationErr.Message)
			var driverErr *conductor.DriverRegistrationError
			require.ErrorAs(t, registrationErr.Cause, &driverErr)
			require.EqualError(
				t,
				got.err,
				fmt.Sprintf(
					"aeron: driver registration error: correlation=%d code=11 message=%s",
					correlationID,
					testCase.driverRejectMessage,
				),
			)
			require.Equal(t, 0, client.PublicationCountForTest())
			_, ok := fixture.Conductor.PendingResult(correlationID)
			require.False(t, ok)
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_AddPublicationKind_offerPhaseTimeout(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			nanoNs := testutil.TestStartNanoNs
			dir := t.TempDir()
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				0,
				time.Microsecond,
				dir,
			)

			pub, err := testCase.add(client)
			require.Nil(t, pub)
			requireAddPhaseTimeout(
				t,
				fixture,
				client,
				err,
				registrationKindFor(testCase.pendingKind),
				(*Client).PublicationCountForTest,
				true,
			)
			requireClientAddRingNoCommittedCommands(t, fixture.Ring)
		})
	}
}

func TestClient_AddPublicationKind_pollPhaseTimeout(t *testing.T) {
	const timeout = 5 * time.Millisecond
	startNs := testutil.TestStartNanoNs

	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			clock := newClientAddTestClock(startNs, timeout)
			dir := t.TempDir()
			client, fixture := newClientAddFixture(
				t,
				clock.NanoClock(),
				timeout,
				time.Microsecond,
				dir,
			)

			resultCh := startClientAddAsync(client, testCase.add)

			testCase.waitForCommand(t, fixture)
			clock.EnterDeadlinePhase()

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.clientResultTimeoutMsg,
			)
			require.Nil(t, got.value)
			requireAddPhaseTimeout(
				t,
				fixture,
				client,
				got.err,
				registrationKindFor(testCase.pendingKind),
				(*Client).PublicationCountForTest,
				false,
			)
		})
	}
}

func TestClient_Add_ringExhaustion_returnsRegistrationError(t *testing.T) {
	const timeout = time.Millisecond
	startNs := testutil.TestStartNanoNs

	type ringExhaustionCase struct {
		name             string
		dir              func(t *testing.T) string
		addCall          func(*Client) (any, error)
		registrationKind string
		countFn          func(*Client) int
	}

	pubCases := clientAddPublicationKindCases()
	tests := make([]ringExhaustionCase, 0, len(pubCases)+1)
	for _, pubCase := range pubCases {
		tests = append(tests, ringExhaustionCase{
			name:             pubCase.name,
			dir:              clientAddMediaDir,
			addCall:          pubCase.add,
			registrationKind: registrationKindFor(pubCase.pendingKind),
			countFn:          (*Client).PublicationCountForTest,
		})
	}
	tests = append(tests, ringExhaustionCase{
		name: "subscription",
		dir:  clientAddMediaDir,
		addCall: func(client *Client) (any, error) {
			return client.AddSubscription(clientAddStreamID, clientAddChannel)
		},
		registrationKind: registrationKindSubscription,
		countFn:          (*Client).SubscriptionCountForTest,
	})

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

func TestClient_AddPublicationKind_ringBackpressureRetryThenSuccess(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			clock := newClientAddTestClock(0, time.Second)
			dir, logPath := testCase.setupLog(t)
			client, fixture := newClientAddFixture(
				t,
				clock.NanoClock(),
				time.Second,
				time.Millisecond,
				dir,
			)
			fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))
			seedFullClientAddRing(t, fixture.Ring)

			resultCh := startClientAddAsync(client, testCase.add)

			waitForClientAddClockReads(clock, 4)
			resetClientAddRingEmpty(fixture.Ring)

			pollDeadline := time.Now().Add(time.Second)
			responded, _ := testCase.pollRespond(t, fixture, logPath, pollDeadline, nil)
			require.True(t, responded, testCase.ringRespondTimeoutMsg)

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.clientResultTimeoutMsg,
			)
			require.NoError(t, got.err)
			require.NotNil(t, got.value)
			require.Equal(t, 1, client.PublicationCountForTest())
			requireActiveCorrelationCleared(t, fixture)
		})
	}
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

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddSubscription(clientAddStreamID, clientAddChannel)
	})

	waitForClientAddClockReads(clock, 4)
	resetClientAddRingEmpty(fixture.Ring)

	pollDeadline := time.Now().Add(time.Second)
	responded, _ := pollAndRespondToAddSubscriptionCommand(t, fixture, pollDeadline, nil)
	require.True(t, responded, "timed out waiting for AddSubscription command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		clientConcurrentResultTimeout,
		"timed out waiting for AddSubscription client result",
	)
	require.NoError(t, got.err)
	require.NotNil(t, got.value)
	require.Equal(t, 1, client.SubscriptionCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_AddPublicationKind_terminationDuringOfferRetry(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
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

			resultCh := startClientAddAsync(client, testCase.add)

			waitForClientAddClockReads(clock, 4)
			requireAddCommandNotOnRing(t, fixture, testCase.addMsgType)

			correlationID := fixture.Conductor.ActiveCorrelationID()
			require.NotEqual(t, conductor.NullCorrelationID, correlationID)
			_, ok := fixture.Conductor.PendingResult(correlationID)
			require.True(t, ok, "expected pending registration during offer-phase backpressure")

			fixture.Ring.SetConsumerHeartbeatTime(conductor.NullCorrelationID)
			clock.SetNanoNs(testutil.TestWorkNanoNs)

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.clientResultTimeoutMsg,
			)
			require.Nil(t, got.value)
			requireDriverShutdownTimeout(t, got.err)
			_, ok = fixture.Conductor.PendingResult(correlationID)
			require.False(t, ok)
			require.Equal(t, 0, client.PublicationCountForTest())
			requireActiveCorrelationCleared(t, fixture)
		})
	}
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

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddSubscription(clientAddStreamID, clientAddChannel)
	})

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
		clientConcurrentResultTimeout,
		"timed out waiting for AddSubscription client result",
	)
	require.Nil(t, got.value)
	requireDriverShutdownTimeout(t, got.err)
	_, ok = fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	require.Equal(t, 0, client.SubscriptionCountForTest())
	requireActiveCorrelationCleared(t, fixture)
}

type clientAddTerminationCase struct {
	name    string
	dir     func(t *testing.T) string
	addCall func(*Client) (any, error)
}

func clientAddTerminationCases() []clientAddTerminationCase {
	pubCases := clientAddPublicationKindCases()
	cases := make([]clientAddTerminationCase, 0, len(pubCases)+1)
	for _, pubCase := range pubCases {
		cases = append(cases, clientAddTerminationCase{
			name:    pubCase.name,
			dir:     clientAddMediaDir,
			addCall: pubCase.add,
		})
	}
	return append(cases, clientAddTerminationCase{
		name: "subscription",
		dir:  clientAddMediaDir,
		addCall: func(client *Client) (any, error) {
			return client.AddSubscription(clientAddStreamID, clientAddChannel)
		},
	})
}

func TestClient_Add_terminationCause(t *testing.T) {
	for _, testCase := range clientAddTerminationCases() {
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
	for _, testCase := range clientAddTerminationCases() {
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

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddSubscription(clientAddStreamID, clientAddChannel)
	})

	pollDeadline := time.Now().Add(time.Second)
	responded, correlationID := pollAndRespondToAddSubscriptionCommand(t, fixture, pollDeadline, nil)
	require.True(t, responded, "timed out waiting for AddSubscription command on ring")

	got := receiveAddResult(
		t,
		resultCh,
		clientConcurrentResultTimeout,
		"timed out waiting for AddSubscription client result",
	)
	require.NoError(t, got.err)
	sub, ok := got.value.(*Subscription)
	require.True(t, ok)
	require.NotNil(t, sub)
	require.Equal(t, correlationID, sub.RegistrationID())
	lookup, ok := client.Subscription(correlationID)
	require.True(t, ok)
	require.Same(t, sub, lookup)
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

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddSubscription(clientAddStreamID, clientAddChannel)
	})

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
		clientConcurrentResultTimeout,
		"timed out waiting for AddSubscription client result",
	)
	require.Nil(t, got.value)
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

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddSubscription(clientAddStreamID, clientAddChannel)
	})

	waitForAddSubscriptionCommand(t, fixture)
	clock.EnterDeadlinePhase()

	got := receiveAddResult(
		t,
		resultCh,
		clientConcurrentResultTimeout,
		"timed out waiting for AddSubscription client result",
	)
	require.Nil(t, got.value)
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
			name:  "exclusivePublicationConductorTerminating",
			setup: setupClientAddConductorTerminating,
			addCall: func(client *Client) error {
				_, err := client.AddExclusivePublication(clientAddStreamID, clientAddChannel)
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
				requireRegisterClientAddPending(
					t,
					fixture,
					conductor.PendingKindAddPublication,
					pendingCorrelationID,
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
			name: "exclusivePublicationDuplicatePending",
			setup: func(t *testing.T, fixture *testutil.ConductorFixture, _ func(int64)) {
				requireRegisterClientAddPending(
					t,
					fixture,
					conductor.PendingKindAddExclusivePublication,
					pendingCorrelationID,
				)
			},
			addCall: func(client *Client) error {
				_, err := client.AddExclusivePublication(clientAddStreamID, clientAddChannel)
				return err
			},
			wantErr: ErrPendingAlreadyRegistered,
			wantMsg: "aeron: correlation already pending",
		},
		{
			name: "subscriptionDuplicatePending",
			setup: func(t *testing.T, fixture *testutil.ConductorFixture, _ func(int64)) {
				requireRegisterClientAddPending(
					t,
					fixture,
					conductor.PendingKindAddSubscription,
					pendingCorrelationID,
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

func TestClient_Add_duplicatePendingAtNullCorrelationID_preservesExisting(t *testing.T) {
	const existingCorrelationID int64 = 0

	runCase := func(
		t *testing.T,
		pendingKind conductor.PendingKind,
		add func(*Client) (any, error),
	) {
		t.Helper()

		var nanoNs int64
		client, fixture := newClientAddFixture(
			t,
			func() int64 { return nanoNs },
			time.Second,
			time.Millisecond,
			t.TempDir(),
		)
		requireRegisterClientAddPending(t, fixture, pendingKind, existingCorrelationID)

		_, err := add(client)
		require.ErrorIs(t, err, ErrPendingAlreadyRegistered)
		require.EqualError(t, err, "aeron: correlation already pending")

		_, ok := fixture.Conductor.PendingResult(existingCorrelationID)
		require.True(t, ok)
		requireActiveCorrelationCleared(t, fixture)
	}

	for _, pubCase := range clientAddPublicationKindCases() {
		t.Run(pubCase.name, func(t *testing.T) {
			runCase(t, pubCase.pendingKind, pubCase.add)
		})
	}
	t.Run("subscription", func(t *testing.T) {
		runCase(t, conductor.PendingKindAddSubscription, func(client *Client) (any, error) {
			return client.AddSubscription(clientAddStreamID, clientAddChannel)
		})
	})
}

func TestClient_OfferAdd_dupRegisterFailurePreservesPendingAtZero(t *testing.T) {
	const (
		existingCorrelationID  int64 = 0
		conflictingCorrelation int64 = 1
	)

	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)

	correlationID, err := client.OfferAddSubscription(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	require.Equal(t, existingCorrelationID, correlationID)
	_, ok := fixture.Conductor.PendingResult(existingCorrelationID)
	require.True(t, ok)

	requireRegisterClientAddPending(
		t,
		fixture,
		conductor.PendingKindAddSubscription,
		conflictingCorrelation,
	)

	_, err = client.AddSubscription(clientAddStreamID, clientAddChannel)
	require.ErrorIs(t, err, ErrPendingAlreadyRegistered)
	require.EqualError(t, err, "aeron: correlation already pending")

	_, ok = fixture.Conductor.PendingResult(existingCorrelationID)
	require.True(t, ok)
	_, ok = fixture.Conductor.PendingResult(conflictingCorrelation)
	require.True(t, ok)
	requireActiveCorrelationCleared(t, fixture)
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

	exclusivePub, err := client.AddExclusivePublication(clientAddStreamID, clientAddChannel)
	require.Nil(t, exclusivePub)
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

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddSubscription(clientAddStreamID, clientAddChannel)
	})

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
		clientConcurrentResultTimeout,
		"timed out waiting for AddSubscription client result",
	)
	require.NoError(t, got.err)
	require.NotNil(t, got.value)
}

func TestClient_AddPublicationKind_pollPhaseLogMapFailure(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
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

			resultCh := startClientAddAsync(client, testCase.add)

			pollDeadline := time.Now().Add(time.Second)
			responded, correlationID := testCase.pollRespond(
				t,
				fixture,
				badLogPath,
				pollDeadline,
				nil,
			)
			require.True(t, responded, testCase.ringRespondTimeoutMsg)

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.clientResultTimeoutMsg,
			)
			require.Nil(t, got.value)
			var registrationErr *RegistrationError
			require.ErrorAs(t, got.err, &registrationErr)
			require.Equal(t, registrationKindFor(testCase.pendingKind), registrationErr.Kind)
			require.Equal(t, correlationID, registrationErr.CorrelationID)
			require.ErrorIs(t, got.err, conductor.ErrLogBufferMapFailed)
			require.EqualError(
				t,
				got.err,
				fmt.Sprintf(
					"aeron: %s registration error: correlation=%d: conductor: failed to map log buffer",
					testCase.registrationErrorLabel,
					correlationID,
				),
			)
			_, ok := fixture.Conductor.PendingResult(correlationID)
			require.False(t, ok)
			require.Equal(t, 0, client.PublicationCountForTest())
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_awaitPendingPoll_closedGuardAfterPollPending_addPublicationKind(t *testing.T) {
	for _, testCase := range clientAddPublicationKindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			dir, logPath := testCase.setupLog(t)
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
					testCase.pendingKind,
					correlationID,
					clientAddStreamID,
					clientAddChannel,
					deadlineNs,
				),
			)
			testCase.signalReady(
				fixture.Conductor,
				clientAddPublicationReadyEvent(correlationID, logPath),
			)
			require.Equal(t, 1, client.PublicationCountForTest())

			_, completed, err := client.AwaitPendingPollOneTickForTest(correlationID, true, func() {
				client.PublishClosedForTest()
			})
			require.True(t, completed)
			requireErrClientClosed(t, err)
			require.Equal(t, 0, client.PublicationCountForTest())
		})
	}
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
	requireErrClientClosed(t, err)
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

func TestClient_ExclusivePublication_lookup_wrongTypeHandle(t *testing.T) {
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

	pub, ok := client.ExclusivePublication(registrationID)
	require.Nil(t, pub)
	require.False(t, ok)
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
			name: "exclusivePublication",
			lookup: func(client *Client) (any, bool) {
				pub, ok := client.ExclusivePublication(clientAddPublicationRegistrationID)
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
		{name: "utf8 multibyte", channel: "\u00fc"},
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
			name: "exclusivePublication",
			addCall: func(client *Client, channel string) (any, error) {
				return client.AddExclusivePublication(clientAddStreamID, channel)
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
			name: "exclusivePublication",
			addCall: func(client *Client) (any, error) {
				return client.AddExclusivePublication(clientAddStreamID, "")
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

	resultCh := startClientAddAsync(client, func(client *Client) (any, error) {
		return client.AddPublication(clientAddStreamID, clientAddChannel)
	})

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
		clientConcurrentResultTimeout,
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

	exclusivePub, err := client.AddExclusivePublication(clientAddStreamID, clientAddChannel)
	require.Nil(t, exclusivePub)
	require.ErrorIs(t, err, ErrClientClosed)

	lookupPub, ok := client.Publication(clientAddPublicationRegistrationID)
	require.Nil(t, lookupPub)
	require.False(t, ok)

	lookupSub, ok := client.Subscription(99)
	require.Nil(t, lookupSub)
	require.False(t, ok)

	lookupExclusive, ok := client.ExclusivePublication(clientAddPublicationRegistrationID)
	require.Nil(t, lookupExclusive)
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
			name: "exclusivePublicationNil",
			run: func(pending conductor.PendingRegistration) (any, error) {
				return exclusivePublicationFromPending(pending)
			},
			wantMsg: "aeron: exclusive publication handle is nil",
		},
		{
			name: "exclusivePublicationWrongType",
			run: func(pending conductor.PendingRegistration) (any, error) {
				pending.Publication = "not-an-exclusive-publication"
				return exclusivePublicationFromPending(pending)
			},
			wantMsg: "aeron: exclusive publication handle string is not *ExclusivePublication",
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

	exclusivePub, err := client.AddExclusivePublication(clientAddStreamID, clientAddChannel)
	require.Nil(t, exclusivePub)
	require.ErrorIs(t, err, ErrClientNotReady)

	sub, err := client.AddSubscription(clientAddStreamID, clientAddChannel)
	require.Nil(t, sub)
	require.ErrorIs(t, err, ErrClientNotReady)

	lookupPub, ok := client.Publication(clientAddPublicationRegistrationID)
	require.Nil(t, lookupPub)
	require.False(t, ok)

	lookupExclusive, ok := client.ExclusivePublication(clientAddPublicationRegistrationID)
	require.Nil(t, lookupExclusive)
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
