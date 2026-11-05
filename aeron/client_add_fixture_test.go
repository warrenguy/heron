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

func newClientAddConfig(
	nanoClock func() int64,
	driverTimeout time.Duration,
	idleSleepDuration time.Duration,
	mediaDriverDir string,
) testutil.ConductorConfig {
	config := testutil.DefaultConductorConfig(nil)
	config.NanoClock = nanoClock
	config.DriverTimeout = driverTimeout
	config.IdleSleepDuration = idleSleepDuration
	config.ImageLifecycle = ConductorImageLifecycle
	config.ResourceLifecycle = ConductorResourceLifecycle
	config.MediaDriverDir = mediaDriverDir
	return config
}

func newClientAddFixture(
	tb testutil.TB,
	nanoClock func() int64,
	driverTimeout time.Duration,
	idleSleepDuration time.Duration,
	mediaDriverDir string,
) (*Client, *testutil.ConductorFixture) {
	tb.Helper()

	config := newClientAddConfig(nanoClock, driverTimeout, idleSleepDuration, mediaDriverDir)
	return newClientAddFixtureFromConfig(tb, config)
}

// newClientAddOfferRetryTerminationFixture builds a client fixture for offer-retry
// driver-shutdown tests.
//
// Sets KeepAliveInterval to 1ns. checkLiveness must run on the first service tick
// after the test advances the frozen nano clock.
//
// Default 500ms keep-alive under a frozen clock:
//   - may skip liveness until the clock advances again
//   - a frozen clock never advances
//   - the offer loop spins until the wall receive timeout
func newClientAddOfferRetryTerminationFixture(
	tb testutil.TB,
	nanoClock func() int64,
	driverTimeout time.Duration,
	idleSleepDuration time.Duration,
	mediaDriverDir string,
) (*Client, *testutil.ConductorFixture) {
	tb.Helper()

	config := newClientAddConfig(nanoClock, driverTimeout, idleSleepDuration, mediaDriverDir)
	config.KeepAliveInterval = time.Nanosecond
	return newClientAddFixtureFromConfig(tb, config)
}

func newClientAddFixtureFromConfig(
	tb testutil.TB,
	config testutil.ConductorConfig,
) (*Client, *testutil.ConductorFixture) {
	tb.Helper()

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
	config.ResourceLingerNs = connectConfig.ResourceLingerNs
	config.PublicationErrorFrameHandler = connectConfig.PublicationErrorFrameHandler
	return config
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
	clientConcurrentResultTimeout = 2 * time.Second
	// clientAddReadsBeforeFirstServiceTick counts the test clock reads before an
	// add's first conductor service tick: the conductor seed, the pending
	// deadline, the offer deadline, the offer loop check, the client seed, then
	// the tick. A test that advances the clock after this count knows the
	// client seeded its service timestamps at the start value.
	clientAddReadsBeforeFirstServiceTick = 6
	concurrentAwaitHammerCheckDuration   = 20 * time.Millisecond
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
		runtime.Gosched()
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

// clientLockFree reports whether clientLock is free. Tries TryLockClientForTest
// and unlocks immediately when the lock is free. Never blocks.
//
// Returns false when another holder owns the lock.
func clientLockFree(client *Client) bool {
	if !client.TryLockClientForTest() {
		return false
	}
	client.UnlockClientForTest()
	return true
}

// receiveCloseResult waits for a Close goroutine result on closeDone.
//
// On timeout, fails with closed, lockFree, and closeDone length diagnostics.
func receiveCloseResult(
	t *testing.T,
	client *Client,
	closeDone <-chan error,
	timeout time.Duration,
	timeoutMsg string,
) error {
	t.Helper()

	select {
	case err := <-closeDone:
		return err
	case <-time.After(timeout):
		lockFree := clientLockFree(client)
		require.FailNow(
			t,
			timeoutMsg,
			"closed=%v lockFree=%v closeDoneLen=%d",
			errors.Is(client.errIfClosed(), ErrClientClosed),
			lockFree,
			len(closeDone),
		)
		return nil
	}
}

// requireClosedWhileAwaitPinned asserts closed is set while pinUnderClientLock
// still holds clientLock. Does not receive from Close's result channel.
//
// Concurrency: polls on the test goroutine only. Fails if clientLock becomes free
// before closed is observed.
func requireClosedWhileAwaitPinned(t *testing.T, client *Client) {
	t.Helper()

	require.False(t, clientLockFree(client), "await pin should hold clientLock")

	deadline := time.Now().Add(clientConcurrentResultTimeout)
	for time.Now().Before(deadline) {
		closed := errors.Is(client.errIfClosed(), ErrClientClosed)
		if clientLockFree(client) {
			require.FailNow(t, "await pin lost clientLock", "closed=%v", closed)
		}
		if closed {
			return
		}
		runtime.Gosched()
	}

	require.FailNow(
		t,
		"closed state not observable while await pinned",
		"closed=%v lockFree=%v",
		errors.Is(client.errIfClosed(), ErrClientClosed),
		clientLockFree(client),
	)
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

func waitForClientAddClockReads(tb testing.TB, clock *clientAddTestClock, minReads int64) {
	tb.Helper()

	deadline := time.Now().Add(clientConcurrentResultTimeout)
	for clock.ClockReads() < minReads {
		if time.Now().After(deadline) {
			tb.Fatalf(
				"timed out waiting for clock reads: got %d, want >= %d",
				clock.ClockReads(),
				minReads,
			)
		}
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
		runtime.Gosched()
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
		runtime.Gosched()
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

func addPublicationIDs(pub any) (registrationID, originalRegistrationID int64) {
	handle, ok := pub.(interface {
		RegistrationID() int64
		OriginalRegistrationID() int64
	})
	if !ok {
		panic(fmt.Sprintf("unexpected publication type %T", pub))
	}
	return handle.RegistrationID(), handle.OriginalRegistrationID()
}

func addLiveClientPublication(
	t *testing.T,
	client *Client,
	fixture *testutil.ConductorFixture,
	logPath string,
) *Publication {
	t.Helper()

	fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))
	correlationID, err := client.OfferAddPublication(clientAddStreamID, clientAddChannel)
	require.NoError(t, err)
	conductor.PublicationReadyForTest(
		fixture.Conductor,
		clientAddPublicationReadyEvent(correlationID, logPath),
	)
	pub, done, err := client.PollAddPublication(correlationID)
	require.NoError(t, err)
	require.True(t, done)
	require.NotNil(t, pub)
	return pub
}

func requireClientAddRingNoCommittedCommands(
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
) {
	t.Helper()
	require.Zero(t, testutil.CommittedNonPaddingRecordCount(ring))
}
