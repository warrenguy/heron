// Concurrent add tests cover three scenarios:
// 1. Close during poll-phase await (concurrentCloseDuringPoll).
// 2. External DoWork hammer during blocking await (concurrentDoWorkDuringBlockingAwait).
// 3. Close during offer-phase ring backpressure (concurrentCloseDuringOfferBackpressure).
//
// Hammer and close-during-await paths assert doWorkLocked vs DoWork:
// locked await/offer/drain paths must call doWorkLocked to avoid self-deadlock.
package aeron

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

type addConcurrentResult struct {
	handle any
	err    error
}

func startConcurrentAddCall(
	client *Client,
	addCall func(*Client) (any, error),
) <-chan addConcurrentResult {
	resultCh := make(chan addConcurrentResult, 1)
	go func() {
		handle, err := addCall(client)
		resultCh <- addConcurrentResult{handle: handle, err: err}
	}()
	return resultCh
}

func requireAddClosedResult(t *testing.T, got addConcurrentResult) {
	t.Helper()

	require.Nil(t, got.handle)
	requireErrClientClosed(t, got.err)
}

type awaitPollSleepingWaiter struct {
	notify       chan struct{}
	blocked      chan struct{}
	proceed      chan struct{}
	blockProceed atomic.Bool
}

func newAwaitPollSleepingWaiter(
	t *testing.T,
	client *Client,
) *awaitPollSleepingWaiter {
	t.Helper()

	waiter := &awaitPollSleepingWaiter{
		notify:  make(chan struct{}, 1),
		blocked: make(chan struct{}),
		proceed: make(chan struct{}, 1),
	}
	client.SetAwaitPollSleepingHookForTest(func() {
		select {
		case waiter.notify <- struct{}{}:
		default:
		}
		if waiter.blockProceed.CompareAndSwap(true, false) {
			waiter.blocked <- struct{}{}
			<-waiter.proceed
		}
	})
	t.Cleanup(func() {
		waiter.releaseHook()
		client.SetAwaitPollSleepingHookForTest(nil)
	})
	return waiter
}

func (waiter *awaitPollSleepingWaiter) armBlock() {
	waiter.drainProceed()
	waiter.blockProceed.Store(true)
}

func (waiter *awaitPollSleepingWaiter) drainProceed() {
	select {
	case <-waiter.proceed:
	default:
	}
}

func (waiter *awaitPollSleepingWaiter) releaseHook() {
	select {
	case waiter.proceed <- struct{}{}:
	default:
	}
}

func (waiter *awaitPollSleepingWaiter) drainNotifications() {
	for {
		select {
		case <-waiter.notify:
		default:
			return
		}
	}
}

func (waiter *awaitPollSleepingWaiter) waitForSleeping(t *testing.T) {
	t.Helper()

	select {
	case <-waiter.notify:
	case <-time.After(clientConcurrentResultTimeout):
		t.Fatal("timed out waiting for await poll sleep under clientLock")
	}
}

func (waiter *awaitPollSleepingWaiter) waitForBlocked(t *testing.T) {
	t.Helper()

	select {
	case <-waiter.blocked:
	case <-time.After(clientConcurrentResultTimeout):
		t.Fatal("timed out waiting for blocked await poll sleep under clientLock")
	}
}

// pinUnderClientLock holds the await poll loop in sleep under clientLock until
// releaseHook. Call after the add command is on the ring so the poll loop is
// active. The caller starts Close or other probes while the await holds the
// lock, then calls releaseHook.
func (waiter *awaitPollSleepingWaiter) pinUnderClientLock(t *testing.T) {
	t.Helper()

	waiter.drainNotifications()
	waiter.waitForSleeping(t)
	waiter.armBlock()
	waiter.waitForBlocked(t)
}

type doWorkHammer struct {
	ticks                atomic.Int32
	pauseTicksAtRegister int32
	pauseCheckActive     atomic.Bool
	lockTakenDuringPause atomic.Bool
	stop                 chan struct{}
	awaitPollSleeping    *awaitPollSleepingWaiter
}

func newDoWorkHammer(t *testing.T, client *Client) *doWorkHammer {
	t.Helper()

	hammer := &doWorkHammer{
		stop: make(chan struct{}),
	}
	hammer.awaitPollSleeping = newAwaitPollSleepingWaiter(t, client)
	var stopOnce sync.Once
	t.Cleanup(func() {
		stopOnce.Do(func() { close(hammer.stop) })
	})
	go func() {
		for {
			select {
			case <-hammer.stop:
				return
			default:
				if !client.TryLockClientForTest() {
					runtime.Gosched()
					continue
				}
				if hammer.pauseCheckActive.Load() {
					hammer.lockTakenDuringPause.Store(true)
				}
				client.DoWorkLockedForTest()
				client.UnlockClientForTest()
				hammer.ticks.Add(1)
			}
		}
	}()
	return hammer
}

func (hammer *doWorkHammer) onCommandSeenPauseCheck(t *testing.T) func(int64) {
	t.Helper()

	return func(_ int64) {
		hammer.awaitPollSleeping.drainNotifications()
		hammer.awaitPollSleeping.waitForSleeping(t)

		hammer.lockTakenDuringPause.Store(false)
		hammer.pauseCheckActive.Store(true)
		defer hammer.pauseCheckActive.Store(false)

		hammer.awaitPollSleeping.armBlock()
		defer hammer.awaitPollSleeping.releaseHook()

		hammer.awaitPollSleeping.waitForBlocked(t)
		require.False(
			t,
			hammer.lockTakenDuringPause.Load(),
			"external DoWork interleaved during blocking await",
		)
		time.Sleep(concurrentAwaitHammerCheckDuration)
		require.False(
			t,
			hammer.lockTakenDuringPause.Load(),
			"external DoWork interleaved during blocking await",
		)
		hammer.pauseTicksAtRegister = hammer.ticks.Load()
	}
}

func (hammer *doWorkHammer) requireResumedAfterAwait(t *testing.T) {
	t.Helper()

	require.Eventually(
		t,
		func() bool {
			return hammer.ticks.Load() > hammer.pauseTicksAtRegister
		},
		clientConcurrentResultTimeout,
		time.Microsecond,
		"hammer did not resume after await",
	)
}

type addConcurrentRespondFunc func(
	*testing.T,
	*testutil.ConductorFixture,
	string,
	time.Time,
	func(int64),
) (bool, int64)

func TestClient_Add_concurrentCloseDuringPoll(t *testing.T) {
	tests := []struct {
		name             string
		addCall          func(*Client) (any, error)
		waitCmd          func(*testing.T, *testutil.ConductorFixture) int64
		conductorCountFn func(*testutil.ConductorFixture) int
		timeoutMsg       string
	}{
		{
			name: "publication",
			addCall: func(client *Client) (any, error) {
				return client.AddPublication(clientAddStreamID, clientAddChannel)
			},
			waitCmd: waitForAddPublicationCommand,
			conductorCountFn: func(fixture *testutil.ConductorFixture) int {
				return conductor.PublicationCountForTest(fixture.Conductor)
			},
			timeoutMsg: "timed out waiting for AddPublication client result",
		},
		{
			name: "subscription",
			addCall: func(client *Client) (any, error) {
				return client.AddSubscription(clientAddStreamID, clientAddChannel)
			},
			waitCmd: waitForAddSubscriptionCommand,
			conductorCountFn: func(fixture *testutil.ConductorFixture) int {
				return conductor.SubscriptionCountForTest(fixture.Conductor)
			},
			timeoutMsg: "timed out waiting for AddSubscription client result",
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
				t.TempDir(),
			)
			waiter := newAwaitPollSleepingWaiter(t, client)

			resultCh := startConcurrentAddCall(client, testCase.addCall)

			correlationID := testCase.waitCmd(t, fixture)

			waiter.pinUnderClientLock(t)

			closeDone := make(chan error, 1)
			go func() {
				closeDone <- client.Close()
			}()

			requireClosedWhileAwaitPinned(t, client)
			waiter.releaseHook()

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.timeoutMsg,
			)
			requireAddClosedResult(t, got)
			_, ok := fixture.Conductor.PendingResult(correlationID)
			require.False(t, ok)
			require.Equal(t, 0, testCase.conductorCountFn(fixture))
			requireActiveCorrelationCleared(t, fixture)

			closeErr := receiveCloseResult(
				t,
				client,
				closeDone,
				clientConcurrentResultTimeout,
				"timed out waiting for Close after poll-phase await",
			)
			require.NoError(t, closeErr)
			require.Contains(t, testutil.CommittedRecordTypeIDs(fixture.Ring), conductor.ClientClose)
			require.Zero(t, client.DoWork())
		})
	}
}

func TestClient_Add_concurrentDoWorkDuringBlockingAwait(t *testing.T) {
	tests := []struct {
		name             string
		addCall          func(*Client) (any, error)
		respond          addConcurrentRespondFunc
		conductorCountFn func(*Client) int
		timeoutMsg       string
	}{
		{
			name: "publication",
			addCall: func(client *Client) (any, error) {
				return client.AddPublication(clientAddStreamID, clientAddChannel)
			},
			respond: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				logPath string,
				deadline time.Time,
				onCommandSeen func(int64),
			) (bool, int64) {
				fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))
				return pollAndRespondToAddPublicationCommand(t, fixture, logPath, deadline, onCommandSeen)
			},
			conductorCountFn: (*Client).PublicationCountForTest,
			timeoutMsg:       "timed out waiting for AddPublication client result",
		},
		{
			name: "subscription",
			addCall: func(client *Client) (any, error) {
				return client.AddSubscription(clientAddStreamID, clientAddChannel)
			},
			respond: func(
				t *testing.T,
				fixture *testutil.ConductorFixture,
				_ string,
				deadline time.Time,
				onCommandSeen func(int64),
			) (bool, int64) {
				return pollAndRespondToAddSubscriptionCommand(t, fixture, deadline, onCommandSeen)
			},
			conductorCountFn: (*Client).SubscriptionCountForTest,
			timeoutMsg:       "timed out waiting for AddSubscription client result",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			dir, logPath := setupClientAddPublicationLog(t)
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Microsecond,
				dir,
			)

			hammer := newDoWorkHammer(t, client)
			resultCh := startConcurrentAddCall(client, testCase.addCall)

			responded, _ := testCase.respond(
				t,
				fixture,
				logPath,
				time.Now().Add(testutil.TestDriverTimeout),
				hammer.onCommandSeenPauseCheck(t),
			)
			require.True(t, responded, "timed out waiting for add command response on ring")

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.timeoutMsg,
			)
			require.NoError(t, got.err)
			require.NotNil(t, got.handle)
			require.Equal(t, 1, testCase.conductorCountFn(client))
			hammer.requireResumedAfterAwait(t)
			requireActiveCorrelationCleared(t, fixture)
		})
	}
}

func TestClient_Add_concurrentCloseDuringOfferBackpressure(t *testing.T) {
	tests := []struct {
		name             string
		addCall          func(*Client) (any, error)
		addCommandTypeID int
		conductorCountFn func(*testutil.ConductorFixture) int
		timeoutMsg       string
	}{
		{
			name: "publication",
			addCall: func(client *Client) (any, error) {
				return client.AddPublication(clientAddStreamID, clientAddChannel)
			},
			addCommandTypeID: conductor.AddPublication,
			conductorCountFn: func(fixture *testutil.ConductorFixture) int {
				return conductor.PublicationCountForTest(fixture.Conductor)
			},
			timeoutMsg: "timed out waiting for AddPublication client result",
		},
		{
			name: "subscription",
			addCall: func(client *Client) (any, error) {
				return client.AddSubscription(clientAddStreamID, clientAddChannel)
			},
			addCommandTypeID: conductor.AddSubscription,
			conductorCountFn: func(fixture *testutil.ConductorFixture) int {
				return conductor.SubscriptionCountForTest(fixture.Conductor)
			},
			timeoutMsg: "timed out waiting for AddSubscription client result",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			clock := newClientAddTestClock(0, time.Second)
			client, fixture := newClientAddFixture(
				t,
				clock.NanoClock(),
				time.Second,
				time.Microsecond,
				t.TempDir(),
			)
			seedFullClientAddRing(t, fixture.Ring)

			resultCh := startConcurrentAddCall(client, testCase.addCall)

			waitForClientAddClockReads(t, clock, 4)
			requireAddCommandNotOnRing(t, fixture, testCase.addCommandTypeID)

			correlationID := fixture.Conductor.ActiveCorrelationID()
			require.NotEqual(t, conductor.NullCorrelationID, correlationID)
			_, ok := fixture.Conductor.PendingResult(correlationID)
			require.True(t, ok, "expected pending registration during offer-phase backpressure")

			closeDone := make(chan error, 1)
			go func() {
				closeDone <- client.Close()
			}()

			got := receiveAddResult(
				t,
				resultCh,
				clientConcurrentResultTimeout,
				testCase.timeoutMsg,
			)
			requireAddClosedResult(t, got)

			_, ok = fixture.Conductor.PendingResult(correlationID)
			require.False(t, ok)
			require.Equal(t, 0, testCase.conductorCountFn(fixture))
			requireActiveCorrelationCleared(t, fixture)

			closeErr := receiveCloseResult(
				t,
				client,
				closeDone,
				clientConcurrentResultTimeout,
				"timed out waiting for Close after offer backpressure await",
			)
			require.NoError(t, closeErr)
			require.Zero(t, client.DoWork())
		})
	}
}
