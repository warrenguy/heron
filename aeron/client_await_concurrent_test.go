package aeron

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func TestClient_NextAvailableSessionID_concurrentDoWorkDuringBlockingAwait(t *testing.T) {
	var nanoNs int64
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Microsecond,
	)

	const wantSessionID int32 = 88

	hammer := newDoWorkHammer(t, client)

	type result struct {
		sessionID int32
		err       error
	}
	resultCh := make(chan result, 1)
	go func() {
		sessionID, err := client.NextAvailableSessionID(clientAwaitTestStreamID)
		resultCh <- result{sessionID: sessionID, err: err}
	}()

	require.True(
		t,
		pollAndRespondToNextAvailableSessionIDCommand(
			t,
			fixture,
			wantSessionID,
			time.Now().Add(testutil.TestDriverTimeout),
			hammer.onCommandSeenPauseCheck(t),
		),
		"timed out waiting for NextAvailableSessionID response on ring",
	)

	got := receiveAddResult(
		t,
		resultCh,
		clientConcurrentResultTimeout,
		"timed out waiting for NextAvailableSessionID client result",
	)
	require.NoError(t, got.err)
	require.Equal(t, wantSessionID, got.sessionID)
	hammer.requireResumedAfterAwait(t)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_NextAvailableSessionID_concurrentCloseDuringPoll(t *testing.T) {
	var nanoNs int64
	client, fixture := newClientAwaitFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
	)

	type result struct {
		sessionID int32
		err       error
	}
	resultCh := make(chan result, 1)
	go func() {
		sessionID, err := client.NextAvailableSessionID(clientAwaitTestStreamID)
		resultCh <- result{sessionID: sessionID, err: err}
	}()

	correlationID := waitForNextAvailableSessionIDCommand(t, fixture)
	require.NoError(t, client.Close())

	got := receiveAddResult(
		t,
		resultCh,
		clientConcurrentResultTimeout,
		"timed out waiting for NextAvailableSessionID client result",
	)
	require.Zero(t, got.sessionID)
	requireErrClientClosed(t, got.err)
	_, ok := fixture.Conductor.PendingResult(correlationID)
	require.False(t, ok)
	requireActiveCorrelationCleared(t, fixture)
	require.Contains(t, testutil.CommittedRecordTypeIDs(fixture.Ring), conductor.ClientClose)
	require.Zero(t, client.DoWork())
}

func TestClient_blockingCommands_serializeWithoutDeadlock(t *testing.T) {
	var nanoNs int64
	dir := t.TempDir()
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		dir,
	)

	const wantSessionID int32 = 55

	logPath := filepath.Join(dir, "serialize.log")
	WriteTestLogFileForTest(t, logPath)
	fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))

	type addResult struct {
		pub *Publication
		err error
	}
	addCh := make(chan addResult, 1)
	go func() {
		pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
		addCh <- addResult{pub: pub, err: err}
	}()

	type sessionResult struct {
		sessionID int32
		err       error
	}
	sessionCh := make(chan sessionResult, 1)
	var startSessionOnce sync.Once
	startSession := func() {
		startSessionOnce.Do(func() {
			go func() {
				sessionID, err := client.NextAvailableSessionID(clientAwaitTestStreamID)
				sessionCh <- sessionResult{sessionID: sessionID, err: err}
			}()
		})
	}

	responded, _ := pollAndRespondToAddPublicationCommand(
		t,
		fixture,
		logPath,
		time.Now().Add(testutil.TestDriverTimeout),
		func(int64) {
			require.NotContains(
				t,
				testutil.CommittedRecordTypeIDs(fixture.Ring),
				conductor.GetNextAvailableSessionID,
				"NextAvailableSessionID reached ring before AddPublication completed",
			)
			startSession()
		},
	)
	require.True(t, responded, "timed out waiting for AddPublication response")

	addGot := receiveAddResult(
		t,
		addCh,
		clientConcurrentResultTimeout,
		"timed out waiting for AddPublication client result",
	)
	require.NoError(t, addGot.err)
	require.NotNil(t, addGot.pub)

	require.True(
		t,
		pollAndRespondToNextAvailableSessionIDCommand(
			t,
			fixture,
			wantSessionID,
			time.Now().Add(testutil.TestDriverTimeout),
			nil,
		),
		"timed out waiting for NextAvailableSessionID response",
	)

	sessionGot := receiveAddResult(
		t,
		sessionCh,
		clientConcurrentResultTimeout,
		"timed out waiting for NextAvailableSessionID client result",
	)
	require.NoError(t, sessionGot.err)
	require.Equal(t, wantSessionID, sessionGot.sessionID)
	requireActiveCorrelationCleared(t, fixture)
}

func TestClient_Close_twoPhasePublishedBeforeTeardown(t *testing.T) {
	var nanoNs int64
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		t.TempDir(),
	)
	waiter := newAwaitPollSleepingWaiter(t, client)

	type result struct {
		pub *Publication
		err error
	}
	addCh := make(chan result, 1)
	go func() {
		pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
		addCh <- result{pub: pub, err: err}
	}()

	waitForAddPublicationCommand(t, fixture)

	waiter.pinUnderClientLock(t)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- client.Close()
	}()

	requireClosedWhileAwaitPinned(t, client)
	waiter.releaseHook()

	addGot := receiveAddResult(
		t,
		addCh,
		clientConcurrentResultTimeout,
		"timed out waiting for AddPublication client result",
	)
	require.Nil(t, addGot.pub)
	requireErrClientClosed(t, addGot.err)

	closeErr := receiveCloseResult(
		t,
		client,
		closeDone,
		clientConcurrentResultTimeout,
		"timed out waiting for Close after await released lock",
	)
	require.NoError(t, closeErr)
	requireActiveCorrelationCleared(t, fixture)
	require.Contains(t, testutil.CommittedRecordTypeIDs(fixture.Ring), conductor.ClientClose)
}

func TestClient_IsTerminating_concurrentCloseDuringAwait(t *testing.T) {
	var nanoNs int64
	client, fixture := newClientAddFixture(
		t,
		func() int64 { return nanoNs },
		time.Second,
		time.Millisecond,
		t.TempDir(),
	)
	waiter := newAwaitPollSleepingWaiter(t, client)

	type result struct {
		pub *Publication
		err error
	}
	addCh := make(chan result, 1)
	go func() {
		pub, err := client.AddPublication(clientAddStreamID, clientAddChannel)
		addCh <- result{pub: pub, err: err}
	}()

	waitForAddPublicationCommand(t, fixture)

	waiter.pinUnderClientLock(t)

	stop := make(chan struct{})
	var hammerWG sync.WaitGroup
	hammerWG.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				client.IsTerminating()
			}
		}
	})

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- client.Close()
	}()

	requireClosedWhileAwaitPinned(t, client)
	waiter.releaseHook()

	addGot := receiveAddResult(
		t,
		addCh,
		clientConcurrentResultTimeout,
		"timed out waiting for AddPublication client result",
	)
	require.Nil(t, addGot.pub)
	requireErrClientClosed(t, addGot.err)

	closeErr := receiveCloseResult(
		t,
		client,
		closeDone,
		clientConcurrentResultTimeout,
		"timed out waiting for Close after await released lock",
	)
	require.NoError(t, closeErr)

	close(stop)
	hammerWG.Wait()
	require.False(t, client.IsTerminating())
	requireActiveCorrelationCleared(t, fixture)
}
