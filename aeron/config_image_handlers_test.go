package aeron

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func requireImageHandlerEqual[T ~func(*Image)](tb testing.TB, want, got T) {
	tb.Helper()
	if want == nil {
		require.Nil(tb, got)
		return
	}
	require.Equal(tb, reflect.ValueOf(want).Pointer(), reflect.ValueOf(got).Pointer())
}

func representativeConfigForConnectConfigTest(tb testing.TB) Config {
	tb.Helper()

	return Config{
		AeronDir:            tb.TempDir(),
		DriverTimeout:       7 * time.Second,
		KeepAliveInterval:   100 * time.Millisecond,
		IdleSleepDuration:   5 * time.Millisecond,
		InterServiceTimeout: 2 * time.Second,
		CloseLingerDuration: 1500 * time.Millisecond,
		EpochClock:          func() int64 { return 42 },
		NanoClock:           func() int64 { return 99 },
		ErrorHandler:        func(error) {},
	}
}

func TestConductorConnectConfig_propagatesConfigScalarFields(t *testing.T) {
	t.Parallel()

	cfg := representativeConfigForConnectConfigTest(t)
	config := ConductorConnectConfigForTest(cfg, testutil.TestClientID)

	require.Equal(t, ConductorImageLifecycle, config.ImageLifecycle)
	require.NotNil(t, config.ResourceLifecycle)
	require.Equal(t, cfg.AeronDir, config.MediaDriverDir)
	require.Equal(
		t,
		cfg.CloseLingerDuration.Nanoseconds(),
		config.CloseLingerNs,
	)
	require.Equal(t, cfg.DriverTimeout, config.DriverTimeout)
	require.Equal(t, cfg.KeepAliveInterval, config.KeepAliveInterval)
	require.Equal(t, cfg.IdleSleepDuration, config.IdleSleepDuration)
	require.Equal(t, cfg.InterServiceTimeout, config.InterServiceTimeout)
	require.Equal(t, testutil.TestClientID, config.ClientID)
	require.Equal(t, int64(42), config.EpochClock())
	require.Equal(t, int64(99), config.NanoClock())
	require.NotNil(t, config.ErrorHandler)
}

func TestConductorConnectConfig_propagatesHandlersToPreSubscription(t *testing.T) {
	t.Parallel()

	availableHandler := AvailableImageHandler(func(*Image) {})
	unavailableHandler := UnavailableImageHandler(func(*Image) {})

	tests := []struct {
		name            string
		cfg             Config
		wantAvailable   AvailableImageHandler
		wantUnavailable UnavailableImageHandler
	}{
		{
			name: "both",
			cfg: Config{
				AvailableImageHandler:   availableHandler,
				UnavailableImageHandler: unavailableHandler,
			},
			wantAvailable:   availableHandler,
			wantUnavailable: unavailableHandler,
		},
		{
			name: "available only",
			cfg: Config{
				AvailableImageHandler: availableHandler,
			},
			wantAvailable: availableHandler,
		},
		{
			name: "unavailable only",
			cfg: Config{
				UnavailableImageHandler: unavailableHandler,
			},
			wantUnavailable: unavailableHandler,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := ConductorConnectConfigForTest(tt.cfg, testutil.TestClientID)
			require.Nil(t, config.ErrorHandler)
			sub := config.ResourceLifecycle.NewPreSubscription(
				TestSubscriptionRegistrationID,
				TestPublicationStreamID,
				clientAddChannel,
			)
			subscription, err := subscriptionFromHandle(sub)
			require.NoError(t, err)

			requireImageHandlerEqual(
				t,
				tt.wantAvailable,
				SubscriptionAvailableImageHandlerForTest(subscription),
			)
			requireImageHandlerEqual(
				t,
				tt.wantUnavailable,
				SubscriptionUnavailableImageHandlerForTest(subscription),
			)
		})
	}
}

func requireImageGoldenFields(tb testing.TB, image *Image) {
	tb.Helper()

	require.Equal(tb, conductorImageLifecycleTestCorrelationID, image.CorrelationID())
	require.Equal(
		tb,
		conductorImageLifecycleTestSubscriberCounter,
		image.SubscriberPositionID(),
	)
	require.Equal(tb, TestSubscriberPositionAtJoin, image.JoinPosition())
	require.Equal(
		tb,
		conductorImageLifecycleGoldenAvailableImage.SessionID,
		image.SessionID(),
	)
}

func requireImageHandlerIdentity(
	tb testing.TB,
	subscriptionImage *Image,
	handlerImage *Image,
) {
	tb.Helper()

	require.Same(tb, subscriptionImage, handlerImage)
	requireImageGoldenFields(tb, handlerImage)
}

func requireAvailableImageHandlerIdentity(
	tb testing.TB,
	sub *Subscription,
	attachedImage *Image,
) {
	tb.Helper()

	subscriptionImage, ok := SubscriptionImageAtIndexForTest(sub, 0)
	require.True(tb, ok)
	requireImageHandlerIdentity(tb, subscriptionImage, attachedImage)
}

func deliverAvailableImageToClientSubscription(
	tb testing.TB,
	client *Client,
	fixture *testutil.ConductorFixture,
	logPath string,
	correlationID int64,
) {
	tb.Helper()

	transmitAvailableImageToClientSubscription(tb, fixture, logPath, correlationID)
	require.Equal(tb, 1, client.DoWork())
}

// transmitAvailableImageToClientSubscription queues an available image event
// for correlationID. The next DoWork call delivers it.
func transmitAvailableImageToClientSubscription(
	tb testing.TB,
	fixture *testutil.ConductorFixture,
	logPath string,
	correlationID int64,
) {
	tb.Helper()

	fixture.Conductor.SetCountersReaderForTest(
		NewTestCountersReaderWithSubscriberPositionForTest(
			tb,
			TestSubscriberPositionAtJoin,
			correlationID,
		),
	)

	event := availableImageEventForConductorTest(logPath)
	event.SubscriptionRegistrationID = correlationID

	transmitConductorImageLifecycleEvent(
		tb,
		fixture.Conductor.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(event),
	)
}

func deliverUnavailableImageToClientSubscription(
	tb testing.TB,
	client *Client,
	adapter *conductor.DriverEventsAdapter,
	correlationID int64,
) {
	tb.Helper()

	transmitUnavailableImageToClientSubscription(tb, adapter, correlationID)
	require.Equal(tb, 1, client.DoWork())
}

func transmitUnavailableImageToClientSubscription(
	tb testing.TB,
	adapter *conductor.DriverEventsAdapter,
	correlationID int64,
) {
	tb.Helper()

	unavailable := conductorImageLifecycleGoldenUnavailableImage
	unavailable.SubscriptionRegistrationID = correlationID
	transmitConductorImageLifecycleEvent(
		tb,
		adapter,
		conductor.OnUnavailableImage,
		conductor.UnavailableImageBytes(unavailable),
	)
}

func TestClient_ConfigAvailableImageHandler_invoked(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	var attachedImage *Image
	cfg := Config{
		AeronDir: dir,
		AvailableImageHandler: func(image *Image) {
			attachedImage = image
		},
	}

	client, fixture := newClientAddFixtureWithConfig(t, cfg)

	correlationID, err := client.OfferAddSubscription(
		conductorImageLifecycleGoldenAvailableImage.StreamID,
		clientAddChannel,
	)
	require.NoError(t, err)

	deliverAvailableImageToClientSubscription(t, client, fixture, logPath, correlationID)

	sub, ok := client.Subscription(correlationID)
	require.True(t, ok)
	require.Equal(t, 1, sub.ImageCount())
	require.True(t, sub.IsConnected())
	require.NotNil(t, attachedImage)
	requireAvailableImageHandlerIdentity(t, sub, attachedImage)
}

type handlerCallEnv struct {
	t             *testing.T
	client        *Client
	fixture       *testutil.ConductorFixture
	correlationID int64
	logPath       string
	gotErrors     []error
}

// logDir selects AeronDir:
//   - true: the directory that holds the log file
//   - false: another temp directory. The log path sits outside
//     the media driver directory.
func startImageHandlerCall(
	t *testing.T,
	logDir bool,
	cfg func(env *handlerCallEnv) Config,
) *handlerCallEnv {
	t.Helper()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")
	env := &handlerCallEnv{t: t, logPath: logPath}
	config := cfg(env)
	if logDir {
		config.AeronDir = dir
	} else {
		config.AeronDir = t.TempDir()
	}
	env.client, env.fixture = newClientAddFixtureWithConfig(t, config)

	var err error
	env.correlationID, err = env.client.OfferAddSubscription(
		conductorImageLifecycleGoldenAvailableImage.StreamID,
		clientAddChannel,
	)
	require.NoError(t, err)
	return env
}

func (env *handlerCallEnv) transmitAvailable() {
	env.t.Helper()

	transmitAvailableImageToClientSubscription(
		env.t,
		env.fixture,
		env.logPath,
		env.correlationID,
	)
}

func (env *handlerCallEnv) doWork() int {
	env.t.Helper()

	var workCount int
	testutil.RequireReturnsWithin(env.t, clientConcurrentResultTimeout, func() {
		workCount = env.client.DoWork()
	})
	return workCount
}

func (env *handlerCallEnv) deliverAvailable() {
	env.t.Helper()

	env.transmitAvailable()
	require.Equal(env.t, 1, env.doWork())
}

func (env *handlerCallEnv) requireOneReentrant() {
	env.t.Helper()

	require.Len(env.t, env.gotErrors, 1)
	require.ErrorIs(env.t, env.gotErrors[0], ErrReentrantCall)
}

func (env *handlerCallEnv) requirePending(want bool) {
	env.t.Helper()

	_, pending := env.fixture.Conductor.PendingResult(env.correlationID)
	require.Equal(env.t, want, pending)
}

func (env *handlerCallEnv) boundSubscription() *Subscription {
	env.t.Helper()

	sub, ok := env.client.Subscription(env.correlationID)
	require.True(env.t, ok)
	BindClientForTest(sub, env.client)
	return sub
}

func handlerCallErrorHandler(env *handlerCallEnv) func(error) {
	return func(err error) {
		env.gotErrors = append(env.gotErrors, err)
	}
}

func startAvailableImageHandlerCall(
	t *testing.T,
	handler func(*handlerCallEnv),
) *handlerCallEnv {
	t.Helper()

	return startImageHandlerCall(t, true, func(env *handlerCallEnv) Config {
		return Config{
			ErrorHandler: handlerCallErrorHandler(env),
			AvailableImageHandler: func(*Image) {
				handler(env)
			},
		}
	})
}

func TestClient_handlerCallsBackIntoClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "availableImageHandlerCancelPending",
			run:  testAvailableImageHandlerCancelPending,
		},
		{
			name: "errorHandlerSubscriptionLookupOnImageError",
			run:  testErrorHandlerSubscriptionLookupOnImageError,
		},
		{
			name: "unavailableImageHandlerCancelPending",
			run:  testUnavailableImageHandlerCancelPending,
		},
		{
			name: "availableImageHandlerAddDestination",
			run:  testAvailableImageHandlerAddDestination,
		},
		{
			name: "errorHandlerCancelPending",
			run:  testErrorHandlerCancelPending,
		},
		{
			name: "availableImageHandlerDoWork",
			run:  testAvailableImageHandlerDoWork,
		},
		{
			name: "availableImageHandlerIsTerminating",
			run:  testAvailableImageHandlerIsTerminating,
		},
		{
			name: "availableImageHandlerClose",
			run:  testAvailableImageHandlerClose,
		},
		{
			name: "availableImageHandlerCounterClose",
			run:  testAvailableImageHandlerCounterClose,
		},
		{
			name: "availableImageHandlerClosedCounterClose",
			run:  testAvailableImageHandlerClosedCounterClose,
		},
		{
			name: "availableImageHandlerAddSubscription",
			run:  testAvailableImageHandlerAddSubscription,
		},
		{
			name: "availableImageHandlerWaitUntilImageCount",
			run:  testAvailableImageHandlerWaitUntilImageCount,
		},
		{
			name: "availableImageHandlerRunDuringClientWork",
			run:  testAvailableImageHandlerRunDuringClientWork,
		},
		{
			name: "availableImageHandlerOfferWithRetry",
			run:  testAvailableImageHandlerOfferWithRetry,
		},
		{
			name: "availableImageHandlerLookupAndClose",
			run:  testAvailableImageHandlerLookupAndClose,
		},
		{
			name: "closeErrorHandlerDoWork",
			run:  testCloseErrorHandlerDoWork,
		},
		{
			name: "availableImageHandlerDoWorkAfterConcurrentClose",
			run:  testAvailableImageHandlerDoWorkAfterConcurrentClose,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func testAvailableImageHandlerCancelPending(t *testing.T) {
	env := startAvailableImageHandlerCall(t, func(env *handlerCallEnv) {
		env.client.CancelPending(env.correlationID)
	})
	env.deliverAvailable()
	env.requireOneReentrant()
	env.requirePending(true)
}

func testErrorHandlerSubscriptionLookupOnImageError(t *testing.T) {
	var subAtReport *Subscription
	env := startImageHandlerCall(t, false, func(env *handlerCallEnv) Config {
		return Config{
			ErrorHandler: func(err error) {
				env.gotErrors = append(env.gotErrors, err)
				subAtReport, _ = env.client.Subscription(env.correlationID)
			},
		}
	})
	env.deliverAvailable()
	require.Len(t, env.gotErrors, 1)
	require.ErrorIs(t, env.gotErrors[0], conductor.ErrLogFileOutsideMediaDriverDir)
	require.NotNil(t, subAtReport)
	require.Equal(t, env.correlationID, subAtReport.RegistrationID())
}

func testUnavailableImageHandlerCancelPending(t *testing.T) {
	env := startImageHandlerCall(t, true, func(env *handlerCallEnv) Config {
		return Config{
			ErrorHandler: handlerCallErrorHandler(env),
			UnavailableImageHandler: func(*Image) {
				env.client.CancelPending(env.correlationID)
			},
		}
	})
	env.deliverAvailable()
	transmitUnavailableImageToClientSubscription(
		t,
		env.fixture.Conductor.EventsAdapterForTest(),
		env.correlationID,
	)

	require.Equal(t, 1, env.doWork())
	env.requireOneReentrant()
	env.requirePending(true)
}

func testAvailableImageHandlerAddDestination(t *testing.T) {
	var (
		sub    *Subscription
		addErr error
	)
	env := startAvailableImageHandlerCall(t, func(*handlerCallEnv) {
		addErr = sub.AddDestination(clientAddChannel)
	})
	sub = env.boundSubscription()
	env.deliverAvailable()
	require.ErrorIs(t, addErr, ErrReentrantCall)
	require.Empty(t, env.gotErrors)
	env.requirePending(true)
}

func testErrorHandlerCancelPending(t *testing.T) {
	env := startImageHandlerCall(t, false, func(env *handlerCallEnv) Config {
		return Config{
			ErrorHandler: func(err error) {
				env.gotErrors = append(env.gotErrors, err)
				env.client.CancelPending(env.correlationID)
			},
		}
	})
	env.deliverAvailable()
	require.Len(t, env.gotErrors, 2)
	require.ErrorIs(t, env.gotErrors[0], conductor.ErrLogFileOutsideMediaDriverDir)
	require.ErrorIs(t, env.gotErrors[1], ErrReentrantCall)
	env.requirePending(true)
}

func testAvailableImageHandlerDoWork(t *testing.T) {
	var inner int
	env := startAvailableImageHandlerCall(t, func(env *handlerCallEnv) {
		inner = env.client.DoWork()
	})
	env.deliverAvailable()
	require.Zero(t, inner)
	env.requireOneReentrant()
}

func testAvailableImageHandlerIsTerminating(t *testing.T) {
	var terminating bool
	env := startAvailableImageHandlerCall(t, func(env *handlerCallEnv) {
		terminating = env.client.IsTerminating()
	})
	env.deliverAvailable()
	require.False(t, terminating)
	require.Empty(t, env.gotErrors)
}

func testAvailableImageHandlerClose(t *testing.T) {
	var closeErr error
	env := startAvailableImageHandlerCall(t, func(env *handlerCallEnv) {
		closeErr = env.client.Close()
	})
	env.deliverAvailable()
	require.ErrorIs(t, closeErr, ErrReentrantCall)
	require.False(t, ClientIsClosedForTest(env.client))
	require.Empty(t, env.gotErrors)
	require.NoError(t, env.client.Close())
	require.True(t, ClientIsClosedForTest(env.client))
}

func testAvailableImageHandlerCounterClose(t *testing.T) {
	var (
		counter  *Counter
		closeErr error
	)
	env := startAvailableImageHandlerCall(t, func(env *handlerCallEnv) {
		closeErr = counter.Close()
	})
	counter = &Counter{
		client:         env.client,
		counterID:      1,
		registrationID: 99,
	}
	env.deliverAvailable()
	require.ErrorIs(t, closeErr, ErrReentrantCall)
	require.False(t, counter.closed.Load())
	require.Empty(t, env.gotErrors)
	require.NoError(t, counter.Close())
	require.True(t, counter.closed.Load())
}

func testAvailableImageHandlerClosedCounterClose(t *testing.T) {
	var (
		counter  *Counter
		closeErr error
	)
	env := startAvailableImageHandlerCall(t, func(env *handlerCallEnv) {
		closeErr = counter.Close()
	})
	counter = &Counter{
		client:         env.client,
		counterID:      1,
		registrationID: 99,
	}
	counter.closed.Store(true)
	env.deliverAvailable()
	require.NoError(t, closeErr)
	require.Empty(t, env.gotErrors)
}

func testAvailableImageHandlerAddSubscription(t *testing.T) {
	var (
		added  *Subscription
		addErr error
	)
	env := startAvailableImageHandlerCall(t, func(env *handlerCallEnv) {
		added, addErr = env.client.AddSubscription(clientAddStreamID, clientAddChannel)
	})
	env.deliverAvailable()
	require.Nil(t, added)
	require.ErrorIs(t, addErr, ErrReentrantCall)
	require.Empty(t, env.gotErrors)
}

func testAvailableImageHandlerWaitUntilImageCount(t *testing.T) {
	var (
		sub     *Subscription
		waitErr error
	)
	env := startAvailableImageHandlerCall(t, func(*handlerCallEnv) {
		waitErr = sub.WaitUntilImageCount(context.Background(), 2)
	})
	sub = env.boundSubscription()
	env.deliverAvailable()
	require.ErrorIs(t, waitErr, ErrReentrantCall)
	require.Empty(t, env.gotErrors)
}

func testAvailableImageHandlerRunDuringClientWork(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	var (
		started bool
		runErr  error
	)
	env := startAvailableImageHandlerCall(t, func(env *handlerCallEnv) {
		runErr = env.client.RunDuringClientWork(context.Background(), func() error {
			started = true
			<-release
			return nil
		})
	})
	env.deliverAvailable()
	require.ErrorIs(t, runErr, ErrReentrantCall)
	require.False(t, started)
	require.Empty(t, env.gotErrors)
}

func testAvailableImageHandlerOfferWithRetry(t *testing.T) {
	var (
		pub      *Publication
		position int64
		offerErr error
	)
	env := startAvailableImageHandlerCall(t, func(*handlerCallEnv) {
		position, offerErr = pub.OfferWithRetry(context.Background(), []byte{0x01})
	})
	pub = &Publication{client: env.client}
	env.deliverAvailable()
	require.Zero(t, position)
	require.ErrorIs(t, offerErr, ErrReentrantCall)
	require.Empty(t, env.gotErrors)
}

func testAvailableImageHandlerLookupAndClose(t *testing.T) {
	var (
		publicationFound bool
		exclusiveFound   bool
		sub              *Subscription
		closeErr         error
	)
	env := startAvailableImageHandlerCall(t, func(env *handlerCallEnv) {
		_, publicationFound = env.client.Publication(env.correlationID)
		_, exclusiveFound = env.client.ExclusivePublication(env.correlationID)
		var ok bool
		sub, ok = env.client.Subscription(env.correlationID)
		if ok {
			closeErr = sub.Close()
		}
	})
	env.deliverAvailable()
	require.False(t, publicationFound)
	require.False(t, exclusiveFound)
	require.NotNil(t, sub)
	require.NoError(t, closeErr)
	require.True(t, sub.IsClosed())
	require.Empty(t, env.gotErrors)
}

func testCloseErrorHandlerDoWork(t *testing.T) {
	var (
		client    *Client
		gotErrors []error
		inner     int
		called    bool
	)
	client, fixture := newClientAddFixtureWithConfig(t, Config{
		ErrorHandler: func(err error) {
			gotErrors = append(gotErrors, err)
			if called || client == nil {
				return
			}
			called = true
			inner = client.DoWork()
		},
	})
	InsertPublicationForTest(fixture.Conductor, 11)

	require.NoError(t, client.Close())
	require.Zero(t, inner)
	require.True(t, called)
	require.Equal(t, 1, reentrantErrorCount(gotErrors))
	require.GreaterOrEqual(t, len(gotErrors), 2)

	before := len(gotErrors)
	require.Zero(t, client.DoWork())
	require.Len(t, gotErrors, before)
}

func testAvailableImageHandlerDoWorkAfterConcurrentClose(t *testing.T) {
	entered := make(chan struct{})
	closedSet := make(chan struct{})
	release := make(chan struct{})
	didWork := make(chan struct{})
	signalClosed := sync.OnceFunc(func() { close(closedSet) })
	releaseHandler := sync.OnceFunc(func() { close(release) })
	t.Cleanup(signalClosed)
	t.Cleanup(releaseHandler)

	var inner int
	env := startAvailableImageHandlerCall(t, func(env *handlerCallEnv) {
		close(entered)
		<-closedSet
		inner = env.client.DoWork()
		close(didWork)
		<-release
	})
	env.transmitAvailable()

	workDone := make(chan int, 1)
	go func() {
		workDone <- env.client.DoWork()
	}()

	receiveAddResult(t, entered, clientConcurrentResultTimeout, "handler did not enter")

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- env.client.Close()
	}()

	require.Eventually(
		t,
		func() bool { return ClientIsClosedForTest(env.client) },
		clientConcurrentResultTimeout,
		time.Millisecond,
		"Close did not set closed",
	)
	signalClosed()

	receiveAddResult(t, didWork, clientConcurrentResultTimeout, "handler DoWork did not return")
	require.Zero(t, inner)
	env.requireOneReentrant()
	releaseHandler()

	workCount := receiveAddResult(t, workDone, clientConcurrentResultTimeout, "DoWork did not return")
	closeErr := receiveCloseResult(
		t,
		env.client,
		closeDone,
		clientConcurrentResultTimeout,
		"Close did not return",
	)
	require.NoError(t, closeErr)

	require.Equal(t, 1, workCount)
	require.True(t, ClientIsClosedForTest(env.client))
	before := reentrantErrorCount(env.gotErrors)
	require.Zero(t, env.client.DoWork())
	require.Equal(t, before, reentrantErrorCount(env.gotErrors))
}

func reentrantErrorCount(errs []error) int {
	count := 0
	for _, err := range errs {
		if errors.Is(err, ErrReentrantCall) {
			count++
		}
	}
	return count
}

func TestClient_ConfigUnavailableImageHandler_invoked(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	var detachedImage *Image
	cfg := Config{
		AeronDir: dir,
		UnavailableImageHandler: func(image *Image) {
			detachedImage = image
		},
	}

	client, fixture := newClientAddFixtureWithConfig(t, cfg)

	correlationID, err := client.OfferAddSubscription(
		conductorImageLifecycleGoldenAvailableImage.StreamID,
		clientAddChannel,
	)
	require.NoError(t, err)

	deliverAvailableImageToClientSubscription(t, client, fixture, logPath, correlationID)

	adapter := fixture.Conductor.EventsAdapterForTest()
	sub, ok := client.Subscription(correlationID)
	require.True(t, ok)
	subscriptionImage, ok := SubscriptionImageAtIndexForTest(sub, 0)
	require.True(t, ok)

	deliverUnavailableImageToClientSubscription(t, client, adapter, correlationID)

	sub, ok = client.Subscription(correlationID)
	require.True(t, ok)
	require.Zero(t, sub.ImageCount())
	require.False(t, sub.IsConnected())
	require.NotNil(t, detachedImage)
	requireImageHandlerIdentity(t, subscriptionImage, detachedImage)
	require.True(t, detachedImage.IsClosed())
}

func TestClient_nilHandlers_lifecycleSucceeds(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	cfg := Config{AeronDir: dir}
	client, fixture := newClientAddFixtureWithConfig(t, cfg)

	correlationID, err := client.OfferAddSubscription(
		conductorImageLifecycleGoldenAvailableImage.StreamID,
		clientAddChannel,
	)
	require.NoError(t, err)

	sub, ok := client.Subscription(correlationID)
	require.True(t, ok)
	require.Nil(t, SubscriptionAvailableImageHandlerForTest(sub))
	require.Nil(t, SubscriptionUnavailableImageHandlerForTest(sub))

	deliverAvailableImageToClientSubscription(t, client, fixture, logPath, correlationID)

	sub, ok = client.Subscription(correlationID)
	require.True(t, ok)
	require.Equal(t, 1, sub.ImageCount())
	subscriptionImage, ok := SubscriptionImageAtIndexForTest(sub, 0)
	require.True(t, ok)
	requireImageGoldenFields(t, subscriptionImage)

	adapter := fixture.Conductor.EventsAdapterForTest()
	deliverUnavailableImageToClientSubscription(t, client, adapter, correlationID)

	sub, ok = client.Subscription(correlationID)
	require.True(t, ok)
	require.Zero(t, sub.ImageCount())
	require.False(t, sub.IsConnected())
	require.Nil(t, SubscriptionAvailableImageHandlerForTest(sub))
	require.Nil(t, SubscriptionUnavailableImageHandlerForTest(sub))
}
