package aeron

import (
	"reflect"
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

func representativeContextForConnectConfigTest(tb testing.TB) Context {
	tb.Helper()

	return Context{
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

func TestConductorConnectConfig_propagatesContextScalarFields(t *testing.T) {
	t.Parallel()

	ctx := representativeContextForConnectConfigTest(t)
	config := ConductorConnectConfigForTest(ctx, testutil.TestClientID)

	require.Equal(t, ConductorImageLifecycle, config.ImageLifecycle)
	require.NotNil(t, config.ResourceLifecycle)
	require.Equal(t, ctx.AeronDir, config.MediaDriverDir)
	require.Equal(
		t,
		ctx.CloseLingerDuration.Nanoseconds(),
		config.CloseLingerNs,
	)
	require.Equal(t, ctx.DriverTimeout, config.DriverTimeout)
	require.Equal(t, ctx.KeepAliveInterval, config.KeepAliveInterval)
	require.Equal(t, ctx.IdleSleepDuration, config.IdleSleepDuration)
	require.Equal(t, ctx.InterServiceTimeout, config.InterServiceTimeout)
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
		ctx             Context
		wantAvailable   AvailableImageHandler
		wantUnavailable UnavailableImageHandler
	}{
		{
			name: "both",
			ctx: Context{
				AvailableImageHandler:   availableHandler,
				UnavailableImageHandler: unavailableHandler,
			},
			wantAvailable:   availableHandler,
			wantUnavailable: unavailableHandler,
		},
		{
			name: "available only",
			ctx: Context{
				AvailableImageHandler: availableHandler,
			},
			wantAvailable: availableHandler,
		},
		{
			name: "unavailable only",
			ctx: Context{
				UnavailableImageHandler: unavailableHandler,
			},
			wantUnavailable: unavailableHandler,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := ConductorConnectConfigForTest(tt.ctx, testutil.TestClientID)
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

	require.Equal(tb, 1, client.DoWork())
}

func deliverUnavailableImageToClientSubscription(
	tb testing.TB,
	client *Client,
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
	require.Equal(tb, 1, client.DoWork())
}

func TestClient_ContextAvailableImageHandler_invoked(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	var attachedImage *Image
	ctx := Context{
		AeronDir: dir,
		AvailableImageHandler: func(image *Image) {
			attachedImage = image
		},
	}

	client, fixture := newClientAddFixtureWithContext(t, ctx)

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

func TestClient_ContextUnavailableImageHandler_invoked(t *testing.T) {
	t.Parallel()

	dir, logPath := setupConductorImageLifecycleLogFile(t, "image.log")

	var detachedImage *Image
	ctx := Context{
		AeronDir: dir,
		UnavailableImageHandler: func(image *Image) {
			detachedImage = image
		},
	}

	client, fixture := newClientAddFixtureWithContext(t, ctx)

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

	ctx := Context{AeronDir: dir}
	client, fixture := newClientAddFixtureWithContext(t, ctx)

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
