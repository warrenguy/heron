package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
)

var conductorSubscriptionHandleErrorCases = []struct {
	name string
	sub  conductor.SubscriptionHandle
	want string
}{
	{
		name: "nil",
		sub:  nil,
		want: "aeron: subscription handle is nil",
	},
	{
		name: "wrong type",
		sub:  stubSubHandle,
		want: "aeron: subscription handle aeron.stubSubscriptionHandle is not *Subscription",
	},
}

func TestConductorResourceLifecycle_NewPublication_inactiveLimitCounter(t *testing.T) {
	t.Parallel()

	logBuffers := NewTestLogBuffersForTest(t, DefaultTestPublicationLogConfig())
	ready := testPublicationBuffersReady()
	emptyReader, err := cnc.NewCountersReader(
		make([]byte, cnc.CounterMetadataLength),
		make([]byte, cnc.CounterValueLength),
	)
	require.NoError(t, err)

	_, err = ConductorResourceLifecycle.NewPublication(
		ready,
		logBuffers,
		emptyReader,
		"aeron:ipc",
	)
	require.ErrorContains(t, err, "publication limit counter")
}

func TestConductorResourceLifecycle_ApplySubscriptionReady_errors(t *testing.T) {
	t.Parallel()

	for _, tt := range conductorSubscriptionHandleErrorCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ConductorResourceLifecycle.ApplySubscriptionReady(
				tt.sub,
				conductor.SubscriptionReady{ChannelStatusCounterID: 12},
			)
			require.EqualError(t, err, tt.want)
		})
	}
}

func TestConductorResourceLifecycle_RetireSubscription_errors(t *testing.T) {
	t.Parallel()

	for _, tt := range conductorSubscriptionHandleErrorCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ConductorResourceLifecycle.RetireSubscription(tt.sub)
			require.EqualError(t, err, tt.want)
		})
	}
}

var conductorPublicationHandleErrorCases = []struct {
	name string
	pub  conductor.PublicationHandle
	want string
}{
	{
		name: "nil",
		pub:  nil,
		want: "aeron: publication handle is nil",
	},
	{
		name: "wrong type",
		pub:  stubPubHandle,
		want: "aeron: publication handle aeron.stubPublicationHandle is not *Publication",
	},
}

func TestConductorResourceLifecycle_RetirePublication_errors(t *testing.T) {
	t.Parallel()

	for _, tt := range conductorPublicationHandleErrorCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ConductorResourceLifecycle.RetirePublication(tt.pub)
			require.EqualError(t, err, tt.want)
		})
	}
}

func TestConductorResourceLifecycle_RetirePublication_closesAndReturnsRegistryKey(t *testing.T) {
	t.Parallel()

	log := NewTestMappedLogForTest(t, DefaultTestPublicationLogConfig())
	ready := testPublicationBuffersReady()
	ready.ChannelStatusCounterID = cnc.NullCounterID
	pub, err := NewPublicationFromReadyForTest(
		ready,
		log,
		NewTestCountersReaderLimitOnlyForTest(t, int64(1)<<40),
		"aeron:ipc",
	)
	require.NoError(t, err)

	registryKey, err := ConductorResourceLifecycle.RetirePublication(pub)
	require.NoError(t, err)
	require.Equal(t, pub.log.Path(), registryKey)
	require.True(t, pub.isClosed())
}

func TestConductorResourceLifecycle_RetireSubscription_releasesImageLogs(t *testing.T) {
	t.Parallel()

	sub := NewSubscriptionForTest(
		TestSubscriptionRegistrationID,
		TestPublicationStreamID,
		TestImageSourceIdentity,
	)
	logBuffers := NewTestLogBuffersForTest(t, DefaultTestPublicationLogConfig())
	image, err := NewImageDirectForTest(
		sub,
		AvailableImageForTest(),
		logBuffers,
		NewTestCountersReaderWithSubscriberPositionForTest(t, TestSubscriberPositionAtJoin),
	)
	require.NoError(t, err)
	SubscriptionAddImageForTest(sub, image)

	logFiles, err := ConductorResourceLifecycle.RetireSubscription(sub)
	require.NoError(t, err)
	require.Equal(t, []string{"test-image.log"}, logFiles)
	require.True(t, sub.IsClosed())
	require.Zero(t, sub.ImageCount())
}

func TestValidateConfig_requiresResourceLifecycleWithImageLifecycle(t *testing.T) {
	t.Parallel()

	err := conductor.ValidateConfig(conductor.Config{
		ImageLifecycle: ConductorImageLifecycle,
	})
	require.ErrorIs(t, err, conductor.ErrResourceLifecycleRequired)
	require.EqualError(t, err, "conductor: ResourceLifecycle is required when ImageLifecycle is set")

	err = conductor.ValidateConfig(conductor.Config{
		ImageLifecycle:    ConductorImageLifecycle,
		ResourceLifecycle: ConductorResourceLifecycle,
	})
	require.NoError(t, err)
}
