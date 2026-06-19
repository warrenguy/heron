package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
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

func TestConductorResourceLifecycle_NewExclusivePublication_inactiveLimitCounter(t *testing.T) {
	t.Parallel()

	logBuffers := NewTestLogBuffersForTest(t, DefaultTestExclusivePublicationLogConfig())
	ready := testPublicationBuffersReady()
	emptyReader, err := cnc.NewCountersReader(
		make([]byte, cnc.CounterMetadataLength),
		make([]byte, cnc.CounterValueLength),
	)
	require.NoError(t, err)

	_, err = ConductorResourceLifecycle.NewExclusivePublication(
		ready,
		logBuffers,
		emptyReader,
		"aeron:ipc",
	)
	require.ErrorContains(t, err, "publication limit counter")
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

func runConductorSubscriptionHandleErrorTests(
	t *testing.T,
	run func(sub conductor.SubscriptionHandle) error,
) {
	t.Helper()

	for _, tt := range conductorSubscriptionHandleErrorCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := run(tt.sub)
			require.EqualError(t, err, tt.want)
		})
	}
}

func TestConductorResourceLifecycle_ApplySubscriptionReady_errors(t *testing.T) {
	t.Parallel()

	runConductorSubscriptionHandleErrorTests(t, func(sub conductor.SubscriptionHandle) error {
		return ConductorResourceLifecycle.ApplySubscriptionReady(
			sub,
			conductor.SubscriptionReady{ChannelStatusCounterID: 12},
		)
	})
}

func TestConductorResourceLifecycle_RetireSubscription_errors(t *testing.T) {
	t.Parallel()

	runConductorSubscriptionHandleErrorTests(t, func(sub conductor.SubscriptionHandle) error {
		_, err := ConductorResourceLifecycle.RetireSubscription(sub)
		return err
	})
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
		want: "aeron: publication handle aeron.stubPublicationHandle is not " +
			"*Publication or *ExclusivePublication",
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

type retirePublicationFixture struct {
	handle   conductor.PublicationHandle
	logPath  string
	isClosed func() bool
}

func newRetirePublicationFixture(
	t *testing.T,
	logCfg TestPublicationLogConfig,
	exclusive bool,
) retirePublicationFixture {
	t.Helper()

	log := NewTestMappedLogForTest(t, logCfg)
	ready := testPublicationBuffersReady()
	ready.ChannelStatusCounterID = cnc.NullCounterID
	counters := NewTestCountersReaderLimitOnlyForTest(t, int64(1)<<40)

	if exclusive {
		ep, err := NewExclusivePublicationFromReadyForTest(ready, log, counters, "aeron:ipc")
		require.NoError(t, err)
		return retirePublicationFixture{ep, ep.log.Path(), ep.IsClosed}
	}

	pub, err := NewPublicationFromReadyForTest(ready, log, counters, "aeron:ipc")
	require.NoError(t, err)
	return retirePublicationFixture{pub, pub.log.Path(), pub.IsClosed}
}

var conductorExclusivePublicationHandleErrorCases = []struct {
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
		want: "aeron: publication handle aeron.stubPublicationHandle is not *ExclusivePublication",
	},
	{
		name: "typed nil",
		pub:  (*ExclusivePublication)(nil),
		want: "aeron: publication handle is nil",
	},
}

func TestPublicationFromHandle_errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
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
		{
			name: "typed nil",
			pub:  (*Publication)(nil),
			want: "aeron: publication handle is nil",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := publicationFromHandle(tt.pub)
			require.EqualError(t, err, tt.want)
		})
	}
}

func TestExclusivePublicationFromHandle_errors(t *testing.T) {
	t.Parallel()

	for _, tt := range conductorExclusivePublicationHandleErrorCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := exclusivePublicationFromHandle(tt.pub)
			require.EqualError(t, err, tt.want)
		})
	}
}

func TestConductorResourceLifecycle_BindPublication_exclusive(t *testing.T) {
	t.Parallel()

	fixture := newRetirePublicationFixture(t, DefaultTestExclusivePublicationLogConfig(), true)
	conductorInstance := testutil.NewConductorFixture(
		t,
		testutil.DefaultConductorConfig(nil),
	).Conductor
	require.NoError(t, ConductorResourceLifecycle.BindPublication(fixture.handle, conductorInstance))
}

func TestConductorResourceLifecycle_MarkPublicationClosed_exclusive(t *testing.T) {
	t.Parallel()

	fixture := newRetirePublicationFixture(t, DefaultTestExclusivePublicationLogConfig(), true)
	ConductorResourceLifecycle.MarkPublicationClosed(fixture.handle)
	require.True(t, fixture.isClosed())
}

func TestConductorResourceLifecycle_RetirePublication_closesAndReturnsRegistryKey(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		logCfg    TestPublicationLogConfig
		exclusive bool
	}{
		{
			name:      "publication",
			logCfg:    DefaultTestPublicationLogConfig(),
			exclusive: false,
		},
		{
			name:      "exclusive",
			logCfg:    DefaultTestExclusivePublicationLogConfig(),
			exclusive: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fixture := newRetirePublicationFixture(t, tt.logCfg, tt.exclusive)
			registryKey, err := ConductorResourceLifecycle.RetirePublication(fixture.handle)
			require.NoError(t, err)
			require.Equal(t, fixture.logPath, registryKey)
			require.True(t, fixture.isClosed())
		})
	}
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

func TestValidateConfig_rejectsNegativeCloseLingerNs(t *testing.T) {
	t.Parallel()

	err := conductor.ValidateConfig(conductor.Config{
		CloseLingerNs: -1,
	})
	require.ErrorIs(t, err, conductor.ErrNegativeCloseLingerNs)
	require.EqualError(t, err, "conductor: CloseLingerNs must not be negative")

	err = conductor.ValidateConfig(conductor.Config{
		CloseLingerNs: 0,
	})
	require.NoError(t, err)
}
