package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/aeron/logbuffer"
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

func emptyCountersReaderForTest(t *testing.T) *cnc.CountersReader {
	t.Helper()

	reader, err := cnc.NewCountersReader(
		make([]byte, cnc.CounterMetadataLength),
		make([]byte, cnc.CounterValueLength),
	)
	require.NoError(t, err)
	return reader
}

var publicationKindLogConfigCases = []struct {
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
}

func newPublicationViaLifecycle(
	exclusive bool,
	ready conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	counters *cnc.CountersReader,
) (conductor.PublicationHandle, error) {
	if exclusive {
		return ConductorResourceLifecycle.NewExclusivePublication(
			ready,
			logBuffers,
			counters,
			"aeron:ipc",
		)
	}
	return ConductorResourceLifecycle.NewPublication(ready, logBuffers, counters, "aeron:ipc")
}

func TestConductorResourceLifecycle_NewPublication_inactiveLimitCounter(t *testing.T) {
	t.Parallel()

	emptyReader := emptyCountersReaderForTest(t)

	for _, tt := range publicationKindLogConfigCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logBuffers := NewTestLogBuffersForTest(t, tt.logCfg)
			_, err := newPublicationViaLifecycle(
				tt.exclusive,
				testPublicationBuffersReady(),
				logBuffers,
				emptyReader,
			)
			require.ErrorContains(t, err, "publication limit counter")
		})
	}
}

// TestConductorResourceLifecycle_NewPublication_handleContract pins the
// ResourceLifecycle seam contract: error paths return an untyped nil handle
// (never a typed-nil boxed *Publication), success paths return a live handle.
func TestConductorResourceLifecycle_NewPublication_handleContract(t *testing.T) {
	t.Parallel()

	for _, tt := range publicationKindLogConfigCases {
		t.Run(tt.name+" error untyped nil", func(t *testing.T) {
			t.Parallel()

			logBuffers := NewTestLogBuffersForTest(t, tt.logCfg)
			handle, err := newPublicationViaLifecycle(
				tt.exclusive,
				testPublicationBuffersReady(),
				logBuffers,
				emptyCountersReaderForTest(t),
			)
			require.Error(t, err)
			if handle != nil {
				t.Fatalf("error path returned typed-nil handle %#v", handle)
			}
		})
		t.Run(tt.name+" success live handle", func(t *testing.T) {
			t.Parallel()

			logBuffers := NewTestLogBuffersForTest(t, tt.logCfg)
			ready := testPublicationBuffersReady()
			ready.ChannelStatusCounterID = cnc.NullCounterID
			counters := NewTestCountersReaderLimitOnlyForTest(t, int64(1)<<40)
			handle, err := newPublicationViaLifecycle(tt.exclusive, ready, logBuffers, counters)
			require.NoError(t, err)

			// From-handle helpers reject nil and typed-nil, so they are the
			// live-handle oracle.
			if tt.exclusive {
				_, err = exclusivePublicationFromHandle(handle)
			} else {
				_, err = publicationFromHandle(handle)
			}
			require.NoError(t, err)
		})
	}
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

type publicationHandleErrorCase struct {
	name string
	pub  conductor.PublicationHandle
	want string
}

var conductorPublicationHandleErrorCases = []publicationHandleErrorCase{
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
	{
		name: "typed nil publication",
		pub:  (*Publication)(nil),
		want: "aeron: publication handle is nil",
	},
	{
		name: "typed nil exclusive publication",
		pub:  (*ExclusivePublication)(nil),
		want: "aeron: publication handle is nil",
	},
}

func runPublicationHandleErrorTests(
	t *testing.T,
	cases []publicationHandleErrorCase,
	run func(pub conductor.PublicationHandle) error,
) {
	t.Helper()

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := run(tt.pub)
			require.EqualError(t, err, tt.want)
		})
	}
}

func TestConductorResourceLifecycle_RetirePublication_errors(t *testing.T) {
	t.Parallel()

	runPublicationHandleErrorTests(
		t,
		conductorPublicationHandleErrorCases,
		func(pub conductor.PublicationHandle) error {
			_, err := ConductorResourceLifecycle.RetirePublication(pub)
			return err
		},
	)
}

func TestConductorResourceLifecycle_BindPublication_errors(t *testing.T) {
	t.Parallel()

	conductorInstance := testutil.NewConductorFixture(
		t,
		testutil.DefaultConductorConfig(nil),
	).Conductor

	runPublicationHandleErrorTests(
		t,
		conductorPublicationHandleErrorCases,
		func(pub conductor.PublicationHandle) error {
			return ConductorResourceLifecycle.BindPublication(pub, conductorInstance)
		},
	)
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

func publicationFromHandleErrorCases(
	kind string,
	typeName string,
	typedNil conductor.PublicationHandle,
) []publicationHandleErrorCase {
	return []publicationHandleErrorCase{
		{
			name: "nil",
			pub:  nil,
			want: "aeron: " + kind + " handle is nil",
		},
		{
			name: "wrong type",
			pub:  stubPubHandle,
			want: "aeron: " + kind + " handle aeron.stubPublicationHandle is not " + typeName,
		},
		{
			name: "typed nil",
			pub:  typedNil,
			want: "aeron: " + kind + " handle is nil",
		},
	}
}

func TestPublicationFromHandle_errors(t *testing.T) {
	t.Parallel()

	runPublicationHandleErrorTests(
		t,
		publicationFromHandleErrorCases("publication", "*Publication", (*Publication)(nil)),
		func(pub conductor.PublicationHandle) error {
			_, err := publicationFromHandle(pub)
			return err
		},
	)
}

func TestExclusivePublicationFromHandle_errors(t *testing.T) {
	t.Parallel()

	runPublicationHandleErrorTests(
		t,
		publicationFromHandleErrorCases(
			"exclusive publication",
			"*ExclusivePublication",
			(*ExclusivePublication)(nil),
		),
		func(pub conductor.PublicationHandle) error {
			_, err := exclusivePublicationFromHandle(pub)
			return err
		},
	)
}

func TestConductorResourceLifecycle_NewPreSubscription_nilHandlers(t *testing.T) {
	t.Parallel()

	sub := ConductorResourceLifecycle.NewPreSubscription(
		TestSubscriptionRegistrationID,
		TestPublicationStreamID,
		TestImageSourceIdentity,
	)
	subscription, err := subscriptionFromHandle(sub)
	require.NoError(t, err)
	require.Nil(t, SubscriptionAvailableImageHandlerForTest(subscription))
	require.Nil(t, SubscriptionUnavailableImageHandlerForTest(subscription))
}

func TestConductorResourceLifecycle_BindSubscription_wiresCountersReader(t *testing.T) {
	t.Parallel()

	conductorInstance := testutil.NewConductorFixture(
		t,
		testutil.DefaultConductorConfig(nil),
	).Conductor
	reader := emptyCountersReaderForTest(t)
	conductorInstance.SetCountersReaderForTest(reader)

	sub := NewSubscriptionForTest(
		TestSubscriptionRegistrationID,
		TestPublicationStreamID,
		TestImageSourceIdentity,
	)

	require.NoError(t, ConductorResourceLifecycle.BindSubscription(sub, conductorInstance))
	require.Same(t, reader, SubscriptionCountersForTest(sub))
}

func TestConductorResourceLifecycle_BindPublication(t *testing.T) {
	t.Parallel()

	for _, tt := range publicationKindLogConfigCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fixture := newRetirePublicationFixture(t, tt.logCfg, tt.exclusive)
			conductorInstance := testutil.NewConductorFixture(
				t,
				testutil.DefaultConductorConfig(nil),
			).Conductor
			require.NoError(
				t,
				ConductorResourceLifecycle.BindPublication(fixture.handle, conductorInstance),
			)

			switch handle := fixture.handle.(type) {
			case *Publication:
				require.Equal(t, conductorInstance, PublicationConductorForTest(handle))
			case *ExclusivePublication:
				require.Equal(
					t,
					conductorInstance,
					ExclusivePublicationConductorForTest(handle),
				)
			default:
				t.Fatalf("unexpected publication handle type %T", fixture.handle)
			}
		})
	}
}

func TestConductorResourceLifecycle_MarkPublicationClosed_exclusive(t *testing.T) {
	t.Parallel()

	fixture := newRetirePublicationFixture(t, DefaultTestExclusivePublicationLogConfig(), true)
	ConductorResourceLifecycle.MarkPublicationClosed(fixture.handle)
	require.True(t, fixture.isClosed())
}

func TestConductorResourceLifecycle_MarkPublicationClosed_typedNilNoPanic(t *testing.T) {
	t.Parallel()

	require.NotPanics(t, func() {
		ConductorResourceLifecycle.MarkPublicationClosed((*Publication)(nil))
	})
	require.NotPanics(t, func() {
		ConductorResourceLifecycle.MarkPublicationClosed((*ExclusivePublication)(nil))
	})
}

func TestConductorResourceLifecycle_RetirePublication_closesAndReturnsRegistryKey(t *testing.T) {
	t.Parallel()

	for _, tt := range publicationKindLogConfigCases {
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
