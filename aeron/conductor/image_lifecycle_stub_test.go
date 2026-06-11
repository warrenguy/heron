package conductor_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

type stubSubscription struct {
	registrationID int64
	streamID       int32
}

func (sub stubSubscription) RegistrationID() int64 { return sub.registrationID }
func (sub stubSubscription) StreamID() int32       { return sub.streamID }

type stubImage struct {
	correlationID int64
	logFileName   string
	closed        bool
}

func (img *stubImage) CorrelationID() int64 { return img.correlationID }
func (img *stubImage) LogFileName() string  { return img.logFileName }
func (img *stubImage) IsClosed() bool       { return img.closed }

type recordingImageLifecycle struct {
	hasImage                       bool
	newImageErr                    error
	addImageOK                     bool
	availableImageHandler          func(conductor.ImageHandle)
	unavailableImageHandler        func(conductor.ImageHandle)
	lastEvent                      conductor.AvailableImage
	lastRemovedCorrelationID       int64
	lastSubscriptionRegistrationID int64
	removedLogFileName             string
	skipMarkImageClosed            bool
	closed                         bool
}

func (lifecycle *recordingImageLifecycle) HasImage(
	_ conductor.SubscriptionHandle,
	_ int64,
) bool {
	return lifecycle.hasImage
}

func (lifecycle *recordingImageLifecycle) NewImage(
	_ conductor.SubscriptionHandle,
	event conductor.AvailableImage,
	_ *logbuffer.LogBuffers,
	_ *cnc.CountersReader,
	_ func(error),
) (conductor.ImageHandle, error) {
	lifecycle.lastEvent = event
	if lifecycle.newImageErr != nil {
		return nil, lifecycle.newImageErr
	}
	return &stubImage{
		correlationID: event.CorrelationID,
		logFileName:   event.LogFileName,
	}, nil
}

func (lifecycle *recordingImageLifecycle) AddImage(
	_ conductor.SubscriptionHandle,
	_ conductor.ImageHandle,
) bool {
	return lifecycle.addImageOK
}

func (lifecycle *recordingImageLifecycle) RemoveImage(
	sub conductor.SubscriptionHandle,
	correlationID int64,
) conductor.ImageHandle {
	lifecycle.lastRemovedCorrelationID = correlationID
	if sub != nil {
		lifecycle.lastSubscriptionRegistrationID = sub.RegistrationID()
	}
	logFileName := lifecycle.removedLogFileName
	if logFileName == "" {
		logFileName = "removed.log"
	}
	return &stubImage{correlationID: correlationID, logFileName: logFileName}
}

func (lifecycle *recordingImageLifecycle) CloseImage(image conductor.ImageHandle) {
	lifecycle.closed = true
	if lifecycle.skipMarkImageClosed {
		return
	}
	if img, ok := image.(*stubImage); ok {
		img.closed = true
	}
}

func (lifecycle *recordingImageLifecycle) AvailableImageHandler(
	_ conductor.SubscriptionHandle,
) func(conductor.ImageHandle) {
	return lifecycle.availableImageHandler
}

func (lifecycle *recordingImageLifecycle) UnavailableImageHandler(
	_ conductor.SubscriptionHandle,
) func(conductor.ImageHandle) {
	return lifecycle.unavailableImageHandler
}

func registerGoldenSubscription(
	conductorInstance *conductor.Conductor,
	registrationID int64,
	streamID int32,
) {
	conductor.RegisterSubscriptionForTest(
		conductorInstance,
		registrationID,
		stubSubscription{
			registrationID: registrationID,
			streamID:       streamID,
		},
	)
}

func registerGoldenImageSubscription(conductorInstance *conductor.Conductor) {
	registerGoldenSubscription(
		conductorInstance,
		goldenAvailableImageEvent.SubscriptionRegistrationID,
		goldenAvailableImageEvent.StreamID,
	)
}

func seedLogBuffersForTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
	logFileName string,
) {
	tb.Helper()
	require.NoError(tb, conductor.SeedLogBuffersRegistryForTest(
		conductor.LogBuffersRegistryForTest(conductorInstance),
		logFileName,
		conductor.NewSyntheticLogBuffersForTest(tb),
	))
}

func seedGoldenAvailableLogBuffers(tb testing.TB, conductorInstance *conductor.Conductor) {
	tb.Helper()
	seedLogBuffersForTest(tb, conductorInstance, goldenAvailableImageEvent.LogFileName)
}

func newConductorWithRecordingLifecycle(
	tb testing.TB,
	lifecycle *recordingImageLifecycle,
	errorHandler func(error),
) *conductor.Conductor {
	tb.Helper()

	config := conductorTestConfig(tb, errorHandler, fixedClocks{})
	config.ImageLifecycle = lifecycle
	config.MediaDriverDir = "/"
	return newConductorForTest(tb, config)
}

func newConductorWithRecordingLifecycleMutableNano(
	tb testing.TB,
	lifecycle *recordingImageLifecycle,
	errorHandler func(error),
	epochMs int64,
	mediaDriverDir string,
) (*conductor.Conductor, *int64) {
	tb.Helper()

	var nanoNs int64
	config := conductorTestConfigMutableNano(tb, errorHandler, epochMs, &nanoNs)
	config.ImageLifecycle = lifecycle
	config.MediaDriverDir = mediaDriverDir
	return newConductorForTest(tb, config), &nanoNs
}

func triggerGoldenUnavailableImage(t *testing.T, conductorInstance *conductor.Conductor) {
	t.Helper()

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnUnavailableImage,
		conductor.UnavailableImageBytes(goldenUnavailableImageEvent),
	)
}

func TestConductor_handleAvailableImage_withoutPreInsert_silentDrop(t *testing.T) {
	t.Parallel()

	lifecycle := &recordingImageLifecycle{addImageOK: true}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, func(error) {})
	seedGoldenAvailableLogBuffers(t, conductorInstance)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)

	require.NoError(
		t,
		conductorInstance.RegisterPendingEntryForTest(
			conductor.PendingRegistration{
				CorrelationID: goldenAvailableImageEvent.SubscriptionRegistrationID,
				Kind:          conductor.PendingKindAddSubscription,
				StreamID:      goldenAvailableImageEvent.StreamID,
				Channel:       pendingTestChannel,
				DeadlineNs:    time.Second.Nanoseconds(),
			},
			true,
		),
	)
	require.Equal(t, 0, conductorInstance.SubscriptionCountForTest())

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(goldenAvailableImageEvent),
	)
	require.Equal(t, 1, conductorInstance.DoWork())

	require.Zero(t, lifecycle.lastEvent.CorrelationID)
	require.Equal(
		t,
		int32(1),
		conductor.LogBuffersRegistryRefCountForTest(
			registry,
			goldenAvailableImageEvent.LogFileName,
		),
	)
}

func TestConductor_handleAvailableImage_emptyMediaDriverDirReportsError(t *testing.T) {
	t.Parallel()

	errorHandler, gotErrors := collectConductorErrors()
	lifecycle := &recordingImageLifecycle{addImageOK: true}
	config := conductorTestConfig(t, errorHandler, fixedClocks{})
	config.ImageLifecycle = lifecycle
	conductorInstance := newConductorForTest(t, config)
	registerGoldenImageSubscription(conductorInstance)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(goldenAvailableImageEvent),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)
	require.ErrorIs(t, (*gotErrors)[0], conductor.ErrMediaDriverDirRequired)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	require.False(t, conductor.LogBuffersRegistryContainsForTest(
		registry,
		goldenAvailableImageEvent.LogFileName,
	))
}

func TestConductor_handleAvailableImage_nilLifecycleNoOp(t *testing.T) {
	t.Parallel()

	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(t, func(error) {}, fixedClocks{}),
	)
	registerGoldenImageSubscription(conductorInstance)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(goldenAvailableImageEvent),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	require.False(t, conductor.LogBuffersRegistryContainsForTest(
		registry,
		goldenAvailableImageEvent.LogFileName,
	))
}

func TestConductor_handleAvailableImage_newImageFailureReleasesAcquire(t *testing.T) {
	t.Parallel()

	errorHandler, gotErrors := collectConductorErrors()
	lifecycle := &recordingImageLifecycle{
		newImageErr: errors.New("conductor: injected new image failure"),
	}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, errorHandler)
	registerGoldenImageSubscription(conductorInstance)
	seedGoldenAvailableLogBuffers(t, conductorInstance)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(goldenAvailableImageEvent),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)
	require.EqualError(t, (*gotErrors)[0], "conductor: injected new image failure")
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	require.True(t, conductor.LogBuffersRegistryContainsForTest(
		registry,
		goldenAvailableImageEvent.LogFileName,
	))
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(
		registry,
		goldenAvailableImageEvent.LogFileName,
	))
}

func TestConductor_handleAvailableImage_addImageFailureReleasesAcquire(t *testing.T) {
	t.Parallel()

	errorHandler, gotErrors := collectConductorErrors()
	lifecycle := &recordingImageLifecycle{addImageOK: false}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, errorHandler)
	registerGoldenImageSubscription(conductorInstance)
	seedGoldenAvailableLogBuffers(t, conductorInstance)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(goldenAvailableImageEvent),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)
	require.EqualError(t, (*gotErrors)[0], "conductor: failed to attach image")
	require.True(t, lifecycle.closed)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(
		registry,
		goldenAvailableImageEvent.LogFileName,
	))
}

func TestConductor_handleUnavailableImage_closeFailureSkipsRelease(t *testing.T) {
	t.Parallel()

	errorHandler, gotErrors := collectConductorErrors()
	lifecycle := &recordingImageLifecycle{
		skipMarkImageClosed: true,
		removedLogFileName:  goldenAvailableImageEvent.LogFileName,
	}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, errorHandler)
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	seedGoldenAvailableLogBuffers(t, conductorInstance)
	registerGoldenImageSubscription(conductorInstance)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnUnavailableImage,
		conductor.UnavailableImageBytes(goldenUnavailableImageEvent),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)
	require.EqualError(t, (*gotErrors)[0], "conductor: image close failed before log buffer release")
	require.Equal(t, int32(1), conductor.LogBuffersRegistryRefCountForTest(
		registry,
		goldenAvailableImageEvent.LogFileName,
	))
}

func TestConductor_handleUnavailableImage_nilLifecycleNoOp(t *testing.T) {
	t.Parallel()

	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(t, func(error) {}, fixedClocks{}),
	)
	registerGoldenImageSubscription(conductorInstance)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnUnavailableImage,
		conductor.UnavailableImageBytes(goldenUnavailableImageEvent),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
}

func assertImageHandlerPanicRecoveryRestoresLock(
	t *testing.T,
	panicWith any,
	wantErr error,
	available bool,
) {
	t.Helper()

	errorHandler, gotErrors := collectConductorErrors()
	lifecycle := &recordingImageLifecycle{addImageOK: true}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, errorHandler)
	registerGoldenImageSubscription(conductorInstance)
	if available {
		seedGoldenAvailableLogBuffers(t, conductorInstance)
	}

	var secondHandlerFired bool
	if available {
		lifecycle.availableImageHandler = func(conductor.ImageHandle) {
			panic(panicWith)
		}
		transmitConductorEvent(
			t,
			conductorInstance.EventsAdapterForTest(),
			conductor.OnAvailableImage,
			conductor.AvailableImageBytes(goldenAvailableImageEvent),
		)
	} else {
		lifecycle.unavailableImageHandler = func(conductor.ImageHandle) {
			panic(panicWith)
		}
		triggerGoldenUnavailableImage(t, conductorInstance)
	}

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)
	if wantErr != nil {
		require.EqualError(t, (*gotErrors)[0], wantErr.Error())
	} else {
		require.Equal(t, panicWith, (*gotErrors)[0])
	}

	if available {
		lifecycle.availableImageHandler = func(conductor.ImageHandle) {
			secondHandlerFired = true
		}
		transmitConductorEvent(
			t,
			conductorInstance.EventsAdapterForTest(),
			conductor.OnAvailableImage,
			conductor.AvailableImageBytes(goldenAvailableImageEvent),
		)
	} else {
		lifecycle.unavailableImageHandler = func(conductor.ImageHandle) {
			secondHandlerFired = true
		}
		triggerGoldenUnavailableImage(t, conductorInstance)
	}

	require.Equal(t, 1, conductorInstance.DoWork())
	require.True(t, secondHandlerFired)

	conductorInstance.InsertPublicationForTest(55, stubPublication{registrationID: 55})
	got, ok := conductorInstance.PublicationByRegID(55)
	require.True(t, ok)
	require.Equal(t, int64(55), got.RegistrationID())
}

func TestConductor_invokeImageHandler_panicRecovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		panicWith any
		wantErr   error
	}{
		{
			name:      "stringPanic",
			panicWith: "string panic",
			wantErr:   errors.New("conductor: image handler panic: string panic"),
		},
		{
			name:      "errorPanic",
			panicWith: errors.New("conductor: handler boom"),
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			assertImageHandlerPanicRecoveryRestoresLock(
				t,
				testCase.panicWith,
				testCase.wantErr,
				true,
			)
		})
	}
}

func TestConductor_invokeImageHandler_suppressesNested(t *testing.T) {
	t.Parallel()

	var outerCalls int
	var nestedDoWorkCalls int
	lifecycle := &recordingImageLifecycle{addImageOK: true}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, func(error) {})
	lifecycle.availableImageHandler = func(conductor.ImageHandle) {
		outerCalls++
		transmitConductorEvent(
			t,
			conductorInstance.EventsAdapterForTest(),
			conductor.OnAvailableImage,
			conductor.AvailableImageBytes(goldenAvailableImageEvent),
		)
		nestedDoWorkCalls++
		_ = conductorInstance.DoWork()
	}
	registerGoldenImageSubscription(conductorInstance)
	seedGoldenAvailableLogBuffers(t, conductorInstance)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnAvailableImage,
		conductor.AvailableImageBytes(goldenAvailableImageEvent),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Equal(t, 1, outerCalls)
	require.Equal(t, 1, nestedDoWorkCalls)
}

func TestConductor_invokeUnavailableImageHandler_panicRecovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		panicWith any
		wantErr   error
	}{
		{
			name:      "stringPanic",
			panicWith: "unavailable string panic",
			wantErr:   errors.New("conductor: image handler panic: unavailable string panic"),
		},
		{
			name:      "errorPanic",
			panicWith: errors.New("conductor: unavailable handler boom"),
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			assertImageHandlerPanicRecoveryRestoresLock(
				t,
				testCase.panicWith,
				testCase.wantErr,
				false,
			)
		})
	}
}

func TestConductor_invokeUnavailableImageHandler_suppressesNested(t *testing.T) {
	t.Parallel()

	var outerCalls int
	var nestedDoWorkCalls int
	lifecycle := &recordingImageLifecycle{}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, func(error) {})
	lifecycle.unavailableImageHandler = func(conductor.ImageHandle) {
		outerCalls++
		transmitConductorEvent(
			t,
			conductorInstance.EventsAdapterForTest(),
			conductor.OnUnavailableImage,
			conductor.UnavailableImageBytes(goldenUnavailableImageEvent),
		)
		nestedDoWorkCalls++
		_ = conductorInstance.DoWork()
	}
	registerGoldenImageSubscription(conductorInstance)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnUnavailableImage,
		conductor.UnavailableImageBytes(goldenUnavailableImageEvent),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Equal(t, 1, outerCalls)
	require.Equal(t, 1, nestedDoWorkCalls)
}
