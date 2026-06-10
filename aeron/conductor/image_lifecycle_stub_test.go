package conductor_test

import (
	"errors"
	"testing"

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
	availableCallback              func(conductor.ImageHandle)
	unavailableCallback            func(conductor.ImageHandle)
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

func (lifecycle *recordingImageLifecycle) AvailableImageCallback(
	_ conductor.SubscriptionHandle,
) func(conductor.ImageHandle) {
	return lifecycle.availableCallback
}

func (lifecycle *recordingImageLifecycle) UnavailableImageCallback(
	_ conductor.SubscriptionHandle,
) func(conductor.ImageHandle) {
	return lifecycle.unavailableCallback
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

func seedGoldenAvailableLogBuffers(tb testing.TB, conductorInstance *conductor.Conductor) {
	tb.Helper()
	require.NoError(tb, conductor.SeedLogBuffersRegistryForTest(
		conductor.LogBuffersRegistryForTest(conductorInstance),
		goldenAvailableImageEvent.LogFileName,
		conductor.NewSyntheticLogBuffersForTest(tb),
	))
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

func TestConductor_onAvailableImage_emptyMediaDriverDirReportsError(t *testing.T) {
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

func TestConductor_onAvailableImage_nilLifecycleNoOp(t *testing.T) {
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

func TestConductor_onAvailableImage_newImageFailureReleasesAcquire(t *testing.T) {
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

func TestConductor_onAvailableImage_addImageFailureReleasesAcquire(t *testing.T) {
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

func TestConductor_onUnavailableImage_closeFailureSkipsRelease(t *testing.T) {
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

func TestConductor_onUnavailableImage_nilLifecycleNoOp(t *testing.T) {
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

func TestConductor_invokeImageCallback_nonErrorPanicRecovery(t *testing.T) {
	t.Parallel()

	errorHandler, gotErrors := collectConductorErrors()
	lifecycle := &recordingImageLifecycle{
		addImageOK: true,
		availableCallback: func(conductor.ImageHandle) {
			panic("string panic")
		},
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
	require.EqualError(t, (*gotErrors)[0], "conductor: image callback panic: string panic")
}

func TestConductor_invokeImageCallback_panicRecovery(t *testing.T) {
	t.Parallel()

	errorHandler, gotErrors := collectConductorErrors()
	panicErr := errors.New("conductor: handler boom")
	lifecycle := &recordingImageLifecycle{
		addImageOK: true,
		availableCallback: func(conductor.ImageHandle) {
			panic(panicErr)
		},
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
	require.Equal(t, panicErr, (*gotErrors)[0])
}

func TestConductor_invokeImageCallback_suppressesNested(t *testing.T) {
	t.Parallel()

	var outerCalls int
	lifecycle := &recordingImageLifecycle{addImageOK: true}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, func(error) {})
	lifecycle.availableCallback = func(conductor.ImageHandle) {
		outerCalls++
		transmitConductorEvent(
			t,
			conductorInstance.EventsAdapterForTest(),
			conductor.OnAvailableImage,
			conductor.AvailableImageBytes(goldenAvailableImageEvent),
		)
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
}

func TestConductor_invokeUnavailableImageCallback_panicRecovery(t *testing.T) {
	t.Parallel()

	errorHandler, gotErrors := collectConductorErrors()
	panicErr := errors.New("conductor: unavailable handler boom")
	lifecycle := &recordingImageLifecycle{
		unavailableCallback: func(conductor.ImageHandle) {
			panic(panicErr)
		},
	}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, errorHandler)
	registerGoldenImageSubscription(conductorInstance)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnUnavailableImage,
		conductor.UnavailableImageBytes(goldenUnavailableImageEvent),
	)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, *gotErrors, 1)
	require.Equal(t, panicErr, (*gotErrors)[0])
}

func TestConductor_invokeUnavailableImageCallback_suppressesNested(t *testing.T) {
	t.Parallel()

	var outerCalls int
	lifecycle := &recordingImageLifecycle{}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, func(error) {})
	lifecycle.unavailableCallback = func(conductor.ImageHandle) {
		outerCalls++
		transmitConductorEvent(
			t,
			conductorInstance.EventsAdapterForTest(),
			conductor.OnUnavailableImage,
			conductor.UnavailableImageBytes(goldenUnavailableImageEvent),
		)
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
}
