//go:build stress

package conductor_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
)

const (
	stressDoWorkIterations          = 50_000
	suspendStressReaderGoroutines   = 16
	suspendStressLookupsPerReader   = 3_125
	suspendStressAlternatingCycles  = 50
	suspendStressAlternatingReaders = 8
	suspendStressAlternatingLookups = 125
)

func lookupSubscription(registrationID int64) func(*conductor.Conductor) error {
	return func(conductorInstance *conductor.Conductor) error {
		got, ok := conductorInstance.SubscriptionByRegID(registrationID)
		if !ok {
			return fmt.Errorf("subscription %d not found", registrationID)
		}
		if got.RegistrationID() != registrationID {
			return fmt.Errorf("got registration ID %d, want %d", got.RegistrationID(), registrationID)
		}
		return nil
	}
}

func TestConductor_resourceByRegID_concurrentWithDoWork_Stress(t *testing.T) {
	tests := []struct {
		name           string
		registrationID int64
		seed           func(*conductor.Conductor)
		lookup         func(*conductor.Conductor) error
	}{
		{
			name:           "publication",
			registrationID: 42,
			seed: func(conductorInstance *conductor.Conductor) {
				conductorInstance.InsertPublicationForTest(
					42,
					stubPublication{registrationID: 42},
				)
			},
			lookup: lookupPublication(42),
		},
		{
			name:           "subscription",
			registrationID: 5005,
			seed: func(conductorInstance *conductor.Conductor) {
				conductorInstance.InsertSubscriptionForTest(
					5005,
					stubSubscription{registrationID: 5005, streamID: 7},
				)
			},
			lookup: lookupSubscription(5005),
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			conductorInstance, _ := newConductorForMutableNanoTest(
				t,
				func(error) {},
				resourceMapTestEpochMs,
			)
			conductorInstance.SeedServiceTimestamps(1)
			testCase.seed(conductorInstance)

			var wg sync.WaitGroup
			wg.Go(func() {
				for range stressDoWorkIterations {
					require.NoError(t, testCase.lookup(conductorInstance))
				}
			})

			for range stressDoWorkIterations {
				conductorInstance.DoWork()
			}

			wg.Wait()
		})
	}
}

func runSuspendedCallbackWindowStress(
	t *testing.T,
	conductorInstance *conductor.Conductor,
	callbackEntered chan struct{},
	releaseCallback chan struct{},
	readerGoroutines int,
	lookupsPerReader int,
	lookup func(*conductor.Conductor) error,
	cycle int,
) {
	t.Helper()

	readerErrors := make(chan error, readerGoroutines)

	var readerWG sync.WaitGroup
	for range readerGoroutines {
		readerWG.Go(func() {
			<-callbackEntered
			for range lookupsPerReader {
				if err := lookup(conductorInstance); err != nil {
					readerErrors <- err
					return
				}
			}
			readerErrors <- nil
		})
	}

	doWorkDone := make(chan int)
	go func() {
		doWorkDone <- conductorInstance.DoWork()
	}()

	<-callbackEntered
	readerWG.Wait()
	for range readerGoroutines {
		select {
		case err := <-readerErrors:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("reader did not finish lookups during suspended callback")
		}
	}

	select {
	case workCount := <-doWorkDone:
		if cycle >= 0 {
			t.Fatalf("cycle %d: DoWork finished before callback released: workCount=%d", cycle, workCount)
		}
		t.Fatalf("DoWork finished before callback released: workCount=%d", workCount)
	default:
	}
	close(releaseCallback)
	require.Equal(t, 1, <-doWorkDone)
}

func runSuspendedCallbackLookupStress(
	t *testing.T,
	available bool,
	lookup func(*conductor.Conductor) error,
	readerGoroutines int,
	lookupsPerReader int,
) {
	t.Helper()

	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})

	lifecycle := &recordingImageLifecycle{addImageOK: true}
	if !available {
		lifecycle = &recordingImageLifecycle{}
	}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, func(error) {})
	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
	registerGoldenImageSubscription(conductorInstance)
	if available {
		seedGoldenAvailableLogBuffers(t, conductorInstance)
	}

	blockCallback := func(conductor.ImageHandle) {
		close(callbackEntered)
		<-releaseCallback
	}
	if available {
		lifecycle.availableCallback = blockCallback
		transmitConductorEvent(
			t,
			conductorInstance.EventsAdapterForTest(),
			conductor.OnAvailableImage,
			conductor.AvailableImageBytes(goldenAvailableImageEvent),
		)
	} else {
		lifecycle.unavailableCallback = blockCallback
		triggerGoldenUnavailableImage(t, conductorInstance)
	}

	runSuspendedCallbackWindowStress(
		t,
		conductorInstance,
		callbackEntered,
		releaseCallback,
		readerGoroutines,
		lookupsPerReader,
		lookup,
		-1,
	)
}

func TestConductor_PublicationByRegID_concurrentDuringImageCallback_Stress(t *testing.T) {
	runSuspendedCallbackLookupStress(
		t,
		true,
		lookupPublication(42),
		suspendStressReaderGoroutines,
		suspendStressLookupsPerReader,
	)
}

func TestConductor_PublicationByRegID_concurrentDuringUnavailableImageCallback_Stress(t *testing.T) {
	runSuspendedCallbackLookupStress(
		t,
		false,
		lookupPublication(42),
		suspendStressReaderGoroutines,
		suspendStressLookupsPerReader,
	)
}

func TestConductor_SubscriptionByRegID_concurrentDuringImageCallback_Stress(t *testing.T) {
	runSuspendedCallbackLookupStress(
		t,
		true,
		lookupSubscription(goldenAvailableImageEvent.SubscriptionRegistrationID),
		suspendStressReaderGoroutines,
		suspendStressLookupsPerReader,
	)
}

func TestConductor_SubscriptionByRegID_concurrentDuringUnavailableImageCallback_Stress(t *testing.T) {
	runSuspendedCallbackLookupStress(
		t,
		false,
		lookupSubscription(goldenUnavailableImageEvent.SubscriptionRegistrationID),
		suspendStressReaderGoroutines,
		suspendStressLookupsPerReader,
	)
}

func TestConductor_PublicationByRegID_suspendWindowAlternating_Stress(t *testing.T) {
	lifecycle := &recordingImageLifecycle{addImageOK: true}
	conductorInstance := newConductorWithRecordingLifecycle(t, lifecycle, func(error) {})
	conductorInstance.InsertPublicationForTest(42, stubPublication{registrationID: 42})
	registerGoldenImageSubscription(conductorInstance)
	seedGoldenAvailableLogBuffers(t, conductorInstance)
	conductorInstance.SeedServiceTimestamps(1)

	for cycle := range suspendStressAlternatingCycles {
		available := cycle%2 == 0
		callbackEntered := make(chan struct{})
		releaseCallback := make(chan struct{})

		blockCallback := func(conductor.ImageHandle) {
			close(callbackEntered)
			<-releaseCallback
		}
		if available {
			lifecycle.availableCallback = blockCallback
			lifecycle.unavailableCallback = nil
			transmitConductorEvent(
				t,
				conductorInstance.EventsAdapterForTest(),
				conductor.OnAvailableImage,
				conductor.AvailableImageBytes(goldenAvailableImageEvent),
			)
		} else {
			lifecycle.unavailableCallback = blockCallback
			lifecycle.availableCallback = nil
			triggerGoldenUnavailableImage(t, conductorInstance)
		}

		runSuspendedCallbackWindowStress(
			t,
			conductorInstance,
			callbackEntered,
			releaseCallback,
			suspendStressAlternatingReaders,
			suspendStressAlternatingLookups,
			lookupPublication(42),
			cycle,
		)
	}
}
