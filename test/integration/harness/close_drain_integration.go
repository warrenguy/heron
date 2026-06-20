//go:build integration

package harness

import (
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/ringbuffer"
)

const (
	closeDrainTypeIDTailCount = 8
	closeDrainErrorLogTailLen = 256
	closeDrainWireProofStep   = "wire-proof"
)

func closeDrainCapturePositionsReady(capture closeDrainWireCapture) bool {
	if !capture.pubWireDecoded || !capture.subWireDecoded {
		return false
	}
	return capture.pubPosition >= 0 && capture.subPosition >= 0
}

func closeDrainWireCaptureReady(capture closeDrainWireCapture) bool {
	hasScanEvidence := len(filterCloseDrainTypeIDs(capture.typeIDs)) > 0 ||
		(capture.pubOfferedDecoded && capture.subOfferedDecoded)
	return capture.pubWireDecoded && capture.subWireDecoded && hasScanEvidence
}

func closeDrainErrorLogTail(b []byte) string {
	return string(b[max(0, len(b)-closeDrainErrorLogTailLen):])
}

func captureCloseDrainDuringClose(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, pubRegistrationID, subRegistrationID int64,
	producerBefore int64,
	closeFn func() error,
	pollTimeout, pollInterval time.Duration,
) (closeDrainWireCapture, error) {
	capture := newCloseDrainWireCapture()

	captureOnce := func() {
		_ = tryCaptureCloseDrainWire(
			ring,
			clientID,
			pubRegistrationID,
			subRegistrationID,
			producerBefore,
			&capture,
		)
		resolveCloseDrainCapturePositions(
			ring,
			&capture,
			clientID,
			pubRegistrationID,
			subRegistrationID,
			producerBefore,
		)
	}

	captureOnce()

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- closeFn()
	}()

	pollInterval = closeDrainPollInterval(pollInterval)
	deadline := time.Now().Add(pollTimeout)

	var (
		closeErr      error
		closeFinished bool
	)

	for {
		captureOnce()

		select {
		case closeErr = <-closeDone:
			closeFinished = true
		default:
		}

		pastDeadline := !time.Now().Before(deadline)

		if closeFinished {
			wireReady := closeDrainWireDecodeReady(capture)
			if (wireReady && closeDrainCapturePositionsReady(capture)) || pastDeadline {
				break
			}
			time.Sleep(pollInterval)
			continue
		}
		if pastDeadline {
			// pollTimeout bounds wire sampling, not closeFn completion; wait for
			// closeErr so teardown result is not lost (client.Close is separately
			// capped by DriverTimeout + CloseLingerDuration).
			closeErr = <-closeDone
			break
		}

		runtime.Gosched()
	}

	captureOnce()

	return capture, closeErr
}

func dumpCloseDrainFailureDiagnostics(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	ctx CloseDrainDiagnosticsContext,
) {
	t.Helper()

	consumer := ring.ConsumerPosition()
	producer := ring.ProducerPosition()
	typeIDs, err := OfferedRecordTypeIDsBetween(ring, ctx.ProducerBefore, producer)
	if err != nil {
		t.Logf("close-drain diagnostics: close-delta scan failed: %v", err)
		typeIDs = ctx.CloseDrainTypeIDs
	}
	if len(typeIDs) == 0 {
		typeIDs, err = OfferedRecordTypeIDsBetween(ring, consumer, producer)
		if err != nil {
			t.Logf("close-drain diagnostics: committed scan failed: %v", err)
		}
	}

	filtered := filterCloseDrainTypeIDs(typeIDs)
	lastTypeIDs := filtered[max(0, len(filtered)-closeDrainTypeIDTailCount):]

	t.Logf(
		"close-drain diagnostics: clientID=%d consumer=%d producer=%d "+
			"snapshotConsumer=%d snapshotProducer=%d pubRegistrationID=%d "+
			"subRegistrationID=%d failedStep=%q provenPub=%d provenSub=%d "+
			"provenClose=%d orderingProof=%s closeDrainTypeIDs=%v lastTypeIDs=%v",
		ctx.ClientID,
		consumer,
		producer,
		ctx.ConsumerBefore,
		ctx.ProducerBefore,
		ctx.PubRegistrationID,
		ctx.SubRegistrationID,
		ctx.LastFailedObserveStep,
		ctx.ProvenPubPosition,
		ctx.ProvenSubPosition,
		ctx.ProvenClosePosition,
		ctx.CloseDrainOrderingProof,
		ctx.CloseDrainTypeIDs,
		lastTypeIDs,
	)

	if len(ctx.ErrorLogSnapshot) == 0 {
		return
	}
	t.Logf("close-drain diagnostics: errorLogTail=%q", closeDrainErrorLogTail(ctx.ErrorLogSnapshot))
}

// DumpCloseDrainGhostReuseFailure logs ring and error-log context when ghost
// subscription reuse fails after client close.
func DumpCloseDrainGhostReuseFailure(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	ctx CloseDrainDiagnosticsContext,
	ghostClient *aeron.Client,
	ghostErr error,
) {
	t.Helper()

	t.Logf("close-drain ghost reuse failed: %v", ghostErr)
	dumpCloseDrainFailureDiagnostics(t, ring, ctx)

	if ghostClient == nil {
		return
	}

	errorLog, err := ghostClient.ErrorLogBuffer()
	if err != nil {
		t.Logf("close-drain ghost diagnostics: ErrorLogBuffer: %v", err)
		return
	}
	t.Logf("close-drain ghost diagnostics: errorLogTail=%q", closeDrainErrorLogTail(errorLog))
}

func snapshotClientErrorLog(client *aeron.Client) []byte {
	if client == nil {
		return nil
	}
	errorLog, err := client.ErrorLogBuffer()
	if err != nil {
		return nil
	}
	return slices.Clone(errorLog)
}

func resolveProvenClientClosePosition(
	capture closeDrainWireCapture,
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, producerBefore, observedClosePosition int64,
) int64 {
	switch {
	case capture.closeWireDecoded:
		return capture.closePosition
	case observedClosePosition >= 0:
		return observedClosePosition
	}

	scanTo := capture.producerAfter
	if scanTo <= producerBefore {
		scanTo = ring.ProducerPosition()
	}
	if capture.subWireDecoded {
		if position := findMatchingClientClosePositionBetween(
			ring,
			clientID,
			capture.subPosition,
			scanTo,
		); position >= 0 {
			return position
		}
	}
	return findMatchingClientClosePositionBetween(ring, clientID, producerBefore, scanTo)
}

func pollCloseDrainWireDecode[T any](
	t testing.TB,
	alreadyDone bool,
	observe func(*int64) (T, bool, error),
	onMatch func(T, int64),
) {
	t.Helper()
	if alreadyDone {
		return
	}

	proven := int64(-1)
	decoded, ok, err := observe(&proven)
	require.NoError(t, err)
	if ok {
		onMatch(decoded, proven)
	}
}

func ensureCloseDrainWireIdentity(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	capture *closeDrainWireCapture,
	clientID, pubRegistrationID, subRegistrationID int64,
	consumerBefore, producerBefore int64,
	pollTimeout, pollInterval time.Duration,
) {
	t.Helper()

	pollInterval = closeDrainPollInterval(pollInterval)

	deadline := time.Now().Add(pollTimeout)
	for {
		pollCloseDrainWireDecode(
			t,
			capture.pubWireDecoded,
			func(proven *int64) (conductor.RemovePublicationMessage, bool, error) {
				return observeOnRing(
					ring,
					consumerBefore,
					producerBefore,
					proven,
					removePublicationObserveSpec(clientID, pubRegistrationID),
				)
			},
			func(decoded conductor.RemovePublicationMessage, proven int64) {
				capture.pubWireDecoded = true
				capture.removePublication = decoded
				if proven >= 0 {
					capture.pubPosition = proven
				}
			},
		)

		pollCloseDrainWireDecode(
			t,
			capture.subWireDecoded,
			func(proven *int64) (conductor.RemoveMessage, bool, error) {
				return observeOnRing(
					ring,
					consumerBefore,
					producerBefore,
					proven,
					removeSubscriptionObserveSpec(clientID, subRegistrationID),
				)
			},
			func(decoded conductor.RemoveMessage, proven int64) {
				capture.subWireDecoded = true
				capture.removeSubscription = decoded
				if proven >= 0 {
					capture.subPosition = proven
				}
			},
		)

		pollCloseDrainWireDecode(
			t,
			capture.closeWireDecoded,
			func(proven *int64) (conductor.CorrelatedMessage, bool, error) {
				return observeClientClose(
					ring,
					clientID,
					consumerBefore,
					producerBefore,
					proven,
				)
			},
			func(_ conductor.CorrelatedMessage, proven int64) {
				if proven >= 0 {
					capture.closeWireDecoded = true
					capture.closePosition = proven
				}
			},
		)

		if capture.pubWireDecoded && capture.subWireDecoded {
			return
		}
		if !time.Now().Before(deadline) {
			return
		}
		time.Sleep(pollInterval)
	}
}

func captureCloseDrainWireDuringClientClose(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, pubRegistrationID, subRegistrationID int64,
	producerBefore int64,
	closeFn func() error,
) (closeDrainWireCapture, int64, error) {
	wireCapture, closeErr := captureCloseDrainDuringClose(
		ring,
		clientID,
		pubRegistrationID,
		subRegistrationID,
		producerBefore,
		closeFn,
		commandPollDeadline,
		time.Millisecond,
	)
	earlyProvenClosePosition := resolveProvenClientClosePosition(
		wireCapture,
		ring,
		clientID,
		producerBefore,
		-1,
	)
	return wireCapture, earlyProvenClosePosition, closeErr
}

func pollCloseDrainRegistrationsReleased(
	client *aeron.Client,
	pubRegistrationID, subRegistrationID int64,
	pollTimeout, pollInterval time.Duration,
) bool {
	if client == nil {
		return false
	}

	return pollUntilBool(
		pollTimeout,
		pollInterval,
		func() bool {
			client.DoWork()
			_, pubOK := client.Publication(pubRegistrationID)
			_, subOK := client.Subscription(subRegistrationID)
			return !pubOK && !subOK
		},
	)
}

func pollReplenishCloseDrainWireCapture(
	ring *ringbuffer.ManyToOneRingBuffer,
	capture *closeDrainWireCapture,
	clientID, pubRegistrationID, subRegistrationID int64,
	producerBefore int64,
	pollTimeout, pollInterval time.Duration,
) {
	pollInterval = closeDrainPollInterval(pollInterval)

	deadline := time.Now().Add(pollTimeout)
	for {
		replenishCloseDrainWireCapture(
			ring,
			capture,
			clientID,
			pubRegistrationID,
			subRegistrationID,
			producerBefore,
		)
		if closeDrainWireDecodeReady(*capture) {
			return
		}
		if !time.Now().Before(deadline) {
			return
		}
		time.Sleep(pollInterval)
	}
}

func proveCloseDrainWireIdentity(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	wireCapture *closeDrainWireCapture,
	diagCtx *CloseDrainDiagnosticsContext,
	client *aeron.Client,
	clientID, pubRegistrationID, subRegistrationID int64,
	consumerBefore, producerBefore int64,
	earlyProvenClosePosition int64,
) (observedClosePosition int64, clientCloseObserved bool) {
	t.Helper()

	replenishCloseDrainWireCapture(
		ring,
		wireCapture,
		clientID,
		pubRegistrationID,
		subRegistrationID,
		producerBefore,
	)

	if !closeDrainWireCaptureReady(*wireCapture) {
		ensureCloseDrainWireIdentity(
			t,
			ring,
			wireCapture,
			clientID,
			pubRegistrationID,
			subRegistrationID,
			consumerBefore,
			producerBefore,
			commandPollDeadline,
			defaultPollInterval,
		)
	}

	if !closeDrainWireDecodeReady(*wireCapture) {
		pollReplenishCloseDrainWireCapture(
			ring,
			wireCapture,
			clientID,
			pubRegistrationID,
			subRegistrationID,
			producerBefore,
			commandPollDeadline,
			defaultPollInterval,
		)
	}

	if !closeDrainWireDecodeReady(*wireCapture) &&
		pollCloseDrainRegistrationsReleased(
			client,
			pubRegistrationID,
			subRegistrationID,
			commandPollDeadline,
			defaultPollInterval,
		) {
		applyCloseDrainWireProofFromRegistrations(
			wireCapture,
			clientID,
			pubRegistrationID,
			subRegistrationID,
		)
		diagCtx.CloseDrainWireProofPath = CloseDrainWireProofClientLookup
	}

	if !closeDrainWireDecodeReady(*wireCapture) {
		diagCtx.LastFailedObserveStep = closeDrainWireProofStep
		dumpCloseDrainFailureDiagnostics(t, ring, *diagCtx)
		requireCloseDrainWireProof(
			t,
			*wireCapture,
			clientID,
			pubRegistrationID,
			subRegistrationID,
		)
	}

	if diagCtx.CloseDrainWireProofPath == "" {
		diagCtx.CloseDrainWireProofPath = CloseDrainWireProofRingDecode
	}
	requireCloseDrainWireProof(t, *wireCapture, clientID, pubRegistrationID, subRegistrationID)

	preferEarlyProven := func(position int64) int64 {
		if position < 0 && earlyProvenClosePosition >= 0 {
			return earlyProvenClosePosition
		}
		return position
	}

	if wireCapture.closeWireDecoded {
		observedClosePosition = wireCapture.closePosition
	} else {
		observedClosePosition = assertClientCloseOfferedOnRing(
			t,
			ring,
			clientID,
			consumerBefore,
			producerBefore,
		)
	}
	clientCloseObserved = true // wire decode or assert returns only on success
	observedClosePosition = preferEarlyProven(observedClosePosition)

	diagCtx.ProvenPubPosition = wireCapture.pubPosition
	diagCtx.ProvenSubPosition = wireCapture.subPosition
	diagCtx.ProvenClosePosition = preferEarlyProven(resolveProvenClientClosePosition(
		*wireCapture,
		ring,
		clientID,
		producerBefore,
		observedClosePosition,
	))
	if wireCapture.closePosition < 0 && diagCtx.ProvenClosePosition >= 0 {
		wireCapture.closePosition = diagCtx.ProvenClosePosition
	}

	return observedClosePosition, clientCloseObserved
}

func proveCloseDrainRecordOrdering(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	wireCapture closeDrainWireCapture,
	diagCtx *CloseDrainDiagnosticsContext,
	producerBefore int64,
	clientCloseObserved bool,
) {
	t.Helper()

	orderingTypeIDs, scanErr := closeDrainOrderingTypeIDs(
		wireCapture,
		ring,
		producerBefore,
		clientCloseObserved,
		diagCtx.ProvenClosePosition,
	)
	require.NoError(t, scanErr)

	WaitForToDriverRingDrained(t, ring)

	diagCtx.CloseDrainOrderingProof = assertCloseDrainRecordOrdering(
		t,
		ring,
		producerBefore,
		orderingTypeIDs,
		wireCapture.pubPosition,
		wireCapture.subPosition,
		diagCtx.ProvenClosePosition,
	)
	if diagCtx.CloseDrainOrderingProof == CloseDrainOrderingNone {
		diagCtx.CloseDrainOrderingProof = closeDrainOrderingFromWireCapture(
			t,
			wireCapture,
			diagCtx.ProvenClosePosition,
			clientCloseObserved,
		)
	}
	if diagCtx.CloseDrainOrderingProof == CloseDrainOrderingNone {
		dumpCloseDrainFailureDiagnostics(t, ring, *diagCtx)
		require.Fail(
			t,
			"close-drain ordering not proven: need proven positions, close-delta type IDs, or wire identity",
		)
	}
}

// AssertClientCloseDrainsRegistrationsOnRing closes the client and asserts
// REMOVE_PUBLICATION, REMOVE_SUBSCRIPTION, and CLIENT_CLOSE on ToDriver.
//
// Requires a quiescent ring snapshot (consumerBefore == producerBefore) taken
// after WaitForToDriverRingDrained. client supplies a pre-close ErrorLogBuffer
// snapshot; do not rely on ErrorLogBuffer after closeFn returns.
//
// Wire identity is proven via offered-path decode captured while closeFn runs,
// or client registration lookup miss when live aeronmd erases consumed ring slots.
//
// Ordering proof tiers (strongest first): proven offered positions, close-delta
// type IDs filtered to close-drain commands (keepalive interleave ignored), then
// wire-identity when removes are identified but ring positions and type IDs are
// unavailable. Wire-identity does not assert pub-before-sub ordering.
func AssertClientCloseDrainsRegistrationsOnRing(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, pubRegistrationID, subRegistrationID int64,
	consumerBefore, producerBefore int64,
	closeFn func() error,
	client *aeron.Client,
) CloseDrainDiagnosticsContext {
	t.Helper()

	requireQuiescentToDriverSnapshot(t, ring, consumerBefore, producerBefore)

	diagCtx := CloseDrainDiagnosticsContext{
		ClientID:          clientID,
		ConsumerBefore:    consumerBefore,
		ProducerBefore:    producerBefore,
		PubRegistrationID: pubRegistrationID,
		SubRegistrationID: subRegistrationID,
		ErrorLogSnapshot:  snapshotClientErrorLog(client),
	}

	wireCapture, earlyProvenClosePosition, closeErr := captureCloseDrainWireDuringClientClose(
		ring,
		clientID,
		pubRegistrationID,
		subRegistrationID,
		producerBefore,
		closeFn,
	)
	diagCtx.CloseDrainTypeIDs = wireCapture.typeIDs

	if closeErr != nil {
		dumpCloseDrainFailureDiagnostics(t, ring, diagCtx)
		require.NoError(t, closeErr, "client close failed during close-drain assert")
	}

	_, clientCloseObserved := proveCloseDrainWireIdentity(
		t,
		ring,
		&wireCapture,
		&diagCtx,
		client,
		clientID,
		pubRegistrationID,
		subRegistrationID,
		consumerBefore,
		producerBefore,
		earlyProvenClosePosition,
	)
	proveCloseDrainRecordOrdering(
		t,
		ring,
		wireCapture,
		&diagCtx,
		producerBefore,
		clientCloseObserved,
	)

	return diagCtx
}
