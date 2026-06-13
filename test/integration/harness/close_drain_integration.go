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
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const (
	closeDrainTypeIDTailCount = 8
	closeDrainErrorLogTailLen = 256
	closeDrainWireProofStep   = "wire-proof"
)

func closeDrainWireCaptureReady(capture closeDrainWireCapture) bool {
	hasScanEvidence := len(filterCloseDrainTypeIDs(capture.typeIDs)) > 0 ||
		(capture.pubOfferedDecoded && capture.subOfferedDecoded)
	return capture.pubWireDecoded && capture.subWireDecoded && hasScanEvidence
}

func closeDrainErrorLogTail(b []byte) string {
	if len(b) <= closeDrainErrorLogTailLen {
		return string(b)
	}
	return string(b[len(b)-closeDrainErrorLogTailLen:])
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

		ready := closeDrainWireCaptureReady(capture)

		if closeFinished && (ready || !time.Now().Before(deadline)) {
			break
		}
		if !time.Now().Before(deadline) {
			// pollTimeout bounds wire sampling, not closeFn completion; wait for
			// closeErr so teardown result is not lost (client.Close is separately
			// capped by DriverTimeout + CloseLingerDuration).
			if !closeFinished {
				closeErr = <-closeDone
			}
			break
		}

		if closeFinished {
			time.Sleep(pollInterval)
		} else {
			runtime.Gosched()
		}
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
	typeIDs, err := offeredRecordTypeIDsBetween(ring, ctx.ProducerBefore, producer)
	if err != nil {
		t.Logf("close-drain diagnostics: close-delta scan failed: %v", err)
		typeIDs = ctx.CloseDrainTypeIDs
	}
	if len(typeIDs) == 0 {
		typeIDs, err = offeredRecordTypeIDsBetween(ring, consumer, producer)
		if err != nil {
			t.Logf("close-drain diagnostics: committed scan failed: %v", err)
		}
	}

	filtered := filterCloseDrainTypeIDs(typeIDs)
	tailTypeIDs := filtered
	if len(filtered) > closeDrainTypeIDTailCount {
		tailTypeIDs = filtered[len(filtered)-closeDrainTypeIDTailCount:]
	}

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
		tailTypeIDs,
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

func proveCloseDrainWireIdentity(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	wireCapture *closeDrainWireCapture,
	diagCtx *CloseDrainDiagnosticsContext,
	clientID, pubRegistrationID, subRegistrationID int64,
	consumerBefore, producerBefore int64,
	earlyProvenClosePosition int64,
) (observedClosePosition int64, clientCloseObserved bool) {
	t.Helper()

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
	if !wireCapture.pubWireDecoded || !wireCapture.subWireDecoded {
		diagCtx.LastFailedObserveStep = closeDrainWireProofStep
		dumpCloseDrainFailureDiagnostics(t, ring, *diagCtx)
	}
	requireCloseDrainWireProof(t, *wireCapture, clientID, pubRegistrationID, subRegistrationID)

	if wireCapture.closeWireDecoded {
		observedClosePosition = wireCapture.closePosition
		clientCloseObserved = true
	} else {
		observedClosePosition = assertClientCloseOfferedOnRing(
			t,
			ring,
			clientID,
			consumerBefore,
			producerBefore,
		)
		clientCloseObserved = true // assert returns only on success
	}
	if observedClosePosition < 0 && earlyProvenClosePosition >= 0 {
		observedClosePosition = earlyProvenClosePosition
	}

	diagCtx.ProvenPubPosition = wireCapture.pubPosition
	diagCtx.ProvenSubPosition = wireCapture.subPosition
	diagCtx.ProvenClosePosition = resolveProvenClientClosePosition(
		*wireCapture,
		ring,
		clientID,
		producerBefore,
		observedClosePosition,
	)
	if diagCtx.ProvenClosePosition < 0 && earlyProvenClosePosition >= 0 {
		diagCtx.ProvenClosePosition = earlyProvenClosePosition
	}
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
		dumpCloseDrainFailureDiagnostics(t, ring, *diagCtx)
		require.Fail(
			t,
			"close-drain ordering not proven: need proven positions or close-delta type IDs",
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
// Wire identity is proven via offered-path decode captured while closeFn runs.
// Ordering uses proven offered positions when captured, else close-delta type IDs
// filtered to close-drain commands (keepalive interleave ignored).
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
