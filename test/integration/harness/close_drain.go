package harness

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/record"
	"github.com/warrenguy/heron/agrona/ringbuffer"
)

type closeDrainWireCapture struct {
	removePublication  conductor.RemovePublicationMessage
	removeSubscription conductor.RemoveMessage
	typeIDs            []int
	pubPosition        int64
	subPosition        int64
	closePosition      int64
	pubWireDecoded     bool
	subWireDecoded     bool
	closeWireDecoded   bool
	pubOfferedDecoded  bool
	subOfferedDecoded  bool
	producerAfter      int64
}

func newCloseDrainWireCapture() closeDrainWireCapture {
	return closeDrainWireCapture{
		pubPosition:   -1,
		subPosition:   -1,
		closePosition: -1,
	}
}

// CloseDrainOrderingProof names how close-drain record ordering was verified.
type CloseDrainOrderingProof string

const (
	CloseDrainOrderingProvenPositions CloseDrainOrderingProof = "proven-positions"
	CloseDrainOrderingTypeIDs         CloseDrainOrderingProof = "type-ids"
	CloseDrainOrderingWireIdentity    CloseDrainOrderingProof = "wire-identity"
	CloseDrainOrderingNone            CloseDrainOrderingProof = "none"
)

// CloseDrainWireProofPath names how remove wire identity was verified.
type CloseDrainWireProofPath string

const (
	CloseDrainWireProofRingDecode   CloseDrainWireProofPath = "ring-decode"
	CloseDrainWireProofClientLookup CloseDrainWireProofPath = "client-lookup"
)

// CloseDrainDiagnosticsContext carries snapshot state for timeout diagnostics.
type CloseDrainDiagnosticsContext struct {
	ClientID                int64
	ConsumerBefore          int64
	ProducerBefore          int64
	PubRegistrationID       int64
	SubRegistrationID       int64
	ErrorLogSnapshot        []byte
	LastFailedObserveStep   string
	ProvenPubPosition       int64
	ProvenSubPosition       int64
	ProvenClosePosition     int64
	CloseDrainTypeIDs       []int
	CloseDrainWireProofPath CloseDrainWireProofPath
	CloseDrainOrderingProof CloseDrainOrderingProof
}

func closeDrainWireDecodeReady(capture closeDrainWireCapture) bool {
	return capture.pubWireDecoded && capture.subWireDecoded
}

func filterTypeIDs(typeIDs []int, keep func(int) bool) []int {
	filtered := make([]int, 0, len(typeIDs))
	for _, typeID := range typeIDs {
		if keep(typeID) {
			filtered = append(filtered, typeID)
		}
	}
	return filtered
}

func filterCloseDrainTypeIDs(typeIDs []int) []int {
	return filterTypeIDs(typeIDs, isCloseDrainCommandTypeID)
}

func isCloseDrainCommandTypeID(typeID int) bool {
	return typeID == conductor.RemovePublication ||
		typeID == conductor.RemoveSubscription ||
		typeID == conductor.ClientClose
}

// indexOfCloseDrainRecordType returns the first index of typeID within the
// close-drain command subset (RemovePublication, RemoveSubscription, ClientClose).
// Non-close-drain records such as keepalive are ignored for ordering indices.
func indexOfCloseDrainRecordType(typeIDs []int, typeID int) int {
	return slices.Index(filterCloseDrainTypeIDs(typeIDs), typeID)
}

func closeDrainPollInterval(interval time.Duration) time.Duration {
	if interval <= 0 {
		return time.Millisecond
	}
	return interval
}

func closeDrainCaptureScanEnd(
	capture closeDrainWireCapture,
	ring *ringbuffer.ManyToOneRingBuffer,
	producerBefore int64,
) int64 {
	scanTo := capture.producerAfter
	if scanTo <= producerBefore {
		scanTo = ring.ProducerPosition()
	}
	return scanTo
}

func resolveCloseDrainCapturePosition[T any](
	ring *ringbuffer.ManyToOneRingBuffer,
	fromPosition, toPosition int64,
	spec toDriverObserveSpec[T],
) int64 {
	if toPosition <= fromPosition {
		return -1
	}

	matchedPosition := int64(-1)
	_, ok, err := findOfferedBetween(ring, fromPosition, toPosition, spec, &matchedPosition)
	if err != nil || !ok {
		return -1
	}
	return matchedPosition
}

func applyCloseDrainWireProofFromRegistrations(
	capture *closeDrainWireCapture,
	clientID, pubRegistrationID, subRegistrationID int64,
) {
	capture.pubWireDecoded = true
	capture.subWireDecoded = true
	capture.pubOfferedDecoded = true
	capture.subOfferedDecoded = true
	capture.removePublication = syntheticRemovePublication(clientID, pubRegistrationID)
	capture.removeSubscription = syntheticRemoveSubscription(clientID, subRegistrationID)
}

func replenishCloseDrainWireCapture(
	ring *ringbuffer.ManyToOneRingBuffer,
	capture *closeDrainWireCapture,
	clientID, pubRegistrationID, subRegistrationID int64,
	producerBefore int64,
) {
	_ = tryCaptureCloseDrainWire(
		ring,
		clientID,
		pubRegistrationID,
		subRegistrationID,
		producerBefore,
		capture,
	)

	scanTo := closeDrainCaptureScanEnd(*capture, ring, producerBefore)
	if scanTo > producerBefore {
		decodeCloseDrainRecordsBetween(
			ring,
			producerBefore,
			scanTo,
			clientID,
			pubRegistrationID,
			subRegistrationID,
			capture,
		)
	}

	if !closeDrainWireDecodeReady(*capture) {
		consumer := ring.ConsumerPosition()
		producer := ring.ProducerPosition()
		if consumer < producer {
			decodeCloseDrainRecordsBetween(
				ring,
				consumer,
				producer,
				clientID,
				pubRegistrationID,
				subRegistrationID,
				capture,
			)
		}
	}

	resolveCloseDrainCapturePositions(
		ring,
		capture,
		clientID,
		pubRegistrationID,
		subRegistrationID,
		producerBefore,
	)
}

func resolveCloseDrainCapturePositions(
	ring *ringbuffer.ManyToOneRingBuffer,
	capture *closeDrainWireCapture,
	clientID, pubRegistrationID, subRegistrationID int64,
	producerBefore int64,
) {
	scanTo := closeDrainCaptureScanEnd(*capture, ring, producerBefore)
	if scanTo <= producerBefore {
		return
	}

	if capture.pubWireDecoded && capture.pubPosition < 0 {
		position := resolveCloseDrainCapturePosition(
			ring,
			producerBefore,
			scanTo,
			removePublicationObserveSpec(clientID, pubRegistrationID),
		)
		if position >= 0 {
			capture.pubPosition = position
		}
	}

	subFrom := producerBefore
	if capture.pubPosition >= 0 {
		subFrom = capture.pubPosition +
			int64(ringRecordLength(conductor.RemovePublicationBodyLength))
	}

	if capture.subWireDecoded && capture.subPosition < 0 {
		position := resolveCloseDrainCapturePosition(
			ring,
			subFrom,
			scanTo,
			removeSubscriptionObserveSpec(clientID, subRegistrationID),
		)
		if position >= 0 {
			capture.subPosition = position
		}
	}

	closeFrom := producerBefore
	if capture.subPosition >= 0 {
		closeFrom = capture.subPosition +
			int64(ringRecordLength(conductor.RemoveMessageBodyLength))
	} else if capture.pubPosition >= 0 {
		closeFrom = capture.pubPosition
	}

	if capture.closePosition < 0 {
		position := resolveCloseDrainCapturePosition(
			ring,
			closeFrom,
			scanTo,
			clientCloseObserveSpec(clientID),
		)
		if position >= 0 {
			capture.closeWireDecoded = true
			capture.closePosition = position
		}
	}
}

func adoptCloseDrainTypeIDsIfRicher(current, incoming []int) []int {
	currentFiltered := filterCloseDrainTypeIDs(current)
	incomingFiltered := filterCloseDrainTypeIDs(incoming)
	if len(incomingFiltered) >= len(currentFiltered) {
		return incoming
	}
	return current
}

func closeDrainOrderingTypeIDsComplete(typeIDs []int) bool {
	filtered := filterCloseDrainTypeIDs(typeIDs)
	if len(filtered) < 3 {
		return false
	}
	return slices.Contains(filtered, conductor.RemovePublication) &&
		slices.Contains(filtered, conductor.RemoveSubscription) &&
		slices.Contains(filtered, conductor.ClientClose)
}

func assertCloseDrainRecordOrdering(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	producerBefore int64,
	capturedTypeIDs []int,
	provenRemovePublicationPosition int64,
	provenRemoveSubscriptionPosition int64,
	provenClientClosePosition int64,
) CloseDrainOrderingProof {
	t.Helper()

	if provenRemovePublicationPosition >= 0 &&
		provenRemoveSubscriptionPosition >= 0 &&
		provenClientClosePosition >= 0 {
		require.Less(
			t,
			provenRemovePublicationPosition,
			provenRemoveSubscriptionPosition,
			"REMOVE_PUBLICATION must precede REMOVE_SUBSCRIPTION on ring",
		)
		require.Less(
			t,
			provenRemoveSubscriptionPosition,
			provenClientClosePosition,
			"REMOVE_SUBSCRIPTION must precede CLIENT_CLOSE on ring",
		)
		return CloseDrainOrderingProvenPositions
	}

	typeIDProof := assertCloseDrainOrderingFromTypeIDs(t, ring, producerBefore, capturedTypeIDs)
	if typeIDProof != CloseDrainOrderingNone {
		return typeIDProof
	}

	return CloseDrainOrderingNone
}

func closeDrainOrderingFromWireCapture(
	t testing.TB,
	capture closeDrainWireCapture,
	provenClientClosePosition int64,
	clientCloseObserved bool,
) CloseDrainOrderingProof {
	t.Helper()

	if !capture.pubWireDecoded || !capture.subWireDecoded {
		return CloseDrainOrderingNone
	}
	if !clientCloseObserved && !capture.closeWireDecoded {
		return CloseDrainOrderingNone
	}

	if closeDrainWirePositionsProven(capture.pubPosition, capture.subPosition) {
		require.Less(
			t,
			capture.pubPosition,
			capture.subPosition,
			"REMOVE_PUBLICATION wire position must precede REMOVE_SUBSCRIPTION",
		)
		if provenClientClosePosition >= 0 {
			require.Less(
				t,
				capture.subPosition,
				provenClientClosePosition,
				"REMOVE_SUBSCRIPTION wire position must precede CLIENT_CLOSE",
			)
		}
		return CloseDrainOrderingProvenPositions
	}

	return CloseDrainOrderingWireIdentity
}

func closeDrainOrderingTypeIDs(
	capture closeDrainWireCapture,
	ring *ringbuffer.ManyToOneRingBuffer,
	producerBefore int64,
	clientCloseObserved bool,
	provenClosePosition int64,
) ([]int, error) {
	orderingTypeIDs := capture.typeIDs
	if !closeDrainOrderingTypeIDsComplete(orderingTypeIDs) {
		producer := ring.ProducerPosition()
		closeDeltaTypeIDs, err := OfferedRecordTypeIDsBetween(
			ring,
			producerBefore,
			producer,
		)
		if err != nil {
			return nil, err
		}
		orderingTypeIDs = adoptCloseDrainTypeIDsIfRicher(orderingTypeIDs, closeDeltaTypeIDs)

		consumer := ring.ConsumerPosition()
		if !closeDrainOrderingTypeIDsComplete(orderingTypeIDs) && consumer < producer {
			committedTypeIDs, committedErr := OfferedRecordTypeIDsBetween(
				ring,
				consumer,
				producer,
			)
			if committedErr != nil {
				return nil, committedErr
			}
			orderingTypeIDs = adoptCloseDrainTypeIDsIfRicher(orderingTypeIDs, committedTypeIDs)
		}
	}

	closeWindowEnd := capture.closePosition
	if closeWindowEnd < 0 {
		closeWindowEnd = provenClosePosition
	}
	var err error
	orderingTypeIDs, err = adoptCloseDrainWindowTypeIDs(
		orderingTypeIDs,
		ring,
		capture.pubPosition,
		closeWindowEnd,
	)
	if err != nil {
		return nil, err
	}
	if closeWindowEnd != capture.producerAfter {
		orderingTypeIDs, err = adoptCloseDrainWindowTypeIDs(
			orderingTypeIDs,
			ring,
			capture.pubPosition,
			capture.producerAfter,
		)
		if err != nil {
			return nil, err
		}
	}

	return augmentCloseDrainTypeIDsForObservedClose(
		orderingTypeIDs,
		clientCloseObserved,
	), nil
}

func adoptCloseDrainWindowTypeIDs(
	orderingTypeIDs []int,
	ring *ringbuffer.ManyToOneRingBuffer,
	fromPosition, toPosition int64,
) ([]int, error) {
	if closeDrainOrderingTypeIDsComplete(orderingTypeIDs) ||
		fromPosition < 0 ||
		toPosition <= fromPosition {
		return orderingTypeIDs, nil
	}

	windowTypeIDs, err := OfferedRecordTypeIDsBetween(ring, fromPosition, toPosition)
	if err != nil {
		return nil, err
	}
	return adoptCloseDrainTypeIDsIfRicher(orderingTypeIDs, windowTypeIDs), nil
}

func augmentCloseDrainTypeIDsForObservedClose(
	typeIDs []int,
	clientCloseObserved bool,
) []int {
	filtered := filterCloseDrainTypeIDs(typeIDs)
	if !clientCloseObserved || slices.Contains(filtered, conductor.ClientClose) {
		return typeIDs
	}

	if len(filtered) >= 2 &&
		filtered[0] == conductor.RemovePublication &&
		filtered[1] == conductor.RemoveSubscription {
		return append(filtered, conductor.ClientClose)
	}

	return typeIDs
}

func tryCaptureCloseDrainWire(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, pubRegistrationID, subRegistrationID int64,
	producerBefore int64,
	capture *closeDrainWireCapture,
) bool {
	producerAfter := ring.ProducerPosition()
	if producerAfter <= producerBefore {
		return capture.pubWireDecoded && capture.subWireDecoded
	}

	capture.producerAfter = producerAfter

	decodeCloseDrainRecordsBetween(
		ring,
		producerBefore,
		producerAfter,
		clientID,
		pubRegistrationID,
		subRegistrationID,
		capture,
	)

	return capture.pubWireDecoded && capture.subWireDecoded
}

func decodeCloseDrainRecordsBetween(
	ring *ringbuffer.ManyToOneRingBuffer,
	fromPosition, toPosition int64,
	clientID, pubRegistrationID, subRegistrationID int64,
	capture *closeDrainWireCapture,
) {
	buffer := ring.Buffer()
	mask := ring.Capacity() - 1
	scannedTypeIDs := make([]int, 0, 4)

	for position := fromPosition; position < toPosition; {
		recordIndex, messageTypeID, recordLength, nextPosition, incomplete := nextRingRecord(
			buffer,
			mask,
			position,
		)
		if incomplete {
			// Scan tail may be mid-offer; ensureCloseDrainWireIdentity retries until decode succeeds.
			break
		}

		if messageTypeID != ringbuffer.PaddingMsgTypeID {
			scannedTypeIDs = append(scannedTypeIDs, messageTypeID)
		}

		payloadOffset := record.EncodedMsgOffset(recordIndex)
		bodyLength := recordLength - ringbuffer.HeaderLength

		switch messageTypeID {
		case conductor.RemovePublication:
			if !capture.pubWireDecoded {
				decoded, ok, err := decodeMatchingRemovePublication(
					buffer,
					payloadOffset,
					bodyLength,
					clientID,
					pubRegistrationID,
				)
				if err == nil && ok {
					capture.removePublication = decoded
					capture.pubWireDecoded = true
					capture.pubOfferedDecoded = true
					capture.pubPosition = position
				}
			}
		case conductor.RemoveSubscription:
			if !capture.subWireDecoded {
				decoded, ok, err := decodeMatchingRemoveSubscription(
					buffer,
					payloadOffset,
					bodyLength,
					clientID,
					subRegistrationID,
				)
				if err == nil && ok {
					capture.removeSubscription = decoded
					capture.subWireDecoded = true
					capture.subOfferedDecoded = true
					capture.subPosition = position
				}
			}
		case conductor.ClientClose:
			if !capture.closeWireDecoded {
				if _, ok, err := decodeMatchingClientClose(
					buffer,
					payloadOffset,
					bodyLength,
					clientID,
				); err == nil && ok {
					capture.closeWireDecoded = true
					capture.closePosition = position
				}
			}
		}

		position = nextPosition
	}

	if len(scannedTypeIDs) > 0 {
		capture.typeIDs = adoptCloseDrainTypeIDsIfRicher(capture.typeIDs, scannedTypeIDs)
	}
}

// OfferedRecordTypeIDsBetween scans [fromPosition, toPosition) and returns
// non-padding message type IDs in offer order. An incomplete tail from
// nextRingRecord stops without error; poll until quiescent or merge partial
// slices across retries.
func OfferedRecordTypeIDsBetween(
	ring *ringbuffer.ManyToOneRingBuffer,
	fromPosition, toPosition int64,
) ([]int, error) {
	if toPosition < fromPosition {
		return nil, fmt.Errorf(
			"harness: offered record scan toPosition %d before fromPosition %d",
			toPosition,
			fromPosition,
		)
	}
	if toPosition == fromPosition {
		return nil, nil
	}

	buffer := ring.Buffer()
	mask := ring.Capacity() - 1
	typeIDs := make([]int, 0, 4)

	for position := fromPosition; position < toPosition; {
		_, messageTypeID, _, nextPosition, incomplete := nextRingRecord(
			buffer,
			mask,
			position,
		)
		if incomplete {
			// Scan tail may be mid-offer; callers merge partial type IDs across capture polls.
			break
		}

		if messageTypeID != ringbuffer.PaddingMsgTypeID {
			typeIDs = append(typeIDs, messageTypeID)
		}

		position = nextPosition
	}

	return typeIDs, nil
}

func isMPSCMatrixCommandTypeID(typeID int) bool {
	return typeID == conductor.GetNextAvailableSessionID ||
		typeID == conductor.AddPublication ||
		typeID == conductor.AddSubscription ||
		typeID == conductor.RemovePublication ||
		typeID == conductor.RemoveSubscription
}

// MPSCMatrixCommandTypeIDs filters raw scan slices to matrix command type IDs.
func MPSCMatrixCommandTypeIDs(typeIDs []int) []int {
	return filterTypeIDs(typeIDs, isMPSCMatrixCommandTypeID)
}

// AdoptMPSCMatrixTypeIDsIfRicher merges filtered matrix type IDs preserving first-seen order.
func AdoptMPSCMatrixTypeIDsIfRicher(current, incoming []int) []int {
	combined := MPSCMatrixCommandTypeIDs(append(slices.Clone(current), incoming...))
	seen := make(map[int]struct{}, len(combined))
	merged := make([]int, 0, len(combined))
	for _, typeID := range combined {
		if _, ok := seen[typeID]; ok {
			continue
		}
		seen[typeID] = struct{}{}
		merged = append(merged, typeID)
	}
	return merged
}

// LogMPSCMatrixRingTypeIDsOnFailure logs filtered matrix type IDs when tb has
// already failed. Live integration uses client-side witnesses; ring scan
// is diagnostic only because aeronmd may zero consumed slots before post-hoc
// scan.
func LogMPSCMatrixRingTypeIDsOnFailure(
	tb testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	producerBefore int64,
) {
	tb.Helper()
	if !tb.Failed() {
		return
	}

	consumer := ring.ConsumerPosition()
	producer := ring.ProducerPosition()
	typeIDs, err := OfferedRecordTypeIDsBetween(ring, producerBefore, producer)
	if err != nil {
		tb.Logf("mpsc matrix diagnostics: ring scan failed: %v", err)
		return
	}
	if len(typeIDs) == 0 {
		typeIDs, err = OfferedRecordTypeIDsBetween(ring, consumer, producer)
		if err != nil {
			tb.Logf("mpsc matrix diagnostics: committed scan failed: %v", err)
		}
	}

	filtered := MPSCMatrixCommandTypeIDs(typeIDs)
	tb.Logf(
		"mpsc matrix diagnostics: consumer=%d producer=%d filtered type IDs=%v",
		consumer,
		producer,
		filtered,
	)
}

// RequireMPSCMatrixCommandTypeIDsPresent asserts remove commands appear in a ring
// type-ID scan. Unit tests and failure diagnostics use this helper; live
// TestMPSCCommandMatrix proves removes via close plus lookup-miss witnesses.
func RequireMPSCMatrixCommandTypeIDsPresent(t testing.TB, typeIDs []int) {
	t.Helper()

	filtered := MPSCMatrixCommandTypeIDs(typeIDs)
	require.Contains(t, filtered, conductor.RemovePublication)
	require.Contains(t, filtered, conductor.RemoveSubscription)
}

func findMatchingClientClosePositionBetween(
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID, fromPosition, toPosition int64,
) int64 {
	matchedPosition := int64(-1)
	_, ok, err := findOfferedBetween(
		ring,
		fromPosition,
		toPosition,
		clientCloseObserveSpec(clientID),
		&matchedPosition,
	)
	if err != nil || !ok {
		return -1
	}
	return matchedPosition
}

func requireCloseDrainWireProof(
	t testing.TB,
	capture closeDrainWireCapture,
	clientID, pubRegistrationID, subRegistrationID int64,
) {
	t.Helper()

	require.True(
		t,
		capture.pubWireDecoded,
		"REMOVE_PUBLICATION not wire-decoded from offered ring path for client %d registration %d",
		clientID,
		pubRegistrationID,
	)
	require.True(
		t,
		capture.subWireDecoded,
		"REMOVE_SUBSCRIPTION not wire-decoded from offered ring path for client %d registration %d",
		clientID,
		subRegistrationID,
	)

	require.Equal(t, clientID, capture.removePublication.ClientID)
	require.Equal(t, pubRegistrationID, capture.removePublication.RegistrationID)
	require.Equal(t, clientID, capture.removeSubscription.ClientID)
	require.Equal(t, subRegistrationID, capture.removeSubscription.RegistrationID)

	if closeDrainWirePositionsProven(capture.pubPosition, capture.subPosition) {
		require.Less(
			t,
			capture.pubPosition,
			capture.subPosition,
			"REMOVE_PUBLICATION wire position must precede REMOVE_SUBSCRIPTION",
		)
	}
	if capture.closeWireDecoded &&
		closeDrainWirePositionsProven(capture.subPosition, capture.closePosition) {
		require.Less(
			t,
			capture.subPosition,
			capture.closePosition,
			"REMOVE_SUBSCRIPTION wire position must precede CLIENT_CLOSE",
		)
	}
}

func closeDrainWirePositionsProven(left, right int64) bool {
	return left >= 0 && right >= 0 && left < right
}

func assertCloseDrainOrderingFromTypeIDs(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	producerBefore int64,
	capturedTypeIDs []int,
) CloseDrainOrderingProof {
	t.Helper()

	typeIDs := capturedTypeIDs
	if typeIDs == nil {
		if ring == nil {
			return CloseDrainOrderingNone
		}
		producerAfter := ring.ProducerPosition()
		var err error
		typeIDs, err = OfferedRecordTypeIDsBetween(ring, producerBefore, producerAfter)
		require.NoError(t, err)
	}

	filtered := filterCloseDrainTypeIDs(typeIDs)
	removePublicationIndex := indexOfCloseDrainRecordType(typeIDs, conductor.RemovePublication)
	removeSubscriptionIndex := indexOfCloseDrainRecordType(typeIDs, conductor.RemoveSubscription)
	clientCloseIndex := indexOfCloseDrainRecordType(typeIDs, conductor.ClientClose)
	if removePublicationIndex < 0 || removeSubscriptionIndex < 0 || clientCloseIndex < 0 {
		return CloseDrainOrderingNone
	}
	require.Less(
		t,
		removePublicationIndex,
		removeSubscriptionIndex,
		"REMOVE_PUBLICATION must precede REMOVE_SUBSCRIPTION on ring (got type IDs %v)",
		filtered,
	)
	require.Less(
		t,
		removeSubscriptionIndex,
		clientCloseIndex,
		"REMOVE_SUBSCRIPTION must precede CLIENT_CLOSE on ring (got type IDs %v)",
		filtered,
	)
	return CloseDrainOrderingTypeIDs
}
