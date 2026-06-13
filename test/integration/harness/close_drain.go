package harness

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/record"
	"github.com/warrenguy/heron/aeron/ringbuffer"
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
	CloseDrainOrderingNone            CloseDrainOrderingProof = "none"
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
	CloseDrainOrderingProof CloseDrainOrderingProof
}

func filterCloseDrainTypeIDs(typeIDs []int) []int {
	filtered := make([]int, 0, len(typeIDs))
	for _, typeID := range typeIDs {
		if isCloseDrainCommandTypeID(typeID) {
			filtered = append(filtered, typeID)
		}
	}
	return filtered
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

func adoptCloseDrainTypeIDsIfRicher(current, incoming []int) []int {
	incomingFiltered := filterCloseDrainTypeIDs(incoming)
	if len(incomingFiltered) >= len(filterCloseDrainTypeIDs(current)) {
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

	return assertCloseDrainOrderingFromTypeIDs(t, ring, producerBefore, capturedTypeIDs)
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
		closeDeltaTypeIDs, err := offeredRecordTypeIDsBetween(
			ring,
			producerBefore,
			ring.ProducerPosition(),
		)
		if err != nil {
			return nil, err
		}
		orderingTypeIDs = adoptCloseDrainTypeIDsIfRicher(orderingTypeIDs, closeDeltaTypeIDs)
	}

	closeWindowEnd := capture.closePosition
	if closeWindowEnd < 0 {
		closeWindowEnd = provenClosePosition
	}
	if !closeDrainOrderingTypeIDsComplete(orderingTypeIDs) &&
		capture.pubPosition >= 0 &&
		closeWindowEnd > capture.pubPosition {
		windowTypeIDs, err := offeredRecordTypeIDsBetween(
			ring,
			capture.pubPosition,
			closeWindowEnd,
		)
		if err != nil {
			return nil, err
		}
		orderingTypeIDs = adoptCloseDrainTypeIDsIfRicher(orderingTypeIDs, windowTypeIDs)
	}

	if !closeDrainOrderingTypeIDsComplete(orderingTypeIDs) &&
		capture.pubPosition >= 0 &&
		capture.producerAfter > capture.pubPosition {
		windowTypeIDs, err := offeredRecordTypeIDsBetween(
			ring,
			capture.pubPosition,
			capture.producerAfter,
		)
		if err != nil {
			return nil, err
		}
		orderingTypeIDs = adoptCloseDrainTypeIDsIfRicher(orderingTypeIDs, windowTypeIDs)
	}

	return augmentCloseDrainTypeIDsForObservedClose(
		orderingTypeIDs,
		clientCloseObserved,
	), nil
}

func augmentCloseDrainTypeIDsForObservedClose(
	typeIDs []int,
	clientCloseObserved bool,
) []int {
	filtered := filterCloseDrainTypeIDs(typeIDs)
	if !clientCloseObserved || indexOfCloseDrainRecordType(typeIDs, conductor.ClientClose) >= 0 {
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

func offeredRecordTypeIDsBetween(
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
		typeIDs, err = offeredRecordTypeIDsBetween(ring, producerBefore, producerAfter)
		require.NoError(t, err)
	}

	removePublicationIndex := indexOfCloseDrainRecordType(typeIDs, conductor.RemovePublication)
	removeSubscriptionIndex := indexOfCloseDrainRecordType(typeIDs, conductor.RemoveSubscription)
	clientCloseIndex := indexOfCloseDrainRecordType(typeIDs, conductor.ClientClose)
	if removePublicationIndex < 0 || removeSubscriptionIndex < 0 || clientCloseIndex < 0 {
		return CloseDrainOrderingNone
	}

	filtered := filterCloseDrainTypeIDs(typeIDs)
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
