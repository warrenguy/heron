//go:build !integration

package harness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/record"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

func closeDrainTestIDs() (clientID, pubRegistrationID, subRegistrationID int64) {
	clientID, pubRegistrationID = controlMinimalTestRemoveIDs()
	return clientID, pubRegistrationID, 9002
}

func closeDrainCommandTypeIDs() []int {
	return []int{conductor.RemovePublication, conductor.RemoveSubscription, conductor.ClientClose}
}

type closeDrainOfferedTriple struct {
	ring              *ringbuffer.ManyToOneRingBuffer
	clientID          int64
	pubRegistrationID int64
	subRegistrationID int64
	producerBefore    int64
	producerAfter     int64
}

func offerCloseDrainTripleForTest(t *testing.T) closeDrainOfferedTriple {
	t.Helper()

	ring, _ := newControlMinimalTestRingBuffer(t)
	clientID, pubRegistrationID, subRegistrationID := closeDrainTestIDs()
	producerBefore := ring.ProducerPosition()
	offerRemovePublicationForTest(t, ring, clientID, 1, pubRegistrationID)
	offerRemoveSubscriptionForTest(t, ring, clientID, 2, subRegistrationID)
	conductor.NewDriverProxy(ring, clientID).ClientClose()

	return closeDrainOfferedTriple{
		ring:              ring,
		clientID:          clientID,
		pubRegistrationID: pubRegistrationID,
		subRegistrationID: subRegistrationID,
		producerBefore:    producerBefore,
		producerAfter:     ring.ProducerPosition(),
	}
}

func closeDrainWireCaptureWithRemoveIdentities() closeDrainWireCapture {
	capture := newCloseDrainWireCapture()
	clientID, pubRegistrationID, subRegistrationID := closeDrainTestIDs()
	capture.removePublication = conductor.RemovePublicationMessage{
		RemoveMessage: conductor.RemoveMessage{
			ClientID:       clientID,
			RegistrationID: pubRegistrationID,
		},
	}
	capture.removeSubscription = conductor.RemoveMessage{
		ClientID:       clientID,
		RegistrationID: subRegistrationID,
	}
	return capture
}

func TestFilterCloseDrainTypeIDs_ignoresKeepalive(t *testing.T) {
	filtered := filterCloseDrainTypeIDs([]int{
		conductor.ClientKeepalive,
		conductor.RemovePublication,
		conductor.ClientKeepalive,
		conductor.RemoveSubscription,
		conductor.ClientClose,
	})
	require.Equal(t, closeDrainCommandTypeIDs(), filtered)
}

func TestIndexOfCloseDrainRecordType_ignoresKeepaliveInterleave(t *testing.T) {
	typeIDs := []int{
		conductor.ClientKeepalive,
		conductor.RemovePublication,
		conductor.ClientKeepalive,
		conductor.RemoveSubscription,
		conductor.ClientClose,
	}
	require.Equal(t, 0, indexOfCloseDrainRecordType(typeIDs, conductor.RemovePublication))
	require.Equal(t, 1, indexOfCloseDrainRecordType(typeIDs, conductor.RemoveSubscription))
	require.Equal(t, 2, indexOfCloseDrainRecordType(typeIDs, conductor.ClientClose))
}

func TestCloseDrainPollInterval_defaultsZero(t *testing.T) {
	require.Equal(t, time.Millisecond, closeDrainPollInterval(0))
	require.Equal(t, 50*time.Millisecond, closeDrainPollInterval(50*time.Millisecond))
}

func TestCloseDrainOrderingTypeIDsComplete(t *testing.T) {
	require.True(t, closeDrainOrderingTypeIDsComplete(closeDrainCommandTypeIDs()))
	require.False(t, closeDrainOrderingTypeIDsComplete([]int{conductor.RemovePublication}))
}

func TestAssertCloseDrainRecordOrdering_provenPositions(t *testing.T) {
	proof := assertCloseDrainRecordOrdering(
		t,
		nil,
		0,
		nil,
		100,
		200,
		300,
	)
	require.Equal(t, CloseDrainOrderingProvenPositions, proof)
}

func TestAssertCloseDrainRecordOrdering_typeIDs(t *testing.T) {
	offered := offerCloseDrainTripleForTest(t)

	typeIDs, err := offeredRecordTypeIDsBetween(
		offered.ring,
		offered.producerBefore,
		offered.producerAfter,
	)
	require.NoError(t, err)

	proof := assertCloseDrainRecordOrdering(
		t,
		offered.ring,
		offered.producerBefore,
		typeIDs,
		-1,
		-1,
		-1,
	)
	require.Equal(t, CloseDrainOrderingTypeIDs, proof)
}

func TestAssertCloseDrainRecordOrdering_partialProvenPositionsFallsThrough(t *testing.T) {
	proof := assertCloseDrainRecordOrdering(t, nil, 0, closeDrainCommandTypeIDs(), 100, 200, -1)
	require.Equal(t, CloseDrainOrderingTypeIDs, proof)
	require.NotEqual(t, CloseDrainOrderingProvenPositions, proof)

	proof = assertCloseDrainRecordOrdering(t, nil, 0, nil, 100, 200, -1)
	require.Equal(t, CloseDrainOrderingNone, proof)
}

func TestAssertCloseDrainOrderingFromTypeIDs_negativePermutedOrder(t *testing.T) {
	capture := &captureFailTB{TB: t}
	typeIDs := []int{
		conductor.RemoveSubscription,
		conductor.RemovePublication,
		conductor.ClientClose,
	}

	_ = assertCloseDrainOrderingFromTypeIDs(capture, nil, 0, typeIDs)
	require.True(t, capture.failed)
}

func TestAssertCloseDrainOrderingFromTypeIDs_emptyReturnsNone(t *testing.T) {
	proof := assertCloseDrainOrderingFromTypeIDs(t, nil, 0, nil)
	require.Equal(t, CloseDrainOrderingNone, proof)
}

func TestAssertCloseDrainRecordOrdering_negativeProvenPositions(t *testing.T) {
	capture := &captureFailTB{TB: t}

	_ = assertCloseDrainRecordOrdering(capture, nil, 0, nil, 300, 200, 100)
	require.True(t, capture.failed)
}

func TestDecodeCloseDrainRecordsBetween_happyTriple(t *testing.T) {
	offered := offerCloseDrainTripleForTest(t)

	var capture closeDrainWireCapture
	decodeCloseDrainRecordsBetween(
		offered.ring,
		offered.producerBefore,
		offered.producerAfter,
		offered.clientID,
		offered.pubRegistrationID,
		offered.subRegistrationID,
		&capture,
	)

	require.True(t, capture.pubWireDecoded)
	require.True(t, capture.subWireDecoded)
	require.True(t, capture.closeWireDecoded)
	require.Less(t, capture.pubPosition, capture.subPosition)
	require.Less(t, capture.subPosition, capture.closePosition)
	requireCloseDrainWireProof(
		t,
		capture,
		offered.clientID,
		offered.pubRegistrationID,
		offered.subRegistrationID,
	)
}

func TestDecodeCloseDrainRecordsBetween_wrongRegistrationIDRejected(t *testing.T) {
	offered := offerCloseDrainTripleForTest(t)

	var capture closeDrainWireCapture
	decodeCloseDrainRecordsBetween(
		offered.ring,
		offered.producerBefore,
		offered.producerAfter,
		offered.clientID,
		offered.pubRegistrationID+1,
		offered.subRegistrationID+1,
		&capture,
	)

	require.False(t, capture.pubWireDecoded)
	require.False(t, capture.subWireDecoded)
	require.True(t, capture.closeWireDecoded)
}

func TestAugmentCloseDrainTypeIDsForObservedClose_appendsClientClose(t *testing.T) {
	augmented := augmentCloseDrainTypeIDsForObservedClose(
		[]int{conductor.RemovePublication, conductor.RemoveSubscription},
		true,
	)
	require.Equal(t, closeDrainCommandTypeIDs(), filterCloseDrainTypeIDs(augmented))
}

func TestAugmentCloseDrainTypeIDsForObservedClose_skipsWithoutObservedClose(t *testing.T) {
	typeIDs := []int{conductor.RemovePublication, conductor.RemoveSubscription}
	augmented := augmentCloseDrainTypeIDsForObservedClose(typeIDs, false)
	require.Equal(t, typeIDs, augmented)
}

func TestAugmentCloseDrainTypeIDsForObservedClose_skipsPermutedPubSubOrder(t *testing.T) {
	typeIDs := []int{conductor.RemoveSubscription, conductor.RemovePublication}
	augmented := augmentCloseDrainTypeIDsForObservedClose(typeIDs, true)
	require.Equal(t, typeIDs, augmented)
	require.NotContains(t, filterCloseDrainTypeIDs(augmented), conductor.ClientClose)
}

func TestTryCaptureCloseDrainWire_offeredTriple(t *testing.T) {
	offered := offerCloseDrainTripleForTest(t)

	var capture closeDrainWireCapture
	require.True(t, tryCaptureCloseDrainWire(
		offered.ring,
		offered.clientID,
		offered.pubRegistrationID,
		offered.subRegistrationID,
		offered.producerBefore,
		&capture,
	))
	require.NotEmpty(t, capture.typeIDs)
	require.Positive(t, capture.producerAfter)
	require.True(t, capture.closeWireDecoded)
}

func TestRequireCloseDrainWireProof_skipsOrderingForObserveFallbackPositions(t *testing.T) {
	capture := closeDrainWireCaptureWithRemoveIdentities()
	capture.pubWireDecoded = true
	capture.subWireDecoded = true

	requireCloseDrainWireProof(
		t,
		capture,
		capture.removePublication.ClientID,
		capture.removePublication.RegistrationID,
		capture.removeSubscription.RegistrationID,
	)
}

func TestRequireCloseDrainWireProof_skipsOrderingForEqualPositions(t *testing.T) {
	capture := closeDrainWireCaptureWithRemoveIdentities()
	capture.pubWireDecoded = true
	capture.subWireDecoded = true
	capture.closeWireDecoded = true
	capture.pubPosition = 0
	capture.subPosition = 0
	capture.closePosition = 0

	requireCloseDrainWireProof(
		t,
		capture,
		capture.removePublication.ClientID,
		capture.removePublication.RegistrationID,
		capture.removeSubscription.RegistrationID,
	)
}

func TestCloseDrainWirePositionsProven_requiresDistinctIncreasing(t *testing.T) {
	require.True(t, closeDrainWirePositionsProven(100, 200))
	require.False(t, closeDrainWirePositionsProven(0, 0))
	require.False(t, closeDrainWirePositionsProven(200, 100))
	require.False(t, closeDrainWirePositionsProven(-1, 100))
}

func TestProveCloseDrainOrdering_rejectsPermutedRingOrder(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)
	clientID, pubRegistrationID, subRegistrationID := closeDrainTestIDs()
	producerBefore := ring.ProducerPosition()
	offerRemoveSubscriptionForTest(t, ring, clientID, 1, subRegistrationID)
	offerRemovePublicationForTest(t, ring, clientID, 2, pubRegistrationID)
	conductor.NewDriverProxy(ring, clientID).ClientClose()

	typeIDs, err := offeredRecordTypeIDsBetween(
		ring,
		producerBefore,
		ring.ProducerPosition(),
	)
	require.NoError(t, err)

	capture := newCloseDrainWireCapture()
	decodeCloseDrainRecordsBetween(
		ring,
		producerBefore,
		ring.ProducerPosition(),
		clientID,
		pubRegistrationID,
		subRegistrationID,
		&capture,
	)

	orderingTypeIDs, err := closeDrainOrderingTypeIDs(capture, ring, producerBefore, true, -1)
	require.NoError(t, err)

	failCapture := &captureFailTB{TB: t}
	_ = assertCloseDrainRecordOrdering(
		failCapture,
		ring,
		producerBefore,
		orderingTypeIDs,
		capture.pubPosition,
		capture.subPosition,
		-1,
	)
	require.True(t, failCapture.failed)
	require.NotEqual(t, closeDrainCommandTypeIDs(), filterCloseDrainTypeIDs(typeIDs))
}

func TestFindOfferedBetween_incompleteHeadAtScanStartReturnsNotFound(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	from := ring.ProducerPosition()
	conductor.NewDriverProxy(ring, clientID).ClientClose()
	buffer.PutInt32(record.LengthOffset(int(from)&(ring.Capacity()-1)), 0)

	_, ok, err := findOfferedBetween(
		ring,
		from,
		ring.ProducerPosition(),
		clientCloseObserveSpec(clientID),
		nil,
	)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestFindOfferedBetween_boundedRangeFindsFirstMatchingRecord(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	proxy := conductor.NewDriverProxy(ring, clientID)
	from := ring.ProducerPosition()
	proxy.ClientClose()
	mid := ring.ProducerPosition()
	proxy.ClientClose()
	to := ring.ProducerPosition()
	spec := clientCloseObserveSpec(clientID)

	_, ok, err := findOfferedBetween(ring, from, mid, spec, nil)
	require.NoError(t, err)
	require.True(t, ok)

	_, ok, err = findOfferedBetween(ring, from, to, spec, nil)
	require.NoError(t, err)
	require.True(t, ok)

	var matchedPosition int64
	_, ok, err = findOfferedBetween(ring, from, mid, spec, &matchedPosition)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, from, matchedPosition)

	_, ok, err = findOfferedBetween(ring, from, to, spec, &matchedPosition)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, from, matchedPosition)
}

func TestFindMatchingClientClosePositionBetween_respectsBoundedToPosition(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	proxy := conductor.NewDriverProxy(ring, clientID)
	from := ring.ProducerPosition()
	proxy.ClientClose()
	afterFirstClose := ring.ProducerPosition()
	proxy.ClientClose()
	to := ring.ProducerPosition()

	require.Equal(
		t,
		from,
		findMatchingClientClosePositionBetween(ring, clientID, from, afterFirstClose),
	)
	require.Equal(
		t,
		from,
		findMatchingClientClosePositionBetween(ring, clientID, from, to),
	)
	require.Equal(
		t,
		int64(-1),
		findMatchingClientClosePositionBetween(ring, clientID, from, from),
	)
	require.Equal(
		t,
		afterFirstClose,
		findMatchingClientClosePositionBetween(ring, clientID, afterFirstClose, to),
	)
}
