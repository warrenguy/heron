//go:build !integration

package harness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/record"
	"github.com/warrenguy/heron/agrona/ringbuffer"
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

	typeIDs, err := OfferedRecordTypeIDsBetween(
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
	capture := &CaptureFailTB{TB: t}
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
	capture := &CaptureFailTB{TB: t}

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

func TestResolveCloseDrainCapturePositions_fillsMissingOfferedPositions(t *testing.T) {
	triple := offerCloseDrainTripleForTest(t)

	capture := closeDrainWireCaptureWithRemoveIdentities()
	capture.pubWireDecoded = true
	capture.subWireDecoded = true
	capture.producerAfter = triple.producerAfter

	resolveCloseDrainCapturePositions(
		triple.ring,
		&capture,
		triple.clientID,
		triple.pubRegistrationID,
		triple.subRegistrationID,
		triple.producerBefore,
	)

	require.GreaterOrEqual(t, capture.pubPosition, triple.producerBefore)
	require.Greater(t, capture.subPosition, capture.pubPosition)
	require.Greater(t, capture.closePosition, capture.subPosition)
}

func TestApplyCloseDrainWireProofFromRegistrations_satisfiesWireProof(t *testing.T) {
	clientID, pubRegistrationID, subRegistrationID := closeDrainTestIDs()

	capture := newCloseDrainWireCapture()
	applyCloseDrainWireProofFromRegistrations(
		&capture,
		clientID,
		pubRegistrationID,
		subRegistrationID,
	)

	requireCloseDrainWireProof(
		t,
		capture,
		clientID,
		pubRegistrationID,
		subRegistrationID,
	)
}

func TestReplenishCloseDrainWireCapture_decodesOfferedTriple(t *testing.T) {
	triple := offerCloseDrainTripleForTest(t)

	capture := newCloseDrainWireCapture()
	replenishCloseDrainWireCapture(
		triple.ring,
		&capture,
		triple.clientID,
		triple.pubRegistrationID,
		triple.subRegistrationID,
		triple.producerBefore,
	)

	require.True(t, closeDrainWireDecodeReady(capture))
	require.GreaterOrEqual(t, capture.pubPosition, triple.producerBefore)
	require.Greater(t, capture.subPosition, capture.pubPosition)
}

func TestCloseDrainOrderingFromWireCapture_acceptsDecodedRemovesWithoutPositions(t *testing.T) {
	capture := closeDrainWireCaptureWithRemoveIdentities()
	capture.pubWireDecoded = true
	capture.subWireDecoded = true

	proof := closeDrainOrderingFromWireCapture(t, capture, -1, true)
	require.Equal(t, CloseDrainOrderingWireIdentity, proof)
}

func TestProveCloseDrainOrdering_rejectsPermutedRingOrder(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)
	clientID, pubRegistrationID, subRegistrationID := closeDrainTestIDs()
	producerBefore := ring.ProducerPosition()
	offerRemoveSubscriptionForTest(t, ring, clientID, 1, subRegistrationID)
	offerRemovePublicationForTest(t, ring, clientID, 2, pubRegistrationID)
	conductor.NewDriverProxy(ring, clientID).ClientClose()

	typeIDs, err := OfferedRecordTypeIDsBetween(
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

	failCapture := &CaptureFailTB{TB: t}
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

func TestFindOfferedBetween_emptyRangeReturnsNotFound(t *testing.T) {
	ring, _ := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	proxy := conductor.NewDriverProxy(ring, clientID)
	from := ring.ProducerPosition()
	proxy.ClientClose()
	to := ring.ProducerPosition()

	requireFindOfferedBetweenRanges(
		t,
		ring,
		clientCloseObserveSpec(clientID),
		[]findOfferedBetweenRangeExpect{
			{from: from, to: from, wantOK: false},
			{from: to, to: from, wantOK: false},
		},
	)
}

func TestFindOfferedBetween_incompleteHeadAtScanStartReturnsNotFound(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	from := ring.ProducerPosition()
	conductor.NewDriverProxy(ring, clientID).ClientClose()
	buffer.PutInt32(record.LengthOffset(int(from)&(ring.Capacity()-1)), 0)

	requireFindOfferedBetweenRanges(
		t,
		ring,
		clientCloseObserveSpec(clientID),
		[]findOfferedBetweenRangeExpect{
			{from: from, to: ring.ProducerPosition(), wantOK: false},
		},
	)
}

func TestFindOfferedBetween_incompleteTailMidScanReturnsNotFound(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	proxy := conductor.NewDriverProxy(ring, clientID)
	from := ring.ProducerPosition()
	proxy.ClientClose()
	secondPosition := ring.ProducerPosition()
	proxy.ClientClose()
	buffer.PutInt32(record.LengthOffset(int(secondPosition)&(ring.Capacity()-1)), 0)
	to := ring.ProducerPosition()

	requireFindOfferedBetweenRanges(
		t,
		ring,
		clientCloseObserveSpec(clientID),
		[]findOfferedBetweenRangeExpect{
			{from: from, to: secondPosition, wantOK: true, wantPosition: -1},
			{from: secondPosition, to: to, wantOK: false},
		},
	)
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

	requireFindOfferedBetweenRanges(
		t,
		ring,
		clientCloseObserveSpec(clientID),
		[]findOfferedBetweenRangeExpect{
			{from: from, to: mid, wantOK: true, wantPosition: -1},
			{from: from, to: to, wantOK: true, wantPosition: -1},
			{from: from, to: mid, wantOK: true, wantPosition: from},
			{from: from, to: to, wantOK: true, wantPosition: from},
		},
	)
}

type findOfferedBetweenRangeExpect struct {
	from         int64
	to           int64
	wantOK       bool
	wantPosition int64
}

func requireFindOfferedBetweenRanges[T any](
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	spec toDriverObserveSpec[T],
	checks []findOfferedBetweenRangeExpect,
) {
	t.Helper()

	for _, check := range checks {
		requireFindOfferedBetween(
			t,
			ring,
			spec,
			check.from,
			check.to,
			check.wantOK,
			check.wantPosition,
		)
	}
}

func requireFindOfferedBetween[T any](
	t *testing.T,
	ring *ringbuffer.ManyToOneRingBuffer,
	spec toDriverObserveSpec[T],
	from, to int64,
	wantOK bool,
	wantPosition int64,
) {
	t.Helper()

	matchedPosition := int64(-1)
	_, ok, err := findOfferedBetween(ring, from, to, spec, &matchedPosition)
	require.NoError(t, err)
	require.Equal(t, wantOK, ok)
	if wantOK && wantPosition >= 0 {
		require.Equal(t, wantPosition, matchedPosition)
	}
	if !wantOK {
		require.Equal(t, int64(-1), matchedPosition)
	}
}

func TestFindOfferedBetween_decodeMatchErrorPropagates(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	clientID, registrationID := controlMinimalTestRemoveIDs()
	from := ring.ProducerPosition()
	offerRemovePublicationForTest(t, ring, clientID, 1, registrationID)

	recordIndex := int(from) & (ring.Capacity() - 1)
	const corruptedBody = 4
	corruptedRecordLength := ringbuffer.HeaderLength + corruptedBody
	aligned := record.AlignRecordLength(corruptedRecordLength)
	buffer.PutInt32(record.LengthOffset(recordIndex), int32(corruptedRecordLength))

	baseSpec := removePublicationObserveSpec(clientID, registrationID)
	spec := toDriverObserveSpec[conductor.RemovePublicationMessage]{
		messageTypeID: baseSpec.messageTypeID,
		recordLength:  aligned,
		decodeMatch:   baseSpec.decodeMatch,
	}

	_, ok, err := findOfferedBetween(ring, from, from+int64(aligned), spec, nil)
	require.Error(t, err)
	require.False(t, ok)
}

func TestFindOfferedBetween_interleaveScenarios(t *testing.T) {
	t.Run("wrongTypeInterleaveFindsRemoveNotClose", func(t *testing.T) {
		ring, _ := newControlMinimalTestRingBuffer(t)

		clientID, pubRegistrationID := controlMinimalTestRemoveIDs()
		proxy := conductor.NewDriverProxy(ring, clientID)
		from := ring.ProducerPosition()
		proxy.ClientClose()
		removePosition := ring.ProducerPosition()
		offerRemovePublicationForTest(t, ring, clientID, 1, pubRegistrationID)
		to := ring.ProducerPosition()

		requireFindOfferedBetweenRanges(
			t,
			ring,
			clientCloseObserveSpec(clientID),
			[]findOfferedBetweenRangeExpect{
				{from: from, to: removePosition, wantOK: true, wantPosition: from},
			},
		)
		requireFindOfferedBetweenRanges(
			t,
			ring,
			removePublicationObserveSpec(clientID, pubRegistrationID),
			[]findOfferedBetweenRangeExpect{
				{from: from, to: removePosition, wantOK: false, wantPosition: -1},
				{from: from, to: to, wantOK: true, wantPosition: removePosition},
			},
		)
	})

	t.Run("wrongClientIDInterleave", func(t *testing.T) {
		ring, _ := newControlMinimalTestRingBuffer(t)

		const (
			wiredClientID int64 = 7
			otherClientID int64 = 9
		)
		proxyWired := conductor.NewDriverProxy(ring, wiredClientID)
		proxyOther := conductor.NewDriverProxy(ring, otherClientID)

		from := ring.ProducerPosition()
		proxyOther.ClientClose()
		proxyWired.ClientClose()
		to := ring.ProducerPosition()
		wiredClosePosition := from + int64(conductor.CorrelatedRingRecordLength())

		requireFindOfferedBetweenRanges(
			t,
			ring,
			clientCloseObserveSpec(otherClientID),
			[]findOfferedBetweenRangeExpect{
				{from: from, to: wiredClosePosition, wantOK: true, wantPosition: from},
			},
		)
		requireFindOfferedBetweenRanges(
			t,
			ring,
			clientCloseObserveSpec(wiredClientID),
			[]findOfferedBetweenRangeExpect{
				{from: from, to: wiredClosePosition, wantOK: false, wantPosition: -1},
				{from: from, to: to, wantOK: true, wantPosition: wiredClosePosition},
			},
		)
	})

	t.Run("wrongRegistrationIDInterleaveRemovePublication", func(t *testing.T) {
		testFindOfferedBetweenWrongRegistrationIDInterleave(
			t,
			offerRemovePublicationForTest,
			removePublicationObserveSpec,
		)
	})

	t.Run("wrongRegistrationIDInterleaveRemoveSubscription", func(t *testing.T) {
		testFindOfferedBetweenWrongRegistrationIDInterleave(
			t,
			offerRemoveSubscriptionForTest,
			removeSubscriptionObserveSpec,
		)
	})
}

func testFindOfferedBetweenWrongRegistrationIDInterleave[T any](
	t *testing.T,
	offer func(
		t *testing.T,
		ring *ringbuffer.ManyToOneRingBuffer,
		clientID, correlationID, registrationID int64,
	),
	spec func(clientID, registrationID int64) toDriverObserveSpec[T],
) {
	t.Helper()

	ring, _ := newControlMinimalTestRingBuffer(t)

	const (
		clientID      int64 = 7
		registrationA int64 = 9001
		registrationB int64 = 9002
	)
	from := ring.ProducerPosition()
	offer(t, ring, clientID, 1, registrationB)
	matchPosition := ring.ProducerPosition()
	offer(t, ring, clientID, 2, registrationA)
	to := ring.ProducerPosition()

	requireFindOfferedBetweenRanges(
		t,
		ring,
		spec(clientID, registrationA),
		[]findOfferedBetweenRangeExpect{
			{from: from, to: to, wantOK: true, wantPosition: matchPosition},
			{from: from, to: matchPosition, wantOK: false, wantPosition: -1},
		},
	)
}

func TestMPSCMatrixCommandTypeIDs_filtersMatrixCommands(t *testing.T) {
	filtered := MPSCMatrixCommandTypeIDs([]int{
		conductor.ClientKeepalive,
		conductor.AddPublication,
		conductor.RemoveSubscription,
		99,
	})
	require.Equal(
		t,
		[]int{conductor.AddPublication, conductor.RemoveSubscription},
		filtered,
	)
}

func TestAdoptMPSCMatrixTypeIDsIfRicher_dedupPreservesOrder(t *testing.T) {
	adopted := AdoptMPSCMatrixTypeIDsIfRicher(
		[]int{conductor.RemovePublication, conductor.AddPublication},
		[]int{conductor.AddPublication, conductor.RemoveSubscription},
	)
	require.Equal(
		t,
		[]int{
			conductor.RemovePublication,
			conductor.AddPublication,
			conductor.RemoveSubscription,
		},
		adopted,
	)
}

func TestRequireMPSCMatrixCommandTypeIDsPresent(t *testing.T) {
	tests := []struct {
		name     string
		typeIDs  []int
		wantFail bool
	}{
		{
			name:     "missingBoth",
			typeIDs:  []int{conductor.AddPublication},
			wantFail: true,
		},
		{
			name: "missingRemovePublication",
			typeIDs: []int{
				conductor.AddPublication,
				conductor.RemoveSubscription,
			},
			wantFail: true,
		},
		{
			name: "missingRemoveSubscription",
			typeIDs: []int{
				conductor.AddPublication,
				conductor.RemovePublication,
			},
			wantFail: true,
		},
		{
			name: "bothRemovesPresent",
			typeIDs: []int{
				conductor.AddPublication,
				conductor.RemovePublication,
				conductor.RemoveSubscription,
			},
			wantFail: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantFail {
				capture := &CaptureFailTB{TB: t}
				RequireMPSCMatrixCommandTypeIDsPresent(capture, tt.typeIDs)
				require.True(t, capture.failed)
				return
			}
			RequireMPSCMatrixCommandTypeIDsPresent(t, tt.typeIDs)
		})
	}
}

func TestFindOfferedBetween_matchAfterPadding(t *testing.T) {
	ring, buffer := newControlMinimalTestRingBuffer(t)

	const clientID int64 = 7
	proxy := conductor.NewDriverProxy(ring, clientID)
	seedControlMinimalRingNearWrap(t, ring, buffer)

	from := ring.ProducerPosition()
	recordLength := conductor.CorrelatedRingRecordLength()
	firstMatchPosition := from + controlMinimalWrapPadding
	mid := firstMatchPosition + int64(recordLength)

	proxy.ClientClose()
	require.Equal(t, mid, ring.ProducerPosition())

	proxy.ClientClose()
	to := ring.ProducerPosition()

	requireFindOfferedBetweenRanges(
		t,
		ring,
		clientCloseObserveSpec(clientID),
		[]findOfferedBetweenRangeExpect{
			{from: from, to: mid, wantOK: true, wantPosition: firstMatchPosition},
			{from: mid, to: to, wantOK: true, wantPosition: mid},
		},
	)
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
