package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestImage_newImage_recordsJoinPosition(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, aeron.TestSubscriberPositionAtJoin)

	require.Equal(t, aeron.TestSubscriberPositionAtJoin, image.JoinPosition())
	require.Equal(t, aeron.TestSubscriberPositionAtJoin, image.Position())
}

func TestImage_accessorsReturnWiredFields(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, aeron.TestSubscriberPositionAtJoin)

	require.Equal(t, aeron.TestImageCorrelationID, image.CorrelationID())
	require.Equal(t, aeron.TestPublicationSessionID, image.SessionID())
	require.Equal(t, aeron.TestPublicationStreamID, image.StreamID())
	require.Equal(t, aeron.TestImageSourceIdentity, image.SourceIdentity())
	require.Equal(t, "test-image.log", image.LogFileName())
	require.Equal(t, aeron.TestSubscriberPositionCounterID, image.SubscriberPositionID())
	require.False(t, image.IsClosed())
}

func TestImage_Position_tracksSubscriberCounter(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, aeron.TestSubscriberPositionAtJoin)

	const updatedPosition int64 = 512
	aeron.ImageSetSubscriberPositionForTest(image, updatedPosition)

	require.Equal(t, aeron.TestSubscriberPositionAtJoin, image.JoinPosition())
	require.Equal(t, updatedPosition, image.Position())
}

func TestImage_internalClose_closedSemantics(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, aeron.TestSubscriberPositionAtJoin)

	require.Equal(t, 0, image.Poll(nil, 1))

	const positionAtClose int64 = 256
	aeron.ImageSetSubscriberPositionForTest(image, positionAtClose)
	aeron.ImageInternalCloseForTest(image)

	require.True(t, image.IsClosed())
	require.Equal(t, positionAtClose, image.Position())
	require.Zero(t, aeron.ImageTermBuffer0CapacityForTest(image))
	require.Zero(t, aeron.ImageHeaderBufferCapacityForTest(image))

	aeron.ImageSetSubscriberPositionForTest(image, 512)
	require.Equal(t, positionAtClose, image.Position())
	require.Equal(t, 0, image.Poll(nil, 1))
}

func TestImage_internalClose_idempotent(t *testing.T) {
	sub := testSubscription(t)
	image := aeron.NewImageForTest(t, sub, 100)

	aeron.ImageSetSubscriberPositionForTest(image, 100)
	aeron.ImageInternalCloseForTest(image)

	aeron.ImageSetSubscriberPositionForTest(image, 200)
	aeron.ImageInternalCloseForTest(image)

	require.Equal(t, int64(100), image.Position())
}

func TestImage_Header_readsCommittedDataFrame(t *testing.T) {
	t.Run("zeroTermOffset", func(t *testing.T) {
		image, frameLength := aeron.NewImageWithCommittedDataFrameForTest(t, 0, 0)

		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		header := aeron.ImageHeaderForTest(image)
		header.SetBuffer(termBuffer)
		header.SetOffset(0)

		require.Equal(t, frameLength, header.FrameLength())
		require.Equal(t, aeron.TestPublicationSessionID, header.SessionID())
		require.Equal(t, aeron.TestPublicationStreamID, header.StreamID())
		require.Equal(t, aeron.TestPublicationInitialTermID, header.TermID())
		require.Equal(t, int32(0), header.TermOffset())
		require.Equal(t, int32(logbuffer.HdrTypeData), header.Type())
		require.Equal(t, byte(logbuffer.Unfragmented), header.Flags())
		require.Equal(t, int64(logbuffer.DefaultReservedValue), header.ReservedValue())
		require.Equal(t, 0, header.Offset())
		require.Equal(
			t,
			int64(logbuffer.AlignFrameLength(int(frameLength))),
			header.Position(),
		)
	})

	t.Run("nonZeroTermOffset", func(t *testing.T) {
		const frameOffset = 64
		image, frameLength := aeron.NewImageWithCommittedDataFrameForTest(t, 0, frameOffset)

		termBuffer := aeron.ImageTermBuffer0ForTest(image)
		header := aeron.ImageHeaderForTest(image)
		header.SetBuffer(termBuffer)
		header.SetOffset(frameOffset)

		expectedPosition := int64(
			logbuffer.AlignFrameLength(int(frameOffset) + int(frameLength)),
		)
		require.Equal(t, expectedPosition, header.Position())
		require.Equal(t, int32(frameOffset), header.TermOffset())
		require.Equal(
			t,
			int32(logbuffer.AlignFrameLength(int(frameOffset)+int(frameLength))),
			header.NextTermOffset(),
		)
	})
}

func TestImage_newImage_errors(t *testing.T) {
	sub := testSubscription(t)
	event := aeron.AvailableImageForTest()

	logBuffers := aeron.NewTestLogBuffersForTest(t, aeron.DefaultTestPublicationLogConfig())

	activeCounters := aeron.NewTestCountersReaderWithSubscriberPositionForTest(
		t,
		aeron.TestSubscriberPositionAtJoin,
	)

	closedLog := aeron.NewTestMappedLogForTest(t, aeron.DefaultTestPublicationLogConfig())
	closedLogBuffers, err := logbuffer.NewLogBuffers(closedLog)
	require.NoError(t, err)
	require.NoError(t, closedLog.Close())

	inactiveCounters := aeron.NewTestCountersReaderLimitOnlyForTest(t, 0)

	tests := []struct {
		name         string
		sub          *aeron.Subscription
		logBuffers   *logbuffer.LogBuffers
		counters     *cnc.CountersReader
		event        conductor.AvailableImage
		wantContains string
	}{
		{
			name:         "nilSubscription",
			sub:          nil,
			logBuffers:   logBuffers,
			counters:     activeCounters,
			event:        event,
			wantContains: "subscription is nil",
		},
		{
			name:         "nilLogBuffers",
			sub:          sub,
			logBuffers:   nil,
			counters:     activeCounters,
			event:        event,
			wantContains: "log buffers is nil",
		},
		{
			name:         "nilCountersReader",
			sub:          sub,
			logBuffers:   logBuffers,
			counters:     nil,
			event:        event,
			wantContains: "counters reader is nil",
		},
		{
			name:         "inactiveSubscriberCounter",
			sub:          sub,
			logBuffers:   logBuffers,
			counters:     inactiveCounters,
			event:        event,
			wantContains: "subscriber position counter",
		},
		{
			name:         "outOfRangeSubscriberCounter",
			sub:          sub,
			logBuffers:   logBuffers,
			counters:     activeCounters,
			event:        aeron.AvailableImageWithSubscriberPositionIDForTest(99),
			wantContains: "subscriber position counter",
		},
		{
			name:         "closedLogBuffers",
			sub:          sub,
			logBuffers:   closedLogBuffers,
			counters:     activeCounters,
			event:        event,
			wantContains: "closed mapped log buffer",
		},
		{
			name:       "mismatchedSubscriberCounterRegistration",
			sub:        sub,
			logBuffers: logBuffers,
			counters: aeron.NewTestCountersReaderWithSubscriberPositionForTest(
				t,
				aeron.TestSubscriberPositionAtJoin,
				aeron.TestSubscriptionRegistrationID+1,
			),
			event:        event,
			wantContains: "subscriber position counter",
		},
		{
			name:       "mismatchedSubscriptionRegistration",
			sub:        sub,
			logBuffers: logBuffers,
			counters:   activeCounters,
			event: func() conductor.AvailableImage {
				mismatch := event
				mismatch.SubscriptionRegistrationID = aeron.TestSubscriptionRegistrationID + 99
				return mismatch
			}(),
			wantContains: "subscription registration",
		},
		{
			name:       "mismatchedSubscriptionStream",
			sub:        sub,
			logBuffers: logBuffers,
			counters:   activeCounters,
			event: func() conductor.AvailableImage {
				mismatch := event
				mismatch.StreamID = aeron.TestPublicationStreamID + 99
				return mismatch
			}(),
			wantContains: "subscription stream",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := aeron.NewImageDirectForTest(
				tc.sub,
				tc.event,
				tc.logBuffers,
				tc.counters,
			)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantContains)
		})
	}
}

func TestImage_newImage_metadataErrors(t *testing.T) {
	sub := testSubscription(t)

	tests := []struct {
		name         string
		mutate       func(atomic.AtomicBuffer)
		wantContains string
	}{
		{
			name: "parseMetadataFailure",
			mutate: func(meta atomic.AtomicBuffer) {
				meta.PutInt32(logbuffer.OffsetPageSize, 3)
			},
			wantContains: "page size",
		},
		{
			name: "metadataTermLengthMismatch",
			mutate: func(meta atomic.AtomicBuffer) {
				meta.PutInt32(logbuffer.OffsetTermLength, aeron.TestPublicationTermLength*2)
			},
			wantContains: "metadata term length",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logBuffers := aeron.NewTestLogBuffersForTest(t, aeron.DefaultTestPublicationLogConfig())

			meta, err := logBuffers.MetaDataBuffer()
			require.NoError(t, err)
			tc.mutate(meta)

			counters := aeron.NewTestCountersReaderWithSubscriberPositionForTest(
				t,
				aeron.TestSubscriberPositionAtJoin,
			)

			_, err = aeron.NewImageDirectForTest(sub, aeron.AvailableImageForTest(), logBuffers, counters)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantContains)
		})
	}
}
