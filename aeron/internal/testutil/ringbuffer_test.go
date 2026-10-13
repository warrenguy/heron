package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/record"
	"github.com/warrenguy/heron/ringbuffer"
)

func TestPublicationCommandIDs_skipOtherCommandTypes(t *testing.T) {
	testCases := []struct {
		name  string
		drain func(require.TestingT, *ringbuffer.ManyToOneRingBuffer) []int64
		want  []int64
	}{
		{
			name:  "add correlation ids",
			drain: AddPublicationCorrelationIDs,
			want:  []int64{10, 12},
		},
		{
			name:  "remove registration ids",
			drain: RemovePublicationRegistrationIDs,
			want:  []int64{100, 101},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ring := NewTestRing(t)
			proxy := conductor.NewDriverProxy(ring, 1)
			require.NoError(t, proxy.AddPublication(10, 1001, "aeron:ipc"))
			require.NoError(t, proxy.RemovePublication(11, 100, false))
			require.NoError(t, proxy.AddExclusivePublication(12, 1001, "aeron:ipc"))
			require.NoError(t, proxy.RemovePublication(13, 101, false))
			require.NoError(t, proxy.AddSubscription(14, 1001, "aeron:ipc"))

			require.Equal(t, testCase.want, testCase.drain(t, ring))
			require.Zero(t, CommittedNonPaddingRecordCount(ring))
		})
	}
}

func TestRemovePublicationRegistrationIDs_drainsAcrossWrap(t *testing.T) {
	removeRecordLength := record.AlignRecordLength(
		ringbuffer.HeaderLength + conductor.RemovePublicationBodyLength,
	)
	testCases := []struct {
		name       string
		bytesToEnd int
	}{
		// First Read returns one record. Second record sits at index 0 past the padding.
		{name: "second record past wrap", bytesToEnd: removeRecordLength + record.Alignment},
		// First Read consumes the padding only and returns zero messages.
		{name: "padding-only tail block", bytesToEnd: record.Alignment},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ring := NewTestRing(t)
			capacity := ring.Capacity()
			start := int64(capacity - testCase.bytesToEnd)
			buffer := ring.Buffer()
			buffer.PutInt64Release(ringbuffer.HeadIndex(capacity), start)
			buffer.PutInt64Release(ringbuffer.HeadCacheIndex(capacity), start)
			buffer.PutInt64Release(ringbuffer.TailIndex(capacity), start)

			proxy := conductor.NewDriverProxy(ring, 1)
			require.NoError(t, proxy.RemovePublication(10, 100, false))
			require.NoError(t, proxy.RemovePublication(11, 101, false))
			require.Greater(t, ring.ProducerPosition(), int64(capacity), "tail wrapped")

			require.Equal(t, []int64{100, 101}, RemovePublicationRegistrationIDs(t, ring))
			require.Equal(t, ring.ProducerPosition(), ring.ConsumerPosition())
		})
	}
}
