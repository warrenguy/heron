package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubscriberPosition_GetSetReleaseCounterID(t *testing.T) {
	reader := NewTestCountersReaderWithSubscriberPositionForTest(t, TestSubscriberPositionAtJoin)
	position := newSubscriberPosition(
		TestSubscriberPositionCounterID,
		reader.CounterValuesBuffer(),
	)

	require.Equal(t, TestSubscriberPositionCounterID, position.CounterID())
	require.Equal(t, TestSubscriberPositionAtJoin, position.Get())

	const updated int64 = 99
	position.SetRelease(updated)
	require.Equal(t, updated, position.Get())
}
