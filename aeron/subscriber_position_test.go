package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/atomicbuffer"
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

func TestNewSubscriberPosition_outOfBoundsPanics(t *testing.T) {
	values := atomicbuffer.Wrap(make([]byte, cnc.CounterValueLength))

	require.Panics(t, func() {
		newSubscriberPosition(1, values)
	}, "id past the values buffer")
	require.Panics(t, func() {
		newSubscriberPosition(-1, values)
	}, "negative id")
}
