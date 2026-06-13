//go:build rapid

package aeron

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestSubscriberPosition_setGetRoundtrip_rapid(t *testing.T) {
	reader := NewTestCountersReaderWithSubscriberPositionForTest(t, TestSubscriberPositionAtJoin)
	position := newSubscriberPosition(
		TestSubscriberPositionCounterID,
		reader.CounterValuesBuffer(),
	)

	rapid.Check(t, func(rt *rapid.T) {
		position.SetRelease(TestSubscriberPositionAtJoin)
		require.Equal(rt, TestSubscriberPositionAtJoin, position.Get())

		valueCount := rapid.IntRange(1, 16).Draw(rt, "valueCount")
		var lastValue int64
		for i := range valueCount {
			value := rapid.Int64().Draw(rt, fmt.Sprintf("value%d", i))
			position.SetRelease(value)
			require.Equal(rt, value, position.Get())
			lastValue = value
		}
		require.Equal(rt, lastValue, position.Get())
	})
}
