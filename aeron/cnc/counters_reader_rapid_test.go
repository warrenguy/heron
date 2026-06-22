//go:build rapid

package cnc_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/agrona/atomic"
)

func TestCountersReader_isCounterActiveRegistrationSource_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		typeID := rapid.SampledFrom([]int32{
			cnc.DriverPublisherLimitTypeID,
			cnc.DriverSubscriberPositionTypeID,
			cnc.DriverSendChannelStatusTypeID,
			cnc.DriverReceiveChannelStatusTypeID,
			cnc.ClientHeartbeatTypeID,
		}).Draw(rt, "typeID")

		registrationID := rapid.Int64().Draw(rt, "registrationID")
		value := rapid.Int64().Draw(rt, "value")
		const counterID int32 = 0

		meta, values := syntheticCountersBuffers(t, 1)
		require.NoError(
			rt,
			writeAllocatedCounterForTest(meta, values, counterID, typeID, registrationID, value),
		)

		reader, err := cnc.NewCountersReader(meta, values)
		require.NoError(rt, err)
		require.True(rt, reader.IsCounterActive(counterID, registrationID, typeID))

		wrongRegistrationID := rapid.Int64().
			Filter(func(id int64) bool { return id != registrationID }).
			Draw(rt, "wrongRegistrationID")
		require.False(rt, reader.IsCounterActive(counterID, wrongRegistrationID, typeID))

		if typeID == cnc.DriverSendChannelStatusTypeID ||
			typeID == cnc.DriverReceiveChannelStatusTypeID {
			atomic.Wrap(meta).PutInt64(
				counterMetadataRegistrationKeyOffset(counterID),
				wrongRegistrationID+999,
			)
			require.True(rt, reader.IsCounterActive(counterID, registrationID, typeID))
		}
	})
}

func TestCounterSlot_Get_matchesCounterValue_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		counterCount := rapid.IntRange(1, 8).Draw(rt, "counterCount")
		counterID := rapid.Int32Range(0, int32(counterCount-1)).Draw(rt, "counterID")
		typeID := rapid.SampledFrom([]int32{
			cnc.DriverPublisherLimitTypeID,
			cnc.DriverSubscriberPositionTypeID,
			cnc.ClientHeartbeatTypeID,
		}).Draw(rt, "typeID")
		registrationID := rapid.Int64().Draw(rt, "registrationID")
		stepCount := rapid.IntRange(1, 32).Draw(rt, "stepCount")

		meta, values := syntheticCountersBuffers(t, counterCount)
		require.NoError(
			rt,
			writeAllocatedCounterForTest(meta, values, counterID, typeID, registrationID, 0),
		)

		reader, err := cnc.NewCountersReader(meta, values)
		require.NoError(rt, err)

		buffer := reader.CounterValuesBuffer()
		slot := cnc.NewCounterSlot(counterID, buffer)
		slotOffset := cnc.CounterOffset(counterID)

		for step := range stepCount {
			value := rapid.Int64().Draw(rt, fmt.Sprintf("value%d", step))
			buffer.PutInt64Release(slotOffset, value)

			require.Equal(rt, value, slot.Get())

			gotViaCounter, err := reader.CounterValue(counterID)
			require.NoError(rt, err)
			require.Equal(rt, value, gotViaCounter)
		}
	})
}
