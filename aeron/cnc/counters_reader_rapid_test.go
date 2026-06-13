//go:build rapid

package cnc_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
)

func TestCountersReader_isCounterActiveRegistrationSource_rapid(t *testing.T) {
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
