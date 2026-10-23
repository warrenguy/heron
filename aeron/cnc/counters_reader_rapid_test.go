//go:build rapid

package cnc_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
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
			atomicbuffer.Wrap(meta).PutInt64(
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

func TestCountersReader_CounterKeyLabel_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		counterCount := rapid.IntRange(1, 4).Draw(rt, "counterCount")
		counterID := rapid.Int32Range(0, int32(counterCount-1)).Draw(rt, "counterID")
		state := rapid.SampledFrom([]int32{
			cnc.CounterRecordUnused,
			cnc.CounterRecordAllocated,
			cnc.CounterRecordReclaimed,
		}).Draw(rt, "state")
		key := rapid.SliceOfN(rapid.Byte(), 0, cnc.CounterMetadataMaxKeyLength).Draw(rt, "key")
		label := string(
			rapid.SliceOfN(
				rapid.ByteRange(0, 127),
				0,
				cnc.CounterMetadataMaxLabelLength,
			).Draw(rt, "label"),
		)
		typeID := rapid.Int32().Draw(rt, "typeID")
		registrationID := rapid.Int64().Draw(rt, "registrationID")

		meta := make([]byte, counterCount*cnc.CounterMetadataLength)
		values := make([]byte, counterCount*cnc.CounterValueLength)
		require.NoError(rt, cnc.WriteCounterRecordWithKey(
			meta,
			values,
			counterID,
			state,
			typeID,
			key,
			label,
			registrationID,
			0,
		))

		var wantKey [cnc.CounterMetadataMaxKeyLength]byte
		copy(wantKey[:], key)
		var payload [cnc.CounterMetadataMaxLabelLength]byte
		copy(payload[:], label)

		labelLength := int32(len(label))
		corrupt := rapid.Bool().Draw(rt, "corrupt")
		if corrupt {
			labelLength = rapid.Int32Range(-8, cnc.CounterMetadataMaxLabelLength+8).
				Draw(rt, "labelLength")
			atomicbuffer.Wrap(meta).PutInt32(counterLabelLengthOffset(counterID), labelLength)
		}

		reader, err := cnc.NewCountersReader(meta, values)
		require.NoError(rt, err)
		allocated := state == cnc.CounterRecordAllocated

		gotKey, ok := reader.CounterKey(counterID)
		require.Equal(rt, allocated, ok)
		if allocated {
			require.Equal(rt, wantKey, gotKey)
		} else {
			require.Zero(rt, gotKey)
		}

		labelInRange := labelLength >= 0 && labelLength <= cnc.CounterMetadataMaxLabelLength
		gotLabel, ok := reader.CounterLabel(counterID)
		require.Equal(rt, allocated && labelInRange, ok)
		switch {
		case !ok:
			require.Empty(rt, gotLabel)
		case corrupt:
			require.Equal(rt, string(payload[:labelLength]), gotLabel)
		default:
			require.Equal(rt, label, gotLabel)
		}

		require.Equal(
			rt,
			registrationID,
			atomicbuffer.Wrap(values).GetInt64(
				cnc.CounterOffset(counterID)+cnc.CounterValueRegistrationIDOffset,
			),
		)

		pastMax := int32(counterCount)
		_, ok = reader.CounterKey(pastMax)
		require.False(rt, ok)
		_, ok = reader.CounterLabel(pastMax)
		require.False(rt, ok)
	})
}

// localSocketAddressScan is the expected result of both Find scans.
type localSocketAddressScan struct {
	addresses  []string
	first      string
	firstFound bool
}

// localSocketAddressModel returns the expected scan of records at counter ids
// 0 onward.
func localSocketAddressModel(
	records []localSocketAddressRecord,
	channelStatusCounterID int32,
) localSocketAddressScan {
	var want localSocketAddressScan
	matched := false
	for _, record := range records {
		if record.state == cnc.CounterRecordUnused {
			break
		}
		if record.state != cnc.CounterRecordAllocated ||
			record.typeID != cnc.DriverLocalSocketAddressStatusTypeID ||
			record.channelStatusCounterID != channelStatusCounterID ||
			record.value != cnc.ChannelEndpointStatusActive {
			continue
		}
		if !matched {
			matched = true
			want.first, want.firstFound = record.address, record.address != ""
		}
		if record.address != "" {
			want.addresses = append(want.addresses, record.address)
		}
	}
	return want
}

func TestFindLocalSocketAddresses_rapid(t *testing.T) {
	t.Parallel()
	const channelStatusCounterID = localSocketAddressChannelStatusCounterID

	rapid.Check(t, func(rt *rapid.T) {
		recordCount := rapid.IntRange(1, 16).Draw(rt, "recordCount")
		records := make([]localSocketAddressRecord, recordCount)
		for index := range records {
			address := fmt.Sprintf("10.0.0.%d:%d", index, 20000+index)
			addressLength := rapid.IntRange(0, len(address)).
				Draw(rt, fmt.Sprintf("addressLength%d", index))
			records[index] = localSocketAddressRecord{
				state: rapid.SampledFrom([]int32{
					cnc.CounterRecordUnused,
					cnc.CounterRecordAllocated,
					cnc.CounterRecordAllocated,
					cnc.CounterRecordReclaimed,
				}).Draw(rt, fmt.Sprintf("state%d", index)),
				typeID: rapid.SampledFrom([]int32{
					cnc.DriverLocalSocketAddressStatusTypeID,
					cnc.DriverLocalSocketAddressStatusTypeID,
					cnc.ClientHeartbeatTypeID,
				}).Draw(rt, fmt.Sprintf("typeID%d", index)),
				channelStatusCounterID: rapid.SampledFrom([]int32{
					channelStatusCounterID,
					channelStatusCounterID,
					channelStatusCounterID + 1,
					cnc.NullCounterID,
				}).Draw(rt, fmt.Sprintf("channelStatusCounterID%d", index)),
				address: address[:addressLength],
				value: rapid.SampledFrom([]int64{
					cnc.ChannelEndpointStatusActive,
					cnc.ChannelEndpointStatusActive,
					0,
					-1,
				}).Draw(rt, fmt.Sprintf("value%d", index)),
			}
		}

		want := localSocketAddressModel(records, channelStatusCounterID)
		reader := localSocketAddressRecordsReader(rt, records...)
		require.Equal(
			rt,
			want.addresses,
			reader.FindLocalSocketAddresses(cnc.ChannelEndpointStatusActive, channelStatusCounterID),
		)
		first, found := reader.FindLocalSocketAddress(
			cnc.ChannelEndpointStatusActive,
			channelStatusCounterID,
		)
		require.Equal(rt, want.firstFound, found)
		require.Equal(rt, want.first, first)
	})
}
