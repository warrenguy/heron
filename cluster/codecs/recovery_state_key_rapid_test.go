//go:build rapid

package codecs_test

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
)

func TestEncodeRecoveryStateKey_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		offset := rapid.OneOf(
			rapid.IntRange(-2, 16),
			rapid.Just(math.MaxInt),
		).Draw(rt, "offset")

		count := rapid.IntRange(0, 12).Draw(rt, "count")
		var ids []int64
		if count > 0 {
			ids = rapid.SliceOfN(rapid.Int64(), count, count).Draw(rt, "ids")
		}
		fields := codecs.RecoveryStateKeyFields{
			LeadershipTermID:     rapid.Int64().Draw(rt, "leadershipTermID"),
			LogPosition:          rapid.Int64().Draw(rt, "logPosition"),
			Timestamp:            rapid.Int64().Draw(rt, "timestamp"),
			ClusterID:            rapid.Int32().Draw(rt, "clusterID"),
			SnapshotRecordingIDs: ids,
		}

		keyLen := 32 + 8*count
		base := 0
		if offset >= 0 && offset <= 16 {
			base = offset
		}
		slack := rapid.IntRange(-8, 32).Draw(rt, "slack")
		bufLen := max(base+keyLen+slack, 0)
		buf := rapid.SliceOfN(rapid.Byte(), bufLen, bufLen).Draw(rt, "buf")
		before := bytes.Clone(buf)

		valid := offset >= 0 &&
			count <= codecs.RecoveryStateMaxServiceCount &&
			offset <= len(buf)-keyLen

		var keyEnd int
		var ok bool
		require.NotPanics(rt, func() {
			keyEnd, ok = codecs.EncodeRecoveryStateKey(buf, offset, fields)
		})
		requireRecoveryStateReader(rt, fields)

		if !valid {
			require.False(rt, ok)
			require.Zero(rt, keyEnd)
			require.Equal(rt, before, buf)
			return
		}

		require.True(rt, ok)
		require.Equal(rt, offset+keyLen, keyEnd)
		require.Equal(rt, before[:offset], buf[:offset])
		require.Equal(rt, before[keyEnd:], buf[keyEnd:])

		got, ok := codecs.TryDecodeRecoveryStateKeyAt(buf, offset)
		require.True(rt, ok)
		require.Equal(rt, fields, got)
		requireRecoveryStateReader(rt, got)
	})
}

func TestTryDecodeRecoveryStateKeyAt_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		bufLen := rapid.IntRange(0, 152).Draw(rt, "bufLen")
		buf := rapid.SliceOfN(rapid.Byte(), bufLen, bufLen).Draw(rt, "buf")

		offset := rapid.OneOf(
			rapid.IntRange(-2, 16),
			rapid.Just(math.MaxInt),
		).Draw(rt, "offset")

		countFits := offset >= 0 && offset <= len(buf)-32
		if countFits {
			count := rapid.Int32Range(-2, 12).Draw(rt, "count")
			binary.LittleEndian.PutUint32(buf[offset+28:], uint32(count))
		}

		want, wantOK := recoveryStateKeyModel(buf, offset)

		var got codecs.RecoveryStateKeyFields
		var ok bool
		require.NotPanics(rt, func() {
			got, ok = codecs.TryDecodeRecoveryStateKeyAt(buf, offset)
		})
		require.Equal(rt, wantOK, ok)
		require.Equal(rt, want, got)
		requireRecoveryStateReader(rt, got)
	})
}

// recoveryStateKeyModel is the decode oracle, built on encoding/binary.
func recoveryStateKeyModel(buf []byte, offset int) (codecs.RecoveryStateKeyFields, bool) {
	if offset < 0 || offset > len(buf)-32 {
		return codecs.RecoveryStateKeyFields{}, false
	}
	key := buf[offset:]
	count := int(int32(binary.LittleEndian.Uint32(key[28:])))
	if count < 0 || count > 10 {
		return codecs.RecoveryStateKeyFields{}, false
	}
	if len(key) < 32+8*count {
		return codecs.RecoveryStateKeyFields{}, false
	}

	fields := codecs.RecoveryStateKeyFields{
		LeadershipTermID: int64(binary.LittleEndian.Uint64(key[0:])),
		LogPosition:      int64(binary.LittleEndian.Uint64(key[8:])),
		Timestamp:        int64(binary.LittleEndian.Uint64(key[16:])),
		ClusterID:        int32(binary.LittleEndian.Uint32(key[24:])),
	}
	for i := range count {
		id := int64(binary.LittleEndian.Uint64(key[32+8*i:]))
		fields.SnapshotRecordingIDs = append(fields.SnapshotRecordingIDs, id)
	}
	return fields, true
}

func requireRecoveryStateReader(rt *rapid.T, fields codecs.RecoveryStateKeyFields) {
	rt.Helper()

	count := len(fields.SnapshotRecordingIDs)
	for serviceID := -2; serviceID <= count+2; serviceID++ {
		got, ok := fields.SnapshotRecordingID(int32(serviceID))
		inRange := serviceID >= 0 && serviceID < count
		require.Equal(rt, inRange, ok, "service id %d", serviceID)
		if !inRange {
			require.Zero(rt, got, "service id %d", serviceID)
			continue
		}
		require.Equal(rt, fields.SnapshotRecordingIDs[serviceID], got, "service id %d", serviceID)
	}
}
