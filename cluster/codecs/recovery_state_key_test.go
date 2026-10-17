package codecs_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
)

type recoveryStateKeyGoldenFixture struct {
	File                 string  `json:"file"`
	Description          string  `json:"description"`
	LeadershipTermID     int64   `json:"leadershipTermId"`
	LogPosition          int64   `json:"logPosition"`
	Timestamp            int64   `json:"timestamp"`
	ClusterID            int32   `json:"clusterId"`
	ServiceCount         int     `json:"serviceCount"`
	SnapshotRecordingIDs []int64 `json:"snapshotRecordingIds"`
}

type recoveryStateKeyGoldenManifest struct {
	KeyLength int                             `json:"keyLength"`
	Fixtures  []recoveryStateKeyGoldenFixture `json:"fixtures"`
}

func loadRecoveryStateKeyGoldenManifest(t *testing.T) recoveryStateKeyGoldenManifest {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", "recovery-state-key", "fixtures.json"))
	require.NoError(t, err)

	var m recoveryStateKeyGoldenManifest
	require.NoError(t, json.Unmarshal(raw, &m))
	require.Equal(t, cnc.CounterMetadataMaxKeyLength, m.KeyLength)
	require.NotEmpty(t, m.Fixtures)
	return m
}

func readRecoveryStateKeyGolden(tb testing.TB, file string, keyLen int) []byte {
	tb.Helper()

	golden, err := os.ReadFile(filepath.Join("testdata", "recovery-state-key", file))
	require.NoError(tb, err)
	require.Len(tb, golden, keyLen, "golden %q must be keyLength bytes", file)
	return golden
}

func TestRecoveryStateKey_goldenFixtures(t *testing.T) {
	t.Parallel()

	m := loadRecoveryStateKeyGoldenManifest(t)
	for _, fixture := range m.Fixtures {
		t.Run(fixture.File, func(t *testing.T) {
			t.Parallel()

			golden := readRecoveryStateKeyGolden(t, fixture.File, m.KeyLength)
			require.Len(t, fixture.SnapshotRecordingIDs, fixture.ServiceCount)

			want := codecs.RecoveryStateKeyFields{
				LeadershipTermID: fixture.LeadershipTermID,
				LogPosition:      fixture.LogPosition,
				Timestamp:        fixture.Timestamp,
				ClusterID:        fixture.ClusterID,
			}
			if fixture.ServiceCount > 0 {
				want.SnapshotRecordingIDs = fixture.SnapshotRecordingIDs
			}

			// decode: the Java bytes yield the manifest fields.
			got, ok := codecs.TryDecodeRecoveryStateKeyAt(golden, 0)
			require.True(t, ok)
			require.Equal(t, want, got)

			// encode: the Go bytes match the Java key, and nothing lands
			// past the key end.
			buf := make([]byte, cnc.CounterMetadataMaxKeyLength)
			keyEnd, ok := codecs.EncodeRecoveryStateKey(buf, 0, want)
			require.True(t, ok)
			require.Equal(t, 32+8*fixture.ServiceCount, keyEnd)
			require.Equal(t, golden[:keyEnd], buf[:keyEnd])
			require.Equal(t, make([]byte, len(buf)-keyEnd), buf[keyEnd:])
		})
	}
}

func TestRecoveryStateKey_constants(t *testing.T) {
	t.Parallel()

	require.Equal(t, int32(204), codecs.RecoveryStateCounterTypeID)
	require.Equal(t, 10, codecs.RecoveryStateMaxServiceCount)
}

func recoveryStateRecordingIDs(count int) []int64 {
	ids := make([]int64, count)
	for i := range ids {
		ids[i] = int64(1000 + i)
	}
	return ids
}

func TestRecoveryStateKey_roundtrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		ids  []int64
	}{
		{name: "nil ids", ids: nil},
		{name: "empty non-nil ids", ids: []int64{}},
		{name: "1 id", ids: recoveryStateRecordingIDs(1)},
		{name: "2 ids", ids: recoveryStateRecordingIDs(2)},
		{name: "10 ids", ids: recoveryStateRecordingIDs(10)},
	}
	for _, tc := range cases {
		for _, offset := range []int{0, 7} {
			t.Run(fmt.Sprintf("%s offset %d", tc.name, offset), func(t *testing.T) {
				t.Parallel()

				fields := codecs.RecoveryStateKeyFields{
					LeadershipTermID:     3,
					LogPosition:          1 << 40,
					Timestamp:            -5,
					ClusterID:            -9,
					SnapshotRecordingIDs: tc.ids,
				}
				keyLen := 32 + 8*len(tc.ids)
				buf := make([]byte, offset+keyLen)

				keyEnd, ok := codecs.EncodeRecoveryStateKey(buf, offset, fields)
				require.True(t, ok)
				require.Equal(t, offset+keyLen, keyEnd)

				got, ok := codecs.TryDecodeRecoveryStateKeyAt(buf, offset)
				require.True(t, ok)
				want := fields
				if len(tc.ids) == 0 {
					want.SnapshotRecordingIDs = nil
				}
				require.Equal(t, want, got)
			})
		}
	}
}

func recoveryStateKeyWithCount(t *testing.T, bufLen int, serviceCount int32) []byte {
	t.Helper()

	buf := make([]byte, bufLen)
	_, ok := codecs.EncodeRecoveryStateKey(buf, 0, codecs.RecoveryStateKeyFields{})
	require.True(t, ok)
	atomicbuffer.Wrap(buf).PutInt32(28, serviceCount)
	return buf
}

func TestTryDecodeRecoveryStateKeyAt_rejects(t *testing.T) {
	t.Parallel()

	full := make([]byte, cnc.CounterMetadataMaxKeyLength)
	cases := []struct {
		name   string
		buf    []byte
		offset int
	}{
		{name: "nil buffer", buf: nil, offset: 0},
		{name: "offset -1", buf: full, offset: -1},
		{name: "offset math.MaxInt", buf: full, offset: math.MaxInt},
		{name: "offset past the end", buf: full, offset: len(full) + 1},
		{name: "31 bytes", buf: make([]byte, 31), offset: 0},
		{name: "count -1", buf: recoveryStateKeyWithCount(t, len(full), -1), offset: 0},
		{name: "count 11", buf: recoveryStateKeyWithCount(t, 32+8*11, 11), offset: 0},
		{name: "count past the buffer", buf: recoveryStateKeyWithCount(t, 32+8*3-1, 3), offset: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var got codecs.RecoveryStateKeyFields
			var ok bool
			require.NotPanics(t, func() {
				got, ok = codecs.TryDecodeRecoveryStateKeyAt(tc.buf, tc.offset)
			})
			require.False(t, ok)
			require.Equal(t, codecs.RecoveryStateKeyFields{}, got)
		})
	}
}

func TestEncodeRecoveryStateKey_rejects(t *testing.T) {
	t.Parallel()

	twoIDs := codecs.RecoveryStateKeyFields{SnapshotRecordingIDs: []int64{1, 2}}
	cases := []struct {
		name   string
		bufLen int
		offset int
		fields codecs.RecoveryStateKeyFields
	}{
		{
			name:   "11 ids",
			bufLen: 32 + 8*11,
			fields: codecs.RecoveryStateKeyFields{SnapshotRecordingIDs: make([]int64, 11)},
		},
		{name: "offset -1", bufLen: 64, offset: -1, fields: twoIDs},
		{name: "offset math.MaxInt", bufLen: 64, offset: math.MaxInt, fields: twoIDs},
		{name: "buffer one byte short", bufLen: 32 + 8*2 - 1, fields: twoIDs},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			buf := bytes.Repeat([]byte{0xa5}, tc.bufLen)
			before := bytes.Clone(buf)

			var keyEnd int
			var ok bool
			require.NotPanics(t, func() {
				keyEnd, ok = codecs.EncodeRecoveryStateKey(buf, tc.offset, tc.fields)
			})
			require.False(t, ok)
			require.Zero(t, keyEnd)
			require.Equal(t, before, buf, "refused encode must not write")
		})
	}
}

func TestRecoveryStateKeyFields_SnapshotRecordingID(t *testing.T) {
	t.Parallel()

	fields := codecs.RecoveryStateKeyFields{SnapshotRecordingIDs: []int64{10, 11}}
	golden := readRecoveryStateKeyGolden(t, "term-only.key", cnc.CounterMetadataMaxKeyLength)
	termOnly, ok := codecs.TryDecodeRecoveryStateKeyAt(golden, 0)
	require.True(t, ok)
	cases := []struct {
		name      string
		fields    codecs.RecoveryStateKeyFields
		serviceID int32
		want      int64
		wantOK    bool
	}{
		{name: "in range", fields: fields, serviceID: 1, want: 11, wantOK: true},
		{name: "service id -1", fields: fields, serviceID: -1},
		{name: "service id equal to the count", fields: fields, serviceID: 2},
		{name: "service id 0 on a term-only key", fields: termOnly, serviceID: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := tc.fields.SnapshotRecordingID(tc.serviceID)
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.want, got)
		})
	}
}
