//go:build integration

package harness

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/cnc/cnctest"
)

const (
	// Fixture type labels for DriverCounters. Scan policy lives in aeron/cnc.
	testCounterTypeA int32 = 200
	testCounterTypeB int32 = 239
	testOtherType    int32 = cnc.ClientHeartbeatTypeID
)

func TestPublicationLimitCounterValue_counterNotFound(t *testing.T) {
	t.Parallel()
	meta := make([]byte, cnc.CounterMetadataLength)
	values := make([]byte, cnc.CounterValueLength)

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	capture := NewCaptureFailTB(t)
	PublicationLimitCounterValue(capture, reader, 9001)
	require.True(t, capture.Failed())
	require.Equal(
		t,
		fmt.Sprintf("publication limit counter not found for registration %d", 9001),
		capture.lastMsg,
	)
}

func TestDriverCounters_Close_idempotent(t *testing.T) {
	t.Parallel()

	counters, err := OpenDriverCounters(cnctest.WriteCncDir(t, nil))
	require.NoError(t, err)

	require.NoError(t, counters.Close())
	require.NoError(t, counters.Close())
	require.NoError(t, (*DriverCounters)(nil).Close())
}

func TestDriverCounters_Open_missing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dir  func(t *testing.T) string
	}{
		{
			name: "missing dir",
			dir: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "missing")
			},
		},
		{
			name: "missing file",
			dir: func(t *testing.T) string {
				return t.TempDir()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := OpenDriverCounters(test.dir(t))
			require.Error(t, err)
		})
	}
}

func TestDriverCounters_Open_andValue(t *testing.T) {
	t.Parallel()

	// RegistrationID stays zero: lookup matches on type id alone.
	aeronDir := cnctest.WriteCncDir(t, []cnctest.CounterRecord{
		{CounterID: 0, State: cnc.CounterRecordAllocated, TypeID: testOtherType, Value: 1},
		{CounterID: 1, State: cnc.CounterRecordAllocated, TypeID: testCounterTypeA, Value: 1234},
	})

	counters, err := OpenDriverCounters(aeronDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, counters.Close()) })

	got, ok := counters.Value(testCounterTypeA)
	require.True(t, ok)
	require.Equal(t, int64(1234), got)

	// Second read takes the cached id.
	got, ok = counters.Value(testCounterTypeA)
	require.True(t, ok)
	require.Equal(t, int64(1234), got)

	_, ok = counters.Value(testCounterTypeB)
	require.False(t, ok)
}

// TestDriverCounters_Value_rescansWhenCachedIDReclaimed asserts a reclaimed
// cached id forces rescan. A reclaimed record must not keep serving its old value.
func TestDriverCounters_Value_rescansWhenCachedIDReclaimed(t *testing.T) {
	t.Parallel()

	aeronDir := cnctest.WriteCncDir(t, []cnctest.CounterRecord{
		{CounterID: 0, State: cnc.CounterRecordAllocated, TypeID: testCounterTypeA, Value: 1234},
		{CounterID: 1, State: cnc.CounterRecordAllocated, TypeID: testCounterTypeA, Value: 4321},
	})

	counters, err := OpenDriverCounters(aeronDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, counters.Close()) })

	got, ok := counters.Value(testCounterTypeA)
	require.True(t, ok)
	require.Equal(t, int64(1234), got)

	cnc.WriteCounterRecord(
		counters.mapped.CountersMetaDataBuffer(),
		counters.mapped.CountersValuesBuffer(),
		0,
		cnc.CounterRecordReclaimed,
		testCounterTypeA,
		0,
		1234,
	)

	got, ok = counters.Value(testCounterTypeA)
	require.True(t, ok)
	require.Equal(t, int64(4321), got)
}

func TestDriverCounters_Value_afterClose(t *testing.T) {
	t.Parallel()

	aeronDir := cnctest.WriteCncDir(t, []cnctest.CounterRecord{
		{CounterID: 0, State: cnc.CounterRecordAllocated, TypeID: testCounterTypeA, Value: 9},
	})
	counters, err := OpenDriverCounters(aeronDir)
	require.NoError(t, err)
	require.NoError(t, counters.Close())

	_, ok := counters.Value(testCounterTypeA)
	require.False(t, ok)
}
