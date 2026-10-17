//go:build integration

package integration_test

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/test/integration/harness"
)

const liveCounterTypeID int32 = 1101

// liveCounterKey returns a 16-byte key unique to clientID. Parallel tests on
// the shared driver never match each other's records.
func liveCounterKey(clientID int64, marker uint64) []byte {
	key := make([]byte, 16)
	binary.LittleEndian.PutUint64(key, uint64(clientID))
	binary.LittleEndian.PutUint64(key[8:], marker)
	return key
}

// liveCounterKeyRegion returns key zero-padded to the full metadata key
// region, as CounterKey returns it.
func liveCounterKeyRegion(key []byte) [cnc.CounterMetadataMaxKeyLength]byte {
	var region [cnc.CounterMetadataMaxKeyLength]byte
	copy(region[:], key)
	return region
}

// requireCounterFreed polls reader for up to timeout until counterID no
// longer holds key. The driver reuses a freed id only after its free-to-reuse
// timeout, so a key mismatch means the driver freed the record.
func requireCounterFreed(
	t *testing.T,
	reader *cnc.CountersReader,
	counterID int32,
	key []byte,
	timeout time.Duration,
	pump func(),
) {
	t.Helper()

	wantKey := liveCounterKeyRegion(key)
	freed := harness.PollUntilBool(
		timeout,
		aeron.DefaultIdleSleepDuration,
		func() bool {
			pump()
			gotKey, ok := reader.CounterKey(counterID)
			return !ok || gotKey != wantKey
		},
	)
	require.True(t, freed, "driver kept counter %d within %s", counterID, timeout)
}

// openCountersReader maps the CnC file of md. It returns a reader that no
// client owns and the driver client liveness timeout. Cleanup unmaps the file.
func openCountersReader(
	t *testing.T,
	md *harness.MediaDriver,
) (*cnc.CountersReader, time.Duration) {
	t.Helper()

	mapped, err := cnc.MapCncFile(md.CncPath())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mapped.Close())
	})
	reader, err := cnc.NewCountersReader(
		mapped.CountersMetaDataBuffer(),
		mapped.CountersValuesBuffer(),
	)
	require.NoError(t, err)
	livenessTimeout := time.Duration(mapped.Metadata().ClientLivenessTimeoutNs)
	require.Positive(t, livenessTimeout)
	return reader, livenessTimeout
}

// TestClientAddCounterKeyLabelAndClose reads a live counter record and frees it with Close.
func TestClientAddCounterKeyLabelAndClose(t *testing.T) {
	md := harness.SharedParallel(t)

	clientA := harness.ConnectClient(t, md, aeron.Config{})
	clientB := harness.ConnectClient(t, md, aeron.Config{})

	key := liveCounterKey(clientA.ClientId(), 0x0102030405060708)
	const label = "add counter key label test"
	counter, err := clientA.AddCounter(liveCounterTypeID, key, label)
	require.NoError(t, err)
	counterID := counter.CounterID()

	reader, err := clientB.CountersReader()
	require.NoError(t, err)
	require.True(t, reader.IsCounterAllocatedOfType(counterID, liveCounterTypeID))
	gotKey, ok := reader.CounterKey(counterID)
	require.True(t, ok)
	require.Equal(t, liveCounterKeyRegion(key), gotKey)
	gotLabel, ok := reader.CounterLabel(counterID)
	require.True(t, ok)
	require.Equal(t, label, gotLabel)
	require.Equal(
		t,
		counter.RegistrationID(),
		reader.CounterValuesBuffer().GetInt64Volatile(
			cnc.CounterOffset(counterID)+cnc.CounterValueRegistrationIDOffset,
		),
	)

	require.NoError(t, counter.Close())
	requireCounterFreed(t, reader, counterID, key, harness.ConnectedPollTimeout, func() {
		clientA.DoWork()
		clientB.DoWork()
	})
	require.NoError(t, counter.Close())
	md.AssertProcessAlive(t)
}

// TestClientCloseFreesOpenCounter asserts Client.Close frees an unclosed live counter.
func TestClientCloseFreesOpenCounter(t *testing.T) {
	md := harness.SharedParallel(t)

	reader, livenessTimeout := openCountersReader(t, md)
	client := harness.ConnectClient(t, md, aeron.Config{})

	key := liveCounterKey(client.ClientId(), 0x1112131415161718)
	counter, err := client.AddCounter(liveCounterTypeID, key, "client close frees counter test")
	require.NoError(t, err)
	counterID := counter.CounterID()

	gotKey, ok := reader.CounterKey(counterID)
	require.True(t, ok)
	require.Equal(t, liveCounterKeyRegion(key), gotKey)

	require.NoError(t, client.Close())
	requireCounterFreed(t, reader, counterID, key, livenessTimeout/2, func() {})
	md.AssertProcessAlive(t)
}
