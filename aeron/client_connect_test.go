package aeron_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func newAdvancingEpochClock(startMs, stepMs int64) conductor.EpochClock {
	epochMs := startMs
	return func() int64 {
		current := epochMs
		epochMs += stepMs
		return current
	}
}

func requireDriverTimeoutCause(tb testutil.TB, err, wantCause error) {
	tb.Helper()

	var timeoutErr *aeron.DriverTimeoutError
	require.ErrorAs(tb, err, &timeoutErr)
	require.ErrorIs(tb, err, wantCause)
}

func TestWaitForDriverHeartbeat_timeouts(t *testing.T) {
	tests := []struct {
		name       string
		heartbeat  int64
		timeout    time.Duration
		epochStart int64
		epochStep  int64
		wantCause  error
	}{
		{
			name:       "not ready",
			timeout:    time.Millisecond,
			epochStart: 1_000,
			epochStep:  100,
			wantCause:  aeron.ErrDriverHeartbeatNotReady,
		},
		{
			name:       "stale at connect",
			heartbeat:  1_000,
			timeout:    time.Second,
			epochStart: 20_000,
			epochStep:  100,
			wantCause:  aeron.ErrDriverHeartbeatStaleAtConnect,
		},
		{
			name:       "driver shutdown",
			heartbeat:  conductor.DriverShutdownHeartbeat,
			timeout:    time.Second,
			epochStart: 1_000,
			epochStep:  100,
			wantCause:  conductor.ErrDriverShutdown,
		},
		{
			name:       "driver shutdown short wall timeout",
			heartbeat:  conductor.DriverShutdownHeartbeat,
			timeout:    2 * time.Millisecond,
			epochStart: 1_337,
			epochStep:  0,
			wantCause:  conductor.ErrDriverShutdown,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ring := testutil.NewTestRing(t)
			if testCase.heartbeat != 0 {
				ring.SetConsumerHeartbeatTime(testCase.heartbeat)
			}

			err := aeron.WaitForDriverHeartbeatForTest(
				context.Background(),
				ring,
				testCase.timeout,
				newAdvancingEpochClock(testCase.epochStart, testCase.epochStep),
			)
			requireDriverTimeoutCause(t, err, testCase.wantCause)
		})
	}
}

func TestWaitForDriverHeartbeat_freshHeartbeat(t *testing.T) {
	ring := testutil.NewTestRing(t)
	ring.SetConsumerHeartbeatTime(9_500)

	err := aeron.WaitForDriverHeartbeatForTest(
		context.Background(),
		ring,
		time.Second,
		func() int64 { return 10_000 },
	)
	require.NoError(t, err)
}

func TestWaitForDriverHeartbeat_freshAtEpochDeadlineBoundary(t *testing.T) {
	ring := testutil.NewTestRing(t)
	ring.SetConsumerHeartbeatTime(1_000)

	err := aeron.WaitForDriverHeartbeatForTest(
		context.Background(),
		ring,
		5*time.Second,
		newAdvancingEpochClock(1_000, 5_000),
	)
	require.NoError(t, err)
}

func TestWaitForDriverHeartbeat_frozenEpochClockWallBackstop(t *testing.T) {
	ring := testutil.NewTestRing(t)

	start := time.Now()
	err := aeron.WaitForDriverHeartbeatForTest(
		context.Background(),
		ring,
		50*time.Millisecond,
		func() int64 { return 1_000 },
	)
	elapsed := time.Since(start)

	requireDriverTimeoutCause(t, err, aeron.ErrDriverHeartbeatNotReady)
	require.Greater(t, elapsed, 40*time.Millisecond)
	require.Less(t, elapsed, 200*time.Millisecond)
}

func TestWaitForDriverHeartbeat_contextCanceled(t *testing.T) {
	ring := testutil.NewTestRing(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := aeron.WaitForDriverHeartbeatForTest(
		ctx,
		ring,
		time.Second,
		func() int64 { return 1_000 },
	)
	require.ErrorIs(t, err, context.Canceled)
}

func TestWaitForDriverHeartbeat_contextDeadline(t *testing.T) {
	ring := testutil.NewTestRing(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := aeron.WaitForDriverHeartbeatForTest(
		ctx,
		ring,
		5*time.Second,
		func() int64 { return 1_000 },
	)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, time.Second)
}

func TestValidateClientID(t *testing.T) {
	tests := []struct {
		name     string
		clientID int64
		wantErr  string
	}{
		{
			name:     "zero is valid",
			clientID: 0,
		},
		{
			name:     "positive is valid",
			clientID: 1,
		},
		{
			name:     "negative is invalid",
			clientID: -1,
			wantErr:  "aeron: invalid client id from driver ring",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := aeron.ValidateClientIDForTest(testCase.clientID)
			if testCase.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, testCase.wantErr)
		})
	}
}

func TestValidateControlProtocolVersion(t *testing.T) {
	tests := []struct {
		name    string
		reader  func(testing.TB) *cnc.CountersReader
		wantErr string
	}{
		{
			name: "match",
			reader: func(tb testing.TB) *cnc.CountersReader {
				return controlProtocolTestReader(tb, int64(conductor.ControlProtocolVersion))
			},
		},
		{
			name: "mismatch",
			reader: func(tb testing.TB) *cnc.CountersReader {
				return controlProtocolTestReader(tb, 2)
			},
			wantErr: "aeron: control protocol version mismatch: got 2 want 65536",
		},
		{
			name: "read error",
			reader: func(tb testing.TB) *cnc.CountersReader {
				meta := make([]byte, cnc.CounterMetadataLength)
				values := make([]byte, cnc.CounterValueLength)
				reader, err := cnc.NewCountersReader(meta, values)
				require.NoError(tb, err)
				return reader
			},
			wantErr: "aeron: read control protocol version counter: " +
				"cnc: counter id 43 out of range: maxCounterId=0",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := aeron.ValidateControlProtocolVersionForTest(testCase.reader(t))
			if testCase.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, testCase.wantErr)
		})
	}
}

func controlProtocolTestReader(tb testing.TB, value int64) *cnc.CountersReader {
	tb.Helper()

	counterCount := int(conductor.SystemCounterControlProtocolVersion) + 1
	meta := make([]byte, counterCount*cnc.CounterMetadataLength)
	values := make([]byte, counterCount*cnc.CounterValueLength)
	cnc.WriteCounterRecord(
		meta,
		values,
		conductor.SystemCounterControlProtocolVersion,
		cnc.CounterRecordAllocated,
		cnc.SystemCounterTypeID,
		0,
		value,
	)
	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)
	return reader
}
