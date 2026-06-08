package conductor_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

const (
	conductorTestClientID            int64 = 42
	conductorTestDriverTimeout             = 10 * time.Second
	conductorTestKeepAliveInterval         = 500 * time.Millisecond
	conductorTestIdleSleepDuration         = 16 * time.Millisecond
	conductorTestInterServiceTimeout       = time.Second
)

var conductorTestWorkNanoNs = (600 * time.Millisecond).Nanoseconds()

func collectConductorErrors() (func(error), *[]error) {
	var gotErrors []error
	return func(err error) {
		gotErrors = append(gotErrors, err)
	}, &gotErrors
}

func TestConductor_staleDriverKeepalive(t *testing.T) {
	var gotErrors []error
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		20_000,
		(600 * time.Millisecond).Nanoseconds(),
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(1_000)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, gotErrors, 1)
	var staleErr *conductor.DriverKeepaliveStaleError
	require.ErrorAs(t, gotErrors[0], &staleErr)
	require.EqualError(
		t,
		gotErrors[0],
		"conductor: media driver keepalive stale: age=19000ms timeout=10000ms",
	)
}

func TestConductor_unpublishedDriverHeartbeatNotStale(t *testing.T) {
	var gotErrors []error
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		20_000,
		(600 * time.Millisecond).Nanoseconds(),
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(0)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.False(t, conductorInstance.IsTerminating())
	require.Empty(t, gotErrors)
}

func TestConductor_driverShutdownHeartbeat(t *testing.T) {
	var gotErrors []error
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		1_000,
		(600 * time.Millisecond).Nanoseconds(),
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(conductor.NullCorrelationID)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, gotErrors, 1)
	require.ErrorIs(t, gotErrors[0], conductor.ErrDriverShutdown)
	require.EqualError(t, gotErrors[0], "conductor: media driver has been shutdown")
}

func TestConductor_heartbeatCounterUpdate(t *testing.T) {
	epochMs := int64(1_700_000_000_123)
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		epochMs,
		(600 * time.Millisecond).Nanoseconds(),
		withHeartbeatCounter(t, conductorTestClientID, 0),
	)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)

	got, err := conductorInstance.CountersReaderForTest().GetCounterValue(0)
	require.NoError(t, err)
	require.Equal(t, epochMs, got)
}

func TestConductor_unregisteredClientSkipsHeartbeatCounter(t *testing.T) {
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(error) {},
		1_700_000_000_123,
		(600 * time.Millisecond).Nanoseconds(),
	)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.False(t, conductorInstance.IsTerminating())
}

func TestConductor_oneBroadcastEventPerDoWork(t *testing.T) {
	errorHandler, gotErrors := collectConductorErrors()
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(t, errorHandler, fixedClocks{nanoNs: 0}),
	)
	adapter := conductorInstance.EventsAdapterForTest()

	transmitConductorEvent(t, adapter, conductor.OnError, []byte{0x01})
	transmitConductorEvent(t, adapter, conductor.OnError, []byte{0x02})

	for step, wantErrs := range []int{1, 2} {
		workCount := conductorInstance.DoWork()
		require.Equal(t, 1, workCount)
		require.Len(t, *gotErrors, wantErrs)
		require.ErrorIs(t, (*gotErrors)[step], conductor.ErrDriverErrorEvent)
		require.EqualError(t, (*gotErrors)[step], "conductor: driver error event received")
	}

	receiver := adapter.CopyReceiverForTest().BroadcastReceiver()
	require.False(t, receiver.ReceiveNext())
}

func TestConductor_onClientTimeoutEvent(t *testing.T) {
	var gotErrors []error
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(
			t,
			func(err error) { gotErrors = append(gotErrors, err) },
			fixedClocks{nanoNs: 0},
		),
	)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnClientTimeout,
		testutil.ClientTimeoutPayload(conductorTestClientID),
	)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, gotErrors, 1)
	require.ErrorIs(t, gotErrors[0], conductor.ErrClientTimeout)
	require.EqualError(t, gotErrors[0], "conductor: client timeout from driver")
}

func TestConductor_serviceIntervalExceeded(t *testing.T) {
	var gotErrors []error
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		0,
		(2 * time.Second).Nanoseconds(),
	)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, gotErrors, 1)
	require.ErrorIs(t, gotErrors[0], conductor.ErrConductorServiceTimeout)
	require.EqualError(t, gotErrors[0], "conductor: service interval exceeded")
}

func TestConductor_serviceIntervalSkipsLivenessOnSameTick(t *testing.T) {
	var gotErrors []error
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		20_000,
		(2 * time.Second).Nanoseconds(),
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(1_000)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, gotErrors, 1)
	require.ErrorIs(t, gotErrors[0], conductor.ErrConductorServiceTimeout)
}

func TestConductor_heartbeatCounterClosed(t *testing.T) {
	var gotErrors []error
	var nanoNs int64
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfigMutableNano(
			t,
			func(err error) { gotErrors = append(gotErrors, err) },
			1_700_000_000_123,
			&nanoNs,
		),
		withHeartbeatCounter(t, conductorTestClientID, 0),
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(1_700_000_000_000)

	nanoNs = (600 * time.Millisecond).Nanoseconds()
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Empty(t, gotErrors)

	meta := make([]byte, cnc.CounterMetadataLength)
	values := make([]byte, cnc.CounterValueLength)
	cnc.WriteCounterRecord(
		meta,
		values,
		0,
		cnc.CounterRecordReclaimed,
		cnc.ClientHeartbeatTypeID,
		conductorTestClientID,
		1_700_000_000_123,
	)
	countersReader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)
	conductorInstance.SetCountersReaderForTest(countersReader)

	nanoNs = (1200 * time.Millisecond).Nanoseconds()
	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, gotErrors, 1)
	var closedErr *conductor.HeartbeatCounterClosedError
	require.ErrorAs(t, gotErrors[0], &closedErr)
	require.EqualError(
		t,
		gotErrors[0],
		"conductor: unexpected close of heartbeat timestamp counter: 0",
	)
}

func TestConductor_unregisteredClientDetectsStaleDriverEachIdleTick(t *testing.T) {
	var gotErrors []error
	var nanoNs int64
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfigMutableNano(
			t,
			func(err error) { gotErrors = append(gotErrors, err) },
			1_700_000_000_123,
			&nanoNs,
		),
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(1_700_000_000_000)

	nanoNs = (600 * time.Millisecond).Nanoseconds()
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Empty(t, gotErrors)

	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(1_000)
	nanoNs = (620 * time.Millisecond).Nanoseconds()

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, gotErrors, 1)
	var staleErr *conductor.DriverKeepaliveStaleError
	require.ErrorAs(t, gotErrors[0], &staleErr)
}

func TestConductor_driverTimeoutBoundary(t *testing.T) {
	driverHeartbeatMs := int64(1_000_000)
	timeoutMs := conductorTestDriverTimeout.Milliseconds()

	t.Run("at boundary stays alive", func(t *testing.T) {
		conductorInstance := newConductorForTestAtWorkNano(
			t,
			func(error) {},
			driverHeartbeatMs+timeoutMs,
			(600 * time.Millisecond).Nanoseconds(),
		)
		conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(driverHeartbeatMs)

		workCount := conductorInstance.DoWork()
		require.Equal(t, 1, workCount)
		require.False(t, conductorInstance.IsTerminating())
	})

	t.Run("one millisecond past boundary terminates", func(t *testing.T) {
		var gotErrors []error
		conductorInstance := newConductorForTestAtWorkNano(
			t,
			func(err error) { gotErrors = append(gotErrors, err) },
			driverHeartbeatMs+timeoutMs+1,
			(600 * time.Millisecond).Nanoseconds(),
		)
		conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(driverHeartbeatMs)

		workCount := conductorInstance.DoWork()
		require.Equal(t, 1, workCount)
		require.True(t, conductorInstance.IsTerminating())
		require.Len(t, gotErrors, 1)
		var staleErr *conductor.DriverKeepaliveStaleError
		require.ErrorAs(t, gotErrors[0], &staleErr)
		require.Equal(t, timeoutMs+1, staleErr.AgeMs)
	})
}

func TestConductor_doWorkAfterTerminatingReturnsZero(t *testing.T) {
	var gotErrors []error
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		20_000,
		(600 * time.Millisecond).Nanoseconds(),
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(1_000)

	require.Equal(t, 1, conductorInstance.DoWork())
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, gotErrors, 1)

	require.Zero(t, conductorInstance.DoWork())
	require.Len(t, gotErrors, 1)
}

func TestConductor_duplicateClientTimeoutIgnored(t *testing.T) {
	var gotErrors []error
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(
			t,
			func(err error) { gotErrors = append(gotErrors, err) },
			fixedClocks{nanoNs: 0},
		),
	)
	adapter := conductorInstance.EventsAdapterForTest()

	timeoutPayload := testutil.ClientTimeoutPayload(conductorTestClientID)
	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, timeoutPayload)
	require.Equal(t, 1, conductorInstance.DoWork())
	require.Len(t, gotErrors, 1)

	transmitConductorEvent(t, adapter, conductor.OnClientTimeout, timeoutPayload)
	require.Equal(t, 1, adapter.Receive(conductor.NullCorrelationID, nil))
	require.Len(t, gotErrors, 1)
}

func TestConductor_broadcastLappedDuringDoWork(t *testing.T) {
	var gotErrors []error
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(
			t,
			func(err error) { gotErrors = append(gotErrors, err) },
			fixedClocks{nanoNs: 0},
		),
	)
	adapter := conductorInstance.EventsAdapterForTest()
	copyReceiver := adapter.CopyReceiverForTest()
	buf := copyReceiver.BroadcastReceiver().Buffer()
	capacity := copyReceiver.BroadcastReceiver().Capacity()
	tx, err := broadcast.NewBroadcastTransmitter(buf)
	require.NoError(t, err)

	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	fillBroadcastUntilLap(t, tx, conductor.OnError, payload, capacity)
	rewindBroadcastTailForLap(t, buf, capacity)

	workCount := conductorInstance.DoWork()
	require.Zero(t, workCount)
	require.False(t, conductorInstance.IsTerminating())
	require.Len(t, gotErrors, 1)
	require.ErrorIs(t, gotErrors[0], conductor.ErrBroadcastLapped)
	require.True(t, adapter.IsInvalid())

	workCount = conductorInstance.DoWork()
	require.Zero(t, workCount)
	require.False(t, conductorInstance.IsTerminating())
	require.Len(t, gotErrors, 1)
}

func TestConductor_terminatesBeforeReceiveOnSameTick(t *testing.T) {
	var gotErrors []error
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		20_000,
		(600 * time.Millisecond).Nanoseconds(),
	)
	conductorInstance.DriverProxyForTest().Ring().SetConsumerHeartbeatTime(1_000)

	transmitConductorEvent(
		t,
		conductorInstance.EventsAdapterForTest(),
		conductor.OnError,
		[]byte{0x01},
	)

	workCount := conductorInstance.DoWork()
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, gotErrors, 1)
	var staleErr *conductor.DriverKeepaliveStaleError
	require.ErrorAs(t, gotErrors[0], &staleErr)
}

func TestConductor_idleDoWorkNoProgress(t *testing.T) {
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(t, func(error) {}, fixedClocks{nanoNs: 0}),
	)

	require.Zero(t, conductorInstance.DoWork())
	require.False(t, conductorInstance.IsTerminating())
}

func TestConductor_serviceTimestampSeeding(t *testing.T) {
	tests := []struct {
		name            string
		seed            bool
		advanceDuration time.Duration
		wantWorkCount   int
		wantTerminating bool
		wantErr         error
	}{
		{
			name:            "seeded avoids false service timeout",
			seed:            true,
			advanceDuration: conductorTestKeepAliveInterval + time.Nanosecond,
			wantWorkCount:   1,
			wantTerminating: false,
		},
		{
			name:            "constructor seed avoids false service timeout",
			seed:            false,
			wantWorkCount:   0,
			wantTerminating: false,
		},
		{
			name:            "service timeout after interServiceTimeout",
			seed:            false,
			advanceDuration: conductorTestInterServiceTimeout + time.Nanosecond,
			wantWorkCount:   1,
			wantTerminating: true,
			wantErr:         conductor.ErrConductorServiceTimeout,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var gotErrors []error
			nanoNs := (2 * time.Second).Nanoseconds()
			conductorInstance := newConductorForTest(
				t,
				conductorTestConfigMutableNano(
					t,
					func(err error) { gotErrors = append(gotErrors, err) },
					1_700_000_000_123,
					&nanoNs,
				),
			)
			if testCase.seed {
				conductorInstance.SeedServiceTimestamps(nanoNs)
			}

			nanoNs += testCase.advanceDuration.Nanoseconds()

			workCount := conductorInstance.DoWork()
			require.Equal(t, testCase.wantWorkCount, workCount)
			require.Equal(t, testCase.wantTerminating, conductorInstance.IsTerminating())
			if testCase.wantErr == nil {
				require.Empty(t, gotErrors)
				return
			}
			require.Len(t, gotErrors, 1)
			require.ErrorIs(t, gotErrors[0], testCase.wantErr)
		})
	}
}

func TestConductor_setCounterValueErrorDoesNotAdvanceKeepaliveTimer(t *testing.T) {
	var gotErrors []error
	workNanoNs := (600 * time.Millisecond).Nanoseconds()
	conductorInstance := newConductorForTestAtWorkNano(
		t,
		func(err error) { gotErrors = append(gotErrors, err) },
		1_700_000_000_123,
		workNanoNs,
	)
	conductorInstance.SetHeartbeatCounterIDForTest(99)

	workCount := conductorInstance.StoreClientHeartbeatForTest(
		workNanoNs,
		1_700_000_000_123,
	)
	require.Equal(t, 1, workCount)
	require.True(t, conductorInstance.IsTerminating())
	require.Len(t, gotErrors, 1)
	require.EqualError(
		t,
		gotErrors[0],
		"cnc: counter id 99 out of range: maxCounterId=0",
	)
	require.Zero(t, conductorInstance.TimeOfLastKeepAliveNsForTest())
}

type fixedClocks struct {
	epochMs int64
	nanoNs  int64
}

func conductorTestConfig(
	tb testing.TB,
	errorHandler func(error),
	clocks fixedClocks,
) conductor.Config {
	tb.Helper()
	return conductorTestConfigMutableNano(tb, errorHandler, clocks.epochMs, &clocks.nanoNs)
}

func conductorTestConfigMutableNano(
	tb testing.TB,
	errorHandler func(error),
	epochMs int64,
	nanoNs *int64,
) conductor.Config {
	tb.Helper()
	return conductor.Config{
		ClientID:            conductorTestClientID,
		DriverTimeout:       conductorTestDriverTimeout,
		KeepAliveInterval:   conductorTestKeepAliveInterval,
		IdleSleepDuration:   conductorTestIdleSleepDuration,
		InterServiceTimeout: conductorTestInterServiceTimeout,
		EpochClock: func() int64 {
			return epochMs
		},
		NanoClock: func() int64 {
			return *nanoNs
		},
		ErrorHandler: errorHandler,
	}
}

type conductorTestOption func(tb testing.TB, meta, values []byte)

func withHeartbeatCounter(_ testing.TB, clientID int64, counterID int32) conductorTestOption {
	return func(tb testing.TB, meta, values []byte) {
		tb.Helper()
		cnc.WriteCounterRecord(
			meta,
			values,
			counterID,
			cnc.CounterRecordAllocated,
			cnc.ClientHeartbeatTypeID,
			clientID,
			0,
		)
	}
}

func newConductorForTest(
	tb testing.TB,
	config conductor.Config,
	options ...conductorTestOption,
) *conductor.Conductor {
	tb.Helper()

	meta := make([]byte, cnc.CounterMetadataLength)
	values := make([]byte, cnc.CounterValueLength)
	for _, option := range options {
		option(tb, meta, values)
	}

	countersReader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)

	return conductor.NewConductorForTest(tb, config, countersReader)
}

func newConductorForTestAtWorkNano(
	tb testing.TB,
	errorHandler func(error),
	epochMs int64,
	workNanoNs int64,
	options ...conductorTestOption,
) *conductor.Conductor {
	tb.Helper()

	var nanoNs int64
	conductorInstance := newConductorForTest(
		tb,
		conductorTestConfigMutableNano(tb, errorHandler, epochMs, &nanoNs),
		options...,
	)
	nanoNs = workNanoNs
	return conductorInstance
}

func fillBroadcastUntilLap(
	tb testing.TB,
	tx *broadcast.BroadcastTransmitter,
	msgTypeID int,
	payload []byte,
	capacity int,
) {
	tb.Helper()

	for range 3*capacity/16 + 1 {
		err := tx.Transmit(msgTypeID, payload)
		require.NoError(tb, err)
	}
}

func rewindBroadcastTailForLap(
	tb testing.TB,
	buf aeronatomic.AtomicBuffer,
	capacity int,
) {
	tb.Helper()

	actualLatest := buf.GetInt64Volatile(broadcast.LatestIndex(capacity))
	buf.PutInt64Release(broadcast.LatestIndex(capacity), 0)
	buf.PutInt64Release(broadcast.LatestIndex(capacity), actualLatest)
}
