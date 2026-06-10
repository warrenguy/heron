//go:build integration

package integration_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/record"
	"github.com/warrenguy/heron/aeron/ringbuffer"
	"github.com/warrenguy/heron/test/integration/harness"
)

const clientStayAliveDuration = 10 * time.Second

func TestClientStayAlive(t *testing.T) {
	md := harness.Start(t)

	client, err := aeron.Connect(aeron.Context{
		AeronDir: md.Dir,
		ErrorHandler: func(err error) {
			require.FailNow(t, "unexpected client error during stay-alive", "%v", err)
		},
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, client.ClientId(), int64(0))
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	deadline := time.Now().Add(clientStayAliveDuration)
	sawProgress := false
	for time.Now().Before(deadline) {
		if client.DoWork() > 0 {
			sawProgress = true
		}
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}
	require.True(t, sawProgress, "client never made progress during stay-alive")
}

func TestClientDetectsDriverDeath(t *testing.T) {
	md := harness.Start(t)

	driverTimeout := 2 * time.Second
	var gotErr error
	client, err := aeron.Connect(aeron.Context{
		AeronDir:      md.Dir,
		DriverTimeout: driverTimeout,
		ErrorHandler: func(err error) {
			gotErr = err
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	for range 20 {
		client.DoWork()
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}

	require.NoError(t, md.Kill())

	deadline := time.Now().Add(driverTimeout + time.Second)
	for time.Now().Before(deadline) {
		client.DoWork()
		if gotErr != nil {
			var timeoutErr *aeron.DriverTimeoutError
			require.ErrorAs(t, gotErr, &timeoutErr)
			var staleErr *conductor.DriverKeepaliveStaleError
			require.ErrorAs(t, timeoutErr.Unwrap(), &staleErr)
			require.Greater(t, staleErr.AgeMs, staleErr.TimeoutMs)
			require.True(t, client.IsTerminating())
			return
		}
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}

	require.Fail(t, "expected DriverTimeoutError after driver kill")
}

func TestClientReadsSystemCounters(t *testing.T) {
	md := harness.Start(t)

	client, err := aeron.Connect(aeron.Context{
		AeronDir: md.Dir,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	countersReader, err := client.CountersReader()
	require.NoError(t, err)

	got, err := countersReader.GetCounterValue(conductor.SystemCounterControlProtocolVersion)
	require.NoError(t, err)
	require.Equal(t, int64(conductor.ControlProtocolVersion), got)
}

func TestClientErrorLogReadable(t *testing.T) {
	md := harness.Start(t)

	client, err := aeron.Connect(aeron.Context{
		AeronDir: md.Dir,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	meta, err := client.CncMetadata()
	require.NoError(t, err)
	errorLog, err := client.ErrorLogBuffer()
	require.NoError(t, err)
	require.Len(t, errorLog, int(meta.ErrorLogBufferLength))
	require.False(t, cnc.ErrorLogExists(errorLog))
}

func TestClientTwoClientsSameDriver(t *testing.T) {
	md := harness.Start(t)

	clientA, err := aeron.Connect(aeron.Context{AeronDir: md.Dir})
	require.NoError(t, err)
	clientB, err := aeron.Connect(aeron.Context{AeronDir: md.Dir})
	require.NoError(t, err)

	clientIDA := clientA.ClientId()
	clientIDB := clientB.ClientId()
	require.GreaterOrEqual(t, clientIDA, int64(0))
	require.GreaterOrEqual(t, clientIDB, int64(0))
	require.NotEqual(t, clientIDA, clientIDB)

	for range 5 {
		clientA.DoWork()
		clientB.DoWork()
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}

	md.AssertProcessAlive(t)

	require.NoError(t, clientA.Close())
	require.NoError(t, clientB.Close())
}

func TestTwoClientsConcurrentRingOffers(t *testing.T) {
	md, attach := harness.StartAttachedCnc(t)
	// Harness attach and each Connect client map the same cnc.dat; ToDriver is one MPSC ring.
	toDriver := attach.Rings.ToDriver

	clientA, err := aeron.Connect(aeron.Context{AeronDir: md.Dir})
	require.NoError(t, err)
	clientB, err := aeron.Connect(aeron.Context{AeronDir: md.Dir})
	require.NoError(t, err)

	clientIDA := clientA.ClientId()
	clientIDB := clientB.ClientId()
	require.GreaterOrEqual(t, clientIDA, int64(0))
	require.GreaterOrEqual(t, clientIDB, int64(0))
	require.NotEqual(t, clientIDA, clientIDB)

	t.Cleanup(func() {
		require.NoError(t, clientA.Close())
		require.NoError(t, clientB.Close())
	})

	consumerBefore := toDriver.ConsumerPosition()
	producerBefore := toDriver.ProducerPosition()

	const offersPerClient = 32
	const actionsPerType = offersPerClient / 2
	keepaliveRecordLength := int64(conductor.CorrelatedRingRecordLength())
	sessionIDRecordLength := int64(record.AlignRecordLength(
		conductor.GetNextAvailableSessionIDBodyLength + ringbuffer.HeaderLength,
	))
	minProducerDelta := int64(2*actionsPerType)*keepaliveRecordLength +
		int64(2*actionsPerType)*sessionIDRecordLength

	var wg sync.WaitGroup
	wg.Add(2)
	offerErrs := make(chan error, 2)

	offer := func(clientID, correlationBase int64) {
		defer wg.Done()

		proxy := conductor.NewDriverProxy(toDriver, clientID)
		for i := range offersPerClient {
			var offerErr error
			if i%2 == 0 {
				offerErr = proxy.ClientKeepalive()
			} else {
				offerErr = proxy.GetNextAvailableSessionID(correlationBase+int64(i), 100)
			}
			if offerErr != nil {
				offerErrs <- offerErr
				return
			}
		}
	}

	go offer(clientIDA, 10_000)
	go offer(clientIDB, 20_000)
	wg.Wait()
	for len(offerErrs) > 0 {
		require.NoError(t, <-offerErrs)
	}

	producerAfter := toDriver.ProducerPosition()
	require.GreaterOrEqual(t, producerAfter-producerBefore, minProducerDelta)

	harness.WaitForToDriverRingDrained(t, toDriver)
	require.Greater(t, toDriver.ConsumerPosition(), consumerBefore)
	require.Equal(t, toDriver.ProducerPosition(), toDriver.ConsumerPosition())

	md.AssertProcessAlive(t)
}
