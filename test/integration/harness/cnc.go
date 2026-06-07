//go:build integration

package harness

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/cnc/cnctest"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const (
	soakDuration              = 15 * time.Second
	soakPollInterval          = 500 * time.Millisecond
	defaultPollInterval       = 50 * time.Millisecond
	heartbeatFreshMaxAgeMs    = 2_000
	heartbeatFreshPollTimeout = 4 * time.Second
	commandPollDeadline       = 2 * time.Second
)

type cncAttach struct {
	Mapped *cnc.MappedCncFile
	Rings  *cnc.Rings
}

func openCnc(t testing.TB, md *MediaDriver) *cncAttach {
	t.Helper()

	mapped, err := cnc.MapCncFile(md.CncPath())
	require.NoError(t, err, "map cnc file")

	t.Cleanup(func() {
		if err := mapped.Close(); err != nil {
			t.Errorf("close mapped cnc file: %v", err)
		}
	})

	rings, err := cnc.WireRings(mapped)
	require.NoError(t, err, "wire rings")

	return &cncAttach{Mapped: mapped, Rings: rings}
}

func StartAttachedCnc(t testing.TB) (*MediaDriver, *cncAttach) {
	t.Helper()

	md := Start(t)
	return md, openCnc(t, md)
}

func freshToClientsReceiver(t testing.TB, attach *cncAttach) *broadcast.BroadcastReceiver {
	t.Helper()

	receiver, err := broadcast.NewBroadcastReceiver(attach.Mapped.ToClientsAtomic())
	require.NoError(t, err)
	return receiver
}

func assertMetadataMatchesMmap(t testing.TB, md *MediaDriver, attach *cncAttach) {
	t.Helper()

	fileMeta, err := cnc.ReadMetadataFile(md.CncPath())
	require.NoError(t, err, "read metadata file")

	mmapMeta, err := cnc.ParseMetadata(attach.Mapped.Bytes()[:cnc.MetaDataLength])
	require.NoError(t, err, "parse mmap metadata")

	require.Equal(t, fileMeta, mmapMeta)
	require.Equal(t, attach.Mapped.Metadata(), attach.Rings.Meta)
}

func assertRingConstruct(t testing.TB, attach *cncAttach) {
	t.Helper()

	require.Positive(t, attach.Rings.Meta.PID)
	cnctest.AssertWiredRingsReady(t, attach.Mapped, attach.Rings)
}

func assertDriverAliveAndPIDMatches(t testing.TB, md *MediaDriver, attach *cncAttach) {
	t.Helper()

	pid, err := md.ProcessPID()
	require.NoError(t, err)
	require.Equal(t, attach.Rings.Meta.PID, int64(pid))
}

func consumerHeartbeatAgeMs(toDriver *ringbuffer.ManyToOneRingBuffer) (ageMs int64, ok bool) {
	heartbeatMs := toDriver.ConsumerHeartbeatTime()
	if heartbeatMs <= 0 {
		return 0, false
	}
	ageMs = time.Now().UnixMilli() - heartbeatMs
	if ageMs < 0 {
		ageMs = 0
	}
	return ageMs, true
}

func consumerHeartbeatWithin(
	toDriver *ringbuffer.ManyToOneRingBuffer,
	maxAgeMs int64,
	inclusive bool,
) bool {
	ageMs, ok := consumerHeartbeatAgeMs(toDriver)
	if !ok {
		return false
	}
	if inclusive {
		return ageMs <= maxAgeMs
	}
	return ageMs < maxAgeMs
}

func assertConsumerHeartbeatFresh(t testing.TB, attach *cncAttach) {
	t.Helper()

	toDriver := attach.Rings.ToDriver

	pollUntil(
		t,
		heartbeatFreshPollTimeout,
		defaultPollInterval,
		func() bool {
			return consumerHeartbeatWithin(toDriver, heartbeatFreshMaxAgeMs, false)
		},
		"consumer heartbeat not fresh",
	)
}

func requireDecodeCorrelationAndInt32(
	t testing.TB,
	toClients *broadcast.BroadcastReceiver,
) conductor.CorrelationAndInt32 {
	t.Helper()

	decoded, err := conductor.DecodeCorrelationAndInt32(
		toClients.Buffer(),
		toClients.Offset(),
	)
	require.NoError(t, err)
	return decoded
}

func waitForNextSessionIDEvent(
	t testing.TB,
	toClients *broadcast.BroadcastReceiver,
	correlationID int64,
) conductor.CorrelationAndInt32 {
	t.Helper()

	var event conductor.CorrelationAndInt32
	pollUntil(
		t,
		commandPollDeadline,
		defaultPollInterval,
		func() bool {
			if !toClients.ReceiveNext() {
				return false
			}
			if !toClients.Validate() {
				return false
			}

			switch toClients.TypeID() {
			case conductor.OnNextAvailableSessionID:
				require.Equal(t, conductor.CorrelationAndInt32BodyLength, toClients.Length())

				event = requireDecodeCorrelationAndInt32(t, toClients)
				require.Equal(t, correlationID, event.CorrelationID)
				return true
			case conductor.OnError:
				require.GreaterOrEqual(
					t,
					toClients.Length(),
					conductor.CorrelationAndInt32BodyLength,
					"ON_ERROR correlation %d",
					correlationID,
				)
				onError := requireDecodeCorrelationAndInt32(t, toClients)
				require.NotEqual(
					t,
					correlationID,
					onError.CorrelationID,
					"ON_ERROR error code %d",
					onError.Int32Value,
				)
				return false
			default:
				require.FailNow(
					t,
					fmt.Sprintf(
						"unexpected broadcast type %d for correlation %d (length %d)",
						toClients.TypeID(),
						correlationID,
						toClients.Length(),
					),
				)
				return false
			}
		},
		"ON_NEXT_AVAILABLE_SESSION_ID not received",
	)
	return event
}

func pollUntil(
	t testing.TB,
	timeout, interval time.Duration,
	ok func() bool,
	failMsg string,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(interval)
	}
	if ok() {
		return
	}
	require.FailNow(t, fmt.Sprintf("%s within %s", failMsg, timeout))
}

func runSoak(t testing.TB, duration, interval time.Duration, tick func()) {
	t.Helper()

	deadline := time.Now().Add(duration)
	tick()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		if !time.Now().Before(deadline) {
			return
		}
		tick()
	}
}
