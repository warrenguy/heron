//go:build integration

package harness

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/cnc/cnctest"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/broadcast"
	"github.com/warrenguy/heron/agrona/ringbuffer"
)

const (
	soakDuration              = 2 * time.Second
	soakPollInterval          = 250 * time.Millisecond
	defaultPollInterval       = 50 * time.Millisecond
	heartbeatFreshMaxAgeMs    = 2_000
	heartbeatFreshPollTimeout = 4 * time.Second
	// CommandPollDeadline bounds ring-drain and short DriverTimeout integration polls.
	CommandPollDeadline = 2 * time.Second
)

func commandPollDeadlineOr(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return CommandPollDeadline
	}
	return timeout
}

type cncAttach struct {
	Mapped *cnc.MappedCncFile
	Rings  *cnc.Rings
}

func attachCncPath(t testing.TB, cncPath string) *cncAttach {
	t.Helper()

	mapped, err := cnc.MapCncFile(cncPath)
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

func openCnc(t testing.TB, md *MediaDriver) *cncAttach {
	t.Helper()

	return attachCncPath(t, md.CncPath())
}

func StartAttachedCnc(t testing.TB) (*MediaDriver, *cncAttach) {
	t.Helper()

	md := Start(t)
	return md, openCnc(t, md)
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

	require.NoError(t, md.aliveError())
	require.Equal(t, attach.Rings.Meta.PID, int64(md.pid))
}

func consumerHeartbeatAgeMs(toDriver *ringbuffer.ManyToOneRingBuffer) (ageMs int64, ok bool) {
	heartbeatMs := toDriver.ConsumerHeartbeatTime()
	if heartbeatMs <= 0 {
		return 0, false
	}
	ageMs = max(time.Now().UnixMilli()-heartbeatMs, 0)
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

// AssertConsumerHeartbeatFresh polls until the driver consumer heartbeat is fresh.
func AssertConsumerHeartbeatFresh(t testing.TB, attach *cncAttach) {
	t.Helper()

	pollUntil(
		t,
		heartbeatFreshPollTimeout,
		defaultPollInterval,
		func() bool {
			return consumerHeartbeatWithin(
				attach.Rings.ToDriver,
				heartbeatFreshMaxAgeMs,
				false,
			)
		},
		"consumer heartbeat not fresh",
	)
}

// WaitForToDriverRingDrainedWithClients pumps each client until the ring drains.
func WaitForToDriverRingDrainedWithClients(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	timeout time.Duration,
	clients ...*aeron.Client,
) {
	t.Helper()

	pollUntil(
		t,
		commandPollDeadlineOr(timeout),
		aeron.DefaultIdleSleepDuration,
		func() bool {
			for _, client := range clients {
				client.DoWork()
			}
			return ring.ConsumerPosition() == ring.ProducerPosition()
		},
		"to-driver ring not drained",
	)
}

func WaitForToDriverRingDrained(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	timeout time.Duration,
) {
	t.Helper()

	pollUntil(
		t,
		commandPollDeadlineOr(timeout),
		defaultPollInterval,
		func() bool {
			return ring.ConsumerPosition() == ring.ProducerPosition()
		},
		"to-driver ring not drained",
	)
}

// DrainToClients consumes broadcast records the shared driver retained from earlier
// package tests, validating each. A positive limit caps drained records so a continuous
// broadcast surfaces as the returned count exceeding the caller's bound instead of an
// unbounded loop; limit <= 0 drains until empty. Returns the count drained.
func DrainToClients(
	t testing.TB,
	ring *broadcast.BroadcastReceiver,
	limit int,
) int {
	t.Helper()

	drained := 0
	for (limit <= 0 || drained < limit) && ring.ReceiveNext() {
		require.True(t, ring.Validate())
		drained++
	}
	return drained
}

// QuiescentToDriverSnapshot drains ToDriver, pumping each client's DoWork when given.
func QuiescentToDriverSnapshot(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	clients ...*aeron.Client,
) (consumerBefore, producerBefore int64) {
	return quiescentToDriverSnapshot(t, ring, 0, clients...)
}

// QuiescentToDriverSnapshotTimeout is QuiescentToDriverSnapshot with an explicit drain budget.
func QuiescentToDriverSnapshotTimeout(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	timeout time.Duration,
	clients ...*aeron.Client,
) (consumerBefore, producerBefore int64) {
	return quiescentToDriverSnapshot(t, ring, timeout, clients...)
}

func quiescentToDriverSnapshot(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	timeout time.Duration,
	clients ...*aeron.Client,
) (consumerBefore, producerBefore int64) {
	t.Helper()

	deadline := commandPollDeadlineOr(timeout)
	if len(clients) == 0 {
		WaitForToDriverRingDrained(t, ring, deadline)
	} else {
		WaitForToDriverRingDrainedWithClients(t, ring, deadline, clients...)
	}

	consumerBefore = ring.ConsumerPosition()
	producerBefore = ring.ProducerPosition()
	requireQuiescentToDriverSnapshot(t, ring, consumerBefore, producerBefore)
	return consumerBefore, producerBefore
}

func requireQuiescentSnapshotCoherence(
	t testing.TB,
	consumerBefore, producerBefore int64,
) {
	t.Helper()

	require.Equal(
		t,
		producerBefore,
		consumerBefore,
		"to-driver ring snapshot must be quiescent (consumerBefore == producerBefore)",
	)
}

func requireQuiescentToDriverSnapshot(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	consumerBefore, producerBefore int64,
) {
	t.Helper()

	requireQuiescentSnapshotCoherence(t, consumerBefore, producerBefore)
	require.Equal(
		t,
		consumerBefore,
		ring.ConsumerPosition(),
		"to-driver ring consumer position changed before observe",
	)
	require.Equal(
		t,
		producerBefore,
		ring.ProducerPosition(),
		"to-driver ring producer position changed before observe",
	)
}

// AssertClientCloseOfferedOnRingAfterClose requires the caller to drain ToDriver
// and snapshot consumerBefore and producerBefore on a quiescent ring.
//
// Backlog at snapshot makes consumed-path observation reject multi-record driver drains.
func AssertClientCloseOfferedOnRingAfterClose(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID int64,
	consumerBefore, producerBefore int64,
	closeFn func() error,
) {
	t.Helper()

	requireQuiescentToDriverSnapshot(t, ring, consumerBefore, producerBefore)

	require.NoError(t, closeFn())
	_ = assertClientCloseOfferedOnRing(
		t,
		ring,
		clientID,
		consumerBefore,
		producerBefore,
	)
}

func assertClientCloseOfferedOnRing(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	clientID int64,
	consumerBefore, producerBefore int64,
) int64 {
	t.Helper()

	want := conductor.CorrelatedMessage{
		ClientID:      clientID,
		CorrelationID: conductor.NullCorrelationID,
	}

	provenClientClosePosition := int64(-1)
	decoded, found, pollErr := pollRingObservation(
		CommandPollDeadline,
		defaultPollInterval,
		func() (conductor.CorrelatedMessage, bool, error) {
			return observeClientClose(
				ring,
				clientID,
				consumerBefore,
				producerBefore,
				&provenClientClosePosition,
			)
		},
	)
	requireObservedRingMessage(
		t,
		decoded,
		found,
		pollErr,
		want,
		"to-driver ring scan failed after client close poll timeout",
		"CLIENT_CLOSE not observed on to-driver ring for client %d",
		clientID,
	)
	return provenClientClosePosition
}

// assertAddPublicationOfferedOnRing requires the caller to drain ToDriver
// and snapshot consumerBefore and producerBefore on a quiescent ring.
//
// Backlog at snapshot makes consumed-path observation reject multi-record driver drains.
func assertAddPublicationOfferedOnRing(
	t testing.TB,
	ring *ringbuffer.ManyToOneRingBuffer,
	want conductor.PublicationMessage,
	consumerBefore, producerBefore int64,
) {
	t.Helper()

	requireQuiescentSnapshotCoherence(t, consumerBefore, producerBefore)

	provenAddPublicationPosition := int64(-1)
	decoded, found, pollErr := pollRingObservation(
		CommandPollDeadline,
		defaultPollInterval,
		func() (conductor.PublicationMessage, bool, error) {
			return observeAddPublication(
				ring,
				want,
				consumerBefore,
				producerBefore,
				&provenAddPublicationPosition,
			)
		},
	)
	requireObservedRingMessage(
		t,
		decoded,
		found,
		pollErr,
		want,
		"to-driver ring scan failed after add publication poll timeout",
		"ADD_PUBLICATION not observed on to-driver ring for correlation %d",
		want.CorrelationID,
	)
}

func pollRingObservation[T any](
	timeout, interval time.Duration,
	observe func() (T, bool, error),
) (decoded T, found bool, pollErr error) {
	found = pollUntilBool(timeout, interval, func() bool {
		var ok bool
		decoded, ok, pollErr = observe()
		if pollErr != nil {
			return false
		}
		return ok
	})
	return decoded, found, pollErr
}

func requireObservedRingMessage[T comparable](
	t testing.TB,
	decoded T,
	found bool,
	pollErr error,
	want T,
	pollErrMsg, notFoundFmt string,
	notFoundArgs ...any,
) {
	t.Helper()

	if pollErr != nil {
		require.NoError(t, pollErr, pollErrMsg)
	}
	require.Truef(t, found, notFoundFmt, notFoundArgs...)
	require.Equal(t, want, decoded)
}

func pollUntilBool(
	timeout, interval time.Duration,
	ok func() bool,
) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return true
		}
		time.Sleep(interval)
	}
	return ok()
}

func pollUntil(
	t testing.TB,
	timeout, interval time.Duration,
	ok func() bool,
	failMsg string,
) {
	t.Helper()

	if pollUntilBool(timeout, interval, ok) {
		return
	}
	require.FailNow(t, fmt.Sprintf("%s within %s", failMsg, timeout))
}

// PollUntilFileWhen polls until ready accepts trimmed file content.
func PollUntilFileWhen(
	path string,
	timeout time.Duration,
	ready func([]byte) bool,
) ([]byte, error) {
	var data []byte
	ok := pollUntilBool(timeout, defaultPollInterval, func() bool {
		raw, err := os.ReadFile(path)
		if err != nil {
			return false
		}

		data = bytes.TrimSpace(raw)
		return ready(data)
	})
	if !ok {
		return nil, fmt.Errorf("%s not ready within %s", path, timeout)
	}

	return data, nil
}

// PollUntilFile polls until path exists and returns non-empty trimmed content.
func PollUntilFile(path string, timeout time.Duration) ([]byte, error) {
	return PollUntilFileWhen(path, timeout, func(data []byte) bool {
		return len(data) > 0
	})
}

// RunSoak runs a short post-assert DoWork soak for integration acceptance tests.
func RunSoak(t testing.TB, duration, interval time.Duration, tick func()) {
	t.Helper()

	deadline := time.Now().Add(duration)
	tick()
	for {
		time.Sleep(interval)
		if !time.Now().Before(deadline) {
			return
		}
		tick()
	}
}
