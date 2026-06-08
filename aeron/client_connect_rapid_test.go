package aeron_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func TestWaitForDriverHeartbeat_driverShutdown_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		timeoutMs := rapid.IntRange(1, 60_000).Draw(t, "timeoutMs")
		startMs := int64(rapid.IntRange(1, 1_000_000).Draw(t, "startMs"))

		ring := testutil.NewTestRing(t)
		ring.SetConsumerHeartbeatTime(conductor.DriverShutdownHeartbeat)

		err := aeron.WaitForDriverHeartbeatForTest(
			ring,
			time.Duration(timeoutMs)*time.Millisecond,
			newAdvancingEpochClock(startMs, 0),
		)

		requireDriverTimeoutCause(t, err, conductor.ErrDriverShutdown)
	})
}

func TestWaitForDriverHeartbeat_freshHeartbeat_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		timeoutMs := rapid.IntRange(1, 60_000).Draw(t, "timeoutMs")
		heartbeat := int64(rapid.IntRange(1, 1_000_000).Draw(t, "heartbeat"))
		ageMs := int64(rapid.IntRange(0, timeoutMs).Draw(t, "ageMs"))
		nowMs := heartbeat + ageMs

		ring := testutil.NewTestRing(t)
		ring.SetConsumerHeartbeatTime(heartbeat)

		err := aeron.WaitForDriverHeartbeatForTest(
			ring,
			time.Duration(timeoutMs)*time.Millisecond,
			newAdvancingEpochClock(nowMs, 0),
		)
		require.NoError(t, err)
	})
}

func TestWaitForDriverHeartbeat_staleAtConnect_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		timeoutMs := rapid.IntRange(1, 60_000).Draw(t, "timeoutMs")
		heartbeat := int64(rapid.IntRange(1, 1_000_000).Draw(t, "heartbeat"))
		extraAge := rapid.IntRange(1, 60_000).Draw(t, "extraAge")
		ageMs := int64(timeoutMs) + int64(extraAge)
		nowMs := heartbeat + ageMs

		ring := testutil.NewTestRing(t)
		ring.SetConsumerHeartbeatTime(heartbeat)

		err := aeron.WaitForDriverHeartbeatForTest(
			ring,
			time.Duration(timeoutMs)*time.Millisecond,
			newAdvancingEpochClock(nowMs, 0),
		)
		requireDriverTimeoutCause(t, err, aeron.ErrDriverHeartbeatStaleAtConnect)
	})
}

func TestWaitForDriverHeartbeat_notReady_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		timeoutMs := rapid.IntRange(1, 60_000).Draw(t, "timeoutMs")
		startMs := int64(rapid.IntRange(1, 1_000_000).Draw(t, "startMs"))
		stepMs := int64(timeoutMs) + int64(rapid.IntRange(1, 10_000).Draw(t, "pastDeadline"))

		ring := testutil.NewTestRing(t)
		if !rapid.Bool().Draw(t, "useUnsetHeartbeat") {
			ring.SetConsumerHeartbeatTime(
				int64(rapid.IntRange(-1_000, -2).Draw(t, "heartbeat")),
			)
		}

		err := aeron.WaitForDriverHeartbeatForTest(
			ring,
			time.Duration(timeoutMs)*time.Millisecond,
			newAdvancingEpochClock(startMs, stepMs),
		)
		requireDriverTimeoutCause(t, err, aeron.ErrDriverHeartbeatNotReady)
	})
}
