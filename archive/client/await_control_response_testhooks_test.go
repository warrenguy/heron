//go:build testhooks

package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

// TestOfferAndAwait_offerRetryDoesNotStarveResponseAwait proves the response await
// starts a fresh ControlResponseTimeout window after the offer succeeds: the offer consumes the
// entire offer-phase budget, yet the already-buffered response is still matched.
func TestOfferAndAwait_offerRetryDoesNotStarveResponseAwait(t *testing.T) {
	const messageTimeout = 100 * time.Millisecond

	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
	sub, termBuffer := setupControlResponsePollFixtures(t)
	poller := newControlResponsePollerForTest(sub)

	correlationID := int64(9100)
	ctx := ArchiveContext{
		ControlResponseTimeout:   messageTimeout,
		ControlOfferRetryTimeout: messageTimeout,
		IdleStrategy:             NewDefaultIdleStrategy(),
		AeronContext: aeron.Context{
			NanoClock: func() int64 { return nowNs.Load() },
		},
	}

	responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    correlationID,
		RelevantID:       777,
		Code:             gensbe.ControlResponseCode_OK,
		Version:          520,
	})
	writeUnfragmentedWireToTerm(t, termBuffer, responseWire)

	offerCalls := 0
	fields, err := offerAndAwaitArchiveControlRequest(
		harness.Client,
		ctx,
		poller.Poll,
		poller,
		func() (int64, bool) {
			offerCalls++
			nowNs.Add(messageTimeout.Nanoseconds())
			return 100, true
		},
		controlResponseTestControlSessionID,
		correlationID,
	)
	require.NoError(t, err)
	require.Equal(t, 1, offerCalls)
	require.Equal(t, int64(777), fields.RelevantID)
	require.Equal(t, correlationID, fields.CorrelationID)
}

func TestAwaitArchiveControlResponse_timeout(t *testing.T) {
	harness, nowNs := controlOfferRetryHarnessWithNanoClock(t, 5_000_000_000)
	sub, _ := setupControlResponsePollFixtures(t)
	poller := newControlResponsePollerForTest(sub)

	correlationID := int64(9999)
	ctx := ArchiveContext{
		ControlResponseTimeout:   time.Millisecond,
		ControlOfferRetryTimeout: time.Millisecond,
		IdleStrategy:             NewDefaultIdleStrategy(),
		AeronContext: aeron.Context{
			NanoClock: func() int64 { return nowNs.Load() },
		},
	}

	pollFn := func() int {
		nowNs.Add(int64(time.Millisecond))
		return poller.Poll()
	}

	_, err := AwaitArchiveControlResponse(
		harness.Client,
		ctx,
		pollFn,
		poller,
		controlResponseTestControlSessionID,
		correlationID,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout")
	require.Contains(t, err.Error(), "9999")
}
