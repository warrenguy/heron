//go:build rapid

package aeron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

var closedGuardAfterPollPendingKinds = []conductor.PendingKind{
	conductor.PendingKindNextSessionID,
	conductor.PendingKindAddPublication,
	conductor.PendingKindAddSubscription,
	conductor.PendingKindAddExclusivePublication,
}

func drawClosedGuardAfterPollPendingKind(rt *rapid.T) conductor.PendingKind {
	rt.Helper()

	kindIdx := rapid.IntRange(0, len(closedGuardAfterPollPendingKinds)-1).Draw(rt, "pendingKind")
	return closedGuardAfterPollPendingKinds[kindIdx]
}

func setupPreMarkedClosedGuardPending(
	t *testing.T,
	rt *rapid.T,
	kind conductor.PendingKind,
) (*Client, int64, func(*Client) int) {
	t.Helper()
	rt.Helper()

	var nanoNs int64
	driverTimeout := time.Second
	idleSleep := time.Millisecond

	switch kind {
	case conductor.PendingKindNextSessionID:
		client, fixture := newClientAwaitFixture(
			t,
			func() int64 { return nanoNs },
			driverTimeout,
			idleSleep,
		)
		correlationID := client.NextCorrelationID()
		deadlineNs := client.nanoClock() + driverTimeout.Nanoseconds()
		wantSessionID := rapid.Int32().Draw(rt, "wantSessionID")
		require.NoError(
			rt,
			fixture.Conductor.RegisterPendingWithDeadline(kind, correlationID, deadlineNs),
		)
		require.NoError(
			rt,
			fixture.Conductor.CompletePendingForTest(correlationID, wantSessionID, nil),
		)
		return client, correlationID, nil

	case conductor.PendingKindAddPublication, conductor.PendingKindAddExclusivePublication:
		testCase := clientAddPublicationKindCaseFor(kind)
		dir, logPath := testCase.setupLog(t)
		client, fixture := newClientAddFixture(
			t,
			func() int64 { return nanoNs },
			driverTimeout,
			idleSleep,
			dir,
		)
		fixture.Conductor.SetCountersReaderForTest(countersReaderForClientAddTest(t))
		correlationID := client.NextCorrelationID()
		deadlineNs := client.nanoClock() + driverTimeout.Nanoseconds()
		require.NoError(
			rt,
			fixture.Conductor.RegisterPendingAddWithDeadline(
				kind,
				correlationID,
				clientAddStreamID,
				clientAddChannel,
				deadlineNs,
			),
		)
		testCase.signalReady(
			fixture.Conductor,
			clientAddPublicationReadyEvent(correlationID, logPath),
		)
		require.Equal(rt, 1, client.PublicationCountForTest())
		return client, correlationID, (*Client).PublicationCountForTest

	case conductor.PendingKindAddSubscription:
		dir := t.TempDir()
		client, fixture := newClientAddFixture(
			t,
			func() int64 { return nanoNs },
			driverTimeout,
			idleSleep,
			dir,
		)
		correlationID := client.NextCorrelationID()
		deadlineNs := client.nanoClock() + driverTimeout.Nanoseconds()
		require.NoError(
			rt,
			fixture.Conductor.RegisterPendingAddWithDeadline(
				kind,
				correlationID,
				clientAddStreamID,
				clientAddChannel,
				deadlineNs,
			),
		)
		conductor.SubscriptionReadyForTest(
			fixture.Conductor,
			clientAddSubscriptionReadyEvent(correlationID),
		)
		require.Equal(rt, 1, client.SubscriptionCountForTest())
		return client, correlationID, (*Client).SubscriptionCountForTest

	default:
		rt.Fatalf("unsupported pending kind: %v", kind)
		return nil, 0, nil
	}
}

// wantGuardClosedBeforeOfferErr mirrors guardClosedBeforeOffer in client_add.go.
// mappedIsNil reflects client.mapped == nil; test fixtures from
// NewClientForTestWithDriverProxy leave mapped unset.
func wantGuardClosedBeforeOfferErr(
	closed bool,
	cncMappedAtOfferStart bool,
	mappedIsNil bool,
	proxyMatch bool,
) error {
	if closed || (cncMappedAtOfferStart && mappedIsNil) {
		return ErrClientClosed
	}
	if !proxyMatch {
		return ErrClientNotReady
	}
	return nil
}

func TestClient_awaitPendingPoll_closedGuardAfterPollPending_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		kind := drawClosedGuardAfterPollPendingKind(rt)
		client, correlationID, countFn := setupPreMarkedClosedGuardPending(t, rt, kind)

		_, completed, err := client.AwaitPendingPollOneTickForTest(correlationID, true, func() {
			client.PublishClosedForTest()
		})
		require.True(rt, completed)
		require.ErrorIs(rt, err, ErrClientClosed)
		require.EqualError(rt, err, ErrClientClosed.Error())
		if countFn != nil {
			require.Equal(rt, 0, countFn(client))
		}
	})
}

func TestClient_guardClosedBeforeOffer_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		closed := rapid.Bool().Draw(rt, "closed")
		cncMappedAtOfferStart := rapid.Bool().Draw(rt, "cncMappedAtOfferStart")
		proxyMatch := rapid.Bool().Draw(rt, "proxyMatch")

		var nanoNs int64
		client, fixture := newClientAddFixture(
			t,
			func() int64 { return nanoNs },
			time.Second,
			time.Millisecond,
			t.TempDir(),
		)
		if closed {
			client.PublishClosedForTest()
		}

		driverProxy := fixture.DriverProxy
		if !proxyMatch {
			driverProxy = conductor.NewDriverProxy(fixture.Ring, testutil.TestClientID+1)
		}

		err := client.GuardClosedBeforeOfferForTest(driverProxy, cncMappedAtOfferStart)
		wantErr := wantGuardClosedBeforeOfferErr(
			closed,
			cncMappedAtOfferStart,
			client.mapped == nil,
			proxyMatch,
		)
		if wantErr == nil {
			require.NoError(rt, err)
			return
		}
		require.ErrorIs(rt, err, wantErr)
		switch wantErr {
		case ErrClientClosed:
			require.EqualError(rt, err, ErrClientClosed.Error())
		case ErrClientNotReady:
			require.EqualError(rt, err, ErrClientNotReady.Error())
		}
	})
}
