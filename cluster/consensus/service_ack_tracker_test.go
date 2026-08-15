package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServiceAckTracker_empty(t *testing.T) {
	t.Parallel()

	tr := newServiceAckTracker(2)
	ok, err := tr.hasReached(100, 0)
	require.False(t, ok)
	require.NoError(t, err)
}

func TestServiceAckTracker_walkOrder(t *testing.T) {
	t.Parallel()

	t.Run("empty lower stops walk before higher mismatch", func(t *testing.T) {
		t.Parallel()
		tr := newServiceAckTracker(2)
		// Queue 0 empty; queue 1 first entry mismatches on ackID. Walk stops at
		// queue 0 and does not examine queue 1.
		require.True(t, tr.offer(1, serviceAck{ackID: 99, logPosition: 100}))
		ok, err := tr.hasReached(100, 0)
		require.False(t, ok)
		require.NoError(t, err)
		require.Equal(t, 0, tr.queueDepth(0))
		require.Equal(t, 1, tr.queueDepth(1))
	})

	t.Run("mismatch lower with empty higher returns error", func(t *testing.T) {
		t.Parallel()
		tr := newServiceAckTracker(2)
		// Queue 0 first entry mismatched; queue 1 empty.
		require.True(t, tr.offer(0, serviceAck{ackID: 99, logPosition: 100}))
		ok, err := tr.hasReached(100, 0)
		require.False(t, ok)
		require.ErrorIs(t, err, ErrServiceAckOutOfSequence)
	})

	t.Run("match lower then mismatch higher returns error", func(t *testing.T) {
		t.Parallel()
		tr := newServiceAckTracker(2)
		// Queue 0 matches; queue 1 mismatches. Walk continues past 0 then fails
		// closed on 1.
		require.True(t, tr.offer(0, serviceAck{ackID: 0, logPosition: 100}))
		require.True(t, tr.offer(1, serviceAck{ackID: 1, logPosition: 100}))
		ok, err := tr.hasReached(100, 0)
		require.False(t, ok)
		require.ErrorIs(t, err, ErrServiceAckOutOfSequence)
		require.Equal(t, 1, tr.queueDepth(0))
		require.Equal(t, 1, tr.queueDepth(1))
	})
}

func TestServiceAckTracker_matchCheckOnly(t *testing.T) {
	t.Parallel()

	tr := newServiceAckTracker(2)
	ack := serviceAck{ackID: 0, logPosition: 42, relevantID: 7}
	for _, id := range []int32{0, 1} {
		require.True(t, tr.offer(id, ack))
	}

	// Twice: same result. Still check-only.
	for range 2 {
		ok, err := tr.hasReached(42, 0)
		require.True(t, ok)
		require.NoError(t, err)
		require.Equal(t, 1, tr.queueDepth(0))
		require.Equal(t, 1, tr.queueDepth(1))
	}
}

func TestServiceAckTracker_mismatchFailClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ack  serviceAck
	}{
		{
			name: "wrong ackID",
			ack:  serviceAck{ackID: 1, logPosition: 100},
		},
		{
			name: "wrong logPosition",
			ack:  serviceAck{ackID: 0, logPosition: 99},
		},
		{
			name: "both wrong",
			ack:  serviceAck{ackID: 5, logPosition: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr := newServiceAckTracker(1)
			require.True(t, tr.offer(0, tt.ack))
			ok, err := tr.hasReached(100, 0)
			require.False(t, ok)
			require.ErrorIs(t, err, ErrServiceAckOutOfSequence)
			require.Equal(t, 1, tr.queueDepth(0))
		})
	}
}

func TestServiceAckTracker_multiService(t *testing.T) {
	t.Parallel()

	t.Run("serviceCount 2 match only when every first entry agrees", func(t *testing.T) {
		t.Parallel()
		tr := newServiceAckTracker(2)
		require.True(t, tr.offer(0, serviceAck{ackID: 0, logPosition: 10}))
		// Service 1 lagging: no offer yet.
		ok, err := tr.hasReached(10, 0)
		require.False(t, ok)
		require.NoError(t, err)

		require.True(t, tr.offer(1, serviceAck{ackID: 0, logPosition: 10}))
		ok, err = tr.hasReached(10, 0)
		require.True(t, ok)
		require.NoError(t, err)
	})

	t.Run("serviceCount 3 lagging service holds false", func(t *testing.T) {
		t.Parallel()
		tr := newServiceAckTracker(3)
		first := serviceAck{ackID: 0, logPosition: 50}
		second := serviceAck{ackID: 1, logPosition: 60}
		// Service 2 still empty while 0 and 1 queue a second entry.
		for _, id := range []int32{0, 1} {
			require.True(t, tr.offer(id, first))
			require.True(t, tr.offer(id, second))
		}

		ok, err := tr.hasReached(50, 0)
		require.False(t, ok)
		require.NoError(t, err)
		require.Equal(t, 2, tr.queueDepth(0))
		require.Equal(t, 2, tr.queueDepth(1))
		require.Equal(t, 0, tr.queueDepth(2))

		require.True(t, tr.offer(2, first))
		ok, err = tr.hasReached(50, 0)
		require.True(t, ok)
		require.NoError(t, err)
	})

	t.Run("interleaved offers", func(t *testing.T) {
		t.Parallel()
		tr := newServiceAckTracker(2)
		require.True(t, tr.offer(1, serviceAck{ackID: 0, logPosition: 1}))
		require.True(t, tr.offer(0, serviceAck{ackID: 0, logPosition: 1}))
		ok, err := tr.hasReached(1, 0)
		require.True(t, ok)
		require.NoError(t, err)
	})
}

func TestServiceAckTracker_serviceCountZero(t *testing.T) {
	t.Parallel()

	tr := newServiceAckTracker(0)
	require.Empty(t, tr.queues)
	ok, err := tr.hasReached(0, 0)
	require.True(t, ok)
	require.NoError(t, err)

	// removeHead is a no-op with zero queues.
	tr.removeHead()
	ok, err = tr.hasReached(99, 7)
	require.True(t, ok)
	require.NoError(t, err)
}

func TestServiceAckTracker_offerBounds(t *testing.T) {
	t.Parallel()

	tr := newServiceAckTracker(2)
	ack := serviceAck{ackID: 0, logPosition: 1}

	require.False(t, tr.offer(-1, ack))
	require.False(t, tr.offer(2, ack))
	require.Equal(t, 0, tr.queueDepth(0))
	require.Equal(t, 0, tr.queueDepth(1))

	// In-range offer still works after refused bounds.
	require.True(t, tr.offer(0, ack))
	require.Equal(t, 1, tr.queueDepth(0))
}

func TestServiceAckTracker_removeHeadFIFO(t *testing.T) {
	t.Parallel()

	tr := newServiceAckTracker(2)
	first := serviceAck{ackID: 0, logPosition: 10}
	second := serviceAck{ackID: 1, logPosition: 20}
	for _, id := range []int32{0, 1} {
		require.True(t, tr.offer(id, first))
		require.True(t, tr.offer(id, second))
	}

	ok, err := tr.hasReached(10, 0)
	require.True(t, ok)
	require.NoError(t, err)

	tr.removeHead()
	require.Equal(t, 1, tr.queueDepth(0))
	require.Equal(t, 1, tr.queueDepth(1))

	// FIFO: next first entries are id 1 at position 20.
	ok, err = tr.hasReached(20, 1)
	require.True(t, ok)
	require.NoError(t, err)
	ok, err = tr.hasReached(10, 0)
	require.False(t, ok)
	require.ErrorIs(t, err, ErrServiceAckOutOfSequence)
}

func TestServiceAckTracker_dropAt(t *testing.T) {
	t.Parallel()

	tr := newServiceAckTracker(2)
	require.True(t, tr.offer(0, serviceAck{ackID: 0, logPosition: 10}))
	require.True(t, tr.offer(0, serviceAck{ackID: 1, logPosition: 20}))
	require.True(t, tr.offer(1, serviceAck{ackID: 0, logPosition: 10}))

	var seen []int64
	tr.dropAt(10, func(_ int32, ack serviceAck) {
		seen = append(seen, ack.ackID)
	})
	require.Equal(t, []int64{0, 0}, seen)
	require.Equal(t, 1, tr.queueDepth(0))
	require.Equal(t, 0, tr.queueDepth(1))
	ok, err := tr.hasReached(20, 1)
	require.False(t, ok)
	require.NoError(t, err)
}

func TestServiceAckTracker_removeHeadEmptyTolerance(t *testing.T) {
	t.Parallel()

	tr := newServiceAckTracker(2)
	require.True(t, tr.offer(0, serviceAck{ackID: 0, logPosition: 1}))
	// Queue 1 empty; removeHead must tolerate.
	tr.removeHead()
	require.Equal(t, 0, tr.queueDepth(0))
	require.Equal(t, 0, tr.queueDepth(1))
}

func TestServiceAckTracker_afterOfferSequenceError(t *testing.T) {
	t.Parallel()

	t.Run("zero services returns nil", func(t *testing.T) {
		t.Parallel()
		tr := newServiceAckTracker(0)
		require.NoError(t, tr.afterOfferSequenceError(0))
	})

	t.Run("empty queue 0 returns nil", func(t *testing.T) {
		t.Parallel()
		tr := newServiceAckTracker(2)
		require.True(t, tr.offer(1, serviceAck{ackID: 99, logPosition: 10}))
		require.NoError(t, tr.afterOfferSequenceError(0))
	})

	t.Run("matched first entry returns nil", func(t *testing.T) {
		t.Parallel()
		tr := newServiceAckTracker(2)
		ack := serviceAck{ackID: 0, logPosition: 42}
		require.True(t, tr.offer(0, ack))
		require.True(t, tr.offer(1, ack))
		require.NoError(t, tr.afterOfferSequenceError(0))
	})

	t.Run("multi-step first entry match returns nil", func(t *testing.T) {
		t.Parallel()
		tr := newServiceAckTracker(1)
		require.True(t, tr.offer(0, serviceAck{ackID: 0, logPosition: 10}))
		require.True(t, tr.offer(0, serviceAck{ackID: 1, logPosition: 20}))
		// Asserts expected ack id 0 still matches while later serviceAckID steps sit
		// behind the first entry.
		require.NoError(t, tr.afterOfferSequenceError(0))
	})

	t.Run("mismatched first entry returns out of sequence", func(t *testing.T) {
		t.Parallel()
		tr := newServiceAckTracker(1)
		require.True(t, tr.offer(0, serviceAck{ackID: 1, logPosition: 10}))
		require.ErrorIs(t, tr.afterOfferSequenceError(0), ErrServiceAckOutOfSequence)
	})
}

func TestAgent_serviceAckHasReachedAndRemove(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{ServiceCount: 2})
	require.Equal(t, int64(0), a.ExpectedServiceAckID())
	require.NoError(t, a.ServiceAckFailure())

	const pos int64 = 100
	ack := serviceAck{ackID: 0, logPosition: pos}
	for _, id := range []int32{0, 1} {
		require.True(t, a.serviceAcks.offer(id, ack))
	}

	ok, err := a.hasReachedServiceAcks(pos)
	require.True(t, ok)
	require.NoError(t, err)
	// hasReachedServiceAcks is check-only: depths and expected id unchanged.
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
	require.Equal(t, 1, a.ServiceAckQueueDepth(1))
	require.Equal(t, int64(0), a.ExpectedServiceAckID())

	a.removeServiceAckHead()
	require.Equal(t, int64(1), a.ExpectedServiceAckID())
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.Equal(t, 0, a.ServiceAckQueueDepth(1))

	// Next check looks for id 1 against empty queues.
	ok, err = a.hasReachedServiceAcks(pos)
	require.False(t, ok)
	require.NoError(t, err)

	// Match expected id 1 via hasReachedServiceAcks only.
	next := serviceAck{ackID: 1, logPosition: pos}
	for _, id := range []int32{0, 1} {
		require.True(t, a.serviceAcks.offer(id, next))
	}
	ok, err = a.hasReachedServiceAcks(pos)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, int64(1), a.ExpectedServiceAckID())
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
	require.Equal(t, 1, a.ServiceAckQueueDepth(1))
	require.NoError(t, a.ServiceAckFailure())

	a.removeServiceAckHead()
	require.Equal(t, int64(2), a.ExpectedServiceAckID())

	// Stale id 1 after expected advances to 2: fail-closed on
	// hasReachedServiceAcks. The check must not mark ServiceAckFailure.
	stale := serviceAck{ackID: 1, logPosition: pos}
	for _, id := range []int32{0, 1} {
		require.True(t, a.serviceAcks.offer(id, stale))
	}
	ok, err = a.hasReachedServiceAcks(pos)
	require.False(t, ok)
	require.ErrorIs(t, err, ErrServiceAckOutOfSequence)
	require.Equal(t, int64(2), a.ExpectedServiceAckID())
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))
	require.Equal(t, 1, a.ServiceAckQueueDepth(1))
	require.NoError(t, a.ServiceAckFailure())
}

func TestAgent_serviceAckGetters(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{ServiceCount: 2})
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.Equal(t, 0, a.ServiceAckQueueDepth(1))
	require.Equal(t, 0, a.ServiceAckQueueDepth(-1))
	require.Equal(t, 0, a.ServiceAckQueueDepth(2))
	require.Equal(t, int64(0), a.ExpectedServiceAckID())
	require.NoError(t, a.ServiceAckFailure())

	require.True(t, a.serviceAcks.offer(0, serviceAck{ackID: 0, logPosition: 1}))
	require.True(t, a.serviceAcks.offer(0, serviceAck{ackID: 1, logPosition: 2}))
	require.True(t, a.serviceAcks.offer(1, serviceAck{ackID: 0, logPosition: 1}))
	require.Equal(t, 2, a.ServiceAckQueueDepth(0))
	require.Equal(t, 1, a.ServiceAckQueueDepth(1))
	require.Equal(t, 0, a.ServiceAckQueueDepth(99))
}

func TestNewAgent_serviceCountNegativePanics(t *testing.T) {
	t.Parallel()

	require.Panics(t, func() {
		_ = NewAgent(AgentConfig{ServiceCount: -1})
	})
}

func TestNewAgent_serviceCountZero(t *testing.T) {
	t.Parallel()

	a := NewAgent(AgentConfig{})
	require.Empty(t, a.serviceAcks.queues)
	ok, err := a.hasReachedServiceAcks(0)
	require.True(t, ok)
	require.NoError(t, err)
	a.removeServiceAckHead()
	require.Equal(t, int64(1), a.ExpectedServiceAckID())
}
