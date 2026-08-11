//go:build testhooks

package service

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestQueueControlIngress_orderAndLimit(t *testing.T) {
	require.Equal(t, 10, defaultControlFragmentLimit)

	q := &QueueControlIngress{}
	for i := range 5 {
		q.Enqueue([]byte{byte(i)})
	}
	var seen []byte
	n := q.Poll(3, func(buf []byte, offset int) {
		require.Equal(t, 0, offset)
		require.Len(t, buf, 1)
		seen = append(seen, buf[0])
	})
	require.Equal(t, 3, n)
	require.Equal(t, []byte{0, 1, 2}, seen)

	n = q.Poll(10, func(buf []byte, offset int) {
		seen = append(seen, buf[0])
	})
	require.Equal(t, 2, n)
	require.Equal(t, []byte{0, 1, 2, 3, 4}, seen)
	require.Equal(t, 0, q.Poll(1, func([]byte, int) {
		t.Fatal("empty queue must not invoke onFragment")
	}))
}

func TestQueueControlIngress_nilAndNonPositiveGuards(t *testing.T) {
	var nilQ *QueueControlIngress
	require.NotPanics(t, func() {
		nilQ.Enqueue([]byte{1})
		require.Equal(t, 0, nilQ.Poll(1, func([]byte, int) {
			t.Fatal("nil queue must not invoke onFragment")
		}))
	})

	q := &QueueControlIngress{}
	for i := range 3 {
		q.Enqueue([]byte{byte(i)})
	}
	require.Equal(t, 0, q.Poll(1, nil))
	require.Equal(t, 0, q.Poll(0, func([]byte, int) {
		t.Fatal("zero limit must not invoke onFragment")
	}))
	require.Equal(t, 0, q.Poll(-1, func([]byte, int) {
		t.Fatal("negative limit must not invoke onFragment")
	}))
	require.Equal(t, 3, q.Poll(10, func([]byte, int) {}))
}

func TestSubscriptionControlIngress_nilPollGuards(t *testing.T) {
	cases := []struct {
		name string
		s    *SubscriptionControlIngress
	}{
		{name: "nil_poller", s: NewSubscriptionControlIngress(nil)},
		{name: "nil_receiver", s: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Zero(t, tc.s.Poll(10, func([]byte, int) {}))
		})
	}
}

func TestSubscriptionControlIngress_nilOnFragmentAndLimit(t *testing.T) {
	sub, image := controlIngressFixtures(t)
	payload := []byte{1, 2, 3}
	writeUnfragmentedFrame(t, image, 0, payload)

	s := NewSubscriptionControlIngress(sub)
	require.Zero(t, s.Poll(10, nil))
	require.Zero(t, s.Poll(0, func([]byte, int) {}))
	require.Zero(t, s.Poll(-1, func([]byte, int) {}))

	// Residual frame still delivers after zero-limit / nil-handler guards.
	var got []byte
	work := s.Poll(10, func(buf []byte, offset int) {
		got = append([]byte(nil), buf...)
		require.Equal(t, 0, offset)
	})
	require.Equal(t, 1, work)
	require.Equal(t, payload, got)
}

func TestSubscriptionControlIngress_unfragmented(t *testing.T) {
	sub, image := controlIngressFixtures(t)

	payload := []byte{9, 8, 7, 6, 5}
	writeUnfragmentedFrame(t, image, 0, payload)

	s := NewSubscriptionControlIngress(sub)
	var got []byte
	var offset int
	work := s.Poll(10, func(buf []byte, off int) {
		got = append([]byte(nil), buf...)
		offset = off
	})
	require.Equal(t, 1, work)
	require.Equal(t, 0, offset)
	require.Equal(t, payload, got)
}

func TestSubscriptionControlIngress_reassembledTrain(t *testing.T) {
	sub, image := controlIngressFixtures(t)

	begin := []byte("begin-part")
	end := []byte("end-part")
	want := append(append([]byte(nil), begin...), end...)

	writeControlFragmented(t, image, 0, []aeron.FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: begin},
		{Flags: logbuffer.EndFlag, Payload: end},
	})

	s := NewSubscriptionControlIngress(sub)
	var deliveries int
	var got []byte
	work := s.Poll(10, func(buf []byte, offset int) {
		deliveries++
		got = append([]byte(nil), buf...)
		require.Equal(t, 0, offset)
	})
	require.Equal(t, 2, work)
	require.Equal(t, 1, deliveries)
	require.Equal(t, want, got)
}

// seedPendingBeginFragment polls only the BEGIN half of a BEGIN/END train so
// the assembler holds a session builder. Returns the ingress and session id.
func seedPendingBeginFragment(t *testing.T) (*SubscriptionControlIngress, int32) {
	t.Helper()
	sub, image := controlIngressFixtures(t)

	begin := []byte("pending-begin-reassembly-bytes")
	writeControlFragmented(t, image, 0, []aeron.FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: begin},
	})

	s := NewSubscriptionControlIngress(sub)
	require.Equal(t, 1, s.Poll(1, func([]byte, int) {
		t.Fatal("BEGIN must not reach onFragment")
	}))
	return s, image.SessionID()
}

func TestSubscriptionControlIngress_FreeSessionBuffer(t *testing.T) {
	ingress, sessionID := seedPendingBeginFragment(t)

	require.True(t, ingress.FreeSessionBuffer(sessionID))
	require.False(t, ingress.FreeSessionBuffer(sessionID))
}

func TestSubscriptionControlIngress_FreeSessionBuffer_wrongSessionID(t *testing.T) {
	ingress, sessionID := seedPendingBeginFragment(t)

	require.False(t, ingress.FreeSessionBuffer(sessionID+1))
	require.True(t, ingress.FreeSessionBuffer(sessionID))
}

func TestSubscriptionControlIngress_Clear(t *testing.T) {
	ingress, sessionID := seedPendingBeginFragment(t)

	ingress.Clear()

	require.False(t, ingress.FreeSessionBuffer(sessionID))
}

func TestSubscriptionControlIngress_teardownGuards_nilReceiverOrAssembler(t *testing.T) {
	cases := []struct {
		name    string
		ingress *SubscriptionControlIngress
	}{
		{name: "nil_receiver", ingress: nil},
		{name: "nil_assembler", ingress: &SubscriptionControlIngress{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.False(t, tc.ingress.FreeSessionBuffer(1))
			require.NotPanics(t, func() { tc.ingress.Clear() })
		})
	}
}

func controlIngressFixtures(t testing.TB) (*aeron.Subscription, *aeron.Image) {
	t.Helper()
	sub := aeron.NewTestSubscriptionForHooks(t)
	image := aeron.NewTestImageForHooks(t, sub, 0)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	return sub, image
}

func writeControlFragmented(
	t testing.TB,
	image *aeron.Image,
	frameOffset int,
	frames []aeron.FragmentedFrameForTest,
) {
	t.Helper()
	aeron.WriteCommittedDataFramesForStreamForTest(
		aeron.ImageTermBufferForHooks(image),
		frameOffset,
		image.SessionID(),
		image.StreamID(),
		hookTermID,
		frames,
	)
}
