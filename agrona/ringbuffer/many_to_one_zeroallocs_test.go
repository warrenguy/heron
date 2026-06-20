package ringbuffer_test

import (
	"testing"

	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/agrona/ringbuffer"
	"github.com/warrenguy/heron/internal/testutil"
)

func TestManyToOneRingBuffer_ZeroAllocs(t *testing.T) {
	src := atomic.Wrap(testPayload64)

	cases := []struct {
		name  string
		setup func(t *testing.T) (*ringbuffer.ManyToOneRingBuffer, atomic.AtomicBuffer)
		fn    func(ring *ringbuffer.ManyToOneRingBuffer, buffer atomic.AtomicBuffer)
	}{
		{
			name:  "Write+Read",
			setup: setupEmptyRing,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ atomic.AtomicBuffer) {
				ok, err := ring.Write(testMsgTypeID, src, 0, len(testPayload64))
				if err != nil || !ok {
					panic("write failed")
				}
				ring.Read(benchmarkRingNoopHandler)
			},
		},
		{
			name:  "TryClaim+Commit+Read",
			setup: setupEmptyRing,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, buffer atomic.AtomicBuffer) {
				index, err := ring.TryClaim(testMsgTypeID, len(testPayload64))
				if err != nil || index < 0 {
					panic("tryClaim failed")
				}
				buffer.PutBytes(index, testPayload64)
				if err := ring.Commit(index); err != nil {
					panic(err)
				}
				ring.Read(benchmarkRingNoopHandler)
			},
		},
		{
			name:  "Read",
			setup: setupRingOneMessage,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ atomic.AtomicBuffer) {
				ringSink = ring.Read(benchmarkRingNoopHandler)
			},
		},
		{
			name:  "ControlledRead",
			setup: setupRingOneMessage,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ atomic.AtomicBuffer) {
				ringSink = ring.ControlledRead(benchmarkControlledContinue)
			},
		},
		{
			name:  "NextCorrelationId",
			setup: setupEmptyRing,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ atomic.AtomicBuffer) {
				ringSink = ring.NextCorrelationID()
			},
		},
		{
			name:  "ReadWithLimit",
			setup: setupRingOneMessage,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ atomic.AtomicBuffer) {
				ringSink = ring.ReadWithLimit(benchmarkRingNoopHandler, 1)
			},
		},
		{
			name:  "Unblock",
			setup: setupUnblockEmptyRing,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ atomic.AtomicBuffer) {
				ringSink = ring.Unblock()
			},
		},
		{
			name:  "Abort",
			setup: setupEmptyRing,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ atomic.AtomicBuffer) {
				index, err := ring.TryClaim(testMsgTypeID, len(testPayload64))
				if err != nil || index < 0 {
					panic("tryClaim failed")
				}
				if err := ring.Abort(index); err != nil {
					panic(err)
				}
				ring.Read(benchmarkRingNoopHandler)
			},
		},
		{
			name:  "Positions",
			setup: setupRingCaughtUp,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ atomic.AtomicBuffer) {
				ringSink = ring.ConsumerPosition()
				ringSink = ring.ProducerPosition()
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ring, buffer := tc.setup(t)
			testutil.AssertZeroAllocs(t, tc.name, 1000, func() {
				tc.fn(ring, buffer)
			})
		})
	}
}
