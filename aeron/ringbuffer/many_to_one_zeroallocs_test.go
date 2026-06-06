package ringbuffer_test

import (
	"testing"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

func TestManyToOneRingBuffer_ZeroAllocs(t *testing.T) {
	src := aeronatomic.Wrap(testPayload64)

	cases := []struct {
		name  string
		setup func(t *testing.T) (*ringbuffer.ManyToOneRingBuffer, aeronatomic.AtomicBuffer)
		fn    func(ring *ringbuffer.ManyToOneRingBuffer, buffer aeronatomic.AtomicBuffer)
	}{
		{
			name:  "Write+Read",
			setup: setupEmptyRing,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ aeronatomic.AtomicBuffer) {
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
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, buffer aeronatomic.AtomicBuffer) {
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
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ aeronatomic.AtomicBuffer) {
				ringSink = ring.Read(benchmarkRingNoopHandler)
			},
		},
		{
			name:  "ControlledRead",
			setup: setupRingOneMessage,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ aeronatomic.AtomicBuffer) {
				ringSink = ring.ControlledRead(benchmarkControlledContinue)
			},
		},
		{
			name:  "NextCorrelationId",
			setup: setupEmptyRing,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ aeronatomic.AtomicBuffer) {
				ringSink = ring.NextCorrelationID()
			},
		},
		{
			name:  "ReadWithLimit",
			setup: setupRingOneMessage,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ aeronatomic.AtomicBuffer) {
				ringSink = ring.ReadWithLimit(benchmarkRingNoopHandler, 1)
			},
		},
		{
			name:  "Unblock",
			setup: setupUnblockEmptyRing,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ aeronatomic.AtomicBuffer) {
				ringSink = ring.Unblock()
			},
		},
		{
			name:  "Abort",
			setup: setupEmptyRing,
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ aeronatomic.AtomicBuffer) {
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
			fn: func(ring *ringbuffer.ManyToOneRingBuffer, _ aeronatomic.AtomicBuffer) {
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
