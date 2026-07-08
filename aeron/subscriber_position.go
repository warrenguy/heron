package aeron

import (
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
)

// SubscriberPosition wraps one driver subscriber-position counter slot in the CNC
// values buffer.
//
// The driver owns counter lifetime for the image session.
//
// Get and SetRelease read and publish the consume position on the poll hot path.
type SubscriberPosition struct {
	counterID    int32
	valuesOffset int
	values       atomicbuffer.AtomicBuffer
}

func newSubscriberPosition(counterID int32, values atomicbuffer.AtomicBuffer) SubscriberPosition {
	return SubscriberPosition{
		counterID:    counterID,
		valuesOffset: cnc.CounterOffset(counterID),
		values:       values,
	}
}

func (position *SubscriberPosition) Get() int64 {
	return position.values.VolatileInt64At(position.valuesOffset)
}

func (position *SubscriberPosition) SetRelease(value int64) {
	position.values.PutInt64Release(position.valuesOffset, value)
}

func (position *SubscriberPosition) CounterID() int32 {
	return position.counterID
}
