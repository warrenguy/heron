package aeron

import (
	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
)

// SubscriberPosition wraps one driver subscriber-position counter slot in the CNC
// values buffer. The driver owns counter lifetime for the image session. Get and
// SetRelease read and publish the consume position on the poll hot path.
type SubscriberPosition struct {
	counterID    int32
	valuesOffset int
	values       aeronatomic.AtomicBuffer
}

func newSubscriberPosition(counterID int32, values aeronatomic.AtomicBuffer) SubscriberPosition {
	return SubscriberPosition{
		counterID:    counterID,
		valuesOffset: cnc.CounterOffset(counterID),
		values:       values,
	}
}

func (position *SubscriberPosition) Get() int64 {
	return position.values.GetInt64Volatile(position.valuesOffset)
}

func (position *SubscriberPosition) SetRelease(value int64) {
	position.values.PutInt64Release(position.valuesOffset, value)
}

func (position *SubscriberPosition) CounterID() int32 {
	return position.counterID
}
