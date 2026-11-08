package aeron

import (
	"sync/atomic"

	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/atomicbuffer"
)

// SubscriberPosition wraps one driver subscriber-position counter slot in the
// CNC values buffer.
//
// The driver owns counter lifetime for the image session.
type SubscriberPosition struct {
	counterID int32
	value     *int64
}

func newSubscriberPosition(counterID int32, values atomicbuffer.AtomicBuffer) SubscriberPosition {
	return SubscriberPosition{
		counterID: counterID,
		value:     values.Int64Pointer(cnc.CounterOffset(counterID)),
	}
}

func (position *SubscriberPosition) Get() int64 {
	return atomic.LoadInt64(position.value)
}

func (position *SubscriberPosition) SetRelease(value int64) {
	atomicbuffer.StoreInt64Release(position.value, value)
}

func (position *SubscriberPosition) CounterID() int32 {
	return position.counterID
}
