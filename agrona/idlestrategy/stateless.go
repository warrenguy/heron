package idlestrategy

import "runtime"

type noOpIdleStrategy struct{}

type busySpinIdleStrategy struct{}

type yieldingIdleStrategy struct{}

// NoOp is a stateless no-idle strategy matching Agrona NoOpIdleStrategy.INSTANCE.
var NoOp IdleStrategy = &noOpIdleStrategy{}

// BusySpin is a stateless busy-spin strategy matching Agrona BusySpinIdleStrategy.INSTANCE.
var BusySpin IdleStrategy = &busySpinIdleStrategy{}

// Yielding is a stateless yield strategy matching Agrona YieldingIdleStrategy.INSTANCE.
var Yielding IdleStrategy = &yieldingIdleStrategy{}

func (s *noOpIdleStrategy) Idle(workCount int) {}

func (s *noOpIdleStrategy) Reset() {}

func (s *busySpinIdleStrategy) Idle(workCount int) {
	if workCount > 0 {
		return
	}
	spinWait()
}

func (s *busySpinIdleStrategy) Reset() {}

func (s *yieldingIdleStrategy) Idle(workCount int) {
	if workCount > 0 {
		return
	}
	runtime.Gosched()
}

func (s *yieldingIdleStrategy) Reset() {}
