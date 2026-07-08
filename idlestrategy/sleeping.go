package idlestrategy

import "time"

// DefaultParkPeriod is Agrona SleepingIdleStrategy.DEFAULT_SLEEP_PERIOD_NS (1000ns).
const DefaultParkPeriod = 1000 * time.Nanosecond

type sleepingIdleStrategy struct {
	sleepPeriod time.Duration
}

// DefaultSleeping returns SleepingIdleStrategy with DefaultParkPeriod.
func DefaultSleeping() IdleStrategy {
	return NewSleeping(DefaultParkPeriod)
}

// NewSleeping returns a park-nanos strategy. Zero or negative period skips sleeping.
func NewSleeping(sleepPeriod time.Duration) IdleStrategy {
	return &sleepingIdleStrategy{sleepPeriod: sleepPeriod}
}

// DefaultSleepingMillis returns SleepingMillisIdleStrategy with a 1ms sleep period.
func DefaultSleepingMillis() IdleStrategy {
	return NewSleepingMillis(time.Millisecond)
}

// NewSleepingMillis returns a time.Sleep-based strategy for local dev or high process counts.
func NewSleepingMillis(sleepPeriod time.Duration) IdleStrategy {
	return NewSleeping(sleepPeriod)
}

func (s *sleepingIdleStrategy) Idle(workCount int) {
	if workCount > 0 {
		return
	}
	if s.sleepPeriod <= 0 {
		return
	}
	time.Sleep(s.sleepPeriod)
}

func (s *sleepingIdleStrategy) Reset() {}
