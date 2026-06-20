package idlestrategy

import (
	"runtime"
	"time"
)

type backoffState int

const (
	backoffStateNotIdle backoffState = iota
	backoffStateSpinning
	backoffStateYielding
	backoffStateParking
)

type backoffIdleStrategy struct {
	maxSpins      int
	maxYields     int
	minParkPeriod time.Duration
	maxParkPeriod time.Duration

	state      backoffState
	spins      int
	yields     int
	parkPeriod time.Duration
}

// NewBackoff returns a spin-yield-park backoff strategy.
func NewBackoff(
	maxSpins int,
	maxYields int,
	minParkPeriod time.Duration,
	maxParkPeriod time.Duration,
) IdleStrategy {
	return &backoffIdleStrategy{
		maxSpins:      maxSpins,
		maxYields:     maxYields,
		minParkPeriod: minParkPeriod,
		maxParkPeriod: maxParkPeriod,
		parkPeriod:    minParkPeriod,
	}
}

func (s *backoffIdleStrategy) Idle(workCount int) {
	if workCount > 0 {
		s.Reset()
		return
	}
	s.idleStep()
}

func (s *backoffIdleStrategy) Reset() {
	s.state = backoffStateNotIdle
	s.spins = 0
	s.yields = 0
	s.parkPeriod = s.minParkPeriod
}

func (s *backoffIdleStrategy) idleStep() {
	switch s.state {
	case backoffStateNotIdle:
		s.state = backoffStateSpinning
		s.spins++
	case backoffStateSpinning:
		spinWait()
		s.spins++
		if s.spins > s.maxSpins {
			s.state = backoffStateYielding
			s.yields = 0
		}
	case backoffStateYielding:
		s.yields++
		if s.yields > s.maxYields {
			s.state = backoffStateParking
			s.parkPeriod = s.minParkPeriod
		} else {
			runtime.Gosched()
		}
	case backoffStateParking:
		time.Sleep(s.parkPeriod)
		if s.minParkPeriod < s.maxParkPeriod {
			s.parkPeriod = min(s.parkPeriod*2, s.maxParkPeriod)
		}
	}
}
