package idlestrategy

// NewDefaultBackoff returns BackoffIdleStrategy(1, 10, DefaultParkPeriod, DefaultParkPeriod)
// matching Java Aeron Archive and Aeron Cluster Context defaults.
func NewDefaultBackoff() IdleStrategy {
	park := DefaultParkPeriod
	return NewBackoff(1, 10, park, park)
}
