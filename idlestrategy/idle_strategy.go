package idlestrategy

// IdleStrategy paces duty cycles from agent work counts. Idle(0) matches Java
// idleStrategy.idle() with no work count.
//
// Backoff resets when workCount > 0. The other implementations return without
// idling when workCount > 0.
//
// Concurrency: implementations are not thread-safe. Use one strategy instance
// per agent goroutine.
type IdleStrategy interface {
	Idle(workCount int)
	Reset()
}
