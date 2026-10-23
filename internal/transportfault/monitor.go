// Package transportfault stores errors from a connect-owned transport
// client and reports when that client has terminated.
package transportfault

import "sync/atomic"

// TerminationReporter reports whether a transport client has entered
// termination. *aeron.Client implements it.
type TerminationReporter interface {
	IsTerminating() bool
}

// Monitor stores errors from one connect-owned transport client.
//
// Concurrency: the handler from Handler runs on any goroutine.
// TerminationCause is invoker-only.
type Monitor struct {
	pending atomic.Pointer[storedError]
}

type storedError struct{ err error }

// Handler returns the ErrorHandler for the owned client. The handler stores
// err as the pending error, then calls next when next is non-nil. The handler
// never calls the transport client.
func (monitor *Monitor) Handler(next func(error)) func(error) {
	return func(err error) {
		monitor.pending.Store(&storedError{err: err})
		if next != nil {
			next(err)
		}
	}
}

// TerminationCause returns the pending error when client reports termination.
// Otherwise returns nil.
//
// Each call clears the pending error. A call with no pending error does not
// call client. A nil Monitor returns nil.
func (monitor *Monitor) TerminationCause(client TerminationReporter) error {
	if monitor == nil {
		return nil
	}
	pending := monitor.pending.Swap(nil)
	if pending == nil || !client.IsTerminating() {
		return nil
	}
	return pending.err
}
