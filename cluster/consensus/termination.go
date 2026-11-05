package consensus

import "errors"

// ErrTerminated matches every *TerminationError with errors.Is.
var ErrTerminated = errors.New("consensus: member terminated")

// TerminationError reports why the member ended.
//
// Behaviour: errors.Is matches ErrTerminated and every error in the Cause
// chain.
type TerminationError struct {
	// Cause is the first fatal error, or the planned reason. A planned
	// termination may leave Cause nil.
	Cause error

	// Expected is true for a planned termination.
	Expected bool
}

func (e *TerminationError) Error() string {
	if e.Cause == nil {
		return ErrTerminated.Error()
	}
	return ErrTerminated.Error() + ": " + e.Cause.Error()
}

func (e *TerminationError) Unwrap() error { return e.Cause }

func (e *TerminationError) Is(target error) bool { return target == ErrTerminated }

// TerminationError returns the *TerminationError once the member has ended,
// or an untyped nil before the end.
//
// Behaviour:
//   - the first fatal mark reports its cause, then ends the member. The value
//     wraps that cause.
//   - a fatal mark after the end does not report
//   - the value stays after ConsensusModule.Close, as [Agent.ModuleState] does
//
// Concurrency: invoker-only.
func (a *Agent) TerminationError() error {
	if a.termination == nil {
		return nil
	}
	return a.termination
}

// terminate ends the member with cause. terminate does not report cause, stop
// the log recording, cancel a take, or retire a wait. ConsensusModule.Close
// stops the recording and cancels the take.
func (a *Agent) terminate(cause error, expected bool, reason string) {
	if a.termination != nil {
		return
	}
	a.termination = &TerminationError{Cause: cause, Expected: expected}
	a.setModuleState(ModuleStateClosed, reason)
}

// failMember reports cause, then ends the member as an unexpected
// termination.
func (a *Agent) failMember(cause error) {
	if a.termination != nil {
		return
	}
	a.reportError(cause)
	a.terminate(cause, false, moduleStateReasonUnexpectedTermination)
}
