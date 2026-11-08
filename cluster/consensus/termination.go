package consensus

import "errors"

// ErrTerminated matches every *TerminationError with errors.Is.
var ErrTerminated = errors.New("consensus: member terminated")

// ErrLocalArchiveDisconnected reports that the member's local archive control
// session is not connected.
var ErrLocalArchiveDisconnected = errors.New("consensus: local archive is not connected")

// ArchiveConnection reports whether the member's local archive control
// response subscription is connected. *archive.Client satisfies it.
type ArchiveConnection interface {
	IsConnected() bool
}

// TerminationError reports why the member ended.
//
// Behaviour: errors.Is matches ErrTerminated and every error in the Cause
// chain.
type TerminationError struct {
	// Cause is the first fatal error, or [ErrClusterTerminated] for a planned
	// termination.
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
// Behaviour: the value wraps the first cause. It stays after
// ConsensusModule.Close, as [Agent.ModuleState] does.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines.
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

// TerminateOnError ends the member with cause as an unexpected termination.
// The caller reports cause first. TerminateOnError does not report it.
//
// Behaviour: ignores a nil cause. Idempotent: the first cause wins.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines. A module error handler may call it.
func (a *Agent) TerminateOnError(cause error) {
	if cause == nil {
		return
	}
	a.terminate(cause, false, moduleStateReasonUnexpectedTermination)
}

// SetArchiveConnection sets or clears the local archive connection. Nil skips
// the slow tick check.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines.
func (a *Agent) SetArchiveConnection(connection ArchiveConnection) {
	a.archiveConnection = connection
}
