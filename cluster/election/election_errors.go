package election

import "errors"

// ErrEvent is the class of election events. errors.Is(err, ErrEvent) reports
// an event. An event is a normal protocol restart, not a fault. Every other
// election error is a fault.
var ErrEvent = errors.New("election: event")

type eventError struct{ text string }

func (e *eventError) Error() string { return e.text }

func (e *eventError) Is(target error) bool { return target == ErrEvent }

var (
	// ErrNewElectionFromCanvass reports a canvass log term above the election
	// term while this leader awaits followers.
	ErrNewElectionFromCanvass error = &eventError{
		"election: new election detected from canvass position",
	}

	// ErrNewLeaderFromCommitPosition reports a CommitPosition from another
	// leader in a newer term while this leader is ready.
	ErrNewLeaderFromCommitPosition error = &eventError{
		"election: new leader detected from commit position",
	}

	// ErrTruncatingClusterLog reports that the election truncated the divergent
	// log tail.
	ErrTruncatingClusterLog error = &eventError{
		"election: truncating cluster log",
	}

	// ErrLogJoinPositionMismatch reports a log image whose join position
	// differs from the election log position.
	ErrLogJoinPositionMismatch error = &eventError{
		"election: log join position mismatch",
	}
)

// ErrCommitPositionTimeout reports that FOLLOWER_LOG_REPLICATION reached its
// deadline before the replicated term could commit. It is a fault.
var ErrCommitPositionTimeout = errors.New("election: timeout awaiting commit position")

var (
	errInvalidNewLeadershipTerm = errors.New("election: invalid newLeadershipTerm")
	errNoCatchupEndpoint        = errors.New(
		"election: no leader or catchup endpoint for catchup position",
	)
	errCatchupDestinationOpen   = errors.New("election: failed to open catchup log destination")
	errCatchupPositionSend      = errors.New("election: failed to send catchup position")
	errCatchupLogJoin           = errors.New("election: failed to join catchup log")
	errCatchupLogJoinAsFollower = errors.New("election: failed to join catchup log as follower")
	errLiveLogJoin              = errors.New("election: failed to join live log")
	errLiveLogJoinAsFollower    = errors.New("election: failed to join live log as follower")
	errReadyFollowerNotify      = errors.New("election: ready follower failed to notify leader")
)
