package election

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestElectionErrorValues(t *testing.T) {
	t.Parallel()

	events := []error{
		ErrNewElectionFromCanvass,
		ErrNewLeaderFromCommitPosition,
		ErrTruncatingClusterLog,
		ErrLogJoinPositionMismatch,
	}
	for _, tc := range []struct {
		name    string
		err     error
		text    string
		isEvent bool
	}{
		{
			name:    "new election from canvass",
			err:     ErrNewElectionFromCanvass,
			text:    "election: new election detected from canvass position",
			isEvent: true,
		},
		{
			name:    "new leader from commit position",
			err:     ErrNewLeaderFromCommitPosition,
			text:    "election: new leader detected from commit position",
			isEvent: true,
		},
		{
			name:    "truncating cluster log",
			err:     ErrTruncatingClusterLog,
			text:    "election: truncating cluster log",
			isEvent: true,
		},
		{
			name:    "log join position mismatch",
			err:     ErrLogJoinPositionMismatch,
			text:    "election: log join position mismatch",
			isEvent: true,
		},
		{
			name: "commit position timeout",
			err:  ErrCommitPositionTimeout,
			text: "election: timeout awaiting commit position",
		},
		{
			name: "invalid newLeadershipTerm",
			err:  errInvalidNewLeadershipTerm,
			text: "election: invalid newLeadershipTerm",
		},
		{
			name: "no catchup endpoint",
			err:  errNoCatchupEndpoint,
			text: "election: no leader or catchup endpoint for catchup position",
		},
		{
			name: "catchup destination open",
			err:  errCatchupDestinationOpen,
			text: "election: failed to open catchup log destination",
		},
		{
			name: "catchup position send",
			err:  errCatchupPositionSend,
			text: "election: failed to send catchup position",
		},
		{
			name: "catchup log join",
			err:  errCatchupLogJoin,
			text: "election: failed to join catchup log",
		},
		{
			name: "catchup log join as follower",
			err:  errCatchupLogJoinAsFollower,
			text: "election: failed to join catchup log as follower",
		},
		{
			name: "live log join",
			err:  errLiveLogJoin,
			text: "election: failed to join live log",
		},
		{
			name: "live log join as follower",
			err:  errLiveLogJoinAsFollower,
			text: "election: failed to join live log as follower",
		},
		{
			name: "ready follower notify",
			err:  errReadyFollowerNotify,
			text: "election: ready follower failed to notify leader",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.EqualError(t, tc.err, tc.text)
			require.Equal(t, tc.isEvent, errors.Is(tc.err, ErrEvent))
			for _, event := range events {
				if event == tc.err {
					continue
				}
				require.NotErrorIs(t, tc.err, event)
			}
		})
	}

	require.EqualError(t, ErrEvent, "election: event")
}
