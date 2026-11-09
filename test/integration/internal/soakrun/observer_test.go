package soakrun

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObserver_flagsViolations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		samples []Sample
		want    string
	}{
		{
			name: "two leaders in one term",
			samples: []Sample{
				complete(0, 0, 0, 1, 10, 10),
				complete(1, 0, 1, 1, 10, 10),
			},
			want: "term 1 has leaders 0 and 1",
		},
		{
			name: "follower naming a second leader",
			samples: []Sample{
				complete(0, 0, 0, 2, 10, 10),
				complete(1, 0, 1, 2, 10, 10),
			},
			want: "term 2 has leaders 0 and 1",
		},
		{
			name: "commit falling in one incarnation",
			samples: []Sample{
				complete(0, 0, 0, 1, 20, 20),
				complete(0, 0, 0, 1, 19, 19),
			},
			want: "member 0 incarnation 0 commit fell from 20 to 19",
		},
		{
			name: "service past commit",
			samples: []Sample{
				complete(0, 0, 0, 1, 10, 11),
			},
			want: "service position 11 passed commit 10",
		},
		{
			name: "negative leader id ignored",
			samples: []Sample{
				complete(0, 0, 0, 1, 10, 10),
				complete(1, 0, -1, 1, 10, 10),
			},
		},
		{
			name: "incomplete election ignored",
			samples: []Sample{
				{
					Slot: 0, Incarnation: 0, ReportedLeaderID: 0,
					LeadershipTermID: 1, CommitPosition: 10, ServiceLogPosition: 10,
				},
				complete(1, 0, 1, 1, 10, 10),
			},
		},
		{
			name: "commit falling across incarnations allowed",
			samples: []Sample{
				complete(0, 0, 0, 1, 20, 20),
				complete(0, 1, 0, 2, 5, 5),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var observer Observer

			for _, sample := range test.samples {
				observer.Observe(sample)
			}

			if test.want == "" {
				require.NoError(t, observer.Err())
				return
			}

			require.EqualError(t, observer.Err(), test.want)
		})
	}
}

func complete(slot, incarnation int, leader int32, term, commit, servicePos int64) Sample {
	return Sample{
		Slot:               slot,
		Incarnation:        incarnation,
		ReportedLeaderID:   leader,
		LeadershipTermID:   term,
		ElectionComplete:   true,
		CommitPosition:     commit,
		ServiceLogPosition: servicePos,
	}
}

func TestCheckAlive(t *testing.T) {
	t.Parallel()

	// Phase: three running children pass.
	running := []ChildState{
		{Slot: 0},
		{Slot: 1},
		{Slot: 2},
	}
	require.NoError(t, CheckAlive(running, -1))

	// Phase: an exited child fails with its slot and exit text.
	exited := []ChildState{
		{Slot: 0},
		{Slot: 1, Exited: true, ExitText: "consensus: local archive is not connected"},
		{Slot: 2},
	}
	require.EqualError(
		t,
		CheckAlive(exited, -1),
		"member 1 exited: consensus: local archive is not connected",
	)

	// Phase: an exited child without an exit record fails.
	missing := []ChildState{{Slot: 0, Exited: true}}
	require.EqualError(t, CheckAlive(missing, -1), "member 0 exited without an exit record")

	// Phase: an exited exempt child passes.
	require.NoError(t, CheckAlive(exited, 1))

	// Phase: an exited child beside the exempt slot fails.
	beside := []ChildState{
		{Slot: 0, Exited: true, ExitText: "boom"},
		{Slot: 1, Exited: true, ExitText: "expected"},
	}
	require.EqualError(t, CheckAlive(beside, 1), "member 0 exited: boom")
}

func TestCheckStop(t *testing.T) {
	t.Parallel()

	stopErr := errors.New("process member: exit code 66")

	// Phase: a nil error passes.
	require.NoError(t, CheckStop(nil, nil))

	// Phase: an error without a report line returns that error.
	require.ErrorIs(t, CheckStop(stopErr, nil), stopErr)

	// Phase: an error beside a report line passes.
	require.NoError(t, CheckStop(stopErr, []string{"WARNING: DATA RACE"}))

	// Phase: a nil error beside a report line passes.
	require.NoError(t, CheckStop(nil, []string{"WARNING: DATA RACE"}))
}
