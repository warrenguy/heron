package soakrun

import (
	"fmt"
	"sync"

	"luas.fast/cluster/election"
)

// A leader reports its own id in ReportedLeaderID. A follower reports its
// LeaderMemberID.
type Sample struct {
	Slot               int
	Incarnation        int
	ReportedLeaderID   int32
	LeadershipTermID   int64
	ElectionComplete   bool
	CommitPosition     int64
	ServiceLogPosition int64
}

// Observer stores the first safety violation in a stream of samples.
//
// Concurrency: safe from any goroutine.
type Observer struct {
	mu      sync.Mutex
	err     error
	leaders map[int64]int32
	commits map[incarnationKey]int64
}

type incarnationKey struct {
	slot        int
	incarnation int
}

func (o *Observer) Observe(sample Sample) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.err != nil {
		return
	}

	if o.leaders == nil {
		o.leaders = make(map[int64]int32)
		o.commits = make(map[incarnationKey]int64)
	}

	if sample.ElectionComplete && sample.ReportedLeaderID != election.NullMemberID {
		leader, seen := o.leaders[sample.LeadershipTermID]
		if seen && leader != sample.ReportedLeaderID {
			o.err = fmt.Errorf(
				"term %d has leaders %d and %d",
				sample.LeadershipTermID,
				leader,
				sample.ReportedLeaderID,
			)
			return
		}

		o.leaders[sample.LeadershipTermID] = sample.ReportedLeaderID
	}

	key := incarnationKey{slot: sample.Slot, incarnation: sample.Incarnation}
	prev, seen := o.commits[key]

	if seen && sample.CommitPosition < prev {
		o.err = fmt.Errorf(
			"member %d incarnation %d commit fell from %d to %d",
			sample.Slot,
			sample.Incarnation,
			prev,
			sample.CommitPosition,
		)
		return
	}

	o.commits[key] = sample.CommitPosition

	if sample.ServiceLogPosition > sample.CommitPosition {
		o.err = fmt.Errorf(
			"service position %d passed commit %d",
			sample.ServiceLogPosition,
			sample.CommitPosition,
		)
	}
}

func (o *Observer) Err() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.err
}

type ChildState struct {
	Slot     int
	Exited   bool
	ExitText string
}

// CheckAlive returns an error that names the first exited child outside
// exemptSlot. An exemptSlot of -1 does not exempt a child.
func CheckAlive(children []ChildState, exemptSlot int) error {
	for _, child := range children {
		if !child.Exited || child.Slot == exemptSlot {
			continue
		}

		if child.ExitText == "" {
			return fmt.Errorf("member %d exited without an exit record", child.Slot)
		}
		return fmt.Errorf("member %d exited: %s", child.Slot, child.ExitText)
	}
	return nil
}

// CheckStop returns stopErr unless reports holds a line.
//
// A later check reads the report.
func CheckStop(stopErr error, reports []string) error {
	if len(reports) > 0 {
		return nil
	}
	return stopErr
}
