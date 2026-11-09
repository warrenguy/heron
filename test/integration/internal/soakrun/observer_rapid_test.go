//go:build rapid

package soakrun

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"luas.fast/cluster/election"
)

func TestObserver_model_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		count := rapid.IntRange(0, 40).Draw(rt, "count")
		samples := make([]Sample, count)

		for i := range samples {
			samples[i] = Sample{
				Slot:               rapid.IntRange(0, 2).Draw(rt, "slot"),
				Incarnation:        rapid.IntRange(0, 2).Draw(rt, "incarnation"),
				ReportedLeaderID:   rapid.Int32Range(-1, 2).Draw(rt, "leader"),
				LeadershipTermID:   rapid.Int64Range(0, 5).Draw(rt, "term"),
				ElectionComplete:   rapid.Bool().Draw(rt, "complete"),
				CommitPosition:     rapid.Int64Range(0, 100).Draw(rt, "commit"),
				ServiceLogPosition: rapid.Int64Range(0, 100).Draw(rt, "service"),
			}
		}

		var observer Observer
		var want error

		for i, sample := range samples {
			observer.Observe(sample)

			if want == nil {
				want = scanViolation(samples[:i+1])
			}

			require.Equal(rt, want, observer.Err())
		}
	})
}

func scanViolation(samples []Sample) error {
	leaders := make(map[int64]int32)
	commits := make(map[incarnationKey]int64)

	for _, sample := range samples {
		if sample.ElectionComplete && sample.ReportedLeaderID != election.NullMemberID {
			leader, ok := leaders[sample.LeadershipTermID]
			if ok && leader != sample.ReportedLeaderID {
				return fmt.Errorf(
					"term %d has leaders %d and %d",
					sample.LeadershipTermID,
					leader,
					sample.ReportedLeaderID,
				)
			}

			leaders[sample.LeadershipTermID] = sample.ReportedLeaderID
		}

		key := incarnationKey{slot: sample.Slot, incarnation: sample.Incarnation}
		previous, seen := commits[key]
		if seen && sample.CommitPosition < previous {
			return fmt.Errorf(
				"member %d incarnation %d commit fell from %d to %d",
				sample.Slot,
				sample.Incarnation,
				previous,
				sample.CommitPosition,
			)
		}

		commits[key] = sample.CommitPosition

		if sample.ServiceLogPosition > sample.CommitPosition {
			return fmt.Errorf(
				"service position %d passed commit %d",
				sample.ServiceLogPosition,
				sample.CommitPosition,
			)
		}
	}

	return nil
}
