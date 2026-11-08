//go:build rapid

package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"luas.fast/cluster/consensus"
)

// runnerModel is the reference duty cycle that Run must match.
type runnerModel struct {
	ticks      int
	idleCounts []int
	resets     int
	err        error
}

func workAt(schedule []int, index int) int {
	if index < len(schedule) {
		return schedule[index]
	}
	return 0
}

// modelRun walks the drawn schedule in Run's check order.
//
// scheduledTransport cancels the context inside tick cancelAt. Run sees the
// cancel at the top of the next tick. Run sees each termination on the tick
// that raises it.
func modelRun(
	transportWork []int,
	archiveWork []int,
	agentWork []int,
	cancelAt int,
	terminateAt int,
	memberEndAt int,
	memberEnd error,
) runnerModel {
	model := runnerModel{resets: 1}
	canceled := false
	for tick := 0; ; tick++ {
		if canceled {
			model.err = context.Canceled
			return model
		}
		model.ticks++
		if tick >= cancelAt {
			canceled = true
		}
		if terminateAt >= 0 && tick >= terminateAt {
			model.err = ErrTransportTerminating
			return model
		}
		if memberEndAt >= 0 && tick >= memberEndAt {
			model.err = memberEnd
			return model
		}
		model.idleCounts = append(model.idleCounts,
			workAt(transportWork, tick)+workAt(archiveWork, tick)+workAt(agentWork, tick))
	}
}

// scheduledTransport terminates from a drawn tick index onward.
// scheduledTransport cancels the run context from another drawn tick index.
type scheduledTransport struct {
	work        []int
	ticks       int
	terminateAt int
	cancelAt    int
	cancel      context.CancelFunc
}

func (s *scheduledTransport) DoWork() int {
	value := workAt(s.work, s.ticks)
	s.ticks++
	if s.ticks > s.cancelAt {
		s.cancel()
	}
	return value
}

func (s *scheduledTransport) IsTerminating() bool {
	return s.terminateAt >= 0 && s.ticks > s.terminateAt
}

func TestRunnerRunMatchesScheduleModel(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		length := rapid.IntRange(1, 40).Draw(rt, "length")
		transportWork := rapid.SliceOfN(rapid.IntRange(0, 8), length, length).Draw(rt, "transportWork")
		archiveWork := rapid.SliceOfN(rapid.IntRange(0, 8), length, length).Draw(rt, "archiveWork")
		agentWork := rapid.SliceOfN(rapid.IntRange(0, 8), length, length).Draw(rt, "agentWork")
		cancelAt := rapid.IntRange(0, length-1).Draw(rt, "cancelAt")
		terminateAt := rapid.IntRange(-1, length-1).Draw(rt, "terminateAt")
		memberEndAt := rapid.IntRange(-1, length-1).Draw(rt, "memberEndAt")
		memberEnd := &consensus.TerminationError{Cause: errors.New("boom")}

		want := modelRun(
			transportWork,
			archiveWork,
			agentWork,
			cancelAt,
			terminateAt,
			memberEndAt,
			memberEnd,
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		transport := &scheduledTransport{
			work:        transportWork,
			terminateAt: terminateAt,
			cancelAt:    cancelAt,
			cancel:      cancel,
		}
		archive := &fakeArchive{work: archiveWork}
		agent := &fakeAgent{work: agentWork}
		if memberEndAt >= 0 {
			agent.endAt = memberEndAt
			agent.endErr = memberEnd
		}
		idle := &recordingIdle{}
		runner := &Runner{
			client:  transport,
			archive: archive,
			agent:   agent,
			idle:    idle,
			clock:   func() int64 { return int64(transport.ticks) },
		}

		err := runner.Run(ctx)

		require.Same(rt, want.err, err, "Run returns the stop value unchanged")
		require.Equal(rt, want.ticks, transport.ticks, "transport ticks")
		require.Equal(rt, want.ticks, archive.ticks, "archive ticks")
		require.Equal(rt, want.ticks, agent.ticks, "agent ticks")
		require.Equal(rt, want.idleCounts, idle.counts, "idle work counts")
		require.Equal(rt, want.resets, idle.resets, "resets")
	})
}

// TestRunnerDoWorkSumsParts asserts DoWork equals the sum of transport, archive,
// and agent work counts for every drawn schedule. Asserts the idle strategy is
// untouched.
func TestRunnerDoWorkSumsParts(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		transportWork := rapid.SliceOf(rapid.IntRange(0, 1000)).Draw(rt, "transportWork")
		archiveWork := rapid.SliceOf(rapid.IntRange(0, 1000)).Draw(rt, "archiveWork")
		agentWork := rapid.SliceOf(rapid.IntRange(0, 1000)).Draw(rt, "agentWork")
		length := min(len(transportWork), len(archiveWork), len(agentWork))
		if length == 0 {
			return
		}
		now := rapid.Int64().Draw(rt, "now")

		transport := &fakeTransport{work: transportWork}
		archive := &fakeArchive{work: archiveWork}
		agent := &fakeAgent{work: agentWork}
		idle := &recordingIdle{}
		runner := &Runner{
			client:  transport,
			archive: archive,
			agent:   agent,
			idle:    idle,
			clock:   func() int64 { return now },
		}

		for i := range length {
			require.Equal(rt, transportWork[i]+archiveWork[i]+agentWork[i], runner.DoWork(), "tick %d", i)
		}
		require.Equal(rt, length, transport.ticks)
		require.Equal(rt, length, archive.ticks)
		require.Equal(rt, length, agent.ticks)
		for _, seen := range agent.times {
			require.Equal(rt, now, seen, "agent reads the injected clock")
		}
		require.Empty(rt, idle.counts)
		require.Zero(rt, idle.resets)
	})
}
