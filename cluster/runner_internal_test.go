package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/idlestrategy"
)

// fakeTransport records ticks and reports termination when set.
type fakeTransport struct {
	work        []int
	ticks       int
	terminating bool
	steps       *[]string
}

func (f *fakeTransport) DoWork() int {
	if f.steps != nil {
		*f.steps = append(*f.steps, "transport")
	}
	value := 0
	if f.ticks < len(f.work) {
		value = f.work[f.ticks]
	}
	f.ticks++
	return value
}

func (f *fakeTransport) IsTerminating() bool { return f.terminating }

// fakeAgent records the clock values each DoWork saw.
type fakeAgent struct {
	work  []int
	ticks int
	times []int64
	steps *[]string
}

func (f *fakeAgent) DoWork(nowNs int64) int {
	if f.steps != nil {
		*f.steps = append(*f.steps, "agent")
	}
	f.times = append(f.times, nowNs)
	value := 0
	if f.ticks < len(f.work) {
		value = f.work[f.ticks]
	}
	f.ticks++
	return value
}

// recordingIdle tallies Idle calls, Reset calls, and work counts.
type recordingIdle struct {
	counts []int
	resets int
	steps  *[]string
}

func (r *recordingIdle) Idle(workCount int) {
	if r.steps != nil {
		*r.steps = append(*r.steps, "idle")
	}
	r.counts = append(r.counts, workCount)
}

func (r *recordingIdle) Reset() { r.resets++ }

func newTestRunner(
	transport *fakeTransport,
	agent *fakeAgent,
	idle idlestrategy.IdleStrategy,
	clock func() int64,
) *Runner {
	return &Runner{client: transport, agent: agent, idle: idle, clock: clock}
}

func TestRunnerDoWorkOrderAndSum(t *testing.T) {
	var steps []string
	transport := &fakeTransport{work: []int{3}, steps: &steps}
	agent := &fakeAgent{work: []int{5}, steps: &steps}
	idle := &recordingIdle{steps: &steps}
	runner := newTestRunner(transport, agent, idle, func() int64 { return 42 })

	require.Equal(t, 8, runner.DoWork())
	require.Equal(t, []string{"transport", "agent"}, steps)
	require.Equal(t, []int64{42}, agent.times)
	require.Empty(t, idle.counts, "DoWork never idles")
	require.Zero(t, idle.resets, "DoWork never resets")
}

func TestRunnerRunIdlesOncePerTick(t *testing.T) {
	var steps []string
	transport := &fakeTransport{work: []int{1, 0, 0}, steps: &steps}
	agent := &fakeAgent{work: []int{0, 2, 0}, steps: &steps}
	idle := &recordingIdle{steps: &steps}

	ctx, cancel := context.WithCancel(context.Background())
	tick := int64(0)
	clock := func() int64 {
		tick++
		if tick == 3 {
			cancel()
		}
		return tick
	}
	runner := newTestRunner(transport, agent, idle, clock)

	require.ErrorIs(t, runner.Run(ctx), context.Canceled)
	require.Equal(t, 3, transport.ticks)
	require.Equal(t, 3, agent.ticks)
	require.Equal(t, []int{1, 2, 0}, idle.counts)
	require.Equal(t, 1, idle.resets)
	require.Equal(t, []string{
		"transport", "agent", "idle",
		"transport", "agent", "idle",
		"transport", "agent", "idle",
	}, steps)
}

func TestRunnerRunExitPaths(t *testing.T) {
	for _, tc := range []struct {
		name        string
		terminating bool
		ctx         func() (context.Context, context.CancelFunc)
		wantErr     error
		wantTicks   int
		wantIdles   int
	}{
		{
			name: "alreadyCancelledRunsNoTick",
			ctx: func() (context.Context, context.CancelFunc) {
				c, x := context.WithCancel(context.Background())
				x()
				return c, func() {}
			},
			wantErr:   context.Canceled,
			wantTicks: 0,
			wantIdles: 0,
		},
		{
			name:        "terminationBeatsIdle",
			terminating: true,
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErr:   ErrTransportTerminating,
			wantTicks: 1,
			wantIdles: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &fakeTransport{terminating: tc.terminating}
			agent := &fakeAgent{}
			idle := &recordingIdle{}
			ctx, cancel := tc.ctx()
			defer cancel()
			runner := newTestRunner(transport, agent, idle, func() int64 { return 0 })

			require.ErrorIs(t, runner.Run(ctx), tc.wantErr)
			require.Equal(t, tc.wantTicks, transport.ticks)
			require.Len(t, idle.counts, tc.wantIdles)
			require.Equal(t, 1, idle.resets, "Run always resets when it returns")
		})
	}
}

func TestNewRunnerValidatesDeps(t *testing.T) {
	complete := RunnerDeps{
		Client: &aeron.Client{},
		Agent:  &consensus.Agent{},
		Idle:   &recordingIdle{},
		Clock:  func() int64 { return 0 },
	}

	for _, tc := range []struct {
		name string
		edit func(*RunnerDeps)
		want string
	}{
		{"nilClient", func(d *RunnerDeps) { d.Client = nil }, "cluster: Runner requires a Client"},
		{"nilAgent", func(d *RunnerDeps) { d.Agent = nil }, "cluster: Runner requires an Agent"},
		{"nilClock", func(d *RunnerDeps) { d.Clock = nil }, "cluster: Runner requires a Clock"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := complete
			tc.edit(&deps)
			runner, err := NewRunner(deps)
			require.EqualError(t, err, tc.want)
			require.Nil(t, runner)
		})
	}

	t.Run("nilIdleDefaults", func(t *testing.T) {
		deps := complete
		deps.Idle = nil
		runner, err := NewRunner(deps)
		require.NoError(t, err)
		require.NotNil(t, runner.idle)
	})

	t.Run("suppliedIdleIsKept", func(t *testing.T) {
		runner, err := NewRunner(complete)
		require.NoError(t, err)
		require.Same(t, complete.Idle, runner.idle)
	})
}
