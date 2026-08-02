package cluster

import (
	"context"
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/idlestrategy"
)

// ErrTransportTerminating reports that [Runner.Run] stopped because the
// transport client entered termination.
//
// Causes:
//   - media-driver shutdown
//   - stale keepalive
//   - client or service timeout
//
// [Runner.Run] returns this sentinel, not a context error. A caller can
// distinguish a requested stop from a lost transport.
//
// A closed client does not produce this sentinel. A closed client reports no
// termination.
var ErrTransportTerminating = errors.New("cluster: transport client is terminating")

// transportInvoker is the transport half of one duty tick.
type transportInvoker interface {
	DoWork() int
	IsTerminating() bool
}

// archiveInvoker is the archive-control half of one duty tick.
type archiveInvoker interface {
	Poll() int
}

// noArchiveInvoker reports zero work when RunnerDeps.Archive is nil.
//
// [NewRunner] installs noArchiveInvoker so [Runner.DoWork] has no nil branch
// and no Runner holds a typed-nil archive.Client.
type noArchiveInvoker struct{}

func (noArchiveInvoker) Poll() int { return 0 }

// agentInvoker is the consensus half of one duty tick.
type agentInvoker interface {
	DoWork(nowNs int64) int
}

// RunnerDeps are the collaborators [NewRunner] needs.
//
// Named for [ConsensusModuleDeps] and [RecoveryDeps], which carry injected
// collaborators the same way.
//
// Ownership: every field is borrowed. [Runner] does not own them. [Runner]
// closes none of them. [Runner] outlives none of them.
type RunnerDeps struct {
	// Client is the transport client whose conductor the duty cycle services.
	// Required. Pass ConsensusModule.Client.
	Client *aeron.Client

	// Agent is the consensus agent the duty cycle ticks after the transport client
	// and the archive control poll. Required. Pass ConsensusModule.Agent.
	Agent *consensus.Agent

	// Archive is the archive control client the duty cycle demuxes. Pass
	// ConsensusModule.ArchiveClient when wiring a ConsensusModule.
	//
	// Nil is allowed. A nil Archive reports zero archive work every tick.
	// OpenConsensusModule supplies a non-nil client. Nil is for tests that
	// drive the agent without an Archive.
	Archive *archive.Client

	// Idle paces [Runner.Run]. Nil takes idlestrategy.NewDefaultBackoff.
	//
	// A strategy carries per-loop state. Strategies are not safe to share.
	// Every Runner must use its own strategy.
	//
	// Pass ConsensusModule.IdleStrategy when that value is per-module.
	// IdleStrategy is per-module when each member's Config supplies its own
	// value, or when Idle stays nil and Config.Normalize fills it.
	Idle idlestrategy.IdleStrategy

	// Clock reads nanoseconds for Agent.DoWork. Required. Not defaulted.
	// Agent deadlines and election deadlines must share one function. Pass
	// ConsensusModule.NanoClock.
	Clock func() int64
}

// Runner drives one consensus member's duty cycle on the calling goroutine.
// Tick order is transport client DoWork, then archive control Poll, then agent
// DoWork.
//
// Build with [NewRunner]. Drive with [Runner.DoWork] for one tick, or with
// [Runner.Run] until the context is done or the transport client enters
// termination.
//
//	module, err := cluster.OpenConsensusModule(ctx, deps)
//	if err != nil {
//		return err
//	}
//	defer module.Close()
//	runner, err := cluster.NewRunner(cluster.RunnerDeps{
//		Client:  module.Client(),
//		Agent:   module.Agent(),
//		Archive: module.ArchiveClient(),
//		Idle:    module.IdleStrategy(),
//		Clock:   module.NanoClock(),
//	})
//	if err != nil {
//		return err
//	}
//	return runner.Run(ctx)
//
// Behaviour: [Runner.Run] does not return because of Close. A closed client
// reports no termination. Run calls Idle(0) until the context is done.
//
// Only a done context or [ErrTransportTerminating] stops the loop. Stop the
// duty cycle before closing the module. Build a new Runner rather than reusing
// one across a close.
//
// Concurrency: invoker-only. One Runner per goroutine. Every Runner method must
// run on that member's duty cycle goroutine. Other goroutines must not call
// Runner methods or non-atomic agent accessors.
// [ConsensusModule.CommitPosition] remains safe to read via the module from any
// goroutine.
//
// Lifetime: Runner remains a usable Go value after [ConsensusModule.Close].
// [ConsensusModule.Close] closes the transport client when the module owns it.
// A client supplied via Config.Aeron stays open. [ConsensusModule.Close] always
// closes the archive control client the Runner borrows. [ConsensusModule.Close]
// clears agent bindings. DoWork after Close reports zero work on a closed client.
// DoWork after Close polls nothing on a closed consensus subscription. DoWork
// after Close reports zero archive work on a closed archive control client.
//
// Ownership: Runner does not own its collaborators. Runner closes none of them.
//
// Do not copy. A copy shares the same collaborators and the same idle strategy.
// The strategy contract forbids sharing.
type Runner struct {
	client  transportInvoker
	archive archiveInvoker
	agent   agentInvoker
	idle    idlestrategy.IdleStrategy
	clock   func() int64
}

// NewRunner binds one duty cycle over a transport client, an optional archive
// control client, and a consensus agent.
//
// Client, Agent, and Clock have no safe default. A nil transport client
// services nothing and reports no error. A nil agent panics on the first tick.
// A defaulted clock would be a second clock the election does not share.
//
// Archive takes a default. A nil Archive installs a no-op invoker that reports
// zero work. OpenConsensusModule supplies a non-nil client. Nil is for tests
// that drive the agent without an Archive.
//
// A nil Idle is filled with idlestrategy.NewDefaultBackoff, matching
// [Config.Normalize].
//
// Rebuilding after [ConsensusModule.Close] fails here rather than later.
// ConsensusModule.Client is nil once closed.
//
// Returns an error when:
//   - Client is nil
//   - Agent is nil
//   - Clock is nil
func NewRunner(deps RunnerDeps) (*Runner, error) {
	if deps.Client == nil {
		return nil, errors.New("cluster: Runner requires a Client")
	}
	if deps.Agent == nil {
		return nil, errors.New("cluster: Runner requires an Agent")
	}
	if deps.Clock == nil {
		return nil, errors.New("cluster: Runner requires a Clock")
	}
	idle := deps.Idle
	if idle == nil {
		idle = idlestrategy.NewDefaultBackoff()
	}
	var archivePoll archiveInvoker = noArchiveInvoker{}
	if deps.Archive != nil {
		archivePoll = deps.Archive
	}
	return &Runner{
		client:  deps.Client,
		archive: archivePoll,
		agent:   deps.Agent,
		idle:    idle,
		clock:   deps.Clock,
	}, nil
}

// DoWork runs one duty tick and returns the total work count.
// Order is transport client, then archive control poll, then the agent at the
// clock's current value.
//
// The transport runs first. Archive control responses arrive on a subscription
// the transport conductor services. The agent acts on those demux effects.
// Reversing the order costs one tick of latency per hop.
//
// DoWork never idles. DoWork never sleeps. The caller paces repeated calls.
//
// Each call performs bounded work: one conductor service tick, one bounded
// archive demux poll (DefaultControlResponseFragmentLimit, ten fragments), and
// one bounded ingress poll. Progress needs repeated calls.
//
// DoWork drives one member. All three parts run in one call.
//
// A multi-member duty cycle that must service every transport before any agent
// must drive the module accessors directly. Do not call DoWork or Run on each
// Runner for that shape.
func (r *Runner) DoWork() int {
	return r.client.DoWork() + r.archive.Poll() + r.agent.DoWork(r.clock())
}

// Run pumps [Runner.DoWork] and idles on each tick's work count until stop.
//
// Returns:
//   - ctx.Err when ctx is done
//   - [ErrTransportTerminating] when the transport client enters termination
//
// Run checks ctx before the first tick. An already-done context runs none.
//
// Run installs no signal handler. A caller that wants one builds ctx with
// signal.NotifyContext.
//
// Run closes nothing when it returns. Run drains nothing when it returns.
// The transport client's Close drains its conductor and its linger log buffers.
// Close the module after Run returns.
//
// Run resets the idle strategy when it returns. A strategy that stopped after
// Idle does not start the next loop still stopped.
func (r *Runner) Run(ctx context.Context) error {
	defer r.idle.Reset()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		work := r.DoWork()
		if r.client.IsTerminating() {
			return ErrTransportTerminating
		}
		r.idle.Idle(work)
	}
}
