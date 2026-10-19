//go:build rapid

package consensus

import (
	"sync/atomic"
	"testing"

	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/codecs"
)

func TestProposeMaxCommit_monotonic_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		a := NewAgent(AgentConfig{
			Commit:                    new(atomic.Int64),
			LeaderHeartbeatIntervalNs: 10,
		})
		n := rapid.IntRange(1, 40).Draw(rt, "n")
		now := int64(0)
		oracle := int64(0)
		for range n {
			q := rapid.Int64Range(0, 1000).Draw(rt, "q")
			// Force gate open each step via interval advance.
			now += a.leaderHeartbeatIntervalNs
			require.Equal(rt, 1, a.UpdateLeaderPosition(now, q))
			oracle = max(oracle, q)
			require.Equal(rt, oracle, a.CommitPosition())
		}
	})
}

func TestUpdateLeaderPosition_commitBoundedByAppend_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		// Apply host min before the interval gate, then UpdateLeaderPosition.
		// No AppendSource: this path does not call consensusWork or rank tables.
		// Append is non-decreasing (log never shrinks). CommitPosition never
		// exceeds the current append.
		a := NewAgent(AgentConfig{
			Commit:                    new(atomic.Int64),
			LeaderHeartbeatIntervalNs: 10,
		})
		n := rapid.IntRange(1, 40).Draw(rt, "n")
		now := int64(0)
		appendPos := int64(0)
		for range n {
			// Non-decreasing append: draw delta, not absolute that can regress.
			delta := rapid.Int64Range(0, 50).Draw(rt, "appendDelta")
			appendPos += delta
			rankedQuorum := rapid.Int64Range(0, appendPos+50).Draw(rt, "rankedQuorum")
			// Host min before gate - do not pass unconstrained ranked quorum.
			q := min(rankedQuorum, appendPos)
			now += a.leaderHeartbeatIntervalNs
			_ = a.UpdateLeaderPosition(now, q)
			require.LessOrEqual(rt, a.CommitPosition(), appendPos)
		}
	})
}

func TestUpdateLeaderPosition_timeOfLastLogUpdate_monotonicOnPublish_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		a := NewAgent(AgentConfig{
			Commit:                    new(atomic.Int64),
			LeaderHeartbeatIntervalNs: 50,
		})
		n := rapid.IntRange(1, 50).Draw(rt, "n")
		now := int64(0)
		prevLogUpdate := int64(0)
		for range n {
			// Mix deltas and quorums so some steps fail the gate.
			delta := rapid.Int64Range(1, 80).Draw(rt, "delta")
			now += delta
			q := rapid.Int64Range(0, 200).Draw(rt, "q")
			commitBefore := a.CommitPosition()

			// Independent oracle for gate open.
			intervalDue := now >= prevLogUpdate+a.leaderHeartbeatIntervalNs
			expectWork := 0
			if q > commitBefore || intervalDue {
				expectWork = 1
			}

			work := a.UpdateLeaderPosition(now, q)
			require.Equal(rt, expectWork, work)

			if work == 1 {
				require.Equal(rt, now, a.timeOfLastLogUpdateNs)
				prevLogUpdate = a.timeOfLastLogUpdateNs
			} else {
				require.Equal(rt, prevLogUpdate, a.timeOfLastLogUpdateNs)
			}
		}
	})
}

// TestElectionInstall_countMonotonicAndSingleRunner_rapid asserts install
// count, single runner, and seed term invariants over mixed duty steps.
//
// An adapter with no image stands for a closed log image.
func TestElectionInstall_countMonotonicAndSingleRunner_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		const timeoutNs = DefaultLeaderHeartbeatTimeoutNs
		pt := &scriptedActiveQuorumTable{active: true}
		a := NewAgent(AgentConfig{
			Role:                     RoleFollower,
			Positions:                pt,
			Commit:                   new(atomic.Int64),
			LeadershipTermID:         rapid.Int64Range(0, 20).Draw(rt, "term"),
			LeaderMemberID:           1,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		control := &fakeLogRecordingControl{}
		a.SetLogRecordingControl(control)
		var seeds []ElectionSeed
		var setElectionSteps int64
		var lastRunner ElectionRunner
		oracleTrigger := ElectionTriggerNone
		a.SetElectionFactory(func(seed ElectionSeed) ElectionRunner {
			seeds = append(seeds, seed)
			r := &orderRunner{n: 1}
			lastRunner = r
			return r
		})

		nowNs := int64(0)
		prevCount := a.ElectionCount()
		require.Equal(rt, ElectionTriggerNone, a.LastElectionTrigger())

		n := rapid.IntRange(1, 40).Draw(rt, "n")
		steps := []string{
			"tick", "completeElection", "higherTerm",
			"quorumLost", "quorumHeld", "setElection", "clearElection",
			"recordingStop", "logImageClosed", "voteRequest",
		}
		for range n {
			step := rapid.SampledFrom(steps).Draw(rt, "step")
			delta := rapid.Int64Range(1, timeoutNs).Draw(rt, "delta")
			nowNs += delta
			countBefore := a.ElectionCount()
			seedsBefore := len(seeds)
			liveBefore := a.Election() != nil

			switch step {
			case "tick":
				_ = a.DoWork(nowNs)
				if a.ElectionCount() > countBefore {
					// Settled tick with active quorum table: only heartbeat.
					oracleTrigger = ElectionTriggerLeaderHeartbeatTimeout
				}
			case "completeElection":
				if a.Election() != nil {
					a.ElectionComplete(nowNs)
				}
			case "higherTerm":
				if a.Election() == nil {
					a.handleSteadyCommitPosition(codecs.CommitPositionFields{
						LeadershipTermID: a.LeadershipTermID() + 1 + rapid.Int64Range(0, 5).Draw(rt, "termBump"),
						LogPosition:      1,
						LeaderMemberID:   2,
					}, nowNs)
					if a.ElectionCount() > countBefore {
						oracleTrigger = ElectionTriggerHigherTerm
					}
				}
			case "quorumLost":
				if a.Election() == nil {
					a.RoleSet(RoleLeader)
					pt.active = false
					_ = a.DoWork(nowNs)
					pt.active = true
					if a.ElectionCount() > countBefore {
						oracleTrigger = ElectionTriggerInactiveFollowerQuorum
					}
				}
			case "quorumHeld":
				if a.Election() == nil {
					a.RoleSet(RoleLeader)
					pt.active = true
					_ = a.DoWork(nowNs)
				}
			case "setElection":
				r := &orderRunner{n: 1}
				lastRunner = r
				a.SetElection(r)
				setElectionSteps++
				oracleTrigger = ElectionTriggerBootstrap
			case "clearElection":
				a.SetElection(nil)
			case "recordingStop":
				control.stopSignalled = true
				control.stopSignalPosition = rapid.Int64Range(0, 1000).Draw(rt, "stopPosition")
				installed := a.logStopTick()
				require.False(rt, control.stopSignalled)
				// Any settled role installs on a STOP.
				require.Equal(rt, !liveBefore, installed == 1)
				if installed == 1 {
					oracleTrigger = ElectionTriggerLogRecordingStopped
				}
			case "logImageClosed":
				leaderBefore := a.Role() == RoleLeader
				a.SetLogAdapter(NewLogAdapter(0))
				installed := a.logStopTick()
				a.SetLogAdapter(nil)
				// A settled non-leader installs. A leader ignores the image.
				require.Equal(rt, !liveBefore && !leaderBefore, installed == 1)
				if installed == 1 {
					oracleTrigger = ElectionTriggerLogImageClosed
				}
			case "voteRequest":
				term := a.LeadershipTermID()
				termDelta := rapid.Int64Range(-2, 3).Draw(rt, "candidateTermDelta")
				buf := encodeRequestVoteWire(t, codecs.RequestVoteFields{
					LogLeadershipTermID: term,
					LogPosition:         1,
					CandidateTermID:     term + termDelta,
					CandidateMemberID:   2,
				})
				a.handleSteadyConsensusFragment(buf, 0, nowNs)
				installed := a.ElectionCount() > countBefore
				require.Equal(rt, !liveBefore && termDelta > 0, installed)
				if installed {
					oracleTrigger = ElectionTriggerVoteRequest
					require.Equal(rt, RoleFollower, a.Role())
				}
			}

			// At most one runner; it is the newest install when non-nil.
			if a.Election() != nil {
				require.Equal(rt, lastRunner, a.Election())
			}

			// Count never decreases.
			require.GreaterOrEqual(rt, a.ElectionCount(), prevCount)
			// Live runner refuses re-install: factory not called.
			if liveBefore && step != "setElection" && step != "clearElection" && step != "completeElection" {
				require.Len(rt, seeds, seedsBefore)
				require.Equal(rt, countBefore, a.ElectionCount())
			}

			// LastElectionTrigger equals the most recent install trigger.
			require.Equal(rt, oracleTrigger, a.LastElectionTrigger())

			// Seed term equals agent term at install time. No image reports
			// end of stream, so every seed carries the null member id.
			for _, seed := range seeds {
				require.Equal(rt, a.LeadershipTermID(), seed.LeadershipTermID)
				require.Equal(rt, nullMemberID, seed.GracefulClosedLeaderID)
			}

			// Recorded factory seeds equal count minus SetElection installs.
			require.Equal(rt, a.ElectionCount()-setElectionSteps, int64(len(seeds)))

			prevCount = a.ElectionCount()
		}
	})
}

// heldClientIngressProbe wraps a [QueueClientIngress]. Poll requires a settled
// agent and counts delivered frames.
type heldClientIngressProbe struct {
	rt        *rapid.T
	agent     *Agent
	queue     QueueClientIngress
	delivered int
}

func (p *heldClientIngressProbe) Poll(
	fragmentLimit int,
	onFragment func(buf []byte, offset int) aeron.ControlledAction,
) int {
	require.Nil(p.rt, p.agent.Election())
	n := p.queue.Poll(fragmentLimit, onFragment)
	p.delivered += n
	return n
}

// TestClientIngress_heldWhileElectionLive_rapid asserts that no client ingress
// frame leaves the image while an election is live. A settled tick delivers up
// to the fragment limit.
func TestClientIngress_heldWhileElectionLive_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		const timeoutNs = DefaultLeaderHeartbeatTimeoutNs
		pt := &scriptedActiveQuorumTable{active: true}
		a := NewAgent(AgentConfig{
			Role:                     RoleFollower,
			Positions:                pt,
			Commit:                   new(atomic.Int64),
			LeadershipTermID:         rapid.Int64Range(0, 20).Draw(rt, "term"),
			LeaderMemberID:           1,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.SetElectionFactory(func(ElectionSeed) ElectionRunner {
			return &orderRunner{n: 1}
		})
		consensus := &QueueConsensusIngress{}
		a.SetIngress(consensus)
		probe := &heldClientIngressProbe{rt: rt, agent: a}
		a.SetClientIngress(probe)
		control := &fakeLogRecordingControl{}
		a.SetLogRecordingControl(control)

		nowNs := int64(0)
		enqueued := 0
		tick := func() {
			queuedBefore := probe.queue.Len()
			deliveredBefore := probe.delivered
			liveBefore := a.Election() != nil
			countBefore := a.ElectionCount()
			_ = a.DoWork(nowNs)
			delivered := probe.delivered - deliveredBefore
			switch {
			case liveBefore:
				require.Zero(rt, delivered)
			case a.ElectionCount() > countBefore:
				// The election installs before the client poll in the same tick.
				require.Zero(rt, delivered)
			default:
				require.Equal(rt, min(queuedBefore, defaultClientIngressFragmentLimit), delivered)
			}
		}

		n := rapid.IntRange(1, 60).Draw(rt, "n")
		steps := []string{
			"enqueue", "tick", "setElection", "completeElection",
			"clearElection", "quorumLost", "higherTermFrame",
			"recordingStop", "logImageClosed", "voteRequestFrame",
		}
		for range n {
			step := rapid.SampledFrom(steps).Draw(rt, "step")
			nowNs += rapid.Int64Range(1, timeoutNs).Draw(rt, "delta")

			switch step {
			case "enqueue":
				count := rapid.IntRange(1, 20).Draw(rt, "count")
				for i := range count {
					probe.queue.Enqueue(encodeSessionKeepAliveFieldsWire(t, a.LeadershipTermID(), int64(i)))
				}
				enqueued += count
			case "tick":
				tick()
			case "setElection":
				a.SetElection(&orderRunner{n: 1})
			case "completeElection":
				if a.Election() != nil {
					a.ElectionComplete(nowNs)
				}
			case "clearElection":
				a.SetElection(nil)
			case "quorumLost":
				a.RoleSet(RoleLeader)
				pt.active = false
				tick()
				pt.active = true
			case "higherTermFrame":
				consensus.Enqueue(encodeCommitPositionWire(t, codecs.CommitPositionFields{
					LeadershipTermID: a.LeadershipTermID() + 1,
					LogPosition:      1,
					LeaderMemberID:   2,
				}))
				tick()
			case "recordingStop":
				control.stopSignalled = true
				control.stopSignalPosition = rapid.Int64Range(0, 1000).Draw(rt, "stopPosition")
				tick()
			case "logImageClosed":
				a.SetLogAdapter(NewLogAdapter(0))
				tick()
				a.SetLogAdapter(nil)
			case "voteRequestFrame":
				consensus.Enqueue(encodeRequestVoteWire(t, codecs.RequestVoteFields{
					LogLeadershipTermID: a.LeadershipTermID(),
					LogPosition:         1,
					CandidateTermID:     a.LeadershipTermID() + 1,
					CandidateMemberID:   2,
				}))
				liveBefore := a.Election() != nil
				countBefore := a.ElectionCount()
				tick()
				if !liveBefore {
					// The heartbeat timeout may install first. The frame then
					// goes to that runner.
					require.Equal(rt, countBefore+1, a.ElectionCount())
					require.Contains(rt, []ElectionTrigger{
						ElectionTriggerVoteRequest,
						ElectionTriggerLeaderHeartbeatTimeout,
					}, a.LastElectionTrigger())
				}
			}

			// Every frame is delivered or still queued.
			require.Equal(rt, enqueued, probe.delivered+probe.queue.Len())
		}
	})
}

// TestSettledHeartbeat_neverFiresBeforeTimeout_rapid asserts the settled
// heartbeat trigger respects stamp + timeout and slow-tick pacing.
func TestSettledHeartbeat_neverFiresBeforeTimeout_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		// Absolute base: zero clocks hide a missing ElectionComplete stamp.
		baseNs := rapid.Int64Range(1_000_000_000_000, 2_000_000_000_000).Draw(rt, "baseNs")
		timeoutNs := rapid.Int64Range(1_000_000, 50_000_000).Draw(rt, "timeoutNs")
		roles := []Role{RoleFollower, RoleCandidate, RoleLeader}
		role := roles[rapid.IntRange(0, len(roles)-1).Draw(rt, "role")]

		pt := &scriptedActiveQuorumTable{active: true}
		a := NewAgent(AgentConfig{
			Role:                     role,
			Positions:                pt,
			LeadershipTermID:         1,
			LeaderMemberID:           1,
			LeaderHeartbeatTimeoutNs: timeoutNs,
		})
		a.SetElectionFactory(func(ElectionSeed) ElectionRunner {
			return &orderRunner{n: 1}
		})

		// Open bootstrap, then complete so the stamp is set.
		a.SetElection(&orderRunner{n: 1})
		a.ElectionComplete(baseNs)
		// Force follower path for heartbeat checks when role is not leader.
		if role != RoleLeader {
			a.RoleSet(RoleFollower)
		}

		nowNs := baseNs
		n := rapid.IntRange(1, 30).Draw(rt, "n")
		for range n {
			// Interleave clock advance, same-term notify, and complete.
			op := rapid.IntRange(0, 2).Draw(rt, "op")
			switch op {
			case 0:
				// Same-term commit from known leader pushes the stamp.
				if a.Election() == nil && a.Role() == RoleFollower {
					a.handleSteadyCommitPosition(codecs.CommitPositionFields{
						LeadershipTermID: 1,
						LogPosition:      10,
						LeaderMemberID:   1,
					}, nowNs)
				}
			case 1:
				if a.Election() != nil {
					a.ElectionComplete(nowNs)
				}
			}

			// Stamp after ops is the bound for this tick.
			stampBeforeTick := a.timeOfLastLogUpdateNs
			delta := rapid.Int64Range(1, timeoutNs).Draw(rt, "tickDelta")
			nowNs += delta
			countBefore := a.ElectionCount()
			_ = a.DoWork(nowNs)
			if a.ElectionCount() > countBefore {
				trigger := a.LastElectionTrigger()
				// Leader with active quorum never installs on heartbeat.
				if role == RoleLeader {
					require.NotEqual(rt, ElectionTriggerLeaderHeartbeatTimeout, trigger)
				}
				if trigger == ElectionTriggerLeaderHeartbeatTimeout {
					// Install only at or after stamp + timeout.
					require.GreaterOrEqual(rt, nowNs, stampBeforeTick+timeoutNs)
					// Detection lands on a slow tick. This test does not bound
					// how far past the timeout the clock draw may land.
				}
			}

			// A same-term notify keeps a settled follower from installing while
			// heartbeats continue. After a notify, the next tick inside the
			// timeout window must not install.
			if a.Election() == nil && a.Role() == RoleFollower {
				a.handleSteadyCommitPosition(codecs.CommitPositionFields{
					LeadershipTermID: 1,
					LogPosition:      10,
					LeaderMemberID:   1,
				}, nowNs)
				mid := nowNs + timeoutNs/2
				if mid > nowNs {
					nowNs = mid
					countMid := a.ElectionCount()
					_ = a.DoWork(nowNs)
					// A settled follower has no install path before stamp + timeout.
					require.Equal(rt, countMid, a.ElectionCount())
				}
			}
		}

		// When still settled as non-leader, a tick past stamp + timeout and
		// the slow-tick deadline installs.
		if a.Election() == nil && a.Role() != RoleLeader {
			a.timeOfLastLogUpdateNs = baseNs
			fireAt := max(baseNs+timeoutNs, a.slowTickDeadlineNs)
			countBefore := a.ElectionCount()
			_ = a.DoWork(fireAt)
			// Detection may land on this tick or need one more interval.
			if a.ElectionCount() == countBefore {
				_ = a.DoWork(fireAt + slowTickIntervalNs)
			}
			require.Greater(rt, a.ElectionCount(), countBefore)
			require.Equal(rt, ElectionTriggerLeaderHeartbeatTimeout, a.LastElectionTrigger())
		}
	})
}

// TestOnSteadyCanvassPosition_guard_rapid asserts settled canvass publish and
// position-update gates over random role, term, and collaborator wiring.
func TestOnSteadyCanvassPosition_guard_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		roles := []Role{RoleFollower, RoleCandidate, RoleLeader}
		role := roles[rapid.IntRange(0, len(roles)-1).Draw(rt, "role")]
		leaderTerm := rapid.Int64Range(0, 20).Draw(rt, "leaderTerm")
		canvassTerm := rapid.Int64Range(0, 25).Draw(rt, "canvassTerm")
		followerID := rapid.Int32Range(0, 10).Draw(rt, "followerID")
		wirePositions := rapid.Bool().Draw(rt, "wirePositions")
		wireEgress := rapid.Bool().Draw(rt, "wireEgress")

		pt := &spyPositionTable{}
		eg := &recordingEgress{}
		cfg := AgentConfig{
			Role:             role,
			Commit:           new(atomic.Int64),
			ThisMemberID:     0,
			LeadershipTermID: leaderTerm,
		}
		if wirePositions {
			cfg.Positions = pt
		}
		if wireEgress {
			cfg.Egress = eg
		}
		a := NewAgent(cfg)

		fields := codecs.CanvassPositionFields{
			LogLeadershipTermID: canvassTerm,
			LogPosition:         rapid.Int64Range(0, 1000).Draw(rt, "logPos"),
			LeadershipTermID:    canvassTerm,
			FollowerMemberID:    followerID,
		}
		a.handleSteadyCanvassPosition(fields, 1)

		expectSideEffects := role == RoleLeader &&
			canvassTerm <= leaderTerm &&
			wirePositions &&
			wireEgress
		if expectSideEffects {
			require.Len(rt, eg.answers, 1)
			require.Len(rt, pt.updates, 1)
		} else {
			require.Empty(rt, eg.answers)
			require.Empty(rt, pt.updates)
		}
	})
}

// recordingModel tracks archive counter state independently of the source.
//
// Counter values are non-decreasing within one contiguous recording session.
// That is the archive's property. The source reports the counter value as-is.
// The property below asserts the source does not break that archive contract.
type recordingModel struct {
	active         bool
	recordingID    int64
	counterPresent bool
	counterValue   int64
	highWaterByID  map[int64]int64
}

// TestRecordingAppendPosition_highWater_rapid asserts recording-derived append
// high-water over start/advance/stop/free/tick sequences.
func TestRecordingAppendPosition_highWater_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		const timeoutNs int64 = 1_000
		eg := &recordingEgress{}
		control := &fakeLogRecordingControl{}
		a := NewAgent(AgentConfig{
			Role:                            RoleFollower,
			Commit:                          new(atomic.Int64),
			Egress:                          eg,
			AppendPositionNotReadyTimeoutNs: timeoutNs,
			LeadershipTermID:                1,
		})
		a.SetLogRecordingControl(control)

		model := &recordingModel{
			recordingID:   nullValue,
			highWaterByID: make(map[int64]int64),
		}
		// Model clock for how long the source has been not-ready.
		modelNotReadySince := int64(nullValue)
		modelFailed := false
		nowNs := int64(0)
		nextRecordingID := int64(1)

		n := rapid.IntRange(1, 60).Draw(rt, "n")
		steps := []string{"start", "advance", "stop", "free", "tick"}
		for range n {
			step := rapid.SampledFrom(steps).Draw(rt, "step")
			switch step {
			case "start":
				if model.active {
					continue
				}
				// Id before active: SetLogRecordingID refuses overwrite while active.
				id := nextRecordingID
				nextRecordingID++
				model.recordingID = id
				model.active = true
				model.counterPresent = false
				model.counterValue = rapid.Int64Range(0, 1000).Draw(rt, "resumePos")
				a.SetLogRecordingID(id)
				control.active = true
			case "advance":
				if !model.active {
					continue
				}
				model.counterPresent = true
				delta := rapid.Int64Range(0, 50).Draw(rt, "advanceDelta")
				model.counterValue += delta
			case "stop":
				if !model.active {
					continue
				}
				model.active = false
				// Leave counterPresent: after stop the counter may still exist
				// until free.
				control.active = false
			case "free":
				if model.active {
					continue
				}
				model.counterPresent = false
			case "tick":
				delta := rapid.Int64Range(1, timeoutNs*2).Draw(rt, "tickDelta")
				nowNs += delta
			}

			counters := countersWithRecordingPosition(
				rt, model.recordingID, model.counterValue, model.counterPresent,
			)
			src := NewRecordingPosAppendPosition(a, counters, cnc.NullValue)
			a.SetAppendSource(src)

			pos, ready := src.AppendPosition()

			// Ready if and only if recording is active, counter present, and id
			// non-null.
			expectReady := model.active &&
				model.counterPresent &&
				model.recordingID != nullValue
			require.Equal(rt, expectReady, ready)

			// Ready position equals the counter value.
			if ready {
				require.Equal(rt, model.counterValue, pos)
				// Ready positions are non-decreasing within one recording id.
				if prev, ok := model.highWaterByID[model.recordingID]; ok {
					require.GreaterOrEqual(rt, pos, prev)
				}
				model.highWaterByID[model.recordingID] = pos
			} else {
				// Not-ready reports position 0.
				require.Equal(rt, int64(0), pos)
			}

			// Drive duty cycle and policy.
			appendsBefore := len(eg.appends)
			failureMarkedBefore := a.AppendRankFailure() != nil
			_ = a.consensusWork(nowNs)

			// Not-ready source or marked failure publishes nothing.
			// Marked failure freezes even when the source would report ready again.
			if !ready || failureMarkedBefore {
				require.Len(rt, eg.appends, appendsBefore)
			}

			// Model-side failure tracking (independent of agent fields).
			if ready {
				modelNotReadySince = nullValue
			} else {
				if modelNotReadySince == nullValue {
					modelNotReadySince = nowNs
				}
				if model.active && nowNs-modelNotReadySince >= timeoutNs {
					modelFailed = true
				}
			}

			// Failure only when active and the not-ready run is past the bound.
			gotFail := a.AppendRankFailure()
			if !modelFailed {
				require.NoError(rt, gotFail)
			} else {
				require.Error(rt, gotFail)
				require.ErrorIs(rt, gotFail, ErrAppendPositionUnavailable)
			}

			// Once set, the failure value does not change.
			if gotFail != nil {
				second := a.AppendRankFailure()
				require.Equal(rt, gotFail, second)
			}
		}
	})
}
