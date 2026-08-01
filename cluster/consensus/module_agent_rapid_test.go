//go:build rapid

package consensus

import (
	"sync/atomic"
	"testing"

	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"

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
		a.onSteadyCanvassPosition(fields, 1)

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
			src := NewRecordingPosAppendPosition(a, counters)
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
