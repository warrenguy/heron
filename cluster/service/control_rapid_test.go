//go:build rapid && testhooks

package service

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
)

// modelAckEntry is one pending ServiceAck in the reference model.
type modelAckEntry struct {
	logPosition int64
	kind        coordinationAckKind
	ready       bool
	joinPending bool
	relevantID  int64
}

type modelFrame struct {
	start, end int64
	snapshot   bool
}

// pending control kinds for fragment-limit / handoff avoidance.
const (
	pendingKindRequest = 0
	pendingKindJoin    = 1
)

// pendingControlOp is one enqueued control message in the reference model.
type pendingControlOp struct {
	kind  int
	apply func()
}

// Asserts delivery-order ack pairing. The agent acks each pending ServiceAck
// exactly once, in queue order, with ids 0..n-1. A drawn start entry is
// index 0 exactly once. Every later ack id shifts by one. The start ack
// carries the configured relevant id. Later acks carry nullValue. The ready
// prefix gate never sends past the first unready entry. A passed request
// position marks ServiceFailure and freezes.
//
// Ops: deliver RequestServiceAck; deliver JoinLog (including superseding an
// unbound pending join); append a committed timer frame; append a committed
// ClusterActionRequest SNAPSHOT; run one DoWork tick. Handoff (JoinLog while
// bound) stays in unit tables.
func TestControlWork_deliveryOrderAck_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		svc := &recordingService{}
		// Setup hooks need *testing.T (full testing.TB). Property body uses rt.
		sub, image := newLogImage(t, testLogSessionID, testLogStreamID, 0)
		committed := int64(0)
		control := &QueueControlIngress{}
		proxy, pub := boundModuleProxy(t)

		var startAck *StartAckConfig
		if rapid.Bool().Draw(rt, "startAck") {
			startAck = &StartAckConfig{
				LogPosition: int64(rapid.IntRange(0, 1_000_000).Draw(rt, "startAckLogPosition")),
				RelevantID:  rapid.Int64().Draw(rt, "startAckRelevantID"),
			}
		}

		agent := NewAgent(AgentConfig{
			ServiceID:         1,
			Service:           svc,
			CommittedPosition: func() int64 { return committed },
			Subscriptions:     &stubAdder{sub: sub},
			Control:           control,
			ModuleProxy:       proxy,
			FragmentLimit:     DefaultLogFragmentLimit,
			StartAck:          startAck,
		})

		var model []modelAckEntry
		if startAck != nil {
			model = append(model, modelAckEntry{
				logPosition: startAck.LogPosition,
				kind:        ackKindStart,
				ready:       false,
				relevantID:  startAck.RelevantID,
			})
		}
		modelLogPos := int64(0)
		modelDrained := 0
		modelFailed := false
		modelBound := false
		frameOffset := 0
		var frames []modelFrame
		nextFrame := 0
		var pending []pendingControlOp

		// markJoinReady sets ready and clears joinPending on one entry.
		markJoinReady := func(e *modelAckEntry) {
			e.ready = true
			e.joinPending = false
		}

		modelReqBound := func() (int64, bool) {
			lowest := int64(math.MaxInt64)
			found := false
			for _, e := range model {
				if e.kind == ackKindRequest && !e.ready {
					if !found || e.logPosition < lowest {
						lowest = e.logPosition
						found = true
					}
				}
			}
			return lowest, found
		}

		applyRequest := func(pos int64) {
			model = append(model, modelAckEntry{
				logPosition: pos,
				kind:        ackKindRequest,
				ready:       false,
				relevantID:  nullValue,
			})
		}

		// applyJoin models handleJoinLog for the unbound path: supersede any
		// pending join (mark ready), append a new unready joinPending entry.
		applyJoin := func(pos int64) {
			for i := range model {
				if model[i].joinPending {
					markJoinReady(&model[i])
				}
			}
			model = append(model, modelAckEntry{
				logPosition: pos,
				kind:        ackKindJoin,
				ready:       false,
				joinPending: true,
				relevantID:  nullValue,
			})
		}

		modelSweep := func() {
			if modelFailed {
				return
			}
			for i := range model {
				e := &model[i]
				if e.kind != ackKindRequest || e.ready {
					continue
				}
				switch {
				case modelLogPos == e.logPosition:
					e.ready = true
				case modelLogPos > e.logPosition:
					modelFailed = true
				}
			}
		}

		// modelDrain mirrors drainReadyCoordinationAck: runs after sweep even
		// when sweep marked failure. Subsequent ticks freeze at modelDoWork top.
		modelDrain := func() {
			if modelDrained < len(model) && model[modelDrained].ready {
				modelDrained++
			}
		}

		modelPoll := func() {
			if modelFailed || !modelBound {
				return
			}
			bound := committed
			if rb, ok := modelReqBound(); ok && rb < bound {
				bound = rb
			}
			frags := 0
			for nextFrame < len(frames) && frags < DefaultLogFragmentLimit {
				f := frames[nextFrame]
				if f.start >= bound {
					break
				}
				modelLogPos = f.end
				if f.snapshot {
					model = append(model, modelAckEntry{
						logPosition: f.end,
						kind:        ackKindSnapshot,
						ready:       true,
						relevantID:  nullValue,
					})
				}
				nextFrame++
				frags++
			}
		}

		// joinWouldHandoff reports whether enqueueing another join leaves a
		// join past the control fragment limit after a join that binds in the
		// same poll (handoff is unit-only; model covers unbound supersession).
		joinWouldHandoff := func() bool {
			if modelBound {
				return true
			}
			// New join fits in this poll batch: nothing past the batch.
			if len(pending) < defaultControlFragmentLimit {
				return false
			}
			// New join lands past this poll's batch. Handoff iff batch binds.
			for i := range defaultControlFragmentLimit {
				if pending[i].kind == pendingKindJoin {
					return true
				}
			}
			return false
		}

		// modelDoWork matches Agent: start-work then ready, control poll,
		// sweep, drain, join bind, poll. Drain still runs after sweep
		// failure. Join and poll freeze after that.
		modelDoWork := func() {
			if modelFailed {
				return
			}
			if len(model) > 0 && model[0].kind == ackKindStart {
				model[0].ready = true
			}
			n := min(len(pending), defaultControlFragmentLimit)
			for _, op := range pending[:n] {
				op.apply()
			}
			pending = pending[n:]

			modelSweep()
			modelDrain()
			if modelFailed {
				return
			}
			for i := range model {
				if model[i].joinPending {
					markJoinReady(&model[i])
					modelBound = true
					break
				}
			}
			modelPoll()
		}

		checkInvariants := func() {
			rt.Helper()
			acks := decodeServiceAcksFromPub(rt, pub)
			require.Len(rt, acks, modelDrained)
			require.Equal(rt, int64(modelDrained), agent.NextAckID())
			if startAck != nil {
				nStart := 0
				for _, e := range model {
					if e.kind == ackKindStart {
						nStart++
					}
				}
				require.Equal(rt, 1, nStart)
				require.Equal(rt, ackKindStart, model[0].kind)
			}
			for i, ack := range acks {
				require.Equal(rt, int64(i), ack.AckID)
				require.Equal(rt, model[i].logPosition, ack.LogPosition)
				require.Equal(rt, model[i].relevantID, ack.RelevantID)
				require.Equal(rt, int32(1), ack.ServiceID)
			}
			if modelFailed {
				require.Error(rt, agent.ServiceFailure())
				return
			}
			require.NoError(rt, agent.ServiceFailure())
			require.Equal(rt, len(model)-modelDrained, agent.PendingCoordinationAcks())
			require.Equal(rt, modelLogPos, agent.LogPosition())
			require.Equal(rt, modelBound, agent.IsLogBound())

			wantReady := 0
			for i := modelDrained; i < len(model); i++ {
				if !model[i].ready {
					break
				}
				wantReady++
			}
			require.Equal(rt, wantReady, agent.ReadyCoordinationAcks())
		}

		appendFrame := func(payload []byte, snapshot bool) {
			span := writeAgentFrame(rt, image, frameOffset, payload)
			start := int64(frameOffset)
			end := start + int64(span)
			frames = append(frames, modelFrame{start: start, end: end, snapshot: snapshot})
			frameOffset += span
			committed = end
		}

		// Start work. A drawn StartAck becomes ready and drains on this
		// tick. Joins are drawn ops, including supersession.
		agent.DoWork(0)
		modelDoWork()
		checkInvariants()

		nOps := rapid.IntRange(1, 40).Draw(rt, "nOps")
		for range nOps {
			if modelFailed || agent.ServiceFailure() != nil {
				break
			}
			op := rapid.IntRange(0, 4).Draw(rt, "op")
			switch op {
			case 0: // RequestServiceAck
				var pos int64
				if len(frames) > 0 && rapid.Bool().Draw(rt, "reqAtFrame") {
					idx := rapid.IntRange(0, len(frames)-1).Draw(rt, "frameIdx")
					pos = frames[idx].end
				} else {
					pos = modelLogPos
				}
				if modelLogPos > 0 && rapid.IntRange(0, 9).Draw(rt, "passedBias") == 0 {
					pos = int64(rapid.IntRange(0, int(modelLogPos-1)).Draw(rt, "passedPos"))
				}
				p := pos
				pending = append(pending, pendingControlOp{
					kind:  pendingKindRequest,
					apply: func() { applyRequest(p) },
				})
				control.Enqueue(encodeRequestServiceAckControl(rt, p))

			case 1: // Append timer frame
				if !agent.IsLogBound() {
					continue
				}
				payload := encodeTimerEvent(rt, codecs.TimerEventFields{
					LeadershipTermID: 1,
					CorrelationID:    int64(len(frames) + 1),
					Timestamp:        int64(len(frames) + 1),
				})
				appendFrame(payload, false)

			case 2: // DoWork
				agent.DoWork(0)
				modelDoWork()
				checkInvariants()

			case 3: // JoinLog (unbound supersession path; handoff is unit-only)
				if modelBound || agent.IsLogBound() || joinWouldHandoff() {
					continue
				}
				// Contiguity: join position must equal agent log position.
				pos := modelLogPos
				pending = append(pending, pendingControlOp{
					kind:  pendingKindJoin,
					apply: func() { applyJoin(pos) },
				})
				control.Enqueue(encodeJoinLogControl(rt, defaultJoinLogFields(pos)))

			case 4: // ClusterActionRequest SNAPSHOT flags 0
				if !agent.IsLogBound() {
					continue
				}
				end := int64(frameOffset) + int64(clusterActionFrameSpan())
				payload := encodeClusterAction(rt, snapshotActionFields(end, int64(len(frames)+1)))
				appendFrame(payload, true)
			}
		}

		for range 20 {
			if agent.ServiceFailure() != nil || modelFailed {
				checkInvariants()
				return
			}
			if len(frames) > 0 {
				committed = frames[len(frames)-1].end
			}
			agent.DoWork(0)
			modelDoWork()
			checkInvariants()
			if agent.PendingCoordinationAcks() == 0 &&
				len(pending) == 0 &&
				nextFrame >= len(frames) {
				break
			}
		}
	})
}
