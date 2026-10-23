//go:build rapid && testhooks

package service

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// modelMessage is one append-order log message with stream positions.
type modelMessage struct {
	kind      string // open, close, message, timer, padding
	sessionID int64
	startPos  int64 // start of last frame (delivery bound uses this)
	endPos    int64
}

// Asserts bounded consumption. Recorded callbacks match the model delivered
// prefix. LogPosition equals the end of the last delivered non-padding
// message; LogPosition may exceed the raw commit under frame-granular
// delivery, and is zero when nothing is delivered. Image position tracks the
// stream cursor including padding. The session registry matches the delivered
// open/close set. Positions are monotonic. No callback delivers twice.
//
// Ops: append a committed frame (or padding), advance commit, DoWork.
// Session-open clash is constrained out; unit rows assert the clash path.
func TestAgent_boundedConsume_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		svc := &recordingService{}
		sub, image := newLogImage(t, testLogSessionID, testLogStreamID, 0)

		committed := int64(0)
		const fragmentLimit = 50
		agent := NewAgent(AgentConfig{
			Service:           svc,
			CommittedPosition: func() int64 { return committed },
			Subscriptions:     &stubAdder{sub: sub},
			FragmentLimit:     fragmentLimit,
		})
		// Inject image: consume property is independent of join lifecycle.
		agent.adapter.SetImage(image)
		agent.adapter.SetMaxLogPosition(1 << 40)
		agent.started = true

		sessionPool := []int64{1, 2, 3, 4, 5}
		appendSessions := make(map[int64]struct{})
		var messages []modelMessage
		// logEnd is the next write offset and the stream end position.
		logEnd := int64(0)

		// nextMsg is the next messages index not yet consumed by the model.
		nextMsg := 0
		var lastDeliveredEnd int64
		modelSessions := make(map[int64]struct{})
		var wantOrder []string
		var wantOpens []int64
		var wantCloses []int64

		// record appends one model entry and advances logEnd by span.
		record := func(m modelMessage, span int) {
			m.startPos = logEnd
			m.endPos = logEnd + int64(span)
			messages = append(messages, m)
			logEnd = m.endPos
		}

		// modelDoWork advances the reference model one Poll-shaped step:
		// messages with last-frame start below committed deliver in order,
		// at most fragmentLimit non-padding fragments (padding free).
		modelDoWork := func() {
			rt.Helper()
			frags := 0
			for nextMsg < len(messages) && frags < fragmentLimit {
				m := &messages[nextMsg]
				if m.startPos >= committed {
					break
				}
				if m.kind == "padding" {
					// Padding advances image position only. No callback. No fragment count.
					nextMsg++
					continue
				}
				lastDeliveredEnd = m.endPos
				wantOrder = append(wantOrder, m.kind)
				switch m.kind {
				case "open":
					modelSessions[m.sessionID] = struct{}{}
					wantOpens = append(wantOpens, m.sessionID)
				case "close":
					delete(modelSessions, m.sessionID)
					wantCloses = append(wantCloses, m.sessionID)
				}
				nextMsg++
				// Unfragmented: one fragment. Fragmented train: two fragments
				// in the real poll; model treats one logical message as one
				// fragment-budget unit. Unit rows pin multi-fragment trains.
				frags++
			}
		}

		assertInvariants := func() {
			rt.Helper()
			require.Equal(rt, wantOrder, svc.order)
			require.Len(rt, svc.opens, len(wantOpens))
			for i, id := range wantOpens {
				require.Equal(rt, id, svc.opens[i].session.ID())
			}
			require.Len(rt, svc.closes, len(wantCloses))
			for i, id := range wantCloses {
				require.Equal(rt, id, svc.closes[i].session.ID())
			}

			// Frame-granular delivery: a frame that starts before the bound
			// delivers whole, so LogPosition may pass the raw commit value.
			// LogPosition must equal the end of the last delivered message
			// (bound rounded up to that message end). Zero when nothing has
			// been delivered yet.
			require.Equal(rt, lastDeliveredEnd, agent.LogPosition())

			// Image position tracks the stream cursor, including padding.
			// LogPosition skips padding. nextMsg advances over padding in the
			// model.
			wantImagePos := int64(0)
			if nextMsg > 0 {
				wantImagePos = messages[nextMsg-1].endPos
			}
			require.Equal(rt, wantImagePos, image.Position())

			// Model session set must match agent registry size and membership.
			require.Equal(rt, len(modelSessions), agent.SessionCount())
			for id := range modelSessions {
				require.True(rt, agent.HasSession(id))
			}
			require.NoError(rt, agent.ServiceFailure())
		}

		// Monotonic position across DoWork.
		prevPos := agent.LogPosition()
		runDoWork := func() {
			rt.Helper()
			modelDoWork()
			agent.DoWork(0)
			require.GreaterOrEqual(rt, agent.LogPosition(), prevPos)
			prevPos = agent.LogPosition()
		}

		stepCount := rapid.IntRange(1, 64).Draw(rt, "stepCount")
		for step := range stepCount {
			op := rapid.IntRange(0, 2).Draw(rt, "op")
			switch op {
			case 0: // append frame (unfragmented + padding; fragmented pinned by unit rows)
				if logEnd > 1<<16 {
					break
				}
				var m modelMessage
				var payload []byte
				ts := int64(step + 1)
				writeAt := int(logEnd)

				var free []int64
				for _, id := range sessionPool {
					if _, open := appendSessions[id]; !open {
						free = append(free, id)
					}
				}
				// Sort keys so the rapid draw is seed-stable. Map range order is
				// not deterministic.
				openIDs := slices.Sorted(maps.Keys(appendSessions))

				kindRoll := rapid.IntRange(0, 4).Draw(rt, "kind")
				noFreeSession := kindRoll == 0 && len(free) == 0
				noOpenSession := kindRoll == 1 && len(openIDs) == 0
				if noFreeSession || noOpenSession {
					// No session available for the drawn kind: append a timer.
					kindRoll = 3
				}

				switch kindRoll {
				case 0: // open
					id := free[rapid.IntRange(0, len(free)-1).Draw(rt, "openID")]
					payload = encodeSessionOpen(t, codecs.SessionOpenEventFields{
						LeadershipTermID: 1,
						ClusterSessionID: id,
						Timestamp:        ts,
						ResponseChannel:  []byte("c"),
					})
					m = modelMessage{kind: "open", sessionID: id}
					appendSessions[id] = struct{}{}

				case 1: // close
					id := openIDs[rapid.IntRange(0, len(openIDs)-1).Draw(rt, "closeID")]
					payload = encodeSessionClose(t, codecs.SessionCloseEventFields{
						LeadershipTermID: 1,
						ClusterSessionID: id,
						Timestamp:        ts,
						CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
					})
					m = modelMessage{kind: "close", sessionID: id}
					delete(appendSessions, id)

				case 2: // message
					id := sessionPool[rapid.IntRange(0, len(sessionPool)-1).Draw(rt, "msgID")]
					bodyLen := rapid.IntRange(0, 16).Draw(rt, "bodyLen")
					body := make([]byte, bodyLen)
					for i := range body {
						body[i] = byte(i)
					}
					payload = encodeSessionMessage(t, codecs.SessionMessageHeaderFields{
						LeadershipTermID: 1,
						ClusterSessionID: id,
						Timestamp:        ts,
					}, body)
					m = modelMessage{kind: "message", sessionID: id}

				case 3: // timer
					payload = encodeTimerEvent(t, codecs.TimerEventFields{
						LeadershipTermID: 1,
						CorrelationID:    int64(step),
						Timestamp:        ts,
					})
					m = modelMessage{kind: "timer"}

				case 4: // padding; IntRange already meets DataHeaderLength
					padLen := int32(logbuffer.AlignFrameLength(
						rapid.IntRange(32, 128).Draw(rt, "padLen"),
					))
					writeAgentPadding(t, image, writeAt, padLen)
					record(modelMessage{kind: "padding"}, int(padLen))
				}

				if payload != nil {
					span := writeAgentFrame(t, image, writeAt, payload)
					record(m, span)
				}

			case 1: // advance commit (strictly increasing, capped at the log end)
				if committed < logEnd {
					committed = rapid.Int64Range(committed+1, logEnd).Draw(rt, "commitPos")
				}

			case 2: // DoWork
				runDoWork()
			}

			assertInvariants()
		}

		// Final drain: commit to log end and DoWork until model catches up.
		committed = logEnd
		for range 64 {
			if nextMsg >= len(messages) {
				break
			}
			runDoWork()
			assertInvariants()
		}
		require.Equal(rt, len(messages), nextMsg)
	})
}
