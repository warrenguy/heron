//go:build rapid

package consensus

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
)

type sessionModelState int

const (
	modelPending sessionModelState = iota
	modelOpen
)

type sessionRegistryOpKind int

const (
	sessionOpAdd sessionRegistryOpKind = iota
	sessionOpMarkOpen
	sessionOpRemove
	sessionOpLookup
	sessionOpClearOKPending
)

// TestSessionManager_lifecycle_rapid asserts registry allocate, add, markOpen,
// remove, setOKPending(false), and lookup against a reference model. Each step
// checks counts, id monotonicity, pending order, okPending FIFO order, and the
// pending-in-byID invariant.
//
// PBT skips the egress walk and event queue. The outcome split is a fixed
// matrix of publication and adder behaviours. Unit rows document each path.
func TestSessionManager_lifecycle_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		m := newSessionManager()
		model := make(map[int64]sessionModelState)
		pendingOrder := make([]int64, 0)
		// okPendingOrder tracks markOpen order for sessions awaiting SessionEvent
		// OK.
		okPendingOrder := make([]int64, 0)
		var lastIssued int64
		issued := make(map[int64]struct{})

		assertInvariants := func() {
			rt.Helper()
			require.Equal(rt, len(pendingOrder), m.pendingCount())
			openN := 0
			for _, e := range model {
				if e == modelOpen {
					openN++
				}
			}
			require.Equal(rt, openN, m.openCount())
			require.Len(rt, m.byID, len(model))
			require.Len(rt, m.okPending, len(okPendingOrder))

			// Pending order equals accept order of remaining pending ids.
			require.Len(rt, m.pending, len(pendingOrder))
			for i, id := range pendingOrder {
				require.Equal(rt, id, m.pending[i].id)
				s, ok := m.sessionByID(id)
				require.True(rt, ok)
				require.Equal(rt, sessionPending, s.state)
				require.Equal(rt, modelPending, model[id])
			}

			// okPending is markOpen FIFO. Every entry is open in the model.
			for i, id := range okPendingOrder {
				require.Equal(rt, id, m.okPending[i].id)
				require.Equal(rt, modelOpen, model[id])
				s, ok := m.sessionByID(id)
				require.True(rt, ok)
				require.Equal(rt, sessionOpen, s.state)
				require.Same(rt, s, m.okPending[i])
			}

			// Every byID entry matches the model state.
			for id, e := range model {
				s, ok := m.sessionByID(id)
				require.True(rt, ok)
				switch e {
				case modelPending:
					require.Equal(rt, sessionPending, s.state)
					require.Equal(rt, -1, m.okPendingIndex(s))
				case modelOpen:
					require.Equal(rt, sessionOpen, s.state)
					wantOnOK := slices.Contains(okPendingOrder, id)
					require.Equal(rt, wantOnOK, m.okPendingIndex(s) >= 0)
				}
			}
		}

		issueID := func() int64 {
			rt.Helper()
			id := m.allocateSessionID()
			require.Greater(rt, id, lastIssued)
			require.Positive(rt, id)
			_, seen := issued[id]
			require.False(rt, seen)
			issued[id] = struct{}{}
			lastIssued = id
			return id
		}

		addOne := func(nowNs int64) {
			rt.Helper()
			fields := codecs.SessionConnectRequestFields{
				CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
				ResponseStreamID: rapid.Int32().Draw(rt, "responseStreamID"),
				ResponseChannel:  []byte("ch"),
			}
			atCap := len(model) >= maxConcurrentSessions
			id := issueID()
			s := m.addPendingConnect(id, fields, nowNs)
			if atCap {
				require.Nil(rt, s)
				require.Len(rt, m.byID, len(model))
				// Reject-consumed id is issued and never reused.
				return
			}
			require.NotNil(rt, s)
			require.Equal(rt, id, s.id)
			model[s.id] = modelPending
			pendingOrder = append(pendingOrder, s.id)
			require.Equal(rt, fields.CorrelationID, s.correlationID)
			require.Equal(rt, fields.ResponseStreamID, s.responseStreamID)
			require.Equal(rt, fields.ResponseChannel, s.responseChannel)
			require.Equal(rt, nowNs, s.lastActivityNs)
			require.Equal(rt, sessionPending, s.state)
			require.Equal(rt, int64(nullValue), s.egressAddCorrelationID)
		}

		// Sometimes fill to capacity so the refuse path is reachable.
		if rapid.Bool().Draw(rt, "seedAtCap") {
			for len(model) < maxConcurrentSessions {
				addOne(rapid.Int64().Draw(rt, "seedNowNs"))
			}
			// Explicit refuse at the cap still consumes an id.
			beforeLen := len(m.byID)
			s := m.addPendingConnect(issueID(), codecs.SessionConnectRequestFields{
				CorrelationID:   0,
				ResponseChannel: []byte("x"),
			}, 0)
			require.Nil(rt, s)
			require.Len(rt, m.byID, beforeLen)
			assertInvariants()
		}

		stepCount := rapid.IntRange(1, 128).Draw(rt, "stepCount")
		for range stepCount {
			// Bias toward add. One extra add weight under cap keeps refuse
			// reachable.
			ops := []sessionRegistryOpKind{
				sessionOpAdd,
				sessionOpAdd,
				sessionOpMarkOpen,
				sessionOpRemove,
				sessionOpLookup,
				sessionOpClearOKPending,
			}
			if n := len(model); n > 0 && n < maxConcurrentSessions {
				ops = append(ops, sessionOpAdd)
			}
			op := rapid.SampledFrom(ops).Draw(rt, "op")

			switch op {
			case sessionOpAdd:
				addOne(rapid.Int64().Draw(rt, "nowNs"))

			case sessionOpMarkOpen:
				var id int64
				useExisting := len(model) > 0 && rapid.Bool().Draw(rt, "markExisting")
				if useExisting {
					id = pickSessionID(rt, model, "existingID")
				} else {
					id = rapid.Int64().Draw(rt, "markUnknownID")
				}
				logPos := rapid.Int64().Draw(rt, "logPos")
				wantOK := false
				if e, ok := model[id]; ok && e == modelPending {
					wantOK = true
				}
				got := m.markOpen(id, logPos)
				require.Equal(rt, wantOK, got)
				if wantOK {
					model[id] = modelOpen
					pendingOrder = removeIDFromOrder(pendingOrder, id)
					okPendingOrder = append(okPendingOrder, id)
					s, ok := m.sessionByID(id)
					require.True(rt, ok)
					require.Equal(rt, logPos, s.openedLogPosition)
					require.Same(rt, s, m.okPending[len(m.okPending)-1])
				}

			case sessionOpClearOKPending:
				var id int64
				useExisting := len(okPendingOrder) > 0 && rapid.Bool().Draw(rt, "clearExisting")
				if useExisting {
					id = rapid.SampledFrom(okPendingOrder).Draw(rt, "clearOKID")
				} else {
					id = rapid.Int64().Draw(rt, "clearUnknownID")
				}
				s, ok := m.sessionByID(id)
				wantOn := slices.Contains(okPendingOrder, id)
				if ok {
					// Sole mutator. Pointer clear matches okPendingWork.
					m.setOKPending(s, false)
				}
				if wantOn {
					require.True(rt, ok)
					okPendingOrder = removeIDFromOrder(okPendingOrder, id)
					require.Equal(rt, -1, m.okPendingIndex(s))
				} else if ok {
					require.Equal(rt, -1, m.okPendingIndex(s))
				}

			case sessionOpRemove:
				var id int64
				useExisting := len(model) > 0 && rapid.Bool().Draw(rt, "removeExisting")
				if useExisting {
					id = pickSessionID(rt, model, "existingID")
				} else {
					id = rapid.Int64().Draw(rt, "removeUnknownID")
				}
				_, exists := model[id]
				got := m.remove(id)
				require.Equal(rt, exists, got)
				if exists {
					delete(model, id)
					okPendingOrder = removeIDFromOrder(okPendingOrder, id)
					pendingOrder = removeIDFromOrder(pendingOrder, id)
				}

			case sessionOpLookup:
				var id int64
				useExisting := len(model) > 0 && rapid.Bool().Draw(rt, "lookupExisting")
				if useExisting {
					id = pickSessionID(rt, model, "existingID")
				} else {
					id = rapid.Int64().Draw(rt, "lookupUnknownID")
				}
				s, ok := m.sessionByID(id)
				e, exists := model[id]
				require.Equal(rt, exists, ok)
				if exists {
					require.NotNil(rt, s)
					switch e {
					case modelPending:
						require.Equal(rt, sessionPending, s.state)
					case modelOpen:
						require.Equal(rt, sessionOpen, s.state)
					}
				}
			}

			assertInvariants()
		}
	})
}

func pickSessionID[V any](rt *rapid.T, m map[int64]V, label string) int64 {
	rt.Helper()
	return rapid.SampledFrom(slices.Sorted(maps.Keys(m))).Draw(rt, label)
}

func removeIDFromOrder(order []int64, id int64) []int64 {
	i := slices.Index(order, id)
	if i < 0 {
		return order
	}
	return slices.Delete(order, i, i+1)
}
