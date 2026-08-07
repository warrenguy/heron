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
)

// TestSessionManager_lifecycle_rapid asserts registry add, markOpen, remove,
// and lookup against a reference model. Each step checks counts, id
// monotonicity, pending order, and the pending-in-byID invariant.
func TestSessionManager_lifecycle_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		m := newSessionManager()
		model := make(map[int64]sessionModelState)
		pendingOrder := make([]int64, 0)
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

			// Pending order equals accept order of remaining pending ids.
			require.Len(rt, m.pending, len(pendingOrder))
			for i, id := range pendingOrder {
				require.Equal(rt, id, m.pending[i].id)
				s, ok := m.sessionByID(id)
				require.True(rt, ok)
				require.Equal(rt, sessionPending, s.state)
				require.Equal(rt, modelPending, model[id])
			}

			// Every byID entry matches the model state.
			for id, e := range model {
				s, ok := m.sessionByID(id)
				require.True(rt, ok)
				switch e {
				case modelPending:
					require.Equal(rt, sessionPending, s.state)
				case modelOpen:
					require.Equal(rt, sessionOpen, s.state)
				}
			}
		}

		addOne := func(nowNs int64) {
			rt.Helper()
			fields := codecs.SessionConnectRequestFields{
				CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
				ResponseStreamID: rapid.Int32().Draw(rt, "responseStreamID"),
				ResponseChannel:  []byte("ch"),
			}
			atCap := len(model) >= maxConcurrentSessions
			s := m.addPendingConnect(fields, nowNs)
			if atCap {
				require.Nil(rt, s)
				require.Equal(rt, lastIssued+1, m.nextSessionID)
				require.Len(rt, m.byID, len(model))
				return
			}
			require.NotNil(rt, s)
			require.Greater(rt, s.id, lastIssued)
			require.Positive(rt, s.id)
			_, seen := issued[s.id]
			require.False(rt, seen)
			issued[s.id] = struct{}{}
			lastIssued = s.id
			model[s.id] = modelPending
			pendingOrder = append(pendingOrder, s.id)
			require.Equal(rt, fields.CorrelationID, s.correlationID)
			require.Equal(rt, fields.ResponseStreamID, s.responseStreamID)
			require.Equal(rt, fields.ResponseChannel, s.responseChannel)
			require.Equal(rt, nowNs, s.lastActivityNs)
			require.Equal(rt, sessionPending, s.state)
		}

		// Sometimes fill to capacity so the refuse path is reachable.
		if rapid.Bool().Draw(rt, "seedAtCap") {
			for len(model) < maxConcurrentSessions {
				addOne(rapid.Int64().Draw(rt, "seedNowNs"))
			}
			// Explicit refuse at the cap.
			beforeNext := m.nextSessionID
			beforeLen := len(m.byID)
			s := m.addPendingConnect(codecs.SessionConnectRequestFields{
				CorrelationID:   0,
				ResponseChannel: []byte("x"),
			}, 0)
			require.Nil(rt, s)
			require.Equal(rt, beforeNext, m.nextSessionID)
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
					id = pickModelSessionID(rt, model)
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
					s, ok := m.sessionByID(id)
					require.True(rt, ok)
					require.Equal(rt, logPos, s.openedLogPosition)
				}

			case sessionOpRemove:
				var id int64
				useExisting := len(model) > 0 && rapid.Bool().Draw(rt, "removeExisting")
				if useExisting {
					id = pickModelSessionID(rt, model)
				} else {
					id = rapid.Int64().Draw(rt, "removeUnknownID")
				}
				_, exists := model[id]
				got := m.remove(id)
				require.Equal(rt, exists, got)
				if exists {
					delete(model, id)
					pendingOrder = removeIDFromOrder(pendingOrder, id)
				}

			case sessionOpLookup:
				var id int64
				useExisting := len(model) > 0 && rapid.Bool().Draw(rt, "lookupExisting")
				if useExisting {
					id = pickModelSessionID(rt, model)
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

func pickModelSessionID(rt *rapid.T, model map[int64]sessionModelState) int64 {
	rt.Helper()
	return rapid.SampledFrom(slices.Sorted(maps.Keys(model))).Draw(rt, "existingID")
}

func removeIDFromOrder(order []int64, id int64) []int64 {
	i := slices.Index(order, id)
	if i < 0 {
		return order
	}
	return slices.Delete(order, i, i+1)
}
