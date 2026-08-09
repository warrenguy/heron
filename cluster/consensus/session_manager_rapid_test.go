//go:build rapid

package consensus

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

type sessionRegistryOpKind int

const (
	sessionOpAdd sessionRegistryOpKind = iota
	sessionOpMarkChallenged
	sessionOpMarkAuthenticated
	sessionOpMarkRejected
	sessionOpRefreshChallenged
	sessionOpMarkOpen
	sessionOpMarkClosing
	sessionOpRemove
	sessionOpLookup
	sessionOpClearOKPending
)

type sessionModelEntry struct {
	state          sessionState
	principal      []byte
	correlationID  int64
	lastActivityNs int64
	closeReason    clusterwire.CloseReason
}

// Asserts registry allocate, add, auth mutators, markOpen, markClosing, remove,
// setOKPending(false), and lookup against a reference model. Each step checks
// counts, id monotonicity, pending order, okPending FIFO order, accept-order
// all, closeReason, closingSessionCount, principal bytes, and the walk-list
// pre-open invariant (excludes open and closing).
//
// PBT skips the egress walk and event queue. Publication and adder outcomes are
// a fixed matrix. Unit rows document each path. PBT also skips authenticator
// consultation. Unit rows document the fixed state x proxy action matrix.
func TestSessionManager_lifecycle_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		m := newSessionManager()
		model := make(map[int64]sessionModelEntry)
		pendingOrder := make([]int64, 0)
		// okPendingOrder tracks markOpen order for sessions awaiting SessionEvent OK.
		okPendingOrder := make([]int64, 0)
		// acceptOrder tracks every live session in accept order (all list).
		acceptOrder := make([]int64, 0)
		var lastIssued int64
		issued := make(map[int64]struct{})

		assertInvariants := func() {
			rt.Helper()
			require.Equal(rt, len(pendingOrder), m.pendingCount())
			openN := 0
			closingN := 0
			for _, e := range model {
				switch e.state {
				case sessionOpen:
					openN++
				case sessionClosing:
					closingN++
				}
			}
			require.Equal(rt, openN, m.openCount())
			require.Equal(rt, closingN, m.closingSessionCount())
			require.Len(rt, model, openN+closingN+len(pendingOrder))
			require.Len(rt, m.byID, len(model))
			require.Len(rt, m.okPending, len(okPendingOrder))
			require.Len(rt, m.all, len(acceptOrder))

			// all equals accept-ordered live sessions.
			for i, id := range acceptOrder {
				require.Equal(rt, id, m.all[i].id)
			}

			// Walk-list entries are exactly the pre-open sessions.
			require.Len(rt, m.pending, len(pendingOrder))
			for i, id := range pendingOrder {
				require.Equal(rt, id, m.pending[i].id)
				s, ok := m.sessionByID(id)
				require.True(rt, ok)
				e := model[id]
				require.NotEqual(rt, sessionOpen, e.state)
				require.NotEqual(rt, sessionClosing, e.state)
				require.Equal(rt, e.state, s.state)
				require.Equal(rt, e.principal, s.encodedPrincipal)
				require.Equal(rt, e.correlationID, s.correlationID)
				require.Equal(rt, e.lastActivityNs, s.lastActivityNs)
			}

			// okPending is markOpen FIFO. Every entry is open in the model.
			for i, id := range okPendingOrder {
				require.Equal(rt, id, m.okPending[i].id)
				require.Equal(rt, sessionOpen, model[id].state)
				s, ok := m.sessionByID(id)
				require.True(rt, ok)
				require.Equal(rt, sessionOpen, s.state)
				require.Same(rt, s, m.okPending[i])
			}

			// Every byID entry matches the model state.
			for id, e := range model {
				s, ok := m.sessionByID(id)
				require.True(rt, ok)
				require.Equal(rt, e.state, s.state)
				require.Equal(rt, e.principal, s.encodedPrincipal)
				require.Equal(rt, e.correlationID, s.correlationID)
				if e.state == sessionClosing {
					require.Equal(rt, e.closeReason, s.closeReason)
				}
				wantOnOK := slices.Contains(okPendingOrder, id)
				require.Equal(rt, wantOnOK, m.okPendingIndex(s) >= 0)
				wantOnWalk := e.state != sessionOpen && e.state != sessionClosing
				require.Equal(rt, wantOnWalk, slices.Contains(pendingOrder, id))
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
			model[s.id] = sessionModelEntry{
				state:          sessionPending,
				correlationID:  fields.CorrelationID,
				lastActivityNs: nowNs,
			}
			pendingOrder = append(pendingOrder, s.id)
			acceptOrder = append(acceptOrder, s.id)
			require.Equal(rt, fields.CorrelationID, s.correlationID)
			require.Equal(rt, fields.ResponseStreamID, s.responseStreamID)
			require.Equal(rt, fields.ResponseChannel, s.responseChannel)
			require.Equal(rt, nowNs, s.lastActivityNs)
			require.Equal(rt, sessionPending, s.state)
			require.Equal(rt, int64(nullValue), s.egressAddCorrelationID)
			require.Nil(rt, s.encodedPrincipal)
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
				sessionOpMarkChallenged,
				sessionOpMarkAuthenticated,
				sessionOpMarkRejected,
				sessionOpRefreshChallenged,
				sessionOpMarkOpen,
				sessionOpMarkClosing,
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

			case sessionOpMarkChallenged:
				id := drawSessionID(rt, model, "challengeID")
				e, ok := model[id]
				wantOK := modelAuthPhaseOK(e, ok)
				got := m.markChallenged(id)
				require.Equal(rt, wantOK, got)
				if wantOK {
					e.state = sessionChallenged
					model[id] = e
				}

			case sessionOpMarkAuthenticated:
				id := drawSessionID(rt, model, "authID")
				// Registry stores the principal given, including over-cap lengths.
				// The proxy length check is outside the registry.
				prinLen := rapid.IntRange(0, maxEncodedPrincipalLength+8).Draw(rt, "prinLen")
				var principal []byte
				if prinLen > 0 {
					principal = rapid.SliceOfN(rapid.Byte(), prinLen, prinLen).Draw(rt, "principal")
				}
				e, ok := model[id]
				wantOK := modelAuthPhaseOK(e, ok)
				got := m.markAuthenticated(id, principal)
				require.Equal(rt, wantOK, got)
				if wantOK {
					e.state = sessionAuthenticated
					e.principal = principal
					model[id] = e
				}

			case sessionOpMarkRejected:
				id := drawSessionID(rt, model, "rejectID")
				e, ok := model[id]
				wantOK := modelAuthPhaseOK(e, ok)
				got := m.markRejected(id)
				require.Equal(rt, wantOK, got)
				if wantOK {
					e.state = sessionRejected
					model[id] = e
				}

			case sessionOpRefreshChallenged:
				id := drawSessionID(rt, model, "refreshID")
				corr := rapid.Int64().Draw(rt, "refreshCorr")
				nowNs := rapid.Int64().Draw(rt, "refreshNow")
				e, ok := model[id]
				wantOK := ok && e.state == sessionChallenged
				got := m.refreshChallenged(id, corr, nowNs)
				require.Equal(rt, wantOK, got)
				if wantOK {
					e.correlationID = corr
					e.lastActivityNs = nowNs
					model[id] = e
				}

			case sessionOpMarkOpen:
				id := drawSessionID(rt, model, "markOpenID")
				logPos := rapid.Int64().Draw(rt, "logPos")
				e, ok := model[id]
				wantOK := ok && e.state == sessionAuthenticated
				got := m.markOpen(id, logPos)
				require.Equal(rt, wantOK, got)
				if wantOK {
					e.state = sessionOpen
					model[id] = e
					pendingOrder = removeIDFromOrder(pendingOrder, id)
					okPendingOrder = append(okPendingOrder, id)
					s, ok := m.sessionByID(id)
					require.True(rt, ok)
					require.Equal(rt, logPos, s.openedLogPosition)
					require.Same(rt, s, m.okPending[len(m.okPending)-1])
				}

			case sessionOpMarkClosing:
				id := drawSessionID(rt, model, "markClosingID")
				reason := rapid.SampledFrom([]clusterwire.CloseReason{
					clusterwire.CloseReason_CLIENT_ACTION,
					clusterwire.CloseReason_TIMEOUT,
					clusterwire.CloseReason_SERVICE_ACTION,
				}).Draw(rt, "closeReason")
				e, ok := model[id]
				wantOK := ok && e.state == sessionOpen
				got := m.markClosing(id, reason)
				require.Equal(rt, wantOK, got)
				if wantOK {
					e.state = sessionClosing
					e.closeReason = reason
					model[id] = e
					okPendingOrder = removeIDFromOrder(okPendingOrder, id)
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
				id := drawSessionID(rt, model, "removeID")
				_, exists := model[id]
				got := m.remove(id)
				require.Equal(rt, exists, got)
				if exists {
					delete(model, id)
					okPendingOrder = removeIDFromOrder(okPendingOrder, id)
					pendingOrder = removeIDFromOrder(pendingOrder, id)
					acceptOrder = removeIDFromOrder(acceptOrder, id)
				}

			case sessionOpLookup:
				id := drawSessionID(rt, model, "lookupID")
				s, ok := m.sessionByID(id)
				e, exists := model[id]
				require.Equal(rt, exists, ok)
				if exists {
					require.NotNil(rt, s)
					require.Equal(rt, e.state, s.state)
					require.Equal(rt, e.principal, s.encodedPrincipal)
				}
			}

			assertInvariants()
		}
	})
}

// modelAuthPhaseOK is true when the model entry is pending or challenged.
func modelAuthPhaseOK(e sessionModelEntry, ok bool) bool {
	return ok && (e.state == sessionPending || e.state == sessionChallenged)
}

func drawSessionID(rt *rapid.T, m map[int64]sessionModelEntry, label string) int64 {
	rt.Helper()
	useExisting := len(m) > 0 && rapid.Bool().Draw(rt, label+"Existing")
	if useExisting {
		return pickSessionID(rt, m, label)
	}
	return rapid.Int64().Draw(rt, label+"Unknown")
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
