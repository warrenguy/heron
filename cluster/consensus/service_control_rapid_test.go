//go:build rapid && testhooks

package consensus

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster/codecs"
)

type serviceControlOpKind int

const (
	serviceControlOpPublish serviceControlOpKind = iota
	serviceControlOpAckNext
	serviceControlOpWrongAck
	serviceControlOpWork
)

// Asserts wait sequencing against a model service that acks every published
// coordination point in publish order (every delivered frame). Ops: publish a
// point, advance one service cursor and inject its next ack, inject a wrong-pair
// ack, run one serviceControlWork tick.
//
// Invariants each step: waits complete in FIFO order only when every service
// has acked the first wait pair; ExpectedServiceAckID advances once per
// completed wait; PendingServiceAckWaits matches the model; after any wrong-pair
// injection the failure marks once and no later wait completes.
func TestServiceControl_waitSequencing_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		serviceCount := rapid.IntRange(1, 3).Draw(rt, "serviceCount")
		a := NewAgent(AgentConfig{ServiceCount: serviceCount})
		// Outer t: rapid.T is not testing.TB (missing ArtifactDir).
		p, _ := boundServiceProxy(t)
		a.SetServiceProxy(p)
		q := &QueueConsensusIngress{}
		a.SetServiceIngress(q)

		// modelWaits is the FIFO of published positions still unconsumed.
		modelWaits := make([]int64, 0)
		// delivered is the full publish-order stream each service walks.
		delivered := make([]int64, 0)
		// cursor[sid] is how many delivered frames service sid has acked.
		cursor := make([]int, serviceCount)
		expectedID := int64(0)
		// failed latches after ErrServiceAckOutOfSequence (marks once). Model
		// then expects no further wait progress.
		failed := false

		assertInvariants := func() {
			rt.Helper()
			require.Equal(rt, len(modelWaits), a.PendingServiceAckWaits())
			require.Equal(rt, expectedID, a.ExpectedServiceAckID())
			if len(modelWaits) == 0 {
				require.Equal(rt, int64(nullValue), a.ExpectedAckPosition())
			} else {
				require.Equal(rt, modelWaits[0], a.ExpectedAckPosition())
			}
			if failed {
				require.ErrorIs(rt, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
			} else {
				require.NoError(rt, a.ServiceAckFailure())
			}
		}

		// everyServiceAckedHead reports whether every service has acked index
		// expectedID (the first wait pair).
		everyServiceAckedHead := func() bool {
			if len(modelWaits) == 0 || failed {
				return false
			}
			for sid := range serviceCount {
				if cursor[sid] <= int(expectedID) {
					return false
				}
			}
			return true
		}

		nOps := rapid.IntRange(1, 40).Draw(rt, "nOps")
		for range nOps {
			kinds := []serviceControlOpKind{
				serviceControlOpPublish,
				serviceControlOpAckNext,
				serviceControlOpWork,
			}
			// Wrong-pair only until failure marks once.
			if !failed {
				kinds = append(kinds, serviceControlOpWrongAck)
			}
			kind := rapid.SampledFrom(kinds).Draw(rt, "kind")

			switch kind {
			case serviceControlOpPublish:
				pos := rapid.Int64Range(1, 1_000_000).Draw(rt, "pos")
				for slices.Contains(delivered, pos) {
					pos = rapid.Int64Range(1, 1_000_000).Draw(rt, "posRetry")
				}
				if failed {
					err := a.RequestServiceAck(pos)
					require.ErrorIs(rt, err, ErrServiceAckOutOfSequence)
				} else {
					require.NoError(rt, a.RequestServiceAck(pos))
					modelWaits = append(modelWaits, pos)
					delivered = append(delivered, pos)
				}

			case serviceControlOpAckNext:
				if failed || len(delivered) == 0 {
					assertInvariants()
					continue
				}
				sid := rapid.IntRange(0, serviceCount-1).Draw(rt, "sid")
				if cursor[sid] >= len(delivered) {
					assertInvariants()
					continue
				}
				idx := cursor[sid]
				q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
					LogPosition: delivered[idx],
					AckID:       int64(idx),
					RelevantID:  int64(sid + 1),
					ServiceID:   int32(sid),
				}))
				require.Equal(rt, 1, a.pollServiceControl())
				cursor[sid]++

			case serviceControlOpWrongAck:
				// Need a first wait and an empty service-0 queue so the wrong
				// pair becomes the first entry (not behind a matching ack).
				if len(modelWaits) == 0 || a.ServiceAckQueueDepth(0) != 0 {
					assertInvariants()
					continue
				}
				// Poison service 0 first entry so hasReached hits a mismatch
				// before any empty lower-id queue. Wait site marks once.
				wrongPos := rapid.Int64Range(1_000_001, 2_000_000).Draw(rt, "wrongPos")
				q.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
					LogPosition: wrongPos,
					AckID:       expectedID,
					RelevantID:  99,
					ServiceID:   0,
				}))
				require.Equal(rt, 1, a.pollServiceControl())
				// Receive-time check uses first-entry position (wrongPos). With
				// empty higher services it returns partial, no mark. Wait site
				// checks the first wait position and marks.
				_ = a.serviceControlWork()
				require.ErrorIs(rt, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
				failed = true

			case serviceControlOpWork:
				beforeWaits := a.PendingServiceAckWaits()
				beforeID := a.ExpectedServiceAckID()
				work := a.serviceControlWork()
				if !failed && everyServiceAckedHead() {
					require.Equal(rt, 1, work)
					require.Equal(rt, beforeWaits-1, a.PendingServiceAckWaits())
					require.Equal(rt, beforeID+1, a.ExpectedServiceAckID())
					modelWaits = modelWaits[1:]
					expectedID++
				} else {
					// Marked failure or partial first entries: no wait completes.
					require.Equal(rt, beforeWaits, a.PendingServiceAckWaits())
					require.Equal(rt, beforeID, a.ExpectedServiceAckID())
					require.Equal(rt, 0, work)
				}
			}
			assertInvariants()
		}
	})
}
