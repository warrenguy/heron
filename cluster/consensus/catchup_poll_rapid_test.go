//go:build rapid && testhooks

package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
)

// catchupPollModel is the reference model for TestCatchupPoll_bound_rapid.
type catchupPollModel struct {
	starts        []int64
	ends          []int64
	fragmentLimit int

	next     int   // next undelivered frame
	position int64 // adapter position

	lastPublished int64
	lastSendNs    int64
	lastUpdateNs  int64
}

// poll delivers frames that start below bound, at most fragmentLimit of them.
// Returns the delivered frame count.
func (m *catchupPollModel) poll(bound int64) int {
	delivered := 0
	for m.next < len(m.starts) && delivered < m.fragmentLimit && m.starts[m.next] < bound {
		m.position = m.ends[m.next]
		m.next++
		delivered++
	}
	return delivered
}

// isPublishDue reports whether a ready read of recorded publishes at nowNs.
func (m *catchupPollModel) isPublishDue(recorded, nowNs, intervalNs int64) bool {
	return recorded > m.lastPublished || nowNs >= m.lastSendNs+intervalNs
}

// TestCatchupPoll_bound_rapid checks CatchupPoll against a reference model over
// random frame layouts, limits, recording lifecycles and tick gaps. Recorded
// and limit positions sit on frame ends, as archive and commit positions do.
//
// Properties per tick:
//   - a ready read polls to min(limit, recorded) and publishes recorded,
//     whatever the recording state
//   - a not-ready read after a recording stop fails and holds the adapter
//   - any other not-ready read holds the adapter
//   - with a source, a tick past the progress timeout fails
//   - commit equals the adapter position and never passes the bound
func TestCatchupPoll_bound_rapid(t *testing.T) {
	t.Parallel()
	const (
		recordingID int64 = 11
		termID      int64 = 3
		intervalNs  int64 = 5
	)
	rapid.Check(t, func(rt *rapid.T) {
		fragmentLimit := rapid.IntRange(1, 4).Draw(rt, "fragmentLimit")
		adapter, image := boundLogAdapterWithLimit(t, fragmentLimit)

		// Contiguous unfragmented frames from position 0. Trailing filler
		// varies the frame length.
		frameCount := rapid.IntRange(1, 10).Draw(rt, "frameCount")
		m := &catchupPollModel{fragmentLimit: fragmentLimit}
		offset := 0
		for i := range frameCount {
			payload := encodeLogAdapterSessionMessagePayload(t, codecs.SessionMessageHeaderFields{
				ClusterSessionID: int64(i + 1),
			})
			filler := rapid.IntRange(0, 96).Draw(rt, "filler")
			payload = append(payload, make([]byte, filler)...)
			writeLogAdapterUnfragmentedFrame(t, image, offset, payload)
			m.starts = append(m.starts, int64(offset))
			offset += logbuffer.AlignFragmentChunkLength(len(payload))
			m.ends = append(m.ends, int64(offset))
		}
		boundaries := append([]int64{0}, m.ends...)
		drawBoundary := func(label string, floor int64) int64 {
			drawn := rapid.SampledFrom(boundaries).Draw(rt, label)
			return max(floor, drawn)
		}

		hasSource := rapid.Bool().Draw(rt, "hasSource")
		timeoutNs := rapid.Int64Range(4, 40).Draw(rt, "timeoutNs")
		source := &scriptedAppendSource{}
		control := &fakeLogRecordingControl{active: true}
		eg := &recordingEgress{}
		cfg := AgentConfig{
			Role:                      RoleFollower,
			Commit:                    new(atomic.Int64),
			LogAdapter:                adapter,
			Egress:                    eg,
			LeadershipTermID:          termID,
			LeaderHeartbeatIntervalNs: intervalNs,
			LeaderHeartbeatTimeoutNs:  timeoutNs,
			LeaderMemberID:            1,
		}
		if hasSource {
			cfg.AppendSource = source
		}
		a := NewAgent(cfg)
		a.SetLogRecordingID(recordingID)
		a.SetLogRecordingControl(control)
		a.CatchupInitiated(0)

		limit := int64(0)
		recorded := int64(0)
		nowNs := int64(0)
		ticks := rapid.IntRange(1, 30).Draw(rt, "ticks")
		for range ticks {
			nowNs += rapid.Int64Range(1, 3).Draw(rt, "stepNs")
			limit = drawBoundary("limit", limit)

			// Recording lifecycle: an active recording may stop. A stopped
			// recording may extend.
			if rapid.IntRange(0, 4).Draw(rt, "lifecycle") == 0 {
				control.active = !control.active
			}
			// Ready is drawn apart from the lifecycle: a ready read wins over
			// a stopped recording.
			ready := rapid.Bool().Draw(rt, "ready")
			if ready {
				recorded = drawBoundary("recorded", recorded)
			}
			reported := int64(0)
			if ready {
				reported = recorded
			}
			source.results = append(source.results, scriptedAppendResult{reported, ready})
			eg.appends = nil

			var bound int64
			publish := false
			var wantErr error
			switch {
			case !hasSource:
				bound = limit
			case ready:
				bound = min(limit, recorded)
				publish = m.isPublishDue(recorded, nowNs, intervalNs)
			case !control.active:
				bound = m.position
				wantErr = errCatchupRecordingStopped
			default:
				bound = m.position
			}
			wantWork := 0
			if wantErr == nil {
				wantWork = m.poll(bound)
			}
			if publish {
				wantWork++
				if recorded > m.lastPublished {
					m.lastUpdateNs = nowNs
				}
				m.lastPublished = recorded
				m.lastSendNs = nowNs
			}
			if hasSource && wantErr == nil && nowNs > m.lastUpdateNs+timeoutNs {
				wantErr = errCatchupNoProgress
			}

			work, err := a.CatchupPoll(limit, nowNs)
			require.ErrorIs(rt, err, wantErr, "bound=%d", bound)
			require.Equal(rt, wantWork, work, "bound=%d", bound)
			require.Equal(rt, m.position, adapter.Position(), "bound=%d", bound)
			require.Equal(rt, m.position, a.CommitPosition())
			if hasSource {
				require.Len(rt, source.results, source.reads, "one source read per tick")
			}

			// Limit and recorded never fall, so every earlier bound sits
			// below them too.
			require.LessOrEqual(rt, a.CommitPosition(), limit)
			if hasSource {
				require.LessOrEqual(rt, a.CommitPosition(), recorded)
			}

			if publish {
				require.Equal(rt, []appendPublishCall{
					{term: termID, pos: recorded, flags: AppendPositionFlagCatchup},
				}, eg.appends)
			} else {
				require.Empty(rt, eg.appends)
			}
		}
	})
}
