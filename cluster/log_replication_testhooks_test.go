//go:build testhooks

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/election"
)

type signalRecord struct {
	correlationID int64
	recordingID   int64
	position      int64
	signal        archivewire.RecordingSignal
}

// signalCapturingReplication stores each OnSignal call. RecordingID returns -1,
// so the election does not pass the id to the host.
type signalCapturingReplication struct {
	signals []signalRecord
}

func (r *signalCapturingReplication) DoWork(int64) (int, error) { return 0, nil }
func (r *signalCapturingReplication) RecordingID() int64        { return -1 }
func (r *signalCapturingReplication) Position() int64           { return -1 }
func (r *signalCapturingReplication) IsDone() bool              { return false }
func (r *signalCapturingReplication) Close()                    {}
func (r *signalCapturingReplication) IsClosed() bool            { return true }

func (r *signalCapturingReplication) OnSignal(
	correlationID, recordingID, position int64,
	signal archivewire.RecordingSignal,
) {
	r.signals = append(r.signals, signalRecord{
		correlationID: correlationID,
		recordingID:   recordingID,
		position:      position,
		signal:        signal,
	})
}

func TestConsensusModule_handleElectionRecordingSignal(t *testing.T) {
	t.Parallel()

	event := codecs.RecordingSignalEventFields{
		ControlSessionID: 4,
		CorrelationID:    77,
		RecordingID:      5,
		SubscriptionID:   6,
		Position:         4096,
		Signal:           archivewire.RecordingSignal_EXTEND,
	}

	t.Run("nil election does not panic", func(t *testing.T) {
		t.Parallel()

		module := &ConsensusModule{}
		require.NotPanics(t, func() { module.handleElectionRecordingSignal(event) })
	})

	t.Run("replicating election receives the four scalars", func(t *testing.T) {
		t.Parallel()

		fx := newFactoryFixture(t)
		e := election.NewElection(fx.base, fx.deps)
		election.SetStateForTest(e, election.ElectionStateFollowerLogReplication)
		replication := &signalCapturingReplication{}
		election.SetLogReplicationForTest(e, replication)
		module := &ConsensusModule{election: e}

		module.handleElectionRecordingSignal(event)
		require.Equal(t, []signalRecord{{
			correlationID: 77,
			recordingID:   5,
			position:      4096,
			signal:        archivewire.RecordingSignal_EXTEND,
		}}, replication.signals)
	})
}
