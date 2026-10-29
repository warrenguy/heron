package cluster

import (
	"fmt"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// Snapshots[0] is the consensus module snapshot. The later rows hold service
// ids 0 through n-1 in order, as [OpenRecovery] checks.
func recoveryStateKey(plan recordinglog.RecoveryPlan) codecs.RecoveryStateKeyFields {
	if len(plan.Snapshots) == 0 {
		return codecs.RecoveryStateKeyFields{
			LeadershipTermID: cnc.NullValue,
			LogPosition:      0,
			Timestamp:        0,
			ClusterID:        0,
		}
	}

	head := plan.Snapshots[0]
	var ids []int64
	for _, row := range plan.Snapshots[1:] {
		ids = append(ids, row.RecordingID)
	}
	return codecs.RecoveryStateKeyFields{
		LeadershipTermID:     head.LeadershipTermID,
		LogPosition:          head.LogPosition,
		Timestamp:            head.Timestamp,
		ClusterID:            0,
		SnapshotRecordingIDs: ids,
	}
}

func addRecoveryStateCounter(
	client *aeron.Client,
	plan recordinglog.RecoveryPlan,
) (*aeron.Counter, error) {
	fields := recoveryStateKey(plan)
	key := make([]byte, cnc.CounterMetadataMaxKeyLength)
	end, ok := codecs.EncodeRecoveryStateKey(key, 0, fields)
	if !ok {
		return nil, fmt.Errorf(
			"cluster: recovery state counter holds at most %d service snapshots, plan holds %d",
			codecs.RecoveryStateMaxServiceCount,
			len(fields.SnapshotRecordingIDs),
		)
	}
	label := fmt.Sprintf(
		"Cluster recovery: leadershipTermId=%d logPosition=%d clusterId=0",
		fields.LeadershipTermID,
		fields.LogPosition,
	)
	counter, err := client.AddCounter(codecs.RecoveryStateCounterTypeID, key[:end], label)
	if err != nil {
		return nil, fmt.Errorf("cluster: add recovery state counter: %w", err)
	}
	return counter, nil
}
