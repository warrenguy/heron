package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/aeron/cnc"
	"luas.fast/cluster/consensus"
)

func TestToggleCounterKeysAndLabels(t *testing.T) {
	t.Parallel()

	require.Equal(t, []byte{0x00, 0x00, 0x00, 0x00}, clusterIDCounterKey)
	require.Equal(t, "Cluster control toggle - clusterId=0", clusterControlToggleLabel)
	require.Equal(t, "Node control toggle - clusterId=0", nodeControlToggleLabel)
}

func TestFindControlToggle(t *testing.T) {
	t.Parallel()

	const counterCount = 5
	meta := make([]byte, counterCount*cnc.CounterMetadataLength)
	values := make([]byte, counterCount*cnc.CounterValueLength)
	records := []struct {
		counterID int32
		typeID    int32
		key       []byte
		value     int64
	}{
		{0, consensus.ClusterConsensusModuleStateCounterTypeID, []byte{0, 0, 0, 0}, 1},
		{1, consensus.ClusterControlToggleCounterTypeID, []byte{1, 0, 0, 0}, 3},
		{2, consensus.ClusterControlToggleCounterTypeID, []byte{0, 0, 0, 0}, 1},
		{3, consensus.NodeControlToggleCounterTypeID, []byte{0, 0, 0, 0}, 1},
		{4, consensus.NodeControlToggleCounterTypeID, []byte{1, 0, 0, 0}, 2},
	}
	for _, record := range records {
		require.NoError(t, cnc.WriteCounterRecordWithKey(
			meta,
			values,
			record.counterID,
			cnc.CounterRecordAllocated,
			record.typeID,
			record.key,
			"toggle",
			int64(record.counterID),
			record.value,
		))
	}
	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	tests := []struct {
		name          string
		find          func(*cnc.CountersReader, int32) (Toggle, bool)
		clusterID     int32
		wantCounterID int32
		wantValue     int64
	}{
		{"control cluster 0", FindControlToggle, 0, 2, 1},
		{"control cluster 1", FindControlToggle, 1, 1, 3},
		{"node cluster 0", FindNodeControlToggle, 0, 3, 1},
		{"node cluster 1", FindNodeControlToggle, 1, 4, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			toggle, ok := tt.find(reader, tt.clusterID)
			require.True(t, ok)
			require.Equal(t, tt.wantCounterID, toggle.CounterID())
			require.Equal(t, tt.wantValue, toggle.Load())

			require.False(t, toggle.CompareAndSet(tt.wantValue+1, 7))
			require.Equal(t, tt.wantValue, toggle.Load())
			require.True(t, toggle.CompareAndSet(tt.wantValue, 7))
			require.Equal(t, int64(7), toggle.Load())
			toggle.Store(tt.wantValue)
			require.Equal(t, tt.wantValue, toggle.Load())
		})
	}

	t.Run("state writes through the toggle", func(t *testing.T) {
		toggle, ok := FindControlToggle(reader, 0)
		require.True(t, ok)
		require.True(t, consensus.ControlToggleSuspend.Toggle(toggle))
		require.Equal(t, int64(consensus.ControlToggleSuspend), toggle.Load())
		require.False(t, consensus.ControlToggleSnapshot.Toggle(toggle))
		require.True(t, consensus.ControlToggleResume.Toggle(toggle))
		require.Equal(t, int64(consensus.ControlToggleResume), toggle.Load())
		toggle.Store(int64(consensus.ControlToggleNeutral))
	})

	t.Run("missing", func(t *testing.T) {
		_, ok := FindControlToggle(reader, 2)
		require.False(t, ok)
		_, ok = FindNodeControlToggle(nil, 0)
		require.False(t, ok)
	})

	t.Run("counter id out of range", func(t *testing.T) {
		toggle := Toggle{counterID: counterCount, reader: reader}
		require.Equal(t, int64(0), toggle.Load())
		require.False(t, toggle.CompareAndSet(0, 1))
		toggle.Store(1)
		require.Equal(t, int64(0), toggle.Load())
	})
}
