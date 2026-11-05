package cluster

import (
	"fmt"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/consensus"
)

const moduleStateCounterLabel = "Consensus Module state - clusterId=0"

// clusterIDCounterKey holds cluster id 0 as a little-endian int32.
var clusterIDCounterKey = []byte{0, 0, 0, 0}

func addClusterCounter(
	client *aeron.Client,
	typeID int32,
	label, name string,
) (
	*aeron.Counter,
	error,
) {
	counter, err := client.AddCounter(typeID, clusterIDCounterKey, label)
	if err != nil {
		return nil, fmt.Errorf("cluster: add %s counter: %w", name, err)
	}
	return counter, nil
}

func addModuleStateCounter(client *aeron.Client) (*aeron.Counter, error) {
	return addClusterCounter(
		client,
		consensus.ClusterConsensusModuleStateCounterTypeID,
		moduleStateCounterLabel,
		"module state",
	)
}

type moduleStateCounter struct {
	counterID int32
	reader    *cnc.CountersReader
}

// StoreModuleState does not store the state code when counterID is out of
// range. OpenConsensusModule wires an allocated counterID.
func (c moduleStateCounter) StoreModuleState(state consensus.ModuleState) {
	_ = c.reader.SetCounterValue(c.counterID, int64(state.Code()))
}
