package cluster

import (
	"fmt"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/consensus"
)

const moduleStateCounterLabel = "Consensus Module state - clusterId=0"

// moduleStateCounterKey holds cluster id 0 as a little-endian int32.
var moduleStateCounterKey = []byte{0, 0, 0, 0}

func addModuleStateCounter(client *aeron.Client) (*aeron.Counter, error) {
	counter, err := client.AddCounter(
		consensus.ClusterConsensusModuleStateCounterTypeID,
		moduleStateCounterKey,
		moduleStateCounterLabel,
	)
	if err != nil {
		return nil, fmt.Errorf("cluster: add module state counter: %w", err)
	}
	return counter, nil
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
