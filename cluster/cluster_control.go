package cluster

import (
	"encoding/binary"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/aeron/cnc"
	"github.com/warrenguy/luas/cluster/consensus"
)

const (
	clusterControlToggleLabel = "Cluster control toggle - clusterId=0"
	nodeControlToggleLabel    = "Node control toggle - clusterId=0"
)

// addClusterControlToggle returns a toggle that reads INACTIVE. The driver
// zeroes a new counter value.
func addClusterControlToggle(client *aeron.Client) (*aeron.Counter, error) {
	return addClusterCounter(
		client,
		consensus.ClusterControlToggleCounterTypeID,
		clusterControlToggleLabel,
		"cluster control toggle",
	)
}

func addNodeControlToggle(client *aeron.Client) (*aeron.Counter, error) {
	return addClusterCounter(
		client,
		consensus.NodeControlToggleCounterTypeID,
		nodeControlToggleLabel,
		"node control toggle",
	)
}

// Toggle is a toggle counter in a counters reader. Write it with
// [consensus.ControlToggleState.Toggle] or
// [consensus.NodeControlToggleState.Toggle].
//
// Behaviour: a value the module does not act on blocks later toggles. Store
// NEUTRAL to clear the value.
//
// Concurrency: safe from any goroutine. Other processes write the same value.
//
// Lifetime: valid while the module that added the counter and the reader's
// mapping stay open. Find the toggle again after the module restarts.
type Toggle struct {
	counterID int32
	reader    *cnc.CountersReader
}

var _ consensus.ToggleCounter = Toggle{}

func (t Toggle) CounterID() int32 { return t.counterID }

// Load returns the toggle value, or 0 when the counter id is out of the
// reader's range.
func (t Toggle) Load() int64 {
	value, err := t.reader.CounterValue(t.counterID)
	if err != nil {
		return 0
	}
	return value
}

// CompareAndSet stores update when the value reads expect. The swap is
// sequentially consistent.
//
// Returns false when the value differs or the counter id is out of the
// reader's range.
func (t Toggle) CompareAndSet(expect, update int64) bool {
	swapped, err := t.reader.CompareAndSetCounterValue(t.counterID, expect, update)
	return err == nil && swapped
}

// Store writes value with release order.
//
// Does not store when the counter id is out of the reader's range.
func (t Toggle) Store(value int64) {
	_ = t.reader.SetCounterValue(t.counterID, value)
}

// FindControlToggle returns the cluster control toggle for clusterID.
// ElectionComplete sets the toggle to NEUTRAL on a leader. A new election
// sets it to INACTIVE.
//
// Behaviour: returns the first allocated toggle whose key holds clusterID.
// Members that share one driver share cluster id 0, so the find returns one
// of them.
//
// Returns false when the reader holds no matching toggle.
func FindControlToggle(reader *cnc.CountersReader, clusterID int32) (Toggle, bool) {
	return findToggle(reader, consensus.ClusterControlToggleCounterTypeID, clusterID)
}

// FindNodeControlToggle returns the node control toggle for clusterID.
//
// Behaviour: returns the first allocated toggle whose key holds clusterID.
// Members that share one driver share cluster id 0, so the find returns one
// of them.
//
// Returns false when the reader holds no matching toggle.
func FindNodeControlToggle(reader *cnc.CountersReader, clusterID int32) (Toggle, bool) {
	return findToggle(reader, consensus.NodeControlToggleCounterTypeID, clusterID)
}

func findToggle(reader *cnc.CountersReader, typeID, clusterID int32) (Toggle, bool) {
	var prefix [4]byte
	//nolint:gosec // G115: the key holds the cluster id bits as written
	binary.LittleEndian.PutUint32(prefix[:], uint32(clusterID))
	counterID, ok := reader.FindCounterOfTypeWithKeyPrefix(typeID, prefix[:])
	if !ok {
		return Toggle{}, false
	}
	return Toggle{counterID: counterID, reader: reader}, true
}
