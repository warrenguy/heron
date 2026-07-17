package sim

import "fmt"

// SkewSpec advances epoch clocks on listed nodes by DeltaMs.
//
// Targeting: NodeIDs when non-empty; else if All, every node; else Payload.NodeID
// alone (including member id 0 - never treat 0 as "unset").
type SkewSpec struct {
	DeltaMs int64
	NodeIDs []int32
	All     bool
}

// ClockSkewNemesis applies AdvanceEpoch on target nodes.
type ClockSkewNemesis struct{}

// OnBuild is a no-op.
func (ClockSkewNemesis) OnBuild(*ElectionSim) error { return nil }

// OnEvent handles skew; unknown actions no-op.
// Unknown or duplicate node IDs error before any advance (no partial skew).
func (ClockSkewNemesis) OnEvent(sim *ElectionSim, ev ScheduledEvent) error {
	payload, ok := ev.Payload.(NemesisPayload)
	if !ok || payload.Action != ActionSkew {
		return nil
	}
	spec, err := payloadAs[SkewSpec](payload.Data, ActionSkew)
	if err != nil {
		return err
	}

	ids := spec.NodeIDs
	if len(ids) == 0 {
		if spec.All {
			for _, node := range sim.Nodes() {
				ids = append(ids, node.id)
			}
		} else {
			ids = []int32{payload.NodeID}
		}
	}

	nodes := make([]*Node, len(ids))
	seen := make(map[int32]struct{}, len(ids))
	for i, id := range ids {
		if _, dup := seen[id]; dup {
			return fmt.Errorf("sim: skew duplicate node %d", id)
		}
		seen[id] = struct{}{}
		node := sim.Node(id)
		if node == nil {
			return fmt.Errorf("sim: skew unknown node %d", id)
		}
		nodes[i] = node
	}
	for _, node := range nodes {
		node.clock.AdvanceEpoch(spec.DeltaMs)
	}
	return nil
}

func (ClockSkewNemesis) ownedActions() []string {
	return []string{ActionSkew}
}

var (
	_ Nemesis     = ClockSkewNemesis{}
	_ actionOwner = ClockSkewNemesis{}
)
