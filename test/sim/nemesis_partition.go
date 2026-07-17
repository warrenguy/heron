package sim

import (
	"errors"
	"fmt"
)

// PartitionSpec describes edges to cut or heal.
type PartitionSpec struct {
	// From/To is one directed edge. When Bidirectional is true both directions
	// are cut/healed; From and To still name the pair.
	From          int32
	To            int32
	Bidirectional bool
}

// PartitionNemesis owns a PartitionPolicy and handles partition/heal actions.
//
// Construct with NewPartitionNemesis; the zero value has no policy.
type PartitionNemesis struct {
	Policy *PartitionPolicy
}

// NewPartitionNemesis returns a nemesis with an empty partition policy.
func NewPartitionNemesis() *PartitionNemesis {
	return &PartitionNemesis{Policy: NewPartitionPolicy()}
}

// OnBuild installs the partition policy as the inner network policy.
//
// When an existing non-default inner policy is present, partition is chained
// first so cut drops still win.
func (n *PartitionNemesis) OnBuild(sim *ElectionSim) error {
	if n.Policy == nil {
		return errors.New("sim: PartitionNemesis has no policy; use NewPartitionNemesis")
	}
	inner := sim.InnerNetworkPolicy()
	if _, allowAll := inner.(AllowAllPolicy); allowAll {
		sim.SetNetworkPolicy(n.Policy)
		return nil
	}
	sim.SetNetworkPolicy(PolicyChain{n.Policy, inner})
	return nil
}

// OnEvent handles partition and heal; unknown actions no-op.
// Unknown members and self edges error before any policy mutation.
func (n *PartitionNemesis) OnEvent(sim *ElectionSim, ev ScheduledEvent) error {
	payload, ok := ev.Payload.(NemesisPayload)
	if !ok || (payload.Action != ActionPartition && payload.Action != ActionHeal) {
		return nil
	}
	spec, err := payloadAs[PartitionSpec](payload.Data, payload.Action)
	if err != nil {
		return err
	}
	for _, id := range []int32{spec.From, spec.To} {
		if sim.Node(id) == nil {
			return fmt.Errorf("sim: %s unknown node %d", payload.Action, id)
		}
	}
	if spec.From == spec.To {
		return fmt.Errorf("sim: %s self edge node %d", payload.Action, spec.From)
	}
	switch payload.Action {
	case ActionPartition:
		if spec.Bidirectional {
			n.Policy.CutBidirectional(spec.From, spec.To)
		} else {
			n.Policy.Cut(spec.From, spec.To)
		}
	case ActionHeal:
		if spec.Bidirectional {
			n.Policy.HealBidirectional(spec.From, spec.To)
		} else {
			n.Policy.Heal(spec.From, spec.To)
		}
	}
	return nil
}

func (*PartitionNemesis) ownedActions() []string {
	return []string{ActionPartition, ActionHeal}
}

var (
	_ Nemesis     = (*PartitionNemesis)(nil)
	_ actionOwner = (*PartitionNemesis)(nil)
)
