package sim

import "errors"

// LossyParams configure drop probability and extra latency range.
type LossyParams struct {
	DropProb   float64
	LatencyMin int64
	LatencyMax int64
}

// LossyNetworkNemesis owns a LossyPolicy and handles set_loss actions.
//
// Construct with NewLossyNetworkNemesis; the zero value has no policy.
type LossyNetworkNemesis struct {
	Policy *LossyPolicy
}

// NewLossyNetworkNemesis returns a nemesis with the given initial loss policy.
func NewLossyNetworkNemesis(p LossyPolicy) *LossyNetworkNemesis {
	return &LossyNetworkNemesis{Policy: &p}
}

// OnBuild installs the lossy policy as the inner network policy.
//
// Chains after any existing non-default inner (e.g. PartitionPolicy) so
// partition+lossy can coexist and cut drops still win.
func (n *LossyNetworkNemesis) OnBuild(sim *ElectionSim) error {
	if n.Policy == nil {
		return errors.New("sim: LossyNetworkNemesis has no policy; use NewLossyNetworkNemesis")
	}
	inner := sim.InnerNetworkPolicy()
	if _, allowAll := inner.(AllowAllPolicy); allowAll {
		sim.SetNetworkPolicy(n.Policy)
		return nil
	}
	sim.SetNetworkPolicy(PolicyChain{inner, n.Policy})
	return nil
}

// OnEvent handles set_loss; unknown actions no-op.
func (n *LossyNetworkNemesis) OnEvent(_ *ElectionSim, ev ScheduledEvent) error {
	payload, ok := ev.Payload.(NemesisPayload)
	if !ok || payload.Action != ActionSetLoss {
		return nil
	}
	params, err := payloadAs[LossyParams](payload.Data, ActionSetLoss)
	if err != nil {
		return err
	}
	n.Policy.DropProb = params.DropProb
	n.Policy.LatencyMin = params.LatencyMin
	n.Policy.LatencyMax = params.LatencyMax
	return nil
}

func (*LossyNetworkNemesis) ownedActions() []string {
	return []string{ActionSetLoss}
}

var (
	_ Nemesis     = (*LossyNetworkNemesis)(nil)
	_ actionOwner = (*LossyNetworkNemesis)(nil)
)
