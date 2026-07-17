//go:build testhooks

package sim

// RestartNemesis handles crash and restart actions via CrashNode/RestartNode.
type RestartNemesis struct{}

// OnBuild is a no-op.
func (RestartNemesis) OnBuild(*ElectionSim) error { return nil }

// OnEvent handles crash/restart; unknown actions no-op.
func (RestartNemesis) OnEvent(sim *ElectionSim, ev ScheduledEvent) error {
	payload, ok := ev.Payload.(NemesisPayload)
	if !ok {
		return nil
	}
	switch payload.Action {
	case ActionCrash:
		return sim.CrashNode(payload.NodeID)
	case ActionRestart:
		return sim.RestartNode(payload.NodeID)
	default:
		return nil
	}
}

func (RestartNemesis) ownedActions() []string {
	return []string{ActionCrash, ActionRestart}
}

var (
	_ Nemesis     = RestartNemesis{}
	_ actionOwner = RestartNemesis{}
)
