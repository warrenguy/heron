//go:build testhooks

package consensus

func (a *Agent) ClusterControlToggleForTest() ToggleCounter {
	return a.controlToggle
}

func (a *Agent) NodeControlToggleForTest() ToggleCounter {
	return a.nodeControlToggle
}
