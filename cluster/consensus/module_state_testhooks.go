//go:build testhooks

package consensus

func (a *Agent) ModuleStateCounterForTest() ModuleStateCounter {
	return a.moduleStateCounter
}
