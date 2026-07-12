package sim

// Nemesis injects faults into ElectionSim. Only NoopNemesis ships today.
type Nemesis interface {
	OnBuild(sim *ElectionSim) error
	OnEvent(sim *ElectionSim, ev ScheduledEvent) error
}

// NoopNemesis is the default nemesis with no fault injection.
type NoopNemesis struct{}

func (NoopNemesis) OnBuild(*ElectionSim) error { return nil }

func (NoopNemesis) OnEvent(*ElectionSim, ScheduledEvent) error { return nil }

// DefaultNemesis returns the no-op nemesis.
func DefaultNemesis() Nemesis { return NoopNemesis{} }

var _ Nemesis = NoopNemesis{}
