package sim

import "fmt"

// Nemesis injects faults into ElectionSim via EventNemesis pops only.
type Nemesis interface {
	OnBuild(sim *ElectionSim) error
	OnEvent(sim *ElectionSim, ev ScheduledEvent) error
}

// payloadAs decodes nemesis event Data as T.
func payloadAs[T any](data any, action string) (T, error) {
	var zero T
	if data == nil {
		return zero, fmt.Errorf("sim: %s payload missing data", action)
	}
	v, ok := data.(T)
	if !ok {
		return zero, fmt.Errorf("sim: %s payload type %T", action, data)
	}
	return v, nil
}

// NemesisPayload is the EventNemesis payload for ScheduleNemesis.
type NemesisPayload struct {
	Action string
	NodeID int32
	Data   any
}

// Action strings for concrete nemesis types.
const (
	ActionPartition = "partition"
	ActionHeal      = "heal"
	ActionSetLoss   = "set_loss"
	ActionSkew      = "skew"
	ActionCrash     = "crash"
	ActionRestart   = "restart"
)

// NoopNemesis is the default nemesis with no fault injection.
type NoopNemesis struct{}

func (NoopNemesis) OnBuild(*ElectionSim) error { return nil }

func (NoopNemesis) OnEvent(*ElectionSim, ScheduledEvent) error { return nil }

// DefaultNemesis returns the no-op nemesis.
func DefaultNemesis() Nemesis { return NoopNemesis{} }

var _ Nemesis = NoopNemesis{}
