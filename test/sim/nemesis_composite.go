package sim

import "fmt"

// CompositeNemesis routes OnBuild/OnEvent to children in registration order.
//
// Duplicate Action ownership among children is rejected at OnBuild.
type CompositeNemesis struct {
	children []Nemesis
}

// NewCompositeNemesis builds an ordered composite of children.
func NewCompositeNemesis(children ...Nemesis) *CompositeNemesis {
	return &CompositeNemesis{children: append([]Nemesis(nil), children...)}
}

// OnBuild validates exclusive Action ownership then builds each child.
func (c *CompositeNemesis) OnBuild(sim *ElectionSim) error {
	if err := c.checkActionOwnership(); err != nil {
		return err
	}
	for _, child := range c.children {
		if child == nil {
			continue
		}
		if err := child.OnBuild(sim); err != nil {
			return err
		}
	}
	return nil
}

// OnEvent dispatches to each child in order; first error wins.
func (c *CompositeNemesis) OnEvent(sim *ElectionSim, ev ScheduledEvent) error {
	for _, child := range c.children {
		if child == nil {
			continue
		}
		if err := child.OnEvent(sim, ev); err != nil {
			return err
		}
	}
	return nil
}

func (c *CompositeNemesis) checkActionOwnership() error {
	owner := make(map[string]Nemesis)
	for _, child := range c.children {
		if child == nil {
			continue
		}
		for _, action := range actionsOwned(child) {
			if prev, ok := owner[action]; ok {
				return fmt.Errorf(
					"sim: composite duplicate action %q owned by %T and %T",
					action, prev, child,
				)
			}
			owner[action] = child
		}
	}
	return nil
}

// actionOwner reports the NemesisPayload actions a nemesis consumes;
// CompositeNemesis uses it to reject duplicate ownership.
type actionOwner interface {
	ownedActions() []string
}

func actionsOwned(n Nemesis) []string {
	if owner, ok := n.(actionOwner); ok {
		return owner.ownedActions()
	}
	return nil
}

func (c *CompositeNemesis) ownedActions() []string {
	var out []string
	for _, child := range c.children {
		if child == nil {
			continue
		}
		out = append(out, actionsOwned(child)...)
	}
	return out
}

var (
	_ Nemesis     = (*CompositeNemesis)(nil)
	_ actionOwner = (*CompositeNemesis)(nil)
)
