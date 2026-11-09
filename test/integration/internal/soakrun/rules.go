package soakrun

import "fmt"

const (
	faultChain    = "bridge faults forward"
	addRulePrefix = "add rule " + faultChain + " ether type ip "
)

// Rules returns the nftables commands for step on memberIndex.
//
// Rules returns nil for a kill step. Packet loss with AllMembers ignores
// memberIndex.
func Rules(step Step, memberIndex int) []string {
	switch step.Kind {
	case Partition:
		return portRules(memberIndex, "drop")

	case PacketLoss:
		statement := fmt.Sprintf("numgen random mod 100 < %d drop", step.LossPercent)

		if step.LossScope == AllMembers {
			return []string{addRulePrefix + statement}
		}
		return portRules(memberIndex, statement)

	case MemberKill, ArchiveKill, DriverKill:
		return nil

	default:
		return nil
	}
}

func HealRules() []string {
	return []string{"flush chain " + faultChain}
}

func portRules(memberIndex int, statement string) []string {
	port := BridgePort(memberIndex)
	return []string{
		addRulePrefix + "iifname " + port + " " + statement,
		addRulePrefix + "oifname " + port + " " + statement,
	}
}

func BridgePort(memberIndex int) string {
	return fmt.Sprintf("m%d-br", memberIndex)
}
