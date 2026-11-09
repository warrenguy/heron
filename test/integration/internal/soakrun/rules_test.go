package soakrun

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFaultRules(t *testing.T) {
	t.Parallel()

	// Phase: a partition names the member's bridge port.
	require.Equal(t, []string{
		"add rule bridge faults forward ether type ip iifname m1-br drop",
		"add rule bridge faults forward ether type ip oifname m1-br drop",
	}, Rules(Step{Kind: Partition}, 1))

	// Phase: OneMember loss names the member's bridge port.
	require.Equal(t, []string{
		"add rule bridge faults forward ether type ip iifname m1-br numgen random mod 100 < 5 drop",
		"add rule bridge faults forward ether type ip oifname m1-br numgen random mod 100 < 5 drop",
	}, Rules(Step{Kind: PacketLoss, LossPercent: 5, LossScope: OneMember}, 1))

	// Phase: AllMembers loss ignores the member index.
	require.Equal(t, []string{
		"add rule bridge faults forward ether type ip numgen random mod 100 < 5 drop",
	}, Rules(Step{Kind: PacketLoss, LossPercent: 5, LossScope: AllMembers}, 1))

	// Phase: heal flushes the fault chain.
	require.Equal(t, []string{
		"flush chain bridge faults forward",
	}, HealRules())
}
