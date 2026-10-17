//go:build testhooks

package election_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/consensus"
)

// Asserts HostAdapter AddLogPublication forwards the agent session id and
// reseed error.
func TestHostAdapter_addLogPublicationForwardsReseedFailure(t *testing.T) {
	t.Parallel()

	members := hostAdapterMembers(3)
	host, agent, _, _ := newTestHostAdapter(t, members, 0)

	publisher := consensus.NewLogPublisher()
	publisher.SetPublication(aeron.NewTestExclusivePublicationForHooks(t))
	agent.SetLogPublisher(publisher)
	agent.SetLogRecordingID(1)

	cause := errors.New("reseed failed")
	agent.SetLogPublicationReseed(func(int64) (int32, error) { return 0, cause })

	target := publisher.Position() + 64
	sessionID, err := host.AddLogPublication(target)
	require.ErrorIs(t, err, cause)
	require.Equal(t, publisher.SessionID(), sessionID)
}
