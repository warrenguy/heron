package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election"
)

func TestElectionStateFromCode_valid(t *testing.T) {
	t.Parallel()

	for code := range int64(18) {
		got, err := election.ElectionStateFromCode(code)
		require.NoError(t, err)
		require.Equal(t, code, int64(got))
	}
}

func TestElectionStateFromCode_invalid(t *testing.T) {
	t.Parallel()

	for _, code := range []int64{-1, 18, 99} {
		_, err := election.ElectionStateFromCode(code)
		require.Error(t, err)
	}
}

func TestElectionStateClosed_code(t *testing.T) {
	t.Parallel()
	require.Equal(t, int32(17), election.ElectionStateClosed.Code())
}

func TestElectionState_nameCount(t *testing.T) {
	t.Parallel()
	require.Equal(t, 18, election.ElectionStateNameCount())
}

func TestElectionState_string(t *testing.T) {
	t.Parallel()

	want := []string{
		"INIT", "CANVASS", "NOMINATE", "CANDIDATE_BALLOT", "FOLLOWER_BALLOT",
		"LEADER_LOG_REPLICATION", "LEADER_REPLAY", "LEADER_INIT", "LEADER_READY",
		"FOLLOWER_LOG_REPLICATION", "FOLLOWER_REPLAY",
		"FOLLOWER_CATCHUP_INIT", "FOLLOWER_CATCHUP_AWAIT", "FOLLOWER_CATCHUP",
		"FOLLOWER_LOG_INIT", "FOLLOWER_LOG_AWAIT", "FOLLOWER_READY", "CLOSED",
	}
	for i, name := range want {
		require.Equal(t, name, election.ElectionState(i).String())
	}
	require.Equal(t, "ElectionState(-1)", election.ElectionState(-1).String())
	require.Equal(t, "ElectionState(99)", election.ElectionState(99).String())
}
