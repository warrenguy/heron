package election

import "fmt"

// ElectionState is the cluster election phase written to
// CLUSTER_ELECTION_STATE_TYPE_ID (207). Mirrors aeron ElectionState.
type ElectionState int32

const (
	ElectionStateInit ElectionState = iota // 0
	ElectionStateCanvass
	ElectionStateNominate
	ElectionStateCandidateBallot
	ElectionStateFollowerBallot
	ElectionStateLeaderLogReplication
	ElectionStateLeaderReplay
	ElectionStateLeaderInit
	ElectionStateLeaderReady
	ElectionStateFollowerLogReplication
	ElectionStateFollowerReplay
	ElectionStateFollowerCatchupInit
	ElectionStateFollowerCatchupAwait
	ElectionStateFollowerCatchup
	ElectionStateFollowerLogInit
	ElectionStateFollowerLogAwait
	ElectionStateFollowerReady
	ElectionStateClosed // 17
)

var electionStateNames = [...]string{
	"INIT", "CANVASS", "NOMINATE", "CANDIDATE_BALLOT", "FOLLOWER_BALLOT",
	"LEADER_LOG_REPLICATION", "LEADER_REPLAY", "LEADER_INIT", "LEADER_READY",
	"FOLLOWER_LOG_REPLICATION", "FOLLOWER_REPLAY",
	"FOLLOWER_CATCHUP_INIT", "FOLLOWER_CATCHUP_AWAIT", "FOLLOWER_CATCHUP",
	"FOLLOWER_LOG_INIT", "FOLLOWER_LOG_AWAIT", "FOLLOWER_READY", "CLOSED",
}

const electionStateCount = len(electionStateNames)

func (s ElectionState) Code() int32 { return int32(s) }

func (s ElectionState) String() string {
	if s < 0 || int(s) >= len(electionStateNames) {
		return fmt.Sprintf("ElectionState(%d)", int32(s))
	}
	return electionStateNames[s]
}

// ElectionStateFromCode decodes a counter value. Valid codes are 0-17.
func ElectionStateFromCode(code int64) (ElectionState, error) {
	if code < 0 || code >= int64(electionStateCount) {
		return 0, fmt.Errorf("election: invalid election state counter code: %d", code)
	}
	return ElectionState(code), nil
}
