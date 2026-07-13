package election

import (
	"fmt"
	"time"

	"github.com/warrenguy/heron/cluster/consensus"
)

type Election struct {
	cfg  ElectionConfig
	deps ElectionDeps

	electionState                    ElectionState
	logPosition                      int64
	appendPosition                   int64
	leadershipTermID                 int64
	logLeadershipTermID              int64
	candidateTermID                  int64
	notifiedCommitPosition           int64
	catchupJoinPosition              int64
	replicationLeadershipTermID      int64
	replicationStopPosition          int64
	replicationDeadlineNs            int64
	replicationTermBaseLogPosition   int64
	leaderRecordingID                int64
	logSessionID                     int32
	gracefulClosedLeaderID           int32
	isNodeStartup                    bool
	isExtendedCanvass                bool
	isFirstInit                      bool
	isLeaderStartup                  bool
	leaderMember                     *Member
	timeOfLastStateChangeNs          int64
	timeOfLastUpdateNs               int64
	timeOfLastCommitPositionUpdateNs int64
	nominationDeadlineNs             int64
	lastPublishedCommitPosition      int64
	lastPublishedAppendPosition      int64

	clusterMembers []*Member
	memberByID     map[int32]*Member
	thisMember     *Member

	logReplay      LogReplay
	logReplication LogReplication
}

var _ consensus.ElectionRunner = (*Election)(nil)

func NewElection(cfg ElectionConfig, deps ElectionDeps) *Election {
	memberByID := validateElectionDeps(cfg, deps)

	nowNs := deps.NanoClock()
	initialTimeOfLastUpdateNs := nowNs - int64(24*time.Hour)

	e := &Election{
		cfg:  cfg,
		deps: deps,

		electionState:                    ElectionStateInit,
		clusterMembers:                   deps.Members,
		memberByID:                       memberByID,
		thisMember:                       deps.ThisMember,
		logPosition:                      cfg.LogPosition,
		appendPosition:                   cfg.AppendPosition,
		leadershipTermID:                 cfg.LeadershipTermID,
		logLeadershipTermID:              cfg.LeadershipTermID,
		candidateTermID:                  cfg.LeadershipTermID,
		notifiedCommitPosition:           0,
		catchupJoinPosition:              NullPosition,
		replicationLeadershipTermID:      NullValue,
		replicationStopPosition:          NullPosition,
		replicationTermBaseLogPosition:   NullValue,
		replicationDeadlineNs:            0,
		nominationDeadlineNs:             0,
		lastPublishedCommitPosition:      0,
		lastPublishedAppendPosition:      0,
		leaderRecordingID:                NullValue,
		logSessionID:                     int32(NullValue),
		gracefulClosedLeaderID:           cfg.GracefulClosedLeaderID,
		isNodeStartup:                    cfg.IsNodeStartup,
		isExtendedCanvass:                cfg.IsNodeStartup,
		isFirstInit:                      true,
		isLeaderStartup:                  false,
		leaderMember:                     nil,
		timeOfLastStateChangeNs:          nowNs,
		timeOfLastUpdateNs:               initialTimeOfLastUpdateNs,
		timeOfLastCommitPositionUpdateNs: initialTimeOfLastUpdateNs,
	}

	if deps.StateObserver != nil {
		deps.StateObserver.OnElectionStateChange(ElectionStateInit)
	}

	return e
}

func validateElectionDeps(cfg ElectionConfig, deps ElectionDeps) map[int32]*Member {
	if deps.Handler == nil {
		panic("election: nil Handler")
	}
	if deps.Host == nil {
		panic("election: nil Host")
	}
	if deps.Publisher == nil {
		panic("election: nil Publisher")
	}
	if deps.NanoClock == nil {
		panic("election: nil NanoClock")
	}
	if deps.EpochClock == nil {
		panic("election: nil EpochClock")
	}
	if deps.NodeState == nil {
		panic("election: nil NodeState")
	}
	if deps.ThisMember == nil {
		panic("election: nil ThisMember")
	}
	if len(deps.Members) == 0 {
		panic("election: empty Members")
	}
	memberByID := make(map[int32]*Member, len(deps.Members))
	for _, m := range deps.Members {
		if _, dup := memberByID[m.ID]; dup {
			panic("election: duplicate member id")
		}
		memberByID[m.ID] = m
	}
	if _, ok := memberByID[deps.ThisMember.ID]; !ok {
		panic("election: ThisMember not in Members")
	}
	if cfg.MemberID != deps.ThisMember.ID {
		panic("election: MemberID mismatch")
	}
	return memberByID
}

func (e *Election) DoWork(nowNs int64) int {
	switch e.electionState {
	case ElectionStateInit:
		return e.handleInit(nowNs)
	case ElectionStateCanvass:
		return e.handleCanvass(nowNs)
	case ElectionStateNominate:
		return e.handleNominate(nowNs)
	case ElectionStateCandidateBallot:
		return e.handleCandidateBallot(nowNs)
	case ElectionStateFollowerBallot:
		return e.handleFollowerBallot(nowNs)
	case ElectionStateLeaderLogReplication:
		return e.handleLeaderLogReplication(nowNs)
	case ElectionStateLeaderReplay:
		return e.handleLeaderReplay(nowNs)
	case ElectionStateLeaderInit:
		return e.handleLeaderInit(nowNs)
	case ElectionStateLeaderReady:
		return e.handleLeaderReady(nowNs)
	case ElectionStateFollowerLogReplication:
		return e.handleFollowerLogReplication(nowNs)
	case ElectionStateFollowerReplay:
		return e.handleFollowerReplay(nowNs)
	case ElectionStateFollowerCatchupInit:
		return e.handleFollowerCatchupInit(nowNs)
	case ElectionStateFollowerCatchupAwait:
		return e.handleFollowerCatchupAwait(nowNs)
	case ElectionStateFollowerCatchup:
		return e.handleFollowerCatchup(nowNs)
	case ElectionStateFollowerLogInit:
		return e.handleFollowerLogInit(nowNs)
	case ElectionStateFollowerLogAwait:
		return e.handleFollowerLogAwait(nowNs)
	case ElectionStateFollowerReady:
		return e.handleFollowerReady(nowNs)
	case ElectionStateClosed:
		return 0
	default:
		panic(fmt.Sprintf("election: unknown state %v", e.electionState))
	}
}

func (e *Election) state(next ElectionState, nowNs int64) {
	e.electionState = next
	e.timeOfLastStateChangeNs = nowNs
	if e.deps.StateObserver != nil {
		e.deps.StateObserver.OnElectionStateChange(next)
	}
}

func (e *Election) State() ElectionState { return e.electionState }

func (e *Election) LeadershipTermID() int64 { return e.leadershipTermID }

func (e *Election) LogPosition() int64 { return e.logPosition }

func (e *Election) AppendPosition() int64 { return e.appendPosition }

func (e *Election) ThisMemberID() int32 { return e.thisMember.ID }

func (e *Election) Leader() *Member { return e.leaderMember }

func (e *Election) LogSessionID() int32 { return e.logSessionID }

func (e *Election) NotifiedCommitPosition() int64 { return e.notifiedCommitPosition }
