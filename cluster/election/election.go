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
	initialTimeOfLastUpdateNs        int64
	initialLogLeadershipTermID       int64
	initialTermBaseLogPosition       int64
	timeOfLastStateChangeNs          int64
	timeOfLastUpdateNs               int64
	timeOfLastCommitPositionUpdateNs int64
	nominationDeadlineNs             int64
	lastPublishedCommitPosition      int64
	lastPublishedAppendPosition      int64
	lastError                        error

	clusterMembers []*Member
	memberByID     map[int32]*Member
	thisMember     *Member

	logReplay      LogReplay
	logReplication LogReplication
}

var _ consensus.ElectionRunner = (*Election)(nil)

func NewElection(cfg ElectionConfig, deps ElectionDeps) *Election {
	if err := ValidateElectionConfig(cfg); err != nil {
		panic(err.Error())
	}
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
		logSessionID:                     NullMemberID,
		gracefulClosedLeaderID:           cfg.GracefulClosedLeaderID,
		isNodeStartup:                    cfg.IsNodeStartup,
		isExtendedCanvass:                cfg.IsNodeStartup,
		isFirstInit:                      true,
		isLeaderStartup:                  false,
		leaderMember:                     nil,
		initialTimeOfLastUpdateNs:        initialTimeOfLastUpdateNs,
		initialLogLeadershipTermID:       cfg.LeadershipTermID,
		initialTermBaseLogPosition:       cfg.TermBaseLogPosition,
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

// state applies a transition and its entry side effects. reason mirrors the
// Java win-reason strings and is reserved for a future electiontrace build tag.
//
// A same-state call is a no-op; entry side effects and interval-timer resets
// apply only when the state actually changes.
func (e *Election) state(next ElectionState, nowNs int64, reason string) {
	_ = reason
	if next == e.electionState {
		return
	}

	// Leaving canvass consumes the startup canvass window; a later re-canvass
	// uses the shorter leader-heartbeat deadline.
	if e.electionState == ElectionStateCanvass {
		e.isExtendedCanvass = false
	}

	switch next {
	case ElectionStateCanvass:
		ResetMembers(e.clusterMembers)

		// ResetMembers clears this node too; restore its advertised position so
		// it counts toward its own quorum and unanimous candidacy.
		e.thisMember.LeadershipTermID = e.leadershipTermID
		e.thisMember.LogPosition = e.appendPosition
		e.deps.Host.RoleSet(consensus.RoleFollower)
	case ElectionStateCandidateBallot:
		e.deps.Host.RoleSet(consensus.RoleCandidate)
	case ElectionStateLeaderLogReplication:
		e.deps.Host.RoleSet(consensus.RoleLeader)
		e.logSessionID = e.deps.Host.AddLogPublication(e.appendPosition)
	case ElectionStateFollowerLogReplication, ElectionStateFollowerReplay:
		e.deps.Host.RoleSet(consensus.RoleFollower)
	case ElectionStateInit,
		ElectionStateNominate,
		ElectionStateFollowerBallot,
		ElectionStateLeaderReplay,
		ElectionStateLeaderInit,
		ElectionStateLeaderReady,
		ElectionStateFollowerCatchupInit,
		ElectionStateFollowerCatchupAwait,
		ElectionStateFollowerCatchup,
		ElectionStateFollowerLogInit,
		ElectionStateFollowerLogAwait,
		ElectionStateFollowerReady,
		ElectionStateClosed:
	}

	e.electionState = next
	e.timeOfLastStateChangeNs = nowNs
	e.timeOfLastUpdateNs = e.initialTimeOfLastUpdateNs
	e.timeOfLastCommitPositionUpdateNs = e.initialTimeOfLastUpdateNs
	if e.deps.StateObserver != nil {
		e.deps.StateObserver.OnElectionStateChange(next)
	}
}

func (e *Election) State() ElectionState { return e.electionState }

// Close releases the replay helper the election still holds and clears the
// replication helper reference.
//
// Behaviour: releases the archive replay session and any replay subscription
// the helper opened. [Election.State] still answers after Close. Close is
// idempotent.
//
// Concurrency: invoker-only. Call after the duty cycle has stopped. May block
// for the archive control response timeout while the replay session stops.
//
// Ownership: owns the replay helper. Does not own the node state file, the
// recording log, or any transport handle.
func (e *Election) Close() {
	e.clearLogReplication()
	e.stopReplay()
}

func (e *Election) LeadershipTermID() int64 { return e.leadershipTermID }

func (e *Election) LogPosition() int64 { return e.logPosition }

func (e *Election) AppendPosition() int64 { return e.appendPosition }

func (e *Election) ThisMemberID() int32 { return e.thisMember.ID }

func (e *Election) Leader() *Member { return e.leaderMember }

func (e *Election) LogSessionID() int32 { return e.logSessionID }

func (e *Election) NotifiedCommitPosition() int64 { return e.notifiedCommitPosition }

// LastError returns the error last passed to HandleError.
//
// Behaviour: returns nil when HandleError has not run, or the last call passed
// nil. Error strings may include archive remote text and channel URIs. Callers
// must not put raw Error() into untrusted sinks without sanitization.
//
// Concurrency: invoker-only.
//
// Lifetime: overwritten on each HandleError. Survives Close.
func (e *Election) LastError() error { return e.lastError }
