package election

import "github.com/warrenguy/heron/cluster/codecs"

func (e *Election) publishNewLeadershipTermOnInterval(quorumPosition, nowNs int64) int {
	if !hasUpdateIntervalExpired(nowNs, e.timeOfLastUpdateNs, e.cfg.LeaderHeartbeatIntervalNs) {
		return 0
	}
	e.timeOfLastUpdateNs = nowNs
	e.publishNewLeadershipTerm(quorumPosition, e.deps.EpochClock())
	return 1
}

// publishCommitPositionOnInterval stops publishing when the quorum position
// regresses during the election; NewLeadershipTerm keeps flowing on its own
// interval, so followers still observe the leader.
func (e *Election) publishCommitPositionOnInterval(quorumPosition, nowNs int64) int {
	advanced := e.lastPublishedCommitPosition < quorumPosition
	repeatDue := e.lastPublishedCommitPosition == quorumPosition &&
		hasUpdateIntervalExpired(
			nowNs,
			e.timeOfLastCommitPositionUpdateNs,
			e.cfg.LeaderHeartbeatIntervalNs,
		)
	if !advanced && !repeatDue {
		return 0
	}
	e.timeOfLastCommitPositionUpdateNs = nowNs
	e.lastPublishedCommitPosition = quorumPosition

	// The election leadershipTermID matches the NewLeadershipTerm events so a
	// follower never sees a CommitPosition for a term it was not offered.
	e.deps.Host.PublishCommitPosition(quorumPosition, e.leadershipTermID)
	return 1
}

func (e *Election) publishNewLeadershipTerm(quorumPosition, timestamp int64) {
	if e.logSessionID == NullMemberID {
		return
	}
	fields := e.newLeadershipTermFields(e.logLeadershipTermID, quorumPosition, timestamp)
	for _, member := range e.clusterMembers {
		if member.ID != e.thisMember.ID {
			e.deps.Publisher.NewLeadershipTerm(PublicationTarget{MemberID: member.ID}, fields)
		}
	}
}

// publishNewLeadershipTermToMember republishes the term to one member, keyed
// to that member's lagging log leadership term.
func (e *Election) publishNewLeadershipTermToMember(
	member *Member,
	logLeadershipTermID, quorumPosition, timestamp int64,
) {
	if member.ID == e.thisMember.ID || e.logSessionID == NullMemberID {
		return
	}
	e.deps.Publisher.NewLeadershipTerm(
		PublicationTarget{MemberID: member.ID},
		e.newLeadershipTermFields(logLeadershipTermID, quorumPosition, timestamp),
	)
}

// newLeadershipTermFields encodes the quorum position as the wire
// CommitPosition field. Successor-term fields come from the recording log
// lookup; with no lookup (or no successor entry) the current term is its own
// successor: NextLeadershipTermID falls back to the election term,
// NextTermBaseLogPosition to the append position, and NextLogPosition to
// NullPosition.
func (e *Election) newLeadershipTermFields(
	logLeadershipTermID, quorumPosition, timestamp int64,
) codecs.NewLeadershipTermFields {
	nextLeadershipTermID := e.leadershipTermID
	nextTermBaseLogPosition := e.appendPosition
	nextLogPosition := int64(NullPosition)
	if e.deps.RecordingLogLookup != nil {
		if next := e.deps.RecordingLogLookup.FindTermEntry(logLeadershipTermID + 1); next != nil {
			nextLeadershipTermID = next.LeadershipTermID
			nextTermBaseLogPosition = next.TermBaseLogPosition
			nextLogPosition = next.LogPosition
			if nextLogPosition == NullPosition {
				nextLogPosition = e.appendPosition
			}
		}
	}

	return codecs.NewLeadershipTermFields{
		LogLeadershipTermID:     logLeadershipTermID,
		NextLeadershipTermID:    nextLeadershipTermID,
		NextTermBaseLogPosition: nextTermBaseLogPosition,
		NextLogPosition:         nextLogPosition,
		LeadershipTermID:        e.leadershipTermID,
		TermBaseLogPosition:     e.appendPosition,
		LogPosition:             e.appendPosition,
		CommitPosition:          quorumPosition,
		LeaderRecordingID:       e.deps.Host.LocalLogRecordingID(),
		Timestamp:               timestamp,
		LeaderMemberID:          e.thisMember.ID,
		LogSessionID:            e.logSessionID,
		AppVersion:              e.cfg.AppVersion,
		IsStartup:               boolToClusterwire(e.isLeaderStartup),
	}
}

func (e *Election) publishFollowerAppendPosition(nowNs int64) int {
	leader := e.leaderMember
	if leader == nil {
		return 0
	}
	positionChanged := e.lastPublishedAppendPosition != e.appendPosition
	intervalDue := hasUpdateIntervalExpired(
		nowNs,
		e.timeOfLastUpdateNs,
		e.cfg.LeaderHeartbeatIntervalNs,
	)
	if !positionChanged && !intervalDue {
		return 0
	}
	published := e.deps.Publisher.AppendPosition(
		PublicationTarget{MemberID: leader.ID},
		codecs.AppendPositionFields{
			LeadershipTermID: e.leadershipTermID,
			LogPosition:      e.appendPosition,
			FollowerMemberID: e.thisMember.ID,
		},
	)
	if !published {
		return 0
	}
	e.lastPublishedAppendPosition = e.appendPosition
	e.timeOfLastUpdateNs = nowNs
	return 1
}
