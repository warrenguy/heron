package service

import (
	"cmp"
	"slices"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/internal/buffer"
)

// MockedOffer is the Offer result on a member that is not the leader.
// Matches upstream ClientSession.MOCKED_OFFER.
const MockedOffer int64 = 1

// ClientSession is one open client session in the service registry.
//
// Concurrency: invoker-only.
//
// Lifetime: the agent builds a session on SessionOpen. A snapshot load in
// [Open] also restores sessions. The load does not call SessionOpen. The
// session leaves the registry on SessionClose. A session held past
// SessionClose stays safe to call.
type ClientSession interface {
	// ID returns the cluster session id.
	ID() int64

	// ResponseStreamID returns the client response stream id.
	ResponseStreamID() int32

	// ResponseChannel returns the client response channel.
	ResponseChannel() string

	// EncodedPrincipal returns the authenticated principal. Empty when
	// the session carries none.
	//
	// Ownership: the session owns the slice. Callers must not modify it.
	EncodedPrincipal() []byte

	// Offer sends payload to the client behind one SessionMessageHeader.
	//
	// Returns:
	//   - MockedOffer when this member is not the leader
	//   - aeron.NotConnected while the session holds no response
	//     publication, including while its add is in flight
	//   - aeron.UnsupportedMessageLength when header plus payload
	//     exceeds the publication MaxMessageLength
	//   - otherwise the publication Offer result
	//
	// Behaviour: DoWork resolves the add, not a callback. A callback that
	// spins on aeron.NotConnected never returns. A closing session still
	// offers until SessionClose removes it. A log unbind closes the
	// publication. A leader join after an election re-adds it. A startup
	// join does not. Offer returns aeron.NotConnected until that add
	// completes.
	Offer(payload []byte) int64

	// Close asks the consensus module to close this session. Same as
	// [Cluster.CloseClientSession] with this id.
	//
	// Returns the [Cluster.CloseClientSession] result. A session that
	// SessionClose removed returns false and sends nothing. Idempotent.
	Close() bool

	// IsClosing reports whether this member sent a close request for the
	// session.
	//
	// Behaviour: each member keeps its own flag. A leader bind clears it.
	// SessionClose keeps it. Services must not change replicated state on it.
	IsClosing() bool
}

// ResponsePublicationAdder adds the concurrent response publication of a
// client session. Contract as [SnapshotPublicationAdder]. OfferAddPublication
// is cold-path only. PollAddPublication runs one conductor tick.
//
// Concurrency: invoker-only.
type ResponsePublicationAdder interface {
	OfferAddPublication(streamID int32, channel string) (int64, error)
	PollAddPublication(correlationID int64) (*aeron.Publication, bool, error)
	CancelPending(correlationID int64)
}

var _ ResponsePublicationAdder = (*aeron.Client)(nil)

// containerClientSession is the registry implementation of ClientSession.
//
// Ownership: the agent owns publication and the in-flight add.
type containerClientSession struct {
	agent            *Agent
	id               int64
	responseStreamID int32
	responseChannel  string
	encodedPrincipal []byte

	// publication is the response publication, or nil.
	publication *aeron.Publication

	// addCorrelationID is the in-flight add, or nullValue.
	addCorrelationID int64

	// closing is true after this member sends a CloseSession frame. A leader
	// bind clears it.
	closing bool
}

var _ ClientSession = (*containerClientSession)(nil)

// ID implements [ClientSession.ID].
func (s *containerClientSession) ID() int64 {
	return s.id
}

// ResponseStreamID implements [ClientSession.ResponseStreamID].
func (s *containerClientSession) ResponseStreamID() int32 {
	return s.responseStreamID
}

// ResponseChannel implements [ClientSession.ResponseChannel].
func (s *containerClientSession) ResponseChannel() string {
	return s.responseChannel
}

// EncodedPrincipal implements [ClientSession.EncodedPrincipal].
func (s *containerClientSession) EncodedPrincipal() []byte {
	return s.encodedPrincipal
}

// Offer implements [ClientSession.Offer].
func (s *containerClientSession) Offer(payload []byte) int64 {
	return s.agent.offerEgress(s, payload)
}

// Close implements [ClientSession.Close].
func (s *containerClientSession) Close() bool {
	return s.agent.CloseClientSession(s.id)
}

// IsClosing implements [ClientSession.IsClosing].
func (s *containerClientSession) IsClosing() bool {
	return s.closing
}

// CloseClientSession implements [Cluster.CloseClientSession].
func (a *Agent) CloseClientSession(id int64) bool {
	session, ok := a.sessions[id]
	if !ok {
		return false
	}
	if session.closing {
		return true
	}
	if !a.canPublish() {
		return false
	}
	result := a.moduleProxy.PublishCloseSession(codecs.CloseSessionFields{
		ClusterSessionID: id,
	})
	if !a.acceptOffer(result, errCloseSessionPublish) {
		return false
	}
	session.closing = true
	return true
}

// joinEgress applies the egress rule of the join that bound this tick.
//
// Behaviour:
//   - A non-leader join closes every response publication. It resolves or
//     cancels every in-flight add. It keeps each closing flag.
//   - A leader join with isStartup false starts one response publication
//     add for each open session that holds none.
//   - A leader join with isStartup true does not start an add.
//   - A leader join clears the closing flag of every open session.
func (a *Agent) joinEgress(event *logJoinEvent) {
	if event.role != codecs.RoleLeader {
		a.disconnectEgress()
		return
	}
	for _, session := range a.sessionList {
		if !event.isStartup {
			a.connectClientSession(session)
		}
		session.closing = false
	}
}

// compareSessionID orders sessionList entries by id.
func compareSessionID(session *containerClientSession, id int64) int {
	return cmp.Compare(session.id, id)
}

func newContainerClientSession(
	a *Agent,
	id int64,
	responseStreamID int32,
	responseChannel, encodedPrincipal []byte,
) *containerClientSession {
	return &containerClientSession{
		agent:            a,
		id:               id,
		responseStreamID: responseStreamID,
		responseChannel:  string(responseChannel),
		encodedPrincipal: encodedPrincipal,
		addCorrelationID: nullValue,
	}
}

// addClientSession inserts session into the map and into sessionList in id
// order. The caller rejects a clash first.
func (a *Agent) addClientSession(session *containerClientSession) {
	a.sessions[session.id] = session
	index, _ := slices.BinarySearchFunc(a.sessionList, session.id, compareSessionID)
	a.sessionList = slices.Insert(a.sessionList, index, session)
}

// restoreClientSession adds one snapshot session to the registry.
//
// Behaviour: does not start a response add. Matches upstream addSession.
//
// Returns errSessionClash when the id is already open.
func (a *Agent) restoreClientSession(fields codecs.ClientSessionFields) error {
	if _, exists := a.sessions[fields.ClusterSessionID]; exists {
		return errSessionClash
	}
	a.addClientSession(newContainerClientSession(
		a,
		fields.ClusterSessionID,
		fields.ResponseStreamID,
		fields.ResponseChannel,
		fields.EncodedPrincipal,
	))
	return nil
}

// removeClientSession deletes id from the map and from sessionList.
//
// Returns the removed session, or nil when id is not open.
func (a *Agent) removeClientSession(id int64) *containerClientSession {
	session, ok := a.sessions[id]
	if !ok {
		return nil
	}
	delete(a.sessions, id)
	if index, found := slices.BinarySearchFunc(a.sessionList, id, compareSessionID); found {
		a.sessionList = slices.Delete(a.sessionList, index, index+1)
	}
	return session
}

// walkClientSessions yields sessionList in ascending id order. NewAgent
// stores it as the ClientSessions sequence.
func (a *Agent) walkClientSessions(yield func(ClientSession) bool) {
	for _, session := range a.sessionList {
		if !yield(session) {
			return
		}
	}
}

// connectClientSession starts the response publication add of session. Does
// nothing without an adder, with a held publication, or with an add in
// flight. An add error leaves the session open with no publication.
func (a *Agent) connectClientSession(session *containerClientSession) {
	if a.responsePublications == nil ||
		session.publication != nil ||
		session.addCorrelationID != nullValue {
		return
	}
	correlationID, err := a.responsePublications.OfferAddPublication(
		session.responseStreamID,
		session.responseChannel,
	)
	if err != nil {
		return
	}
	session.addCorrelationID = correlationID
	a.responseAdds = append(a.responseAdds, session)
}

// responseAddWork polls each in-flight response publication add once. A
// ready add stores its publication. An error, or done with no publication,
// ends the add. The session then stays open with no publication.
//
// Returns the count of adds still in flight plus publications stored.
func (a *Agent) responseAddWork() int {
	work := 0
	kept := a.responseAdds[:0]
	for _, session := range a.responseAdds {
		publication, done, err := a.responsePublications.PollAddPublication(
			session.addCorrelationID,
		)
		if err == nil && !done {
			kept = append(kept, session)
			work++
			continue
		}
		if err == nil && publication != nil {
			session.publication = publication
			work++
		}
		session.addCorrelationID = nullValue
	}
	clear(a.responseAdds[len(kept):])
	a.responseAdds = kept
	return work
}

// disconnectClientSession ends the in-flight add of session, then closes its
// publication. Idempotent.
func (a *Agent) disconnectClientSession(session *containerClientSession) {
	if session.addCorrelationID != nullValue {
		a.endResponseAdd(session)
		if index := slices.Index(a.responseAdds, session); index >= 0 {
			a.responseAdds = slices.Delete(a.responseAdds, index, index+1)
		}
	}
	closeResponsePublication(session)
}

// disconnectEgress ends the in-flight add and closes the response
// publication of every open session, in id order. It then ends any add left
// for a session outside the registry. Registry entries stay.
func (a *Agent) disconnectEgress() {
	for _, session := range a.sessionList {
		if session.addCorrelationID != nullValue {
			a.endResponseAdd(session)
		}
		closeResponsePublication(session)
	}
	for _, session := range a.responseAdds {
		if session.addCorrelationID != nullValue {
			a.endResponseAdd(session)
		}
	}
	clear(a.responseAdds)
	a.responseAdds = a.responseAdds[:0]
}

// endResponseAdd ends the in-flight add of session. Clears its correlation
// id. The caller removes session from responseAdds.
//
// The add polls once before any cancel. CancelPending leaves a ready-marked
// add registered, so the poll collects that publication and closes it. Any
// other poll result cancels the add.
func (a *Agent) endResponseAdd(session *containerClientSession) {
	correlationID := session.addCorrelationID
	publication, done, err := a.responsePublications.PollAddPublication(correlationID)
	if err == nil && done && publication != nil {
		_ = publication.Close()
	} else {
		a.responsePublications.CancelPending(correlationID)
	}
	session.addCorrelationID = nullValue
}

// closeResponsePublication closes the response publication of session, if
// held.
func closeResponsePublication(session *containerClientSession) {
	if session.publication != nil {
		_ = session.publication.Close()
		session.publication = nil
	}
}

// offerEgress offers payload to session behind one SessionMessageHeader.
// The header carries the session id, the cluster time, and
// egressLeadershipTermID.
//
// Returns the [ClientSession.Offer] codes. Marks no failure on any result.
func (a *Agent) offerEgress(session *containerClientSession, payload []byte) int64 {
	if a.role != codecs.RoleLeader {
		return MockedOffer
	}
	publication := session.publication
	if publication == nil {
		return aeron.NotConnected
	}
	headerLength := codecs.SessionMessageHeaderEncodedLen()
	total := headerLength + len(payload)
	if total > publication.MaxMessageLength() {
		return aeron.UnsupportedMessageLength
	}
	a.egressScratch = buffer.GrowScratch(a.egressScratch, total)
	fields := codecs.SessionMessageHeaderFields{
		LeadershipTermID: a.egressLeadershipTermID,
		ClusterSessionID: session.id,
		Timestamp:        a.clusterTime,
	}
	if _, err := codecs.EncodeIngressHeaderScratch(a.egressScratch, fields); err != nil {
		return aeron.NotConnected
	}
	copy(a.egressScratch[headerLength:], payload)
	return publication.Offer(a.egressScratch)
}
