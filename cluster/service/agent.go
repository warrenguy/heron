package service

import (
	"errors"
	"fmt"
	"iter"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// Sticky failure causes marked once on the service path. Unexported: tests
// match with errors.Is against the package-level values.
var (
	errLogNotContiguous       = errors.New("service: log not contiguous")
	errSessionClash           = errors.New("service: session id clash")
	errTimerPublish           = errors.New("service: timer publish failed")
	errCloseSessionPublish    = errors.New("service: close session publish failed")
	errTakeSnapshot           = errors.New("service: take snapshot failed")
	errActionPositionMismatch = errors.New("service: cluster action log position mismatch")
	errUnknownClusterAction   = errors.New("service: unknown cluster action")
)

// AgentConfig configures NewAgent. Zero-value fields have the defaults stated
// per field.
type AgentConfig struct {
	// ServiceID is this container's service id (upstream default 0).
	ServiceID int32

	// Service receives log-event callbacks. Nil is allowed: dispatch updates
	// agent state and skips callbacks.
	Service ClusteredService

	// CommittedPosition supplies the committed log prefix this member treats
	// as safe for application consumption. Nil is allowed: the log poll is
	// skipped until wired. The service consumes nothing without a bound.
	//
	// Handoff unbind still needs image Position at or above MaxLogPosition
	// (the handoff cap) or a closed image. With a nil commit, an open image,
	// and Position below the handoff logPosition, the pending join stays
	// unbound: no unbind and no bind of that join.
	CommittedPosition func() int64

	// Subscriptions adds the log subscription at join time. Nil is allowed: a
	// delivered join stays pending until wired.
	Subscriptions SubscriptionAdder

	// FragmentLimit caps fragments per log poll.
	// Zero or negative takes DefaultLogFragmentLimit.
	FragmentLimit int

	// Control polls module-to-service control frames (stream 104). Nil is
	// allowed: control work polls nothing and delivers no frame.
	Control ControlIngress

	// ModuleProxy sends ServiceAck, timer and CloseSession frames (stream
	// 105). Nil is allowed: ready acks stay queued. Timer and close requests
	// return false. A proxy with no bound publication behaves as nil.
	ModuleProxy *ConsensusModuleProxy

	// StartAck enables the spontaneous start ack a Java consensus module
	// consumes during recovery. Nil disables the start ack. See
	// [StartAckConfig].
	StartAck *StartAckConfig

	// Archive is the archive client DoWork polls. Snapshot takes record
	// through Archive and install its session handler, so Archive must carry
	// no session handler of its own. Nil polls none and fails each take. Agent
	// does not own Archive.
	Archive *archive.Client

	// SnapshotPublications adds each snapshot publication. Nil is allowed: a
	// snapshot action then fails the take. Agent does not own the adder.
	SnapshotPublications SnapshotPublicationAdder

	// SnapshotChannel is the snapshot publication channel. Empty takes
	// DefaultSnapshotChannel. NewAgent does not validate it. [Open] does.
	SnapshotChannel string

	// SnapshotStreamID is the snapshot publication stream. Zero takes
	// DefaultSnapshotStreamID. NewAgent does not validate it. [Open] does.
	SnapshotStreamID int32

	// ResponsePublications adds the response publication of each session a
	// leader opens. A leader join with isStartup false adds one for each open
	// session that holds none. Nil is allowed: ClientSession.Offer then
	// returns aeron.NotConnected. Agent does not own the adder.
	ResponsePublications ResponsePublicationAdder
}

// Agent owns the service duty cycle: log join, bounded log consumption,
// session registry, and the pending coordination-ack queue.
//
// Concurrency: invoker-only. The harness drives DoWork after runner.DoWork on
// one goroutine. Agent never calls DoWork on aeron.Client. The log
// subscription add, the snapshot publication add poll, and the response
// publication add poll run conductor ticks.
//
// Lifetime: Agent closes the log subscription in Close. The take closes its
// snapshot publication and start request when it ends. Close closes both
// while a take runs. Close ends every in-flight response add and closes
// every response publication. Unbind and a non-leader join close every
// response publication. A leader join with isStartup false re-adds them.
// Agent does not own the aeron.Client.
//
// Ownership: Agent owns the BoundedLogAdapter, the log subscription, and the
// snapshot publication and start request of a running take. Agent owns the
// response publications and their in-flight adds. Agent does not own Images
// the subscription holds. Unbind clears the adapter Image reference only.
// Agent does not own the archive client.
type Agent struct {
	serviceID         int32
	service           ClusteredService
	committedPosition func() int64
	subscriptions     SubscriptionAdder
	control           ControlIngress
	moduleProxy       *ConsensusModuleProxy

	// archive is the polled archive client, or nil. Never a typed nil.
	archive snapshotArchiveControl

	// newSnapshotStart builds one start recording request. Set with archive.
	newSnapshotStart func(streamID int32, channel []byte) snapshotStartRequest

	// snapshotPublications adds each snapshot publication, or nil.
	snapshotPublications SnapshotPublicationAdder
	snapshotChannel      string
	snapshotStreamID     int32

	// snapshotRowOffer offers one snapshot row. Nil offers through
	// ExclusivePublication.Offer. Tests substitute.
	snapshotRowOffer func(publication *aeron.ExclusivePublication, row []byte) int64

	// take is the snapshot take in flight, or nil.
	take *snapshotTake

	// snapshotScratch is the reused row encode buffer.
	snapshotScratch []byte

	// timeUnit and appVersion come from the last NewLeadershipTermEvent.
	// timeUnit starts at ClusterTimeUnit_NULL_VALUE: no unit known yet.
	timeUnit   clusterwire.ClusterTimeUnit
	appVersion int32

	// lastSnapshotRecordingID is the RelevantID of the last sent snapshot
	// ack. Starts at nullValue.
	lastSnapshotRecordingID int64

	adapter *BoundedLogAdapter

	// serviceFailure is the first fatal service-path failure, or nil.
	// Marked once; never cleared. DoWork returns 0 while marked.
	serviceFailure error

	// pendingJoin is the last JoinLog not yet bound, or nil. One slot. A newer
	// join supersedes an unbound older join and marks the prior join entry
	// ready.
	pendingJoin *logJoinEvent

	// coordinationAcks is the FIFO of pending ServiceAck entries. Drain is
	// prefix-gated: only a ready first queued entry may send. A start
	// entry, when configured, is queued at NewAgent.
	coordinationAcks []coordinationAck

	// sessions is the log-derived registry keyed by cluster session id.
	sessions map[int64]*containerClientSession

	// sessionList holds the sessions of the map in ascending id order.
	sessionList []*containerClientSession

	// clientSessions is the cached ClientSessions sequence. NewAgent builds
	// it once. ClientSessions then allocates nothing.
	clientSessions iter.Seq[ClientSession]

	// responsePublications adds each response publication, or nil. Never a
	// typed nil.
	responsePublications ResponsePublicationAdder

	// responseAdds holds the sessions with a response add in flight.
	responseAdds []*containerClientSession

	// egressScratch is the reused egress frame buffer.
	egressScratch []byte

	// egressLeadershipTermID is the egress header leadershipTermId. Each
	// NewLeadershipTermEvent writes it. Starts at nullValue.
	egressLeadershipTermID int64

	logSubscription *aeron.Subscription

	leadershipHandler NewLeadershipTermEventHandler
	snapshotHandler   TakeSnapshotHandler

	// ackID is the next ServiceAck id to send. Starts 0. Advances only on a
	// successful drain send.
	ackID int64

	// controlOnFragment is the cached Poll handler. NewAgent builds the handler
	// once so each controlWork tick does not allocate a method value.
	controlOnFragment func([]byte, int)

	// unreadyRequestBound is the minimum logPosition among unready request-kind
	// entries after the last sweep. hasUnreadyRequestBound is false when no such
	// entry exists. DoWork uses the pair as the log poll upper bound.
	unreadyRequestBound    int64
	hasUnreadyRequestBound bool

	started bool
	closed  bool

	role        codecs.Role
	memberID    int32
	logPosition int64
	clusterTime int64
}

// logJoinEvent holds one delivered JoinLog not yet bound, or the join just
// stored for handoff while an older image still drains to the cap.
type logJoinEvent struct {
	logPosition    int64
	maxLogPosition int64
	memberID       int32
	logSessionID   int32
	logStreamID    int32
	isStartup      bool
	role           codecs.Role
	logChannel     string
}

// coordinationAckKind discriminates the readiness rule of one entry.
type coordinationAckKind uint8

const (
	// ackKindJoin: ready at join completion or supersession.
	ackKindJoin coordinationAckKind = iota

	// ackKindRequest: ready when the agent log position equals the requested
	// position.
	ackKindRequest

	// ackKindSnapshot: ready when the take resolves. No sweep rule.
	ackKindSnapshot

	// ackKindStart: queued at NewAgent. Ready after successful start work,
	// or when Service is nil. A Start error leaves the entry unready.
	ackKindStart
)

// coordinationAck is one pending ServiceAck. Drain is prefix-gated: only a
// ready first queued entry may send.
//
// Readiness:
//   - start: queued at NewAgent. Ready after successful start work, or when
//     Service is nil. A Start error leaves the start entry unready.
//   - join: ready at bind or supersession
//   - request: ready when the agent log position equals the entry
//     logPosition
//   - snapshot: ready when the take resolves, on success or failure
//
// failure holds the cause of a failed snapshot take. The drain marks it
// once the ack sends, or on a terminal offer code. With no ack sender,
// failSnapshotEntry marks it at once and the entry stays queued.
type coordinationAck struct {
	logPosition int64
	kind        coordinationAckKind
	ready       bool
	relevantID  int64
	failure     error
}

// Compile-time assertion: Agent satisfies Cluster.
var _ Cluster = (*Agent)(nil)

// NewAgent builds an Agent from cfg. The adapter is created with no Image.
// memberID starts at -1 and logPosition at 0 (cold-start service).
//
// When StartAck is set and LogPosition is non-negative, NewAgent queues one
// unready start entry. A negative LogPosition queues nothing. [Open]
// refuses a negative LogPosition.
//
// NewLeadershipTermEventHandler and TakeSnapshotHandler are detected once
// via type assertion on cfg.Service.
func NewAgent(cfg AgentConfig) *Agent {
	a := &Agent{
		serviceID:               cfg.ServiceID,
		service:                 cfg.Service,
		committedPosition:       cfg.CommittedPosition,
		subscriptions:           cfg.Subscriptions,
		control:                 cfg.Control,
		moduleProxy:             cfg.ModuleProxy,
		snapshotChannel:         cfg.SnapshotChannel,
		snapshotStreamID:        cfg.SnapshotStreamID,
		timeUnit:                clusterwire.ClusterTimeUnit_NULL_VALUE,
		lastSnapshotRecordingID: nullValue,
		memberID:                -1,
		sessions:                make(map[int64]*containerClientSession),
		egressLeadershipTermID:  nullValue,
	}
	if a.snapshotChannel == "" {
		a.snapshotChannel = DefaultSnapshotChannel
	}
	if a.snapshotStreamID == 0 {
		a.snapshotStreamID = DefaultSnapshotStreamID
	}
	if client := cfg.Archive; client != nil {
		a.archive = client
		a.newSnapshotStart = func(streamID int32, channel []byte) snapshotStartRequest {
			return client.NewAsyncStartRecording(
				streamID,
				archivewire.SourceLocation_LOCAL,
				true,
				channel,
			)
		}
	}
	if !isNilOrTypedNil(cfg.SnapshotPublications) {
		a.snapshotPublications = cfg.SnapshotPublications
	}
	if !isNilOrTypedNil(cfg.ResponsePublications) {
		a.responsePublications = cfg.ResponsePublications
	}
	if nlt, ok := cfg.Service.(NewLeadershipTermEventHandler); ok {
		a.leadershipHandler = nlt
	}
	if snap, ok := cfg.Service.(TakeSnapshotHandler); ok {
		a.snapshotHandler = snap
	}
	a.controlOnFragment = a.handleControlFragment
	a.clientSessions = a.walkClientSessions
	a.adapter = NewBoundedLogAdapter(a, cfg.FragmentLimit)
	if sa := cfg.StartAck; sa != nil && sa.LogPosition >= 0 {
		a.coordinationAcks = append(a.coordinationAcks, coordinationAck{
			logPosition: sa.LogPosition,
			kind:        ackKindStart,
			relevantID:  sa.RelevantID,
		})
	}
	return a
}

// DoWork runs one duty tick. nowNs is the clock for the snapshot take
// deadlines: nanoseconds from a monotonic source, such as
// time.Now().UnixNano(). A clock that never advances fires no deadline, so
// a lost START or STOP signal then stalls the take forever.
//
// Order:
//  1. serviceFailure or closed: return 0
//  2. start work once: Service.Start(agent). A Start error marks failure
//     once. The start entry stays unready. Success or a nil Service marks
//     the start entry ready when one is queued.
//  3. archive Poll when an archive client is wired
//  4. snapshot take work while a take runs
//  5. control work: poll control, request readiness sweep, drain at most one
//     ready first queued ack. An entry with a failure marks it on send or
//     on a terminal offer code
//  6. response add work while responseAdds is non-empty: poll each in-flight
//     response publication add once
//  7. join work while pendingJoin is non-nil and no image is bound. A bind
//     runs the join egress rule
//  8. log poll when the image is bound and CommittedPosition is non-nil. The
//     poll bound is min(committed, lowest unready request entry position).
//     While a take runs, the bound also holds at the take action position
//  9. unbind when zero polled and (image closed or position at maxLogPosition).
//     Unbind closes every response publication. It resolves or cancels every
//     in-flight add
//
// A take that resolves in step 4 can ack in step 5 of the same tick.
//
// Returns the work count: Start, the archive Poll count, each completed take
// state, control fragments, each sent ack, each in-flight response add poll,
// each ready response add, each join bind, and each polled fragment. An add
// that a bind starts counts when its poll runs. The disconnect polls of an
// unbind or a non-leader join do not count.
//
// Concurrency: invoker-only.
func (a *Agent) DoWork(nowNs int64) int {
	if a.closed || a.serviceFailure != nil {
		return 0
	}

	work := 0

	if !a.started {
		a.started = true
		work++
		if a.service != nil {
			if err := a.service.Start(a); err != nil {
				a.markFailure(err)
				return work
			}
		}
		a.markAckReady(ackKindStart)
	}

	if a.archive != nil {
		work += a.archive.Poll()
	}

	if a.take != nil {
		work += a.snapshotTakeWork(nowNs)
		if a.serviceFailure != nil {
			return work
		}
	}

	work += a.controlWork()
	if a.serviceFailure != nil {
		return work
	}

	if len(a.responseAdds) > 0 {
		work += a.responseAddWork()
	}

	if a.pendingJoin != nil && a.adapter.Image() == nil {
		work += a.joinWork()
		if a.serviceFailure != nil {
			return work
		}
	}

	polled := 0
	if a.adapter.Image() != nil && a.committedPosition != nil {
		bound := a.committedPosition()
		if a.hasUnreadyRequestBound {
			bound = min(bound, a.unreadyRequestBound)
		}
		if a.take != nil {
			bound = min(bound, a.take.logPosition)
		}
		polled = a.adapter.Poll(bound)
		work += polled
		if a.serviceFailure != nil {
			return work
		}
	}

	a.maybeUnbind(polled)
	return work
}

// Close ends any snapshot take. A take that Close ends sends no ack. Close
// then disconnects the egress of every session. Close then unbinds the log
// image and closes the owned log subscription. Registry entries stay.
//
// Idempotent. The Agent is inert after Close.
//
// Ownership: closes the snapshot publication and start request of a running
// take. Ends every in-flight response add and closes every response
// publication. Closes the log subscription Agent added. Does not close
// Images.
func (a *Agent) Close() {
	if a.closed {
		return
	}
	a.closed = true
	a.closeSnapshotTake()
	a.disconnectEgress()
	a.adapter.SetImage(nil)
	if a.logSubscription != nil {
		_ = a.logSubscription.Close()
		a.logSubscription = nil
	}
}

// Role returns the role adopted from the last bound JoinLog, or
// [codecs.RoleFollower] after unbind or before the first bind.
func (a *Agent) Role() codecs.Role {
	return a.role
}

// MemberID returns the member id from the last bound JoinLog, or -1 before the
// first bind.
func (a *Agent) MemberID() int32 {
	return a.memberID
}

// LogPosition returns the last consumed frame-end position, including padding
// adoption on unbind.
func (a *Agent) LogPosition() int64 {
	return a.logPosition
}

// ClusterTime returns the timestamp of the last dispatched log event.
func (a *Agent) ClusterTime() int64 {
	return a.clusterTime
}

// ScheduleTimer implements [Cluster.ScheduleTimer].
func (a *Agent) ScheduleTimer(correlationID, deadline int64) bool {
	if !a.canPublish() {
		return false
	}
	result := a.moduleProxy.PublishScheduleTimer(codecs.ScheduleTimerFields{
		CorrelationID: correlationID,
		Deadline:      deadline,
	})
	return a.acceptOffer(result, errTimerPublish)
}

// CancelTimer implements [Cluster.CancelTimer].
func (a *Agent) CancelTimer(correlationID int64) bool {
	if !a.canPublish() {
		return false
	}
	result := a.moduleProxy.PublishCancelTimer(codecs.CancelTimerFields{
		CorrelationID: correlationID,
	})
	return a.acceptOffer(result, errTimerPublish)
}

// acceptOffer maps a module-proxy offer result to a sent bool.
//
//   - Positive result: returns true.
//   - Terminal publication code: marks cause once, wrapped with the offer
//     code, and returns false.
//   - Retriable code: returns false without marking.
func (a *Agent) acceptOffer(result int64, cause error) bool {
	if result > 0 {
		return true
	}
	if !aeron.IsOfferRetriable(result) {
		a.markFailure(fmt.Errorf("%w: %d", cause, result))
	}
	return false
}

// SessionCount returns the number of open sessions in the log-derived registry.
func (a *Agent) SessionCount() int {
	return len(a.sessions)
}

// HasSession reports whether sessionID is present in the registry.
func (a *Agent) HasSession(sessionID int64) bool {
	_, ok := a.sessions[sessionID]
	return ok
}

// ClientSession implements [Cluster.ClientSession].
func (a *Agent) ClientSession(id int64) (ClientSession, bool) {
	session, ok := a.sessions[id]
	if !ok {
		return nil, false
	}
	return session, true
}

// ClientSessions implements [Cluster.ClientSessions].
func (a *Agent) ClientSessions() iter.Seq[ClientSession] {
	return a.clientSessions
}

// PendingResponsePublicationAdds returns the number of sessions with a
// response publication add in flight.
func (a *Agent) PendingResponsePublicationAdds() int {
	return len(a.responseAdds)
}

// HasPendingJoin reports whether a JoinLog is stored and not yet bound.
func (a *Agent) HasPendingJoin() bool {
	return a.pendingJoin != nil
}

// IsLogBound reports whether a log Image is bound on the adapter.
func (a *Agent) IsLogBound() bool {
	return a.adapter.Image() != nil
}

// PendingCoordinationAcks returns the number of coordination-ack entries not
// yet drained (all entries still held).
func (a *Agent) PendingCoordinationAcks() int {
	return len(a.coordinationAcks)
}

// ReadyCoordinationAcks returns how many leading ack queue entries are ready to
// send. Counts the ready prefix only. The scan stops at the first unready
// entry. Drain is prefix-gated on the same rule.
func (a *Agent) ReadyCoordinationAcks() int {
	n := 0
	for i := range a.coordinationAcks {
		if !a.coordinationAcks[i].ready {
			break
		}
		n++
	}
	return n
}

// NextAckID returns the next ServiceAck id this service will send. Starts at 0.
// Advances only on a successful send, so the value equals the sent-ack count.
func (a *Agent) NextAckID() int64 {
	return a.ackID
}

// LastSnapshotRecordingID returns the RelevantID of the last sent snapshot
// ack, or -1 before any snapshot ack sends. A sent ack of a failed take
// carries -1.
func (a *Agent) LastSnapshotRecordingID() int64 {
	return a.lastSnapshotRecordingID
}

// ServiceFailure returns the first marked service-path failure, or nil.
func (a *Agent) ServiceFailure() error {
	return a.serviceFailure
}

// handleJoinLog is the JoinLog delivery entry. Unit tests and the control poll
// call it in-package.
//
// Order:
//  1. When a join is already pending, mark its coordinationAck ready
//     (supersession).
//  2. Append one join-kind coordinationAck for event.logPosition, ready false.
//  3. Cap a bound image at event.logPosition (handoff).
//  4. Store event as pendingJoin.
func (a *Agent) handleJoinLog(event logJoinEvent) {
	if a.closed {
		return
	}

	if a.pendingJoin != nil {
		a.markAckReady(ackKindJoin)
	}

	a.coordinationAcks = append(a.coordinationAcks, coordinationAck{
		logPosition: event.logPosition,
		kind:        ackKindJoin,
		relevantID:  nullValue,
	})

	if a.adapter.Image() != nil {
		a.adapter.SetMaxLogPosition(event.logPosition)
	}

	a.pendingJoin = &event
}

// failSnapshotEntry sets the first unready snapshot entry ready with cause.
// With no such entry, or no ack sender, it marks cause at once. Otherwise
// the drain marks cause.
func (a *Agent) failSnapshotEntry(cause error) {
	e := a.markAckReady(ackKindSnapshot)
	if e == nil || !a.canPublish() {
		a.markFailure(cause)
	}
	if e != nil {
		e.failure = cause
	}
}

// markAckReady sets ready on the first unready entry of kind and returns
// it. Returns nil when no entry matches. At most one start entry is queued
// (NewAgent). At most one join entry is unready (the current pending join).
func (a *Agent) markAckReady(kind coordinationAckKind) *coordinationAck {
	for i := range a.coordinationAcks {
		e := &a.coordinationAcks[i]
		if e.kind == kind && !e.ready {
			e.ready = true
			return e
		}
	}
	return nil
}

// joinWork progresses a pending join when no image is bound.
//
// Behaviour: a bind runs the join egress rule (joinEgress) after it marks
// the join ack ready.
//
// Returns 1 when the join binds; 0 otherwise. Marks sticky failure on
// subscription add error or contiguity mismatch.
func (a *Agent) joinWork() int {
	event := a.pendingJoin
	if event == nil {
		return 0
	}

	// Reuse the subscription when channel and stream match. Otherwise close and re-add.
	if a.logSubscription != nil &&
		(a.logSubscription.Channel() != event.logChannel ||
			a.logSubscription.StreamID() != event.logStreamID) {
		_ = a.logSubscription.Close()
		a.logSubscription = nil
	}
	if a.logSubscription == nil {
		if a.subscriptions == nil {
			return 0
		}
		sub, err := a.subscriptions.AddSubscription(event.logStreamID, event.logChannel)
		if err != nil {
			a.markFailure(err)
			return 0
		}
		a.logSubscription = sub
	}

	image := findImageBySessionID(a.logSubscription, event.logSessionID)
	if image == nil {
		return 0
	}

	// Contiguity: event position matches consumed position; image join matches
	// event position. Mismatch marks sticky failure and does not bind.
	if event.logPosition != a.logPosition || image.JoinPosition() != event.logPosition {
		a.markFailure(errLogNotContiguous)
		return 0
	}

	a.adapter.SetImage(image)
	a.adapter.SetMaxLogPosition(event.maxLogPosition)
	a.memberID = event.memberID
	a.role = event.role
	a.markAckReady(ackKindJoin)
	a.joinEgress(event)
	a.pendingJoin = nil
	return 1
}

// maybeUnbind releases the log image when the poll made no progress and the
// image is closed or at the maxLogPosition cap.
//
// Adopts max(image.Position(), logPosition) first so padding consumed without a
// callback still advances the contiguity position (upstream closeLog rule).
// Then disconnects the egress of every session. Role becomes
// [codecs.RoleFollower]. Does not close the subscription.
func (a *Agent) maybeUnbind(polled int) {
	image := a.adapter.Image()
	if image == nil || polled != 0 {
		return
	}
	pos := image.Position()
	atCap := pos >= a.adapter.MaxLogPosition()
	if !image.IsClosed() && !atCap {
		return
	}
	if pos > a.logPosition {
		a.logPosition = pos
	}
	a.adapter.SetImage(nil)
	a.disconnectEgress()
	a.role = codecs.RoleFollower
}

func (a *Agent) markFailure(err error) {
	if a.serviceFailure == nil {
		a.serviceFailure = err
	}
}

// adoptConsumed sets logPosition to the frame end and clusterTime to the event
// timestamp.
func (a *Agent) adoptConsumed(logPosition, clusterTime int64) {
	a.logPosition = logPosition
	a.clusterTime = clusterTime
}

// findImageBySessionID walks subscription images for an open image with the
// given session id. ImageAt reports nil and closed images as absent.
func findImageBySessionID(sub *aeron.Subscription, sessionID int32) *aeron.Image {
	if sub == nil {
		return nil
	}
	for i := range sub.ImageCount() {
		image, ok := sub.ImageAt(i)
		if ok && image.SessionID() == sessionID {
			return image
		}
	}
	return nil
}

func (a *Agent) handleSessionMessage(
	logPosition, sessionID, timestamp int64,
	payload []byte,
) aeron.ControlledAction {
	a.adoptConsumed(logPosition, timestamp)
	if a.service != nil {
		session, _ := a.ClientSession(sessionID)
		a.service.SessionMessage(session, timestamp, payload)
	}
	return aeron.ControlledContinue
}

func (a *Agent) handleSessionOpen(
	logPosition int64,
	fields codecs.SessionOpenEventFields,
) aeron.ControlledAction {
	// Clash: mark sticky failure and abort before stamping position so the
	// unconsumed frame end is not adopted.
	if _, exists := a.sessions[fields.ClusterSessionID]; exists {
		a.markFailure(errSessionClash)
		return aeron.ControlledAbort
	}

	a.adoptConsumed(logPosition, fields.Timestamp)
	session := &containerClientSession{
		agent:            a,
		id:               fields.ClusterSessionID,
		responseStreamID: fields.ResponseStreamID,
		responseChannel:  string(fields.ResponseChannel),
		encodedPrincipal: fields.EncodedPrincipal,
		addCorrelationID: nullValue,
	}
	if a.role == codecs.RoleLeader {
		a.connectClientSession(session)
	}
	a.addClientSession(session)
	if a.service != nil {
		a.service.SessionOpen(session, fields.Timestamp)
	}
	return aeron.ControlledContinue
}

func (a *Agent) handleSessionClose(
	logPosition int64,
	fields codecs.SessionCloseEventFields,
) aeron.ControlledAction {
	a.adoptConsumed(logPosition, fields.Timestamp)

	session := a.removeClientSession(fields.ClusterSessionID)
	if session == nil {
		// Unknown session: skip callback, continue (upstream continues).
		return aeron.ControlledContinue
	}
	a.disconnectClientSession(session)
	if a.service != nil {
		a.service.SessionClose(session, fields.Timestamp, fields.CloseReason)
	}
	return aeron.ControlledContinue
}

func (a *Agent) handleTimerEvent(
	logPosition int64,
	fields codecs.TimerEventFields,
) aeron.ControlledAction {
	a.adoptConsumed(logPosition, fields.Timestamp)
	if a.service != nil {
		a.service.TimerEvent(fields.CorrelationID, fields.Timestamp)
	}
	return aeron.ControlledContinue
}

func (a *Agent) handleNewLeadershipTermEvent(
	logPosition int64,
	fields codecs.NewLeadershipTermEventFields,
) aeron.ControlledAction {
	a.adoptConsumed(logPosition, fields.Timestamp)
	a.timeUnit = fields.TimeUnit
	if a.timeUnit == clusterwire.ClusterTimeUnit_NULL_VALUE {
		a.timeUnit = clusterwire.ClusterTimeUnit_MILLIS
	}
	a.appVersion = fields.AppVersion
	a.egressLeadershipTermID = fields.LeadershipTermID
	if a.leadershipHandler != nil {
		a.leadershipHandler.NewLeadershipTermEvent(
			fields.LeadershipTermID,
			fields.LogPosition,
			fields.Timestamp,
			fields.TermBaseLogPosition,
			fields.LeaderMemberID,
			fields.LogSessionID,
			fields.TimeUnit,
			fields.AppVersion,
		)
	}
	return aeron.ControlledContinue
}

// handleClusterAction delivers one ClusterActionRequest from the log adapter.
//
// Order:
//  1. Note time and position from the frame. Runs for every action kind.
//  2. Decoded LogPosition must equal the frame-end position. Mismatch
//     marks failure and returns ControlledAbort. Noted values stay.
//  3. SUSPEND and RESUME return ControlledContinue. No hook. No entry.
//  4. Unknown Action, including NULL_VALUE, marks failure and returns
//     ControlledAbort.
//  5. SNAPSHOT with Flags != 0 returns ControlledContinue. No hook. No
//     entry.
//  6. SNAPSHOT with Flags == 0 appends one unready snapshot entry, then
//     starts a take. A take that cannot start fails the entry. Returns
//     ControlledBreak. The log poll holds at the action position until the
//     take resolves.
func (a *Agent) handleClusterAction(
	logPosition int64,
	fields codecs.ClusterActionRequestFields,
) aeron.ControlledAction {
	a.adoptConsumed(logPosition, fields.Timestamp)

	if fields.LogPosition != logPosition {
		return a.handleDispatchFailure(errActionPositionMismatch)
	}

	switch fields.Action {
	case clusterwire.ClusterAction_SUSPEND, clusterwire.ClusterAction_RESUME:
		return aeron.ControlledContinue
	case clusterwire.ClusterAction_SNAPSHOT:
		if fields.Flags != 0 {
			return aeron.ControlledContinue
		}
		a.startSnapshotTake(logPosition, fields.LeadershipTermID)
		return aeron.ControlledBreak
	case clusterwire.ClusterAction_NULL_VALUE:
		fallthrough
	default:
		return a.handleDispatchFailure(errUnknownClusterAction)
	}
}

func (a *Agent) handleDispatchFailure(err error) aeron.ControlledAction {
	a.markFailure(err)
	return aeron.ControlledAbort
}
