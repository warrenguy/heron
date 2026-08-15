package service

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// Sticky failure causes marked once on the service path. Unexported: tests
// match with errors.Is against the package-level values.
var (
	errLogNotContiguous       = errors.New("service: log not contiguous")
	errSessionClash           = errors.New("service: session id clash")
	errTimerPublish           = errors.New("service: timer publish failed")
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

	// ModuleProxy sends ServiceAck and timer-request frames (stream 105). Nil
	// is allowed: ready acks stay queued. A proxy with no bound publication
	// drains nothing, same as nil.
	ModuleProxy *ConsensusModuleProxy

	// SnapshotPublication is handed to TakeSnapshot on a snapshot action.
	// Nil is allowed. Nothing else reads the field.
	SnapshotPublication *aeron.ExclusivePublication
}

// Agent owns the service duty cycle: log join, bounded log consumption,
// session registry, and the pending coordination-ack queue.
//
// Concurrency: invoker-only. The harness drives DoWork after runner.DoWork on
// one goroutine. Agent does not pump aeron.Client.
//
// Lifetime: Agent owns the log subscription it adds and closes it in Close.
// Agent does not own the aeron.Client.
//
// Ownership: Agent owns the BoundedLogAdapter and the log subscription. Agent
// does not own Images the subscription holds; unbind clears the adapter Image
// reference only.
type Agent struct {
	serviceID         int32
	service           ClusteredService
	committedPosition func() int64
	subscriptions     SubscriptionAdder
	control           ControlIngress
	moduleProxy       *ConsensusModuleProxy

	adapter *BoundedLogAdapter

	// serviceFailure is the first fatal service-path failure, or nil.
	// Marked once; never cleared. DoWork returns 0 while marked.
	serviceFailure error

	// pendingJoin is the last JoinLog not yet bound, or nil. One slot. A newer
	// join supersedes an unbound older join and marks the prior join entry
	// ready.
	pendingJoin *logJoinEvent

	// coordinationAcks is the FIFO of delivered coordination frames not yet
	// acked. Drain is prefix-gated: only a ready first queued entry may send.
	// Join entries become ready at bind or supersession. Request entries become
	// ready when logPosition equals the requested position. Snapshot entries
	// are ready on append.
	coordinationAcks []coordinationAck

	// sessions is the log-derived registry keyed by cluster session id.
	sessions map[int64]*clientSessionRecord

	logSubscription *aeron.Subscription

	leadershipHandler   NewLeadershipTermEventHandler
	snapshotHandler     TakeSnapshotHandler
	snapshotPublication *aeron.ExclusivePublication

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

	// ackKindSnapshot: ready on append. No sweep rule.
	ackKindSnapshot
)

// coordinationAck is one delivered coordination frame awaiting its ack.
//
// Join entries set ready at bind or supersession. Request entries set ready
// when the agent log position equals the entry logPosition. Snapshot entries
// are ready on append. Drain is prefix-gated: only a ready first queued entry
// may send.
type coordinationAck struct {
	logPosition int64
	kind        coordinationAckKind
	ready       bool
	relevantID  int64
}

// clientSessionRecord holds open-event fields for one log-derived session. The
// sessions map key is the session id.
//
// Ownership: the decoder clones the response channel and principal out of the
// term buffer. The record retains those slices; the slices never alias the log.
type clientSessionRecord struct {
	responseStreamID int32
	responseChannel  []byte
	encodedPrincipal []byte
}

// Compile-time assertion: Agent satisfies Cluster.
var _ Cluster = (*Agent)(nil)

// NewAgent builds an Agent from cfg. The adapter is created with no Image.
// memberID starts at -1 and logPosition at 0 (cold-start service).
//
// NewLeadershipTermEventHandler and TakeSnapshotHandler are detected once
// via type assertion on cfg.Service.
func NewAgent(cfg AgentConfig) *Agent {
	a := &Agent{
		serviceID:           cfg.ServiceID,
		service:             cfg.Service,
		committedPosition:   cfg.CommittedPosition,
		subscriptions:       cfg.Subscriptions,
		control:             cfg.Control,
		moduleProxy:         cfg.ModuleProxy,
		snapshotPublication: cfg.SnapshotPublication,
		memberID:            -1,
		sessions:            make(map[int64]*clientSessionRecord),
	}
	if nlt, ok := cfg.Service.(NewLeadershipTermEventHandler); ok {
		a.leadershipHandler = nlt
	}
	if snap, ok := cfg.Service.(TakeSnapshotHandler); ok {
		a.snapshotHandler = snap
	}
	a.controlOnFragment = a.handleControlFragment
	a.adapter = NewBoundedLogAdapter(a, cfg.FragmentLimit)
	return a
}

// DoWork runs one duty tick. nowNs is reserved for later depth; no step reads
// nowNs yet.
//
// Order:
//  1. serviceFailure or closed: return 0
//  2. start work once: Service.Start(agent); error marks failure once
//  3. control work: poll control, request readiness sweep, drain at most one
//     ready first queued ack
//  4. join work while pendingJoin is non-nil and no image is bound
//  5. log poll when image bound and CommittedPosition non-nil; bound is
//     min(committed, lowest unready request entry position)
//  6. unbind when zero polled and (image closed or position at maxLogPosition)
//
// Returns the work count: Start, control fragments, each sent ack, each join
// bind, and each polled fragment.
//
// Concurrency: invoker-only.
func (a *Agent) DoWork(nowNs int64) int {
	_ = nowNs
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
	}

	work += a.controlWork()
	if a.serviceFailure != nil {
		return work
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
		polled = a.adapter.Poll(bound)
		work += polled
		if a.serviceFailure != nil {
			return work
		}
	}

	a.maybeUnbind(polled)
	return work
}

// Close unbinds the log image and closes the owned log subscription.
//
// Idempotent. The Agent is inert after Close.
//
// Ownership: closes the log subscription Agent added. Does not close Images.
func (a *Agent) Close() {
	if a.closed {
		return
	}
	a.closed = true
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
	if a.moduleProxy == nil || a.moduleProxy.Publication() == nil {
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
	if a.moduleProxy == nil || a.moduleProxy.Publication() == nil {
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
		a.markJoinAckReady()
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

// markJoinAckReady sets ready on the first unready join-kind entry. At most
// one join entry is unready (the current pending join).
func (a *Agent) markJoinAckReady() {
	for i := range a.coordinationAcks {
		e := &a.coordinationAcks[i]
		if e.kind == ackKindJoin && !e.ready {
			e.ready = true
			return
		}
	}
}

// joinWork progresses a pending join when no image is bound.
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
	a.markJoinAckReady()
	a.pendingJoin = nil
	return 1
}

// maybeUnbind releases the log image when the poll made no progress and the
// image is closed or at the maxLogPosition cap.
//
// Adopts max(image.Position(), logPosition) first so padding consumed without a
// callback still advances the contiguity position (upstream closeLog rule). Role
// becomes [codecs.RoleFollower]. Does not close the subscription.
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
		a.service.SessionMessage(sessionID, timestamp, payload)
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
	a.sessions[fields.ClusterSessionID] = &clientSessionRecord{
		responseStreamID: fields.ResponseStreamID,
		responseChannel:  fields.ResponseChannel,
		encodedPrincipal: fields.EncodedPrincipal,
	}
	if a.service != nil {
		a.service.SessionOpen(fields.ClusterSessionID, fields.Timestamp)
	}
	return aeron.ControlledContinue
}

func (a *Agent) handleSessionClose(
	logPosition int64,
	fields codecs.SessionCloseEventFields,
) aeron.ControlledAction {
	a.adoptConsumed(logPosition, fields.Timestamp)

	if _, ok := a.sessions[fields.ClusterSessionID]; !ok {
		// Unknown session: skip callback, continue (upstream continues).
		return aeron.ControlledContinue
	}
	delete(a.sessions, fields.ClusterSessionID)
	if a.service != nil {
		a.service.SessionClose(
			fields.ClusterSessionID,
			fields.Timestamp,
			fields.CloseReason,
		)
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
//  6. SNAPSHOT with Flags == 0 runs TakeSnapshot when a handler is cached.
//     Hook error marks failure and returns ControlledAbort. No entry.
//     Success or no handler appends one ready snapshot entry and returns
//     ControlledContinue.
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
		if a.snapshotHandler != nil {
			if err := a.snapshotHandler.TakeSnapshot(a.snapshotPublication); err != nil {
				return a.handleDispatchFailure(fmt.Errorf("%w: %w", errTakeSnapshot, err))
			}
		}
		a.coordinationAcks = append(a.coordinationAcks, coordinationAck{
			logPosition: logPosition,
			kind:        ackKindSnapshot,
			ready:       true,
			relevantID:  nullValue,
		})
		return aeron.ControlledContinue
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
