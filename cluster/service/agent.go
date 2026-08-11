package service

import (
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
)

// Sticky failure causes marked once on the service path. Unexported: tests
// match with errors.Is against the package-level values.
var (
	errLogNotContiguous = errors.New("service: log not contiguous")
	errSessionClash     = errors.New("service: session id clash")
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

	adapter *BoundedLogAdapter

	// serviceFailure is the first fatal service-path failure, or nil.
	// Marked once; never cleared. DoWork returns 0 while marked.
	serviceFailure error

	// pendingJoin is the last JoinLog not yet bound, or nil. One slot. A newer
	// join supersedes an unbound older join. Only the current pendingJoin ack
	// entry is unready. That entry is always the last coordinationAcks entry.
	pendingJoin *logJoinEvent

	// coordinationAcks is the FIFO of delivered coordination frames not yet
	// acked. Ready entries form a queue prefix: every entry when pendingJoin is
	// nil; every entry but the last when pendingJoin is non-nil.
	coordinationAcks []coordinationAck

	// sessions is the log-derived registry keyed by cluster session id.
	sessions map[int64]*clientSessionRecord

	logSubscription *aeron.Subscription

	leadershipHandler NewLeadershipTermEventHandler

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

// coordinationAck is one delivered coordination frame awaiting its ack.
//
// Ready state is derived. Only the current pendingJoin entry is unready. That
// entry is always the last queue entry. Bind or supersession clears the pending
// slot and makes the entry ready.
type coordinationAck struct {
	logPosition int64
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
// NewLeadershipTermEventHandler is detected once via type assertion on
// cfg.Service.
func NewAgent(cfg AgentConfig) *Agent {
	a := &Agent{
		serviceID:         cfg.ServiceID,
		service:           cfg.Service,
		committedPosition: cfg.CommittedPosition,
		subscriptions:     cfg.Subscriptions,
		memberID:          -1,
		sessions:          make(map[int64]*clientSessionRecord),
	}
	if nlt, ok := cfg.Service.(NewLeadershipTermEventHandler); ok {
		a.leadershipHandler = nlt
	}
	a.adapter = NewBoundedLogAdapter(a, cfg.FragmentLimit)
	return a
}

// DoWork runs one duty tick. nowNs is reserved for later depth; no step reads
// it yet.
//
// Order:
//  1. serviceFailure or closed: return 0
//  2. start work once: Service.Start(agent); error marks sticky failure
//  3. join work while pendingJoin is non-nil and no image is bound
//  4. log poll when image bound and CommittedPosition non-nil
//  5. unbind when zero polled and (image closed or position at maxLogPosition)
//
// Returns the work count: Start, each join bind, and each polled fragment.
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

	if a.pendingJoin != nil && a.adapter.Image() == nil {
		work += a.joinWork()
		if a.serviceFailure != nil {
			return work
		}
	}

	polled := 0
	if a.adapter.Image() != nil && a.committedPosition != nil {
		polled = a.adapter.Poll(a.committedPosition())
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

// ReadyCoordinationAcks returns how many ack queue entries are ready to send.
// Ready entries form a queue prefix. The count is len(coordinationAcks) when no
// join is pending, and len-1 when a join is still unbound. That unbound join is
// always the last entry.
func (a *Agent) ReadyCoordinationAcks() int {
	n := len(a.coordinationAcks)
	if a.pendingJoin != nil {
		return n - 1
	}
	return n
}

// ServiceFailure returns the first marked service-path failure, or nil.
func (a *Agent) ServiceFailure() error {
	return a.serviceFailure
}

// handleJoinLog is the JoinLog delivery entry. Unit tests and the control poll
// call it in-package.
//
// Order:
//  1. Append one coordinationAck for event.logPosition.
//  2. Cap a bound image at event.logPosition (handoff).
//  3. Store event as pendingJoin. Supersedes any unbound join. The prior entry
//     becomes ready by queue position.
func (a *Agent) handleJoinLog(event logJoinEvent) {
	if a.closed {
		return
	}

	a.coordinationAcks = append(a.coordinationAcks, coordinationAck{
		logPosition: event.logPosition,
	})

	if a.adapter.Image() != nil {
		a.adapter.SetMaxLogPosition(event.logPosition)
	}

	a.pendingJoin = &event
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
	if a.logSubscription != nil {
		same := a.logSubscription.Channel() == event.logChannel &&
			a.logSubscription.StreamID() == event.logStreamID
		if !same {
			_ = a.logSubscription.Close()
			a.logSubscription = nil
		}
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

func (a *Agent) handleDispatchFailure(err error) aeron.ControlledAction {
	a.markFailure(err)
	return aeron.ControlledAbort
}
