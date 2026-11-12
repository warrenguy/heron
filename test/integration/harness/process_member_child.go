//go:build integration

package harness

import (
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive"
	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
	"github.com/warrenguy/luas/cluster/service"
)

// ProcessMemberEnv is the environment variable that carries a ProcessMemberSpec.
// An empty value means this process is the test, not a child member.
const ProcessMemberEnv = "LUAS_PROCESS_MEMBER"

// ProcessMemberSpec is the JSON value of ProcessMemberEnv.
//
// StatusIntervalMs is the wall-time gap between status records. Zero writes a
// status record on every sample. An empty Workload selects the sequence
// service. LogChannel and ReplicationChannel replace the loopback channels
// when set.
type ProcessMemberSpec struct {
	MemberID           int32
	MembersString      string
	ClusterDir         string
	AeronDir           string
	StatusIntervalMs   int
	Workload           string
	LogChannel         string
	ReplicationChannel string
}

// RunProcessMemberIfChild returns at once when ProcessMemberEnv is empty. A
// set value runs one cluster member. The call then exits the process. TestMain
// must call this before m.Run.
func RunProcessMemberIfChild() {
	raw := os.Getenv(ProcessMemberEnv)
	if raw == "" {
		return
	}
	os.Exit(runProcessMember(raw))
}

// watchLifeline calls exit(2) when reading r returns EOF or an error. The
// parent never writes stdin, so EOF means the parent is gone. The child skips
// cleanup on that path.
func watchLifeline(r io.Reader, exit func(code int)) {
	_, _ = io.Copy(io.Discard, r)
	exit(2)
}

func runProcessMember(raw string) int {
	// SIGUSR1 kills the process by default, so install both handlers first.
	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stopSignals()
	start := make(chan os.Signal, 1)
	signal.Notify(start, syscall.SIGUSR1)
	defer signal.Stop(start)

	sink := &statusSink{enc: json.NewEncoder(os.NewFile(3, "member-status"))}

	var spec ProcessMemberSpec
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		return sink.writeExit(err)
	}

	go watchLifeline(os.Stdin, os.Exit)

	member, err := openChildMember(ctx, spec, sink)
	if err != nil {
		return sink.writeExit(err)
	}
	return member.run(ctx, start)
}

type childMember struct {
	module     *cluster.ConsensusModule
	container  *service.Container
	runner     *cluster.Runner
	svc        childService
	sink       *statusSink
	interval   time.Duration
	nextStatus time.Time
}

type childService interface {
	service.ClusteredService
	DecodeError() error
	appliedCount() int64
	applyDigest() uint64
	echoFailures() int64
}

func openChildMember(
	ctx context.Context,
	spec ProcessMemberSpec,
	sink *statusSink,
) (*childMember, error) {
	svc, err := newChildService(spec.Workload, sink)
	if err != nil {
		return nil, err
	}

	m := &childMember{
		svc:      svc,
		sink:     sink,
		interval: time.Duration(spec.StatusIntervalMs) * time.Millisecond,
	}

	cfg := MemberClusterConfig(spec.MemberID, spec.MembersString, spec.ClusterDir, spec.AeronDir)
	cfg.ServiceCount = 1
	cfg.LogChannel = cmp.Or(spec.LogChannel, cfg.LogChannel)
	cfg.ReplicationChannel = cmp.Or(spec.ReplicationChannel, cfg.ReplicationChannel)

	m.module, err = cluster.OpenConsensusModule(ctx, cluster.ConsensusModuleDeps{Config: cfg})
	if err != nil {
		return nil, fmt.Errorf("open consensus module: %w", err)
	}

	serviceCfg := service.Config{
		Aeron:             m.module.Client(),
		Service:           m.svc,
		CommittedPosition: m.module.CommitPosition,
		Archive:           &archive.Config{},
		Recovery:          &service.RecoveryConfig{},
	}
	m.container, err = service.Open(serviceCfg)
	if err != nil {
		_ = m.close()
		return nil, fmt.Errorf("open service: %w", err)
	}

	err = m.module.Agent().SetServiceLiveness(serviceCfg.ServiceID, m.container.Agent())
	if err != nil {
		_ = m.close()
		return nil, fmt.Errorf("set service liveness: %w", err)
	}

	m.runner, err = cluster.NewRunner(cluster.RunnerDeps{
		Client:  m.module.Client(),
		Agent:   m.module.Agent(),
		Archive: m.module.ArchiveClient(),
		Idle:    m.module.IdleStrategy(),
		Clock:   m.module.NanoClock(),
	})
	if err != nil {
		_ = m.close()
		return nil, fmt.Errorf("open runner: %w", err)
	}
	return m, nil
}

// run returns the process exit code.
func (m *childMember) run(ctx context.Context, start <-chan os.Signal) int {
	m.sink.write(m.readyRecord())

	if !m.waitForStart(ctx, start) {
		return m.shutdown()
	}
	return m.dutyLoop(ctx)
}

// waitForStart pumps the transport client until the start signal arrives.
// It returns false when ctx ends first.
func (m *childMember) waitForStart(ctx context.Context, start <-chan os.Signal) bool {
	pump := time.NewTicker(time.Millisecond)
	defer pump.Stop()

	for {
		select {
		case <-ctx.Done():
			return false

		case <-start:
			return true

		case <-pump.C:
			_ = m.module.Client().DoWork()
			m.sampleStatus(false)
		}
	}
}

// dutyLoop calls Runner.DoWork, not Runner.Run, to keep the status write beside
// the service tick.
func (m *childMember) dutyLoop(ctx context.Context) int {
	idle := m.module.IdleStrategy()
	defer idle.Reset()
	client := m.module.Client()
	clock := m.module.NanoClock()
	agent := m.module.Agent()
	serviceAgent := m.container.Agent()

	for {
		if ctx.Err() != nil {
			return m.shutdown()
		}
		work := m.runner.DoWork() + serviceAgent.DoWork(clock())

		exit := chooseDutyLoopExit(
			client.IsTerminating(),
			agent.TerminationError(),
			m.svc.DecodeError(),
		)
		if exit != nil {
			code := 3

			if exit.Terminated {
				code = 4

				if err := m.closeWithin(10 * time.Second); err != nil {
					fmt.Fprintf(os.Stderr, "close: %v\n", err)
				}
			}

			m.sink.write(memberRecord{Exit: exit})
			return code
		}

		m.sampleStatus(true)
		idle.Idle(work)
	}
}

// chooseDutyLoopExit returns the exit record for one cycle's conditions, or
// nil when the loop continues.
func chooseDutyLoopExit(
	transportTerminating bool,
	terminationError, decodeError error,
) *MemberExit {
	switch {
	case transportTerminating:
		return &MemberExit{Error: cluster.ErrTransportTerminating.Error()}

	case terminationError != nil:
		return &MemberExit{Error: terminationError.Error(), Terminated: true}

	case decodeError != nil:
		return &MemberExit{Error: ExitTextDecodeRequest + ": " + decodeError.Error()}

	default:
		return nil
	}
}

func (m *childMember) closeWithin(bound time.Duration) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- m.close()
	}()

	timer := time.NewTimer(bound)
	defer timer.Stop()

	select {
	case err := <-errCh:
		return err
	case <-timer.C:
		return fmt.Errorf("process member: close bound %s elapsed", bound)
	}
}

func (m *childMember) shutdown() int {
	return m.sink.writeExit(m.close())
}

// close closes the container before the module. The container may be nil.
func (m *childMember) close() error {
	return errors.Join(m.container.Close(), m.module.Close())
}

// sampleStatus writes a status record when the interval has passed since the
// last one. The first call always writes.
func (m *childMember) sampleStatus(started bool) {
	now := time.Now()
	if now.Before(m.nextStatus) {
		return
	}
	m.sink.write(m.statusRecord(started))
	m.nextStatus = now.Add(m.interval)
}

func (m *childMember) readyRecord() memberRecord {
	return memberRecord{
		Ready: &MemberReady{
			ClientID:                 m.module.Client().ClientId(),
			ColdStart:                m.module.IsColdStart(),
			TermRecordingID:          m.module.RecordingLog().TermRecordingID(),
			LogRecordingStopPosition: m.module.LogRecordingStopPosition(),
		},
	}
}

func (m *childMember) statusRecord(started bool) memberRecord {
	module := m.module
	agent := module.Agent()
	logSession, logBound := logSessionID(module.LogStreams())

	return memberRecord{
		Status: &MemberStatus{
			Started:             started,
			MemberID:            module.ThisMemberID(),
			Role:                module.Role(),
			LeaderMemberID:      module.LeaderMemberID(),
			LeadershipTermID:    module.LeadershipTermID(),
			ElectionState:       module.ElectionState(),
			ElectionComplete:    module.ElectionComplete(),
			CommitPosition:      module.CommitPosition(),
			ElectionCount:       agent.ElectionCount(),
			LastElectionTrigger: agent.LastElectionTrigger(),
			LocalLogRecordingID: agent.LocalLogRecordingID(),
			RecordingActive:     agent.IsRecordingActive(),
			LogPublicationBound: logBound,
			LogSessionID:        logSession,
			LogImageSessions:    logImageSessions(module.LogStreams()),
			ServiceLogPosition:  m.container.Agent().LogPosition(),
			AppliedCount:        m.svc.appliedCount(),
			EchoFailures:        m.svc.echoFailures(),
			ApplyDigest:         m.svc.applyDigest(),
		},
	}
}

// logSessionID returns the log publication session id and whether that
// publication is bound.
func logSessionID(streams *cluster.LogStreams) (int32, bool) {
	pub := streams.Publication()
	if pub == nil {
		return 0, false
	}
	return pub.SessionID(), true
}

func logImageSessions(streams *cluster.LogStreams) []int32 {
	sub := streams.Subscription()
	if sub == nil {
		return nil
	}

	var ids []int32
	for i := range sub.ImageCount() {
		if img, ok := sub.ImageAt(i); ok {
			ids = append(ids, img.SessionID())
		}
	}
	return ids
}

// statusSink writes member records as JSON lines to the status pipe.
type statusSink struct {
	enc *json.Encoder
}

func (s *statusSink) write(rec memberRecord) {
	if err := s.enc.Encode(rec); err != nil {
		fmt.Fprintf(os.Stderr, "status write: %v\n", err)
	}
}

// writeExit writes the exit record for err. It returns 0 for a nil err, else 1.
func (s *statusSink) writeExit(err error) int {
	if err == nil {
		s.write(memberRecord{Exit: &MemberExit{}})
		return 0
	}
	s.write(memberRecord{Exit: &MemberExit{Error: err.Error()}})
	return 1
}

type sequenceService struct {
	sink         *statusSink
	applied      int64
	failedEchoes int64
}

var _ service.ClusteredService = (*sequenceService)(nil)

func (s *sequenceService) Start(service.Cluster) error { return nil }

func (s *sequenceService) SessionOpen(service.ClientSession, int64) {}

func (s *sequenceService) SessionClose(service.ClientSession, int64, clusterwire.CloseReason) {}

func (s *sequenceService) TimerEvent(int64, int64) {}

func (s *sequenceService) DecodeError() error { return nil }

func (s *sequenceService) appliedCount() int64 { return s.applied }

func (s *sequenceService) applyDigest() uint64 { return 0 }

func (s *sequenceService) echoFailures() int64 { return s.failedEchoes }

func (s *sequenceService) SessionMessage(session service.ClientSession, _ int64, payload []byte) {
	if len(payload) < 8 {
		return
	}
	seq := int64(binary.BigEndian.Uint64(payload[:8]))
	s.applied++
	s.sink.write(memberRecord{Applied: &MemberApplied{Seq: seq}})

	// A service message or a closed session carries a nil session.
	if session == nil {
		return
	}

	result := session.Offer(payload)

	// An offer that ends the egress term writes nothing. The next offer lands in
	// the new term.
	if result == aeron.AdminAction {
		result = session.Offer(payload)
	}

	if result < 0 {
		s.failedEchoes++
	}
}
