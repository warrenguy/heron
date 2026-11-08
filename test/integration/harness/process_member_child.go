//go:build integration

package harness

import (
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

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/service"
)

// ProcessMemberEnv is the environment variable that carries a ProcessMemberSpec.
// An empty value means this process is the test, not a child member.
const ProcessMemberEnv = "HERON_PROCESS_MEMBER"

// ProcessMemberSpec is the JSON value of ProcessMemberEnv.
//
// StatusIntervalMs is the wall-time gap between status records. Zero writes a
// status record on every sample.
type ProcessMemberSpec struct {
	MemberID         int32
	MembersString    string
	ClusterDir       string
	AeronDir         string
	StatusIntervalMs int
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
	seq        *sequenceService
	sink       *statusSink
	interval   time.Duration
	nextStatus time.Time
}

// openChildMember closes what it already opened when a step fails.
func openChildMember(
	ctx context.Context,
	spec ProcessMemberSpec,
	sink *statusSink,
) (*childMember, error) {
	m := &childMember{
		seq:      &sequenceService{sink: sink},
		sink:     sink,
		interval: time.Duration(spec.StatusIntervalMs) * time.Millisecond,
	}

	cfg := MemberClusterConfig(spec.MemberID, spec.MembersString, spec.ClusterDir, spec.AeronDir)
	cfg.ServiceCount = 1

	var err error
	m.module, err = cluster.OpenConsensusModule(ctx, cluster.ConsensusModuleDeps{Config: cfg})
	if err != nil {
		return nil, fmt.Errorf("open consensus module: %w", err)
	}

	m.container, err = service.Open(service.Config{
		Aeron:             m.module.Client(),
		Service:           m.seq,
		CommittedPosition: m.module.CommitPosition,
		Archive:           &archive.Config{},
		Recovery:          &service.RecoveryConfig{},
	})
	if err != nil {
		_ = m.close()
		return nil, fmt.Errorf("open service: %w", err)
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

		if client.IsTerminating() || serviceAgent.ServiceFailure() != nil {
			return 3
		}

		if err := agent.TerminationError(); err != nil {
			return m.sink.writeExit(errors.Join(err, m.close()))
		}

		m.sampleStatus(true)
		idle.Idle(work)
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
			LogSessionID:        logSessionID(module.LogStreams()),
			LogImageSessions:    logImageSessions(module.LogStreams()),
			ServiceLogPosition:  m.container.Agent().LogPosition(),
			AppliedCount:        m.seq.applied,
			EchoFailures:        m.seq.echoFailures,
		},
	}
}

// logSessionID returns the log publication session id, or 0 when the
// publication is unbound.
func logSessionID(streams *cluster.LogStreams) int32 {
	pub := streams.Publication()
	if pub == nil {
		return 0
	}
	return pub.SessionID()
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

// sequenceService reports the leading sequence of each session message on the
// status pipe. It then echoes the message back to its session.
type sequenceService struct {
	sink         *statusSink
	applied      int64
	echoFailures int64
}

var _ service.ClusteredService = (*sequenceService)(nil)

func (s *sequenceService) Start(service.Cluster) error { return nil }

func (s *sequenceService) SessionOpen(service.ClientSession, int64) {}

func (s *sequenceService) SessionClose(service.ClientSession, int64, clusterwire.CloseReason) {}

func (s *sequenceService) TimerEvent(int64, int64) {}

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
		s.echoFailures++
	}
}
