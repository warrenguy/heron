//go:build integration

package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/cluster/election"
)

const raceReportPrefix = "WARNING: DATA RACE"

// memberRecord is one JSON line on the child status pipe. The child sets
// exactly one field.
type memberRecord struct {
	Ready   *MemberReady   `json:",omitempty"`
	Status  *MemberStatus  `json:",omitempty"`
	Applied *MemberApplied `json:",omitempty"`
	Exit    *MemberExit    `json:",omitempty"`
}

// MemberReady is the record a child writes after it opens its module.
type MemberReady struct {
	ClientID                 int64
	ColdStart                bool
	TermRecordingID          int64
	LogRecordingStopPosition int64
}

// MemberStatus is one sample of a child member.
//
// Started is false while the child waits for the start signal and true once
// the duty loop runs.
//
// LogSessionID is the log publication's session id. An unbound publication
// reports 0, which is also a valid id. LogPublicationBound distinguishes the
// two cases.
type MemberStatus struct {
	Started             bool
	MemberID            int32
	Role                consensus.Role
	LeaderMemberID      int32
	LeadershipTermID    int64
	ElectionState       election.ElectionState
	ElectionComplete    bool
	CommitPosition      int64
	ElectionCount       int64
	LastElectionTrigger consensus.ElectionTrigger
	LocalLogRecordingID int64
	RecordingActive     bool
	LogPublicationBound bool
	LogSessionID        int32
	LogImageSessions    []int32
	ServiceLogPosition  int64
	AppliedCount        int64
	EchoFailures        int64
	ApplyDigest         uint64
}

// MemberApplied is one sequenced session message the service applied.
type MemberApplied struct {
	Seq int64
}

// ExitTextDecodeRequest is the exit-record prefix for a request decode error.
const ExitTextDecodeRequest = "keyvalue: decode request"

// MemberExit is the record a child writes when it exits. Error is empty on a
// clean stop.
type MemberExit struct {
	Error      string
	Terminated bool
}

// ProcessMember is one child process incarnation. Call every method from the
// test goroutine.
//
// A reader goroutine calls the status handler after it stores each status
// record. The handler must not block or call ProcessMember methods.
type ProcessMember struct {
	cmd     *exec.Cmd
	logPath string

	statusHandler func(MemberStatus)

	mu      sync.Mutex
	ready   *MemberReady
	status  *MemberStatus
	applied []MemberApplied
	exitRec *MemberExit

	waitDone chan struct{}
	doneCh   chan struct{}
}

// startProcessMember starts cmd with a status pipe on fd 3 and a lifeline
// pipe on stdin. Stdout and stderr go to logPath.
//
// The parent closes its status write end after Start, so the reader sees EOF
// when the child exits.
func startProcessMember(
	cmd *exec.Cmd,
	logPath string,
	handler func(MemberStatus),
) (*ProcessMember, error) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("process member: log file: %w", err)
	}

	statusR, statusW, err := os.Pipe()
	if err != nil {
		closeFiles(logFile)
		return nil, fmt.Errorf("process member: status pipe: %w", err)
	}

	if _, err := cmd.StdinPipe(); err != nil {
		closeFiles(logFile, statusR, statusW)
		return nil, fmt.Errorf("process member: lifeline pipe: %w", err)
	}

	cmd.ExtraFiles = []*os.File{statusW}
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		closeFiles(logFile, statusR, statusW)
		return nil, fmt.Errorf("process member: start: %w", err)
	}

	closeFiles(logFile, statusW)

	member := &ProcessMember{
		cmd:           cmd,
		logPath:       logPath,
		statusHandler: handler,
		waitDone:      make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	go member.run(statusR)
	return member, nil
}

func closeFiles(files ...*os.File) {
	for _, f := range files {
		_ = f.Close()
	}
}

func (m *ProcessMember) run(statusR *os.File) {
	defer close(m.doneCh)

	go func() {
		_ = m.cmd.Wait()
		close(m.waitDone)
	}()

	m.readStatus(statusR)
	<-m.waitDone
}

func (m *ProcessMember) readStatus(statusR *os.File) {
	defer closeFiles(statusR)

	dec := json.NewDecoder(statusR)
	for {
		var rec memberRecord

		// SIGKILL can cut the last line. Decode then fails on the fragment.
		if err := dec.Decode(&rec); err != nil {
			return
		}

		m.store(rec)
	}
}

func (m *ProcessMember) store(rec memberRecord) {
	m.mu.Lock()

	switch {
	case rec.Ready != nil:
		m.ready = rec.Ready

	case rec.Status != nil:
		m.status = rec.Status

	case rec.Applied != nil:
		m.applied = append(m.applied, *rec.Applied)

	case rec.Exit != nil:
		m.exitRec = rec.Exit
	}

	m.mu.Unlock()

	if rec.Status != nil && m.statusHandler != nil {
		m.statusHandler(rec.Status.clone())
	}
}

func (s *MemberStatus) clone() MemberStatus {
	cp := *s
	cp.LogImageSessions = slices.Clone(s.LogImageSessions)
	return cp
}

// Kill sends SIGKILL, then waits for process exit and status EOF.
func (m *ProcessMember) Kill() error {
	err := m.signal(syscall.SIGKILL)
	<-m.doneCh
	return err
}

// SignalStop sends SIGTERM. It does not wait.
func (m *ProcessMember) SignalStop() error {
	return m.signal(syscall.SIGTERM)
}

// HasExited reports whether process exit and status EOF both happened.
func (m *ProcessMember) HasExited() bool {
	return isClosed(m.doneCh)
}

// AwaitExit waits up to bound for process exit and status EOF.
//
// AwaitExit does not send a signal or check the exit code.
func (m *ProcessMember) AwaitExit(bound time.Duration) error {
	if !m.awaitDone(bound) {
		return fmt.Errorf("process member: exit bound %s elapsed", bound)
	}

	return nil
}

func (m *ProcessMember) awaitDone(bound time.Duration) bool {
	timer := time.NewTimer(bound)
	defer timer.Stop()

	select {
	case <-m.doneCh:
		return true
	case <-timer.C:
		// select picks at random among ready cases.
		return isClosed(m.doneCh)
	}
}

// AwaitStop waits up to bound for process exit and status EOF.
//
// Returns an error when:
//   - the bound elapses before the child finishes
//   - the exit code is not 0
//   - the exit record is missing
//
// AwaitStop sends SIGKILL before it returns the bound error.
func (m *ProcessMember) AwaitStop(bound time.Duration) error {
	if !m.awaitDone(bound) {
		_ = m.Kill()
		return fmt.Errorf("process member: stop bound %s elapsed", bound)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error

	if code := m.cmd.ProcessState.ExitCode(); code != 0 {
		errs = append(errs, fmt.Errorf("process member: exit code %d", code))
	}

	if m.exitRec == nil {
		errs = append(errs, errors.New("process member: exit record missing"))
	}

	return errors.Join(errs...)
}

// Stop sends SIGTERM, then waits up to bound.
func (m *ProcessMember) Stop(bound time.Duration) error {
	if err := m.SignalStop(); err != nil {
		return err
	}

	return m.AwaitStop(bound)
}

// SignalStart sends SIGUSR1. The child leaves its start wait on that signal.
func (m *ProcessMember) SignalStart() error {
	return m.signal(syscall.SIGUSR1)
}

func (m *ProcessMember) signal(sig os.Signal) error {
	err := m.cmd.Process.Signal(sig)
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}

	return err
}

func (m *ProcessMember) isLive() bool {
	return !isClosed(m.doneCh)
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// Ready returns a copy of the ready record.
func (m *ProcessMember) Ready() (MemberReady, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ready == nil {
		return MemberReady{}, false
	}

	return *m.ready, true
}

// Status returns a copy of the latest status record.
func (m *ProcessMember) Status() (MemberStatus, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.status == nil {
		return MemberStatus{}, false
	}

	return m.status.clone(), true
}

// Applied returns a copy of every applied record, oldest first.
func (m *ProcessMember) Applied() []MemberApplied {
	m.mu.Lock()
	defer m.mu.Unlock()

	return slices.Clone(m.applied)
}

// ExitRecord returns a copy of the exit record.
func (m *ProcessMember) ExitRecord() (MemberExit, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.exitRec == nil {
		return MemberExit{}, false
	}

	return *m.exitRec, true
}

// ProcessState returns the reaped child's state, or nil before the reap.
//
// The state can be non-nil before HasExited reports true, while the reader
// still stores the exit record.
func (m *ProcessMember) ProcessState() *os.ProcessState {
	if !isClosed(m.waitDone) {
		return nil
	}

	return m.cmd.ProcessState
}

// LogPath returns the child's stdout and stderr log.
func (m *ProcessMember) LogPath() string {
	return m.logPath
}

// RaceReports returns the header line of each race detector report in the log.
func (m *ProcessMember) RaceReports() []string {
	data, err := os.ReadFile(m.logPath)
	if err != nil {
		return nil
	}

	var lines []string

	for line := range strings.SplitSeq(string(data), "\n") {
		if strings.HasPrefix(line, raceReportPrefix) {
			lines = append(lines, line)
		}
	}

	return lines
}
