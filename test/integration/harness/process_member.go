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

	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
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
// Started is false while the child waits for the start signal. It is true
// once the duty loop runs.
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
	LogSessionID        int32
	LogImageSessions    []int32
	ServiceLogPosition  int64
	AppliedCount        int64
	EchoFailures        int64
}

// MemberApplied is one sequenced session message the service applied.
type MemberApplied struct {
	Seq int64
}

// MemberExit is the record a child writes after a stop, a failed setup, or a
// member termination. Error is empty on a clean stop.
type MemberExit struct {
	Error string
}

// ProcessMember is one child process incarnation.
//
// One goroutine reads status records to EOF, then reaps the child, then closes
// doneCh. Every other method runs on the test goroutine.
//
// ProcessMember owns the child and the read end of the status pipe. cmd owns
// the lifeline pipe on the child's stdin. cmd.Wait closes the write end after
// the child exits.
type ProcessMember struct {
	cmd     *exec.Cmd
	logPath string

	mu      sync.Mutex
	ready   *MemberReady
	status  *MemberStatus
	applied []MemberApplied
	exitRec *MemberExit

	doneCh chan struct{}
}

// startProcessMember starts cmd with a status pipe on fd 3 and a lifeline
// pipe on stdin. Stdout and stderr go to logPath.
//
// The parent closes its status write end after Start, so the reader sees EOF
// when the child exits.
func startProcessMember(cmd *exec.Cmd, logPath string) (*ProcessMember, error) {
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
		cmd:     cmd,
		logPath: logPath,
		doneCh:  make(chan struct{}),
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

	m.readStatus(statusR)
	_ = m.cmd.Wait()
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
	defer m.mu.Unlock()

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

// AwaitStop waits up to bound for process exit and status EOF.
//
// Returns an error when:
//   - the bound elapses before the child finishes
//   - the exit code is not 0
//   - the exit record is missing
//
// AwaitStop sends SIGKILL before it returns the bound error.
func (m *ProcessMember) AwaitStop(bound time.Duration) error {
	timer := time.NewTimer(bound)
	defer timer.Stop()

	select {
	case <-m.doneCh:
	case <-timer.C:
		// select picks at random among ready cases. Re-check before the kill.
		if !isClosed(m.doneCh) {
			_ = m.Kill()
			return fmt.Errorf("process member: stop bound %s elapsed", bound)
		}
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

	cp := *m.status
	cp.LogImageSessions = slices.Clone(m.status.LogImageSessions)
	return cp, true
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
func (m *ProcessMember) ProcessState() *os.ProcessState {
	if !isClosed(m.doneCh) {
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
