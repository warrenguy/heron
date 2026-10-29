package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

const clusterLockFileName = "cluster.lock"

// DirectoryLockWait caps how long [OpenRecovery] retries for cluster.lock.
//
// Own release uses [releaseClusterLock]: unlock then close. That path does not
// need this wait. The cap bounds a hold this process cannot free:
//   - a live Recovery in this process or another
//   - a residual open-file description after a dead holder (forked child before
//     exec, or unfinished close)
//
// A second OpenRecovery against a live holder fails with wrapped EWOULDBLOCK
// once DirectoryLockWait elapses.
const DirectoryLockWait = 250 * time.Millisecond

const (
	// directoryLockSpin is the busy-wait duration before poll sleeps begin. A
	// forked child holds the description until the child execs. The fork window
	// usually closes inside directoryLockSpin, so the common path does not sleep.
	directoryLockSpin = 2 * time.Millisecond

	// directoryLockPoll is the sleep between attempts after directoryLockSpin. A
	// durable holder must not busy-loop for the full DirectoryLockWait.
	directoryLockPoll = 1 * time.Millisecond
)

// lockClusterDirectory takes the exclusive advisory lock on an open
// cluster.lock descriptor. path is the cluster.lock path for testhooks
// lock-blocked hooks. Production builds ignore path.
//
// Behaviour:
//   - Takes LOCK_EX|LOCK_NB. Returns nil on the first success.
//   - Retries on EWOULDBLOCK only. Every other errno returns at once.
//   - Calls trackDirectoryLockBlocked(path) on each EWOULDBLOCK before the
//     next attempt.
//   - Spins for directoryLockSpin, then sleeps directoryLockPoll between
//     attempts.
//   - Returns the last EWOULDBLOCK once DirectoryLockWait elapses.
//
// Never blocks without a bound. The caller owns the descriptor either way.
func lockClusterDirectory(fd int, path string) error {
	start := time.Now()
	for {
		err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			return err
		}
		trackDirectoryLockBlocked(path)

		elapsed := time.Since(start)
		if elapsed >= DirectoryLockWait {
			return err
		}
		if elapsed >= directoryLockSpin {
			time.Sleep(directoryLockPoll)
		}
	}
}

// releaseClusterLock unlocks cluster.lock with LOCK_UN, then closes the file.
//
// [Recovery.Close] and the [OpenRecovery] fail path call this after the
// descriptor is open. LOCK_UN on a descriptor that never took the lock is a
// no-op success.
//
// When unlock succeeds, the exclusive hold is gone before this function returns.
// flock is open-file-description scoped. Close alone leaves the hold until the
// last description reference goes away. LOCK_UN frees the hold while a forked
// child or a dup still references the description. O_CLOEXEC drops the
// descriptor at execve only and does not unlock.
//
// Discards unlock errors. The kernel still drops the lock when the last
// description reference goes away. A failed LOCK_UN on NFS or FUSE must not
// surface as a close error.
//
// Returns only the Close error.
func releaseClusterLock(lockFile *os.File) error {
	if lockFile == nil {
		return nil
	}
	_ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN)
	return lockFile.Close()
}

// RecoveryDeps are the inputs [OpenRecovery] needs.
//
// Named for [election.ElectionDeps] and [ConsensusPublicationsDeps], which carry
// injected collaborators the same way.
type RecoveryDeps struct {
	// Config is the normalized member config. [OpenRecovery] calls Validate.
	// [OpenRecovery] returns the Validate error unchanged. [OpenRecovery] then
	// reads ClusterDir, NodeStateFileSyncLevel, ServiceCount, and Election.
	// Validate still enforces the full config contract.
	//
	// Recovery writes five Config.Election fields. [OpenRecovery] overwrites any
	// caller values present. Setting those fields has no effect. Setting those
	// fields produces no error:
	//   - Always written: IsNodeStartup
	//   - From the recovery plan: LeadershipTermID, TermBaseLogPosition,
	//     LogPosition, AppendPosition
	// All other Election fields pass through unchanged (timeouts, MemberID,
	// AppointedLeaderID, GracefulClosedLeaderID, AppVersion).
	//
	// [OpenRecovery] does not retain Config.
	Config Config

	// ArchiveLister resolves recording extents for a recording log that carries
	// term entries. Production passes archive.NewLister over a connected control
	// client. Unit tests pass a map-backed fake.
	//
	// Nil is allowed when the recording log is empty. Nil with a non-empty
	// recording log is refused. recordinglog.PlanRecovery panics if a lookup is
	// required and the lister is nil.
	//
	// Non-nil with an empty recording log is allowed. Recovery reads this field
	// only. Recovery connects no archive client.
	ArchiveLister recordinglog.ArchiveLister

	// TermRecordingStopper stops the recording log's term recording before
	// [OpenRecovery] plans recovery. Production passes the archive-backed
	// stopper.
	//
	// Nil skips the stop.
	TermRecordingStopper TermRecordingStopper
}

// TermRecordingStopper stops the recording log's term recording at open.
//
// Concurrency: invoker-only. [OpenRecovery] calls StopTermRecording at most
// once, before it plans recovery.
type TermRecordingStopper interface {
	// StopTermRecording stops recordingID by identity when it is active.
	//
	// Returns an error when:
	//   - the archive refuses the stop
	//   - the stop or the catalog read cannot reach the archive
	StopTermRecording(recordingID int64) error
}

// Recovery is the member's start-of-life durable state: recording log, node
// state file, recovery plan from the recording log, and election configuration
// seeded from that plan.
//
// Open with [OpenRecovery]. Recovery owns the directory lock and both file
// handles and closes all three. The election holds the files behind interfaces
// and closes neither.
//
// Concurrency: invoker-only. [*recordinglog.Log] and [*election.NodeStateFile]
// are single-writer with no internal locking. Every method must run on the same
// goroutine as [election.Election.DoWork] and consensus.Agent.DoWork, matching
// [ConsensusPublications]. Across processes and members, an exclusive advisory
// lock on cluster.lock in the cluster directory enforces that contract for the
// lifetime of this value.
//
// Lifetime: outlives the election. The election writes candidate terms through
// NodeState and term coherency through RecordingLog for the process life. Close
// belongs in module teardown after the election finishes and transport handles
// close.
//
// Do not copy. All three handles are pointers. A value copy shares the same
// handles; both values look like owners responsible for Close. Every accessor
// takes a pointer receiver. Pass the [*Recovery].
type Recovery struct {
	lockFile       *os.File
	recordingLog   *recordinglog.Log
	nodeState      *election.NodeStateFile
	plan           recordinglog.RecoveryPlan
	electionConfig election.ElectionConfig
}

// OpenRecovery opens durable member state for one cluster directory.
//
// Open order:
//  1. Validate Config
//  2. create the cluster directory
//  3. take an exclusive lock on cluster.lock
//  4. open the recording log
//  5. refuse a non-empty log without a lister
//  6. stop the term recording when the log names one and a stopper exists
//  7. plan recovery
//  8. check the snapshot group
//  9. seed the election configuration
//  10. open the node state file
//
// Directory mode is 0700 when created. An existing directory keeps its mode.
// OpenRecovery does not chmod a directory it did not create. The lock file and
// both data files use mode 0600.
//
// The lock is an advisory flock on cluster.lock. OpenRecovery takes the lock
// before the recording log opens. [releaseClusterLock] releases the lock on
// Close and on open-fail unwind.
//
// OpenRecovery retries a held lock for up to [DirectoryLockWait]. A second
// OpenRecovery against a live holder fails with wrapped EWOULDBLOCK after the
// wait.
//
// Node state sync level comes from deps.Config. Allowed values are
// [recordinglog.FileSyncData] and [recordinglog.FileSyncFull]
// ([Config.Validate] guarantees). The recovery plan uses
// [recordinglog.NewRecoveryPlanConfig] with ServiceCount. The replicated
// recording id is always null.
//
// Seeded election config is deps.Config.Election with IsNodeStartup set and the
// four recovery fields written from the plan. All other fields pass through,
// including timeouts and member-id fields Config.Normalize builds.
//
// Returns an error when:
//   - Config fails Validate
//   - directory cannot be created
//   - lock cannot be taken
//   - either data file cannot be opened
//   - non-empty recording log without a lister
//   - the term recording stop fails. An unknown recording id wraps
//     recordinglog.ErrUnknownRecordingID
//   - planning fails
//   - the snapshot group lacks a service snapshot
//   - the plan holds a snapshot and no log term
//
// On error nothing stays open. The lock is released. The returned value is nil.
func OpenRecovery(deps RecoveryDeps) (*Recovery, error) {
	if err := deps.Config.Validate(); err != nil {
		return nil, err
	}

	dir := deps.Config.ClusterDir
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("cluster: create cluster dir %s: %w", dir, err)
	}

	// Exclusive advisory lock for the life of Recovery. See [releaseClusterLock]
	// and [DirectoryLockWait]. O_CLOEXEC so an exec'd child cannot keep the
	// descriptor.
	path := filepath.Join(dir, clusterLockFileName)
	fd, err := unix.Open(
		path,
		unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("cluster: lock cluster dir %s: %w", dir, err)
	}
	lockFile := os.NewFile(uintptr(fd), path)

	// On success Recovery owns the handles. Until then error paths unwind opened
	// handles in reverse open order.
	succeeded := false
	defer func() {
		if !succeeded {
			_ = releaseClusterLock(lockFile)
		}
	}()

	if err := lockClusterDirectory(fd, path); err != nil {
		return nil, fmt.Errorf("cluster: lock cluster dir %s: %w", dir, err)
	}

	recordingLog, err := recordinglog.Open(dir, true)
	if err != nil {
		return nil, fmt.Errorf("cluster: open recording log in %s: %w", dir, err)
	}
	defer func() {
		if !succeeded {
			_ = recordingLog.Close()
		}
	}()

	entries := recordingLog.Entries()
	if deps.ArchiveLister == nil && len(entries) > 0 {
		return nil, fmt.Errorf(
			"cluster: recording log in %s holds %d entries and needs an ArchiveLister",
			dir,
			len(entries),
		)
	}

	// Stop under the directory lock: no other process owns the recording now.
	termRecordingID := recordingLog.TermRecordingID()
	stopper := deps.TermRecordingStopper
	if stopper != nil && termRecordingID != recordinglog.NullRecordingID {
		if err := stopper.StopTermRecording(termRecordingID); err != nil {
			return nil, fmt.Errorf(
				"cluster: stop term recording %d in %s: %w",
				termRecordingID,
				dir,
				err,
			)
		}
	}

	plan, err := recordingLog.PlanRecovery(
		deps.ArchiveLister,
		recordinglog.NewRecoveryPlanConfig(deps.Config.ServiceCount),
	)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan recovery from %s: %w", dir, err)
	}

	if len(plan.Snapshots) > 0 {
		head := plan.Snapshots[0]
		if plan.Log == nil {
			return nil, fmt.Errorf(
				"cluster: recovery plan holds a snapshot at log position %d and no log term",
				head.LogPosition,
			)
		}
		if id, missing := missingServiceSnapshot(plan, deps.Config.ServiceCount); missing {
			return nil, fmt.Errorf(
				"cluster: recovery plan snapshot group at log position %d in term %d lacks service %d",
				head.LogPosition,
				head.LeadershipTermID,
				id,
			)
		}
	}

	electionConfig := deps.Config.Election
	electionConfig.IsNodeStartup = true
	election.ApplyRecoveryPlan(&electionConfig, plan)

	nodeState, err := election.OpenNodeStateFile(
		dir,
		true,
		deps.Config.NodeStateFileSyncLevel,
	)
	if err != nil {
		return nil, fmt.Errorf("cluster: open node state file in %s: %w", dir, err)
	}

	succeeded = true
	return &Recovery{
		lockFile:       lockFile,
		recordingLog:   recordingLog,
		nodeState:      nodeState,
		plan:           plan,
		electionConfig: electionConfig,
	}, nil
}

// Plan returns the computed recovery plan.
//
// The zero plan on a nil receiver.
//
// RecoveryPlan.Log is a pointer into Recovery state. Treat RecoveryPlan.Log
// as read-only. Recovery does not mutate the plan after OpenRecovery returns.
func (r *Recovery) Plan() recordinglog.RecoveryPlan {
	if r == nil {
		return recordinglog.RecoveryPlan{}
	}
	return r.plan
}

// ElectionConfig returns the seeded election configuration.
//
// Pass it to [election.NewElection]. It is a value; a caller that wants to
// change a field after recovery changes its own copy.
//
// The zero configuration on a nil receiver, which [election.NewElection]
// rejects by panic through [election.ValidateElectionConfig], so a caller
// cannot silently start an election on a failed recovery.
func (r *Recovery) ElectionConfig() election.ElectionConfig {
	if r == nil {
		return election.ElectionConfig{}
	}
	return r.electionConfig
}

// missingServiceSnapshot returns the lowest service id, from 0 through
// serviceCount-1, that does not have a row after the module snapshot.
// The module row is plan.Snapshots[0]. The planner keeps the later rows
// unique, in range and in service id order.
//
// Returns 0, false when every id has a row.
func missingServiceSnapshot(plan recordinglog.RecoveryPlan, serviceCount int) (int, bool) {
	services := plan.Snapshots[1:]
	for id, row := range services {
		if int(row.ServiceID) != id {
			return id, true
		}
	}
	if len(services) < serviceCount {
		return len(services), true
	}
	return 0, false
}

// RecordingLog returns the open recording log.
//
// Nil after Close and on a nil receiver. Pass it to both
// [election.ElectionDeps.RecordingLog] and
// [election.ElectionDeps.RecordingLogLookup]; [*recordinglog.Log] satisfies both
// interfaces as-built.
func (r *Recovery) RecordingLog() *recordinglog.Log {
	if r == nil {
		return nil
	}
	return r.recordingLog
}

// NodeState returns the open node state file.
//
// Nil after Close and on a nil receiver. Pass it to
// [election.ElectionDeps.NodeState]; [*election.NodeStateFile] satisfies
// [election.CandidateTermStore] as-built.
func (r *Recovery) NodeState() *election.NodeStateFile {
	if r == nil {
		return nil
	}
	return r.nodeState
}

// IsColdStart reports whether the plan carries no log term and no snapshot,
// which is the state of a member with no usable cluster history.
//
// True does not imply recording.log was absent. A file whose entries are all
// invalid, or whose only term entries carry a null recording id, also plans
// empty.
//
// False on a nil receiver, which is the one case where this is NOT
// Plan().IsEmpty(): the zero RecoveryPlan is empty by that method's own
// definition, but a failed open established nothing and must not be reported as
// a cold start. Implemented as r != nil && r.plan.IsEmpty(), never by calling
// Plan.
func (r *Recovery) IsColdStart() bool {
	return r != nil && r.plan.IsEmpty()
}

// Close closes the node state file, then the recording log, then releases the
// directory lock through [releaseClusterLock].
//
// Idempotent. Safe on a nil receiver. Each handle is nil-checked before Close.
// Library Close methods panic on a nil receiver. Each field is set to nil after
// the first close so accessors return nil. A second Close is a no-op. The second
// call does not re-enter library Close.
//
// After Close the value is inert. RecordingLog and NodeState return nil. Plan
// and ElectionConfig still report what recovery computed. Those fields are
// values. A caller may read them while tearing down.
//
// Unlock-before-close is [releaseClusterLock]. [DirectoryLockWait] bounds holds
// this Close cannot free.
//
// Returns the joined close errors from the node state file, recording log, and
// lock file Close, or nil. Unlock errors are discarded.
func (r *Recovery) Close() error {
	if r == nil {
		return nil
	}

	var err error
	if r.nodeState != nil {
		err = errors.Join(err, r.nodeState.Close())
		r.nodeState = nil
	}
	if r.recordingLog != nil {
		err = errors.Join(err, r.recordingLog.Close())
		r.recordingLog = nil
	}
	if r.lockFile != nil {
		err = errors.Join(err, releaseClusterLock(r.lockFile))
		r.lockFile = nil
	}
	return err
}
