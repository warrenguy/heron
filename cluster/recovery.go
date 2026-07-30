package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"

	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

const clusterLockFileName = "cluster.lock"

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
//  6. plan recovery
//  7. refuse a plan that carries a snapshot
//  8. seed the election configuration
//  9. open the node state file
//
// Directory mode is 0700 when created. An existing directory keeps its mode.
// OpenRecovery does not chmod a directory it did not create. The lock file and
// both data files use mode 0600.
//
// The lock is an advisory flock on cluster.lock. OpenRecovery takes the lock
// before the recording log opens. Close releases the lock when Close closes the
// descriptor. The kernel also releases the lock on process exit. A second
// OpenRecovery on the same directory fails with wrapped EWOULDBLOCK in this
// process or another.
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
//   - planning fails
//   - plan carries a snapshot
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

	// Exclusive advisory lock for the life of Recovery. Kernel releases on
	// descriptor close, including process death; no stale-lock cleanup. O_CLOEXEC
	// so an exec'd child cannot keep the lock after the parent exits.
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

	// On success Recovery owns the handles. Until then error paths unwind
	// opened handles in reverse open order.
	succeeded := false
	defer func() {
		if !succeeded {
			_ = lockFile.Close()
		}
	}()

	// EWOULDBLOCK here means another descriptor holds the directory, in this
	// process or another one.
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
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

	plan, err := recordingLog.PlanRecovery(
		deps.ArchiveLister,
		recordinglog.NewRecoveryPlanConfig(deps.Config.ServiceCount),
	)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan recovery from %s: %w", dir, err)
	}

	if len(plan.Snapshots) > 0 {
		return nil, fmt.Errorf(
			"cluster: recovery plan holds %d snapshots and snapshot load is not implemented",
			len(plan.Snapshots),
		)
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
// RecoveryPlan.Snapshots is always empty on a plan OpenRecovery returned:
// a plan carrying a snapshot is refused, because nothing in this tree loads one.
// RecoveryPlan.Log is a pointer into Recovery state, so treat it as read-only;
// nothing in Recovery mutates the plan after OpenRecovery returns.
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
// directory lock.
//
// Idempotent, and safe on a nil receiver. Each handle is nil-checked before
// Close, because the library Close methods panic on a nil receiver, and each
// field is set to nil after the first close so accessors return nil and a
// second Close is a no-op without re-entering library Close.
//
// After Close the value is inert: RecordingLog and NodeState return nil and the
// cluster directory can be opened by another member. Plan and ElectionConfig
// keep reporting what recovery computed, because they are values and a caller
// may still want them while tearing down.
//
// Returns the joined close errors, or nil.
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
		err = errors.Join(err, r.lockFile.Close())
		r.lockFile = nil
	}
	return err
}
