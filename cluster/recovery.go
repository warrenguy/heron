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
	// Config is the normalized member config. [OpenRecovery] calls Validate and
	// returns its error unchanged, then reads ClusterDir,
	// NodeStateFileSyncLevel, ServiceCount, ArchiveClientEnabled, and Election.
	// Validate still enforces the full config contract.
	//
	// Five fields of Config.Election are recovery output, not caller input, and
	// [OpenRecovery] overwrites whatever is in them: IsNodeStartup, which is
	// always set, and LeadershipTermID, TermBaseLogPosition, LogPosition, and
	// AppendPosition, which come from the recovery plan. Setting any of the five
	// has no effect and produces no error. Every other Election field is carried
	// through unchanged, including the five timeouts, MemberID,
	// AppointedLeaderID, GracefulClosedLeaderID, and AppVersion.
	//
	// [OpenRecovery] does not retain Config.
	Config Config

	// ArchiveLister resolves recording extents for a recording log that carries
	// term entries. Production passes archive.NewLister over a connected control
	// client; unit tests pass a map-backed fake.
	//
	// Nil is legal only when Config.ArchiveClientEnabled is false and the
	// recording log is empty: no lookup is needed. ArchiveClientEnabled true
	// always requires a non-nil lister, including cold-start empty dirs.
	// Nil with a non-empty recording log is refused, because
	// recordinglog.PlanRecovery has no nil check and panics if a lookup is
	// required.
	//
	// Non-nil with Config.ArchiveClientEnabled false is also legal: the switch
	// governs whether the assembly connects an archive client, and this field is
	// what recovery actually consumes.
	ArchiveLister recordinglog.ArchiveLister
}

// Recovery is the member's start-of-life durable state: the recording log, the
// node state file, the recovery plan computed from the recording log, and the
// election configuration seeded from that plan.
//
// Open with [OpenRecovery]. Recovery owns the directory lock and both file
// handles and closes all three; the election holds the files behind interfaces
// and closes neither.
//
// Concurrency: invoker-only. [*recordinglog.Log] is single-writer with no
// internal locking and [*election.NodeStateFile] is single-writer per process,
// so every method must run on the same goroutine as [election.Election.DoWork]
// and consensus.Agent.DoWork, matching [ConsensusPublications]. Across processes
// and across members in one process, that single-writer contract is enforced by
// an exclusive advisory lock [OpenRecovery] holds on cluster.lock in the cluster
// directory for the lifetime of this value.
//
// Lifetime: outlives the election. The election writes candidate terms through
// NodeState and term coherency through RecordingLog for the whole life of the
// process, so Close belongs in the module teardown, after the election is done
// and after the transport handles are closed.
//
// Do not copy. All three handles are pointers, so a value copy is a silent
// alias: two values that each look like an owner and each look responsible for
// Close. Every accessor takes a pointer receiver. Pass the [*Recovery].
type Recovery struct {
	lockFile       *os.File
	recordingLog   *recordinglog.Log
	nodeState      *election.NodeStateFile
	plan           recordinglog.RecoveryPlan
	electionConfig election.ElectionConfig
}

// OpenRecovery creates the cluster directory, takes an exclusive lock on it,
// opens the recording log and the node state file, plans recovery, and seeds the
// election configuration.
//
// The directory is created at mode 0700 when absent. An existing directory keeps
// its mode; OpenRecovery does not chmod one it did not create. The lock file and
// both data files are created at mode 0600.
//
// The lock is an advisory flock on cluster.lock, taken before either file is
// opened and released when Close closes the descriptor, which the kernel also
// does on process exit. A second OpenRecovery against the same directory fails
// with a wrapped EWOULDBLOCK, whether it is in another process or in this one.
//
// The node state file sync level comes from deps.Config and is
// recordinglog.FileSyncData or recordinglog.FileSyncFull, which [Config.Validate]
// guarantees. The recovery plan uses
// recordinglog.NewRecoveryPlanConfig(deps.Config.ServiceCount), so the
// replicated recording id is always null.
//
// The seeded election configuration is deps.Config.Election with IsNodeStartup
// set and the four recovery fields written from the plan. Every other field is
// carried through unchanged, including the five timeouts and the three member
// id fields Config.Normalize materialized.
//
// Errors when deps.Config fails Validate, when deps.Config.ArchiveClientEnabled
// is set without a lister, when the directory cannot be created, when the lock
// cannot be taken, when either file cannot be opened, when the recording log
// holds entries and no lister was supplied, when planning fails, and when the
// plan carries a snapshot. On error nothing stays open, the lock is released,
// and the returned value is nil.
func OpenRecovery(deps RecoveryDeps) (*Recovery, error) {
	if err := deps.Config.Validate(); err != nil {
		return nil, err
	}

	if deps.Config.ArchiveClientEnabled && deps.ArchiveLister == nil {
		return nil, errors.New("cluster: ArchiveLister is required when ArchiveClientEnabled")
	}

	dir := deps.Config.ClusterDir
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("cluster: create cluster dir %s: %w", dir, err)
	}

	// Hold an exclusive advisory lock for the whole life of the Recovery. The
	// kernel releases it when the descriptor closes, including on process death,
	// so there is no stale-lock state to clean up. O_CLOEXEC closes the
	// descriptor on exec so a child that execs cannot keep the lock after the
	// parent exits.
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

	// Ownership moves to the returned Recovery on success; until then every
	// error return unwinds the handles opened so far, in reverse open order.
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
