package election

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/election/nodewire"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/mmap"
)

var (
	ErrCorruptNodeState             = errors.New("election: node-state.dat corrupt")
	ErrIncompatibleNodeStateVersion = errors.New("election: node-state.dat version incompatible")
	ErrNodeStateClosed              = errors.New("election: node state file closed")
)

// CandidateTermStore persists durable candidate-term state.
type CandidateTermStore interface {
	CandidateTerm() (CandidateTerm, error)
	UpdateCandidateTermID(
		candidateTermID, logPosition, timestampMs int64,
	) error
	ProposeMaxCandidateTermID(
		candidateTermID, logPosition, timestampMs int64,
	) (int64, error)
	Close() error
}

var _ CandidateTermStore = (*NodeStateFile)(nil)

// NodeStateFile is a mmap-backed handle over node-state.dat.
//
// Single-writer per process: one handle owns the mapping and there is no
// internal locking. Concurrent use is a caller bug. MAP_SHARED durability
// assumes no second process maps the same path; files are created at mode 0600.
type NodeStateFile struct {
	mapping              *mmap.Mapping
	syncLevel            recordinglog.FileSyncLevel
	closed               bool
	candidateTermOffset  int
	candidateTermBodyOff int
	buf                  atomicbuffer.AtomicBuffer
}

// CandidateTerm is a flyweight over durable candidate-term fields.
//
// A view copied before the parent NodeStateFile is closed may still hold a stale
// buf slice header; reading it after Close is a caller bug under the single-writer
// contract. CandidateTerm() on a closed NodeStateFile returns ErrNodeStateClosed.
type CandidateTerm struct {
	buf                 atomicbuffer.AtomicBuffer
	candidateTermOffset int
	bodyOff             int
}

// OpenNodeStateFile loads or creates node-state.dat under dir.
// When createNew is true but the file already exists, the existing file is loaded
// instead of being recreated.
func OpenNodeStateFile(
	dir string,
	createNew bool,
	syncLevel recordinglog.FileSyncLevel,
) (*NodeStateFile, error) {
	path := filepath.Join(dir, NodeStateFileName)

	if createNew {
		file, err := openNodeStateFileCreateExclusive(path)
		if err == nil {
			return openNewNodeStateFile(dir, path, file, syncLevel)
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		return openExistingNodeStateFile(path, syncLevel)
	}

	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf(
				"node state file does not exist and createNew=false: %w",
				os.ErrNotExist,
			)
		}
		return nil, err
	}
	return openExistingNodeStateFile(path, syncLevel)
}

func openNewNodeStateFile(
	dir, path string,
	file *os.File,
	syncLevel recordinglog.FileSyncLevel,
) (*NodeStateFile, error) {
	if err := truncateNodeStateFile(file, int64(MinimumNodeStateFileLength)); err != nil {
		abortCreateNodeStateFile(path, file)
		return nil, err
	}

	mapping, err := mapNodeStateFile(file, MinimumNodeStateFileLength)
	if err != nil {
		abortCreateNodeStateFile(path, file)
		return nil, err
	}

	candidateTermOffset, candidateTermBodyOff, err := callInitialiseOnCreate(
		mapping.Data,
		NodeStateSemanticVersion,
	)
	if err != nil {
		_ = mapping.Close()
		removePartialNodeStateFile(path)
		return nil, err
	}

	recordinglog.SyncDirectory(dir)

	f := &NodeStateFile{
		mapping:              mapping,
		syncLevel:            syncLevel,
		candidateTermOffset:  candidateTermOffset,
		candidateTermBodyOff: candidateTermBodyOff,
		buf:                  atomicbuffer.Wrap(mapping.Data),
	}
	if err := f.force(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func openExistingNodeStateFile(
	path string,
	syncLevel recordinglog.FileSyncLevel,
) (*NodeStateFile, error) {
	file, info, err := openNodeStateFileExisting(path)
	if err != nil {
		return nil, err
	}

	if err := validateNodeStateFileSize(info.Size()); err != nil {
		_ = file.Close()
		return nil, err
	}

	if err := tightenNodeStateFileMode(file, info); err != nil {
		_ = file.Close()
		return nil, err
	}

	mapping, err := mmap.MapOpenedFile(file, MinimumNodeStateFileLength)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	f := &NodeStateFile{
		mapping:   mapping,
		syncLevel: syncLevel,
		buf:       atomicbuffer.Wrap(mapping.Data),
	}
	if err := f.loadInitialState(); err != nil {
		_ = mapping.Close()
		return nil, err
	}
	// Java NodeStateFile forces the mapped buffer on open when fileSyncLevel > 0
	// even though load performs no writes.
	if err := f.force(); err != nil {
		_ = mapping.Close()
		return nil, err
	}
	return f, nil
}

func (f *NodeStateFile) loadInitialState() error {
	buf := f.buf.Bytes()
	if len(buf) < int(nodewire.NodeStateHeaderSbeBlockLength) {
		return ErrCorruptNodeState
	}

	var nsh nodewire.NodeStateHeader
	nsh.WrapForDecode(
		buf,
		0,
		uint64(nodewire.NodeStateHeaderSbeBlockLength),
		uint64(nodewire.NodeStateHeaderSbeSchemaVersion),
		uint64(len(buf)),
	)
	version := nsh.Version()
	if cnc.MajorVersion(version) != cnc.MajorVersion(NodeStateSemanticVersion) {
		return ErrIncompatibleNodeStateVersion
	}

	headerPos, ok, err := scanForTemplateOffset(
		buf,
		int(nodewire.NodeStateHeaderSbeBlockLength),
		nodewire.CandidateTermSbeTemplateID,
	)
	if err != nil {
		return err
	}
	if !ok {
		return ErrCorruptNodeState
	}

	if err := validateLoadedLayout(buf, headerPos); err != nil {
		return err
	}

	bodyOff, termOffset, err := candidateTermOffsets(headerPos)
	if err != nil {
		return err
	}
	f.candidateTermBodyOff = bodyOff
	f.candidateTermOffset = termOffset
	return nil
}

// Close is idempotent: a second call returns nil without touching the mapping.
func (f *NodeStateFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	f.buf = atomicbuffer.AtomicBuffer{}
	return f.mapping.Close()
}

func (f *NodeStateFile) CandidateTerm() (CandidateTerm, error) {
	if f.closed {
		return CandidateTerm{}, ErrNodeStateClosed
	}
	return CandidateTerm{
		buf:                 f.buf,
		candidateTermOffset: f.candidateTermOffset,
		bodyOff:             f.candidateTermBodyOff,
	}, nil
}

func (ct CandidateTerm) CandidateTermID() int64 {
	return ct.buf.GetInt64Volatile(ct.candidateTermOffset)
}

func (ct CandidateTerm) TimestampMs() int64 {
	var candidate nodewire.CandidateTerm
	return ct.buf.GetInt64(ct.bodyOff + int(candidate.TimestampEncodingOffset()))
}

func (ct CandidateTerm) LogPosition() int64 {
	var candidate nodewire.CandidateTerm
	return ct.buf.GetInt64(ct.bodyOff + int(candidate.LogPositionEncodingOffset()))
}

// UpdateCandidateTermID persists the candidate term unconditionally and syncs when
// syncLevel requires it.
//
// Callers needing monotonic guard should use ProposeMaxCandidateTermID instead.
func (f *NodeStateFile) UpdateCandidateTermID(
	candidateTermID, logPosition, timestampMs int64,
) error {
	if f.closed {
		return ErrNodeStateClosed
	}

	bodyOff := f.candidateTermBodyOff
	var ct nodewire.CandidateTerm
	f.buf.PutInt64(bodyOff+int(ct.LogPositionEncodingOffset()), logPosition)
	f.buf.PutInt64(bodyOff+int(ct.TimestampEncodingOffset()), timestampMs)
	f.buf.PutInt64Release(f.candidateTermOffset, candidateTermID)
	return f.force()
}

// ProposeMaxCandidateTermID updates only when candidateTermID exceeds the stored value.
func (f *NodeStateFile) ProposeMaxCandidateTermID(
	candidateTermID, logPosition, timestampMs int64,
) (int64, error) {
	if f.closed {
		return 0, ErrNodeStateClosed
	}

	existing := f.buf.GetInt64Volatile(f.candidateTermOffset)
	if candidateTermID <= existing {
		return existing, nil
	}

	if err := f.UpdateCandidateTermID(candidateTermID, logPosition, timestampMs); err != nil {
		return 0, err
	}
	return candidateTermID, nil
}

func (f *NodeStateFile) force() error {
	if f.syncLevel > recordinglog.FileSyncNone {
		return f.mapping.Sync()
	}
	return nil
}
