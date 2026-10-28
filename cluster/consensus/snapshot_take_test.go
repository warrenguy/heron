//go:build testhooks

package consensus

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

const (
	// fakeSnapshotPreProofID is the recording id a fake start reports before
	// START.
	fakeSnapshotPreProofID int64 = 4242

	// fakeSnapshotFirstRecordingID is the START id of the first fake start.
	fakeSnapshotFirstRecordingID int64 = 31

	// fakeOfferAccept scripts an accepted row offer.
	fakeOfferAccept int64 = 1

	testSnapshotChannel        = "aeron:ipc?alias=snapshot"
	testSnapshotStreamID int32 = 107
)

// fakeSnapshotStart is one scripted start recording request.
type fakeSnapshotStart struct {
	streamID int32
	channel  string

	// offerRetries counts TryOffer calls that return false, nil before the
	// accept. offerNever keeps every call false, nil.
	offerRetries int
	offerNever   bool
	offerErr     error

	// signalDelay counts PollSignal calls that return false, nil before
	// START. signalNever keeps every call false, nil.
	signalDelay int
	signalNever bool
	signalErr   error

	recordingID int64
	proven      bool
	polls       int
	closed      int
}

func (s *fakeSnapshotStart) TryOffer() (bool, error) {
	if s.offerErr != nil {
		return false, s.offerErr
	}
	if s.offerNever {
		return false, nil
	}
	if s.offerRetries > 0 {
		s.offerRetries--
		return false, nil
	}
	return true, nil
}

func (s *fakeSnapshotStart) PollSignal() (bool, error) {
	if s.signalErr != nil {
		return true, s.signalErr
	}
	s.polls++
	if s.signalNever || s.polls <= s.signalDelay {
		return false, nil
	}
	s.proven = true
	return true, nil
}

func (s *fakeSnapshotStart) RecordingID() int64 {
	if !s.proven {
		return fakeSnapshotPreProofID
	}
	return s.recordingID
}

func (s *fakeSnapshotStart) Close() { s.closed++ }

// fakeSnapshotPublication copies each accepted row and returns a rising
// position.
type fakeSnapshotPublication struct {
	sessionID int32
	position  int64

	// offerCodes scripts one result per Offer call. fakeOfferAccept accepts
	// the row. Every other value returns as the offer code. An empty script
	// accepts.
	offerCodes []int64
	closeErr   error

	rows          [][]byte
	closed        int
	closePosition int64
}

func (p *fakeSnapshotPublication) Offer(buffer []byte) int64 {
	if len(p.offerCodes) > 0 {
		code := p.offerCodes[0]
		p.offerCodes = p.offerCodes[1:]
		if code != fakeOfferAccept {
			return code
		}
	}
	p.rows = append(p.rows, bytes.Clone(buffer))
	p.position += int64(len(buffer))
	return p.position
}

func (p *fakeSnapshotPublication) Position() int64 { return p.position }

func (p *fakeSnapshotPublication) SessionID() int32 { return p.sessionID }

func (p *fakeSnapshotPublication) Close() error {
	if p.closed == 0 {
		p.closePosition = p.position
	}
	p.closed++
	return p.closeErr
}

// fakeSnapshotArchive is the snapshot recording control and the snapshot
// publication adder over scripted outcomes.
type fakeSnapshotArchive struct {
	offerRetryTimeoutNs int64
	responseTimeoutNs   int64

	// startScript adjusts each new start before the take sees it.
	startScript func(start *fakeSnapshotStart)
	starts      []*fakeSnapshotStart

	watching    bool
	watchedID   int64
	stopPolls   int
	earlyStop   bool
	stopNever   bool
	stopDelay   int
	stopShortBy int64

	// reportedStop is the last STOP position StopPosition reported after a
	// close.
	reportedStop    int64
	reportedStopped bool

	// pubScript adjusts each new publication before the take sees it.
	pubScript   func(publication *fakeSnapshotPublication)
	pubs        []*fakeSnapshotPublication
	offerAddErr error

	// addPendingPolls counts not-done polls before each add completes.
	addPendingPolls int
	pollAddErr      error
	pollAddErrDone  bool
	nilPublication  bool

	addOffers []snapshotAddOffer
	pending   map[int64]int
	nextCorr  int64
	cancels   []int64
}

type snapshotAddOffer struct {
	streamID int32
	channel  string
}

var (
	_ SnapshotRecordingControl = (*fakeSnapshotArchive)(nil)
	_ SnapshotPublicationAdder = (*fakeSnapshotArchive)(nil)
	_ SnapshotStartRequest     = (*fakeSnapshotStart)(nil)
	_ SnapshotPublication      = (*fakeSnapshotPublication)(nil)
)

func newFakeSnapshotArchive() *fakeSnapshotArchive {
	return &fakeSnapshotArchive{
		offerRetryTimeoutNs: 1000,
		responseTimeoutNs:   500,
		stopDelay:           1,
		pending:             make(map[int64]int),
	}
}

func (f *fakeSnapshotArchive) NewStartRecording(
	streamID int32,
	channel string,
) SnapshotStartRequest {
	start := &fakeSnapshotStart{
		streamID:    streamID,
		channel:     channel,
		signalDelay: 1,
		recordingID: fakeSnapshotFirstRecordingID + int64(len(f.starts)),
	}
	if f.startScript != nil {
		f.startScript(start)
	}
	f.starts = append(f.starts, start)
	return start
}

func (f *fakeSnapshotArchive) ControlTimeoutsNs() (offerRetryTimeoutNs, responseTimeoutNs int64) {
	return f.offerRetryTimeoutNs, f.responseTimeoutNs
}

func (f *fakeSnapshotArchive) WatchStop(recordingID int64) {
	f.watching = true
	f.watchedID = recordingID
	f.stopPolls = 0
}

// StopPosition reports STOP stopDelay calls after the latest publication
// closes. earlyStop reports STOP at once.
func (f *fakeSnapshotArchive) StopPosition() (stopPosition int64, stopped bool) {
	if !f.watching || len(f.pubs) == 0 {
		return 0, false
	}
	pub := f.pubs[len(f.pubs)-1]
	if f.earlyStop {
		return pub.position, true
	}
	if pub.closed == 0 || f.stopNever {
		return 0, false
	}
	f.stopPolls++
	if f.stopPolls <= f.stopDelay {
		return 0, false
	}
	f.reportedStop = pub.closePosition - f.stopShortBy
	f.reportedStopped = true
	return f.reportedStop, true
}

func (f *fakeSnapshotArchive) ClearStop() {
	f.watching = false
}

func (f *fakeSnapshotArchive) OfferAddExclusivePublication(
	streamID int32,
	channel string,
) (int64, error) {
	f.addOffers = append(f.addOffers, snapshotAddOffer{streamID: streamID, channel: channel})
	if f.offerAddErr != nil {
		return 0, f.offerAddErr
	}
	f.nextCorr++
	f.pending[f.nextCorr] = f.addPendingPolls
	return f.nextCorr, nil
}

func (f *fakeSnapshotArchive) PollAddExclusivePublication(
	correlationID int64,
) (SnapshotPublication, bool, error) {
	polls, ok := f.pending[correlationID]
	if !ok {
		return nil, false, aeron.ErrAwaitInterrupted
	}
	if f.pollAddErr != nil {
		if f.pollAddErrDone {
			delete(f.pending, correlationID)
		}
		return nil, f.pollAddErrDone, f.pollAddErr
	}
	if polls > 0 {
		f.pending[correlationID] = polls - 1
		return nil, false, nil
	}
	delete(f.pending, correlationID)
	if f.nilPublication {
		return nil, true, nil
	}
	pub := &fakeSnapshotPublication{sessionID: int32(-100 - len(f.pubs))}
	if f.pubScript != nil {
		f.pubScript(pub)
	}
	f.pubs = append(f.pubs, pub)
	return pub, true, nil
}

func (f *fakeSnapshotArchive) CancelPending(correlationID int64) {
	f.cancels = append(f.cancels, correlationID)
	delete(f.pending, correlationID)
}

// requireNoPreProofID asserts that no stored result names the fake's
// pre-proof id.
func requireNoPreProofID(t testing.TB, a *Agent) {
	t.Helper()
	result, ok := a.LastSnapshotTake()
	if ok {
		require.NotEqual(t, fakeSnapshotPreProofID, result.RecordingID)
	}
}

// decodeSnapshotRows decodes each row by its template id.
func decodeSnapshotRows(t fragmentRequireT, rows [][]byte) []any {
	t.Helper()
	decoded := make([]any, 0, len(rows))
	for i, row := range rows {
		_, templateID, _, _, ok := codecs.DecodeMessageHeader(row, 0)
		require.True(t, ok, "row %d header", i)
		var fields any
		switch templateID {
		case clusterwire.SnapshotMarkerSbeTemplateID:
			fields, ok = codecs.TryDecodeSnapshotMarkerAt(row, 0)
		case clusterwire.ConsensusModuleSbeTemplateID:
			fields, ok = codecs.TryDecodeConsensusModuleAt(row, 0)
		case clusterwire.ClusterSessionSbeTemplateID:
			fields, ok = codecs.TryDecodeClusterSessionAt(row, 0)
		case clusterwire.TimerSbeTemplateID:
			fields, ok = codecs.TryDecodeTimerAt(row, 0)
		case clusterwire.PendingMessageTrackerSbeTemplateID:
			fields, ok = codecs.TryDecodePendingMessageTrackerAt(row, 0)
		default:
			require.Failf(t, "unexpected template id", "row %d: template id %d", i, templateID)
		}
		require.True(t, ok, "row %d decode", i)
		decoded = append(decoded, fields)
	}
	return decoded
}

// snapshotMarkerRow returns the module marker row at logPosition.
func snapshotMarkerRow(
	logPosition, leadershipTermID int64,
	appVersion int32,
	mark clusterwire.SnapshotMark,
) codecs.SnapshotMarkerFields {
	return codecs.SnapshotMarkerFields{
		TypeID:           codecs.ConsensusModuleSnapshotTypeID,
		LogPosition:      logPosition,
		LeadershipTermID: leadershipTermID,
		Index:            0,
		Mark:             mark,
		TimeUnit:         clusterwire.ClusterTimeUnit_NANOS,
		AppVersion:       appVersion,
	}
}

func TestSnapshotTake_recordsModuleSnapshot(t *testing.T) {
	t.Parallel()

	const (
		termID     int64 = 7
		appVersion int32 = 3
		nowNs      int64 = 1000
		openedAt   int64 = 64
		channel          = "aeron:udp?endpoint=localhost:20121"
	)
	f := newTakeFixture(t, AgentConfig{
		Role:             RoleLeader,
		ServiceCount:     1,
		LeadershipTermID: termID,
		AppVersion:       appVersion,
	})
	a, archive := f.agent, f.archive

	// Session 4 learns first, so the registry walk order differs from id
	// order.
	for _, id := range []int64{4, 2} {
		a.sessions.learnSessionOpen(codecs.SessionOpenEventFields{
			ClusterSessionID: id,
			CorrelationID:    id * 10,
			ResponseStreamID: 102,
			ResponseChannel:  []byte(channel),
		}, openedAt*id, nowNs)
	}
	require.True(t, a.sessions.markClosing(4, clusterwire.CloseReason_TIMEOUT))
	scheduleTimer(a, 9, 5_000_000_000)
	scheduleTimer(a, 3, 6_000_000_000)

	actionPos := f.request(t, nowNs)
	require.True(t, a.HasPendingSnapshotAction())
	f.ack(t, actionPos, 0, 77, 0)
	result, _ := f.tickUntilResolved(t, nowNs, 1)
	require.False(t, a.HasPendingSnapshotAction())

	require.Len(t, archive.pubs, 1)
	pub := archive.pubs[0]
	require.Equal(t, []snapshotAddOffer{{
		streamID: testSnapshotStreamID,
		channel:  testSnapshotChannel,
	}}, archive.addOffers)
	require.Len(t, archive.starts, 1)
	require.Equal(t, testSnapshotStreamID, archive.starts[0].streamID)
	require.Equal(t, testSnapshotChannel+"|session-id=-100", archive.starts[0].channel)

	want := []any{
		snapshotMarkerRow(actionPos, termID, appVersion, clusterwire.SnapshotMark_BEGIN),
		codecs.ConsensusModuleFields{
			NextSessionID:          5,
			NextServiceSessionID:   math.MinInt64 + 1,
			LogServiceSessionID:    math.MinInt64,
			PendingMessageCapacity: 0,
		},
		codecs.ClusterSessionFields{
			ClusterSessionID:   2,
			CorrelationID:      20,
			OpenedLogPosition:  2 * openedAt,
			TimeOfLastActivity: -1,
			CloseReason:        clusterwire.CloseReason_NULL_VALUE,
			ResponseStreamID:   102,
			ResponseChannel:    []byte(channel),
		},
		codecs.ClusterSessionFields{
			ClusterSessionID:   4,
			CorrelationID:      40,
			OpenedLogPosition:  4 * openedAt,
			TimeOfLastActivity: -1,
			CloseReason:        clusterwire.CloseReason_TIMEOUT,
			ResponseStreamID:   102,
			ResponseChannel:    []byte(channel),
		},
		codecs.TimerFields{CorrelationID: 3, Deadline: 6_000_000_000},
		codecs.TimerFields{CorrelationID: 9, Deadline: 5_000_000_000},
		codecs.PendingMessageTrackerFields{
			NextServiceSessionID:   math.MinInt64 + 1,
			LogServiceSessionID:    math.MinInt64,
			PendingMessageCapacity: 0,
			ServiceID:              0,
		},
		snapshotMarkerRow(actionPos, termID, appVersion, clusterwire.SnapshotMark_END),
	}
	require.Equal(t, want, decodeSnapshotRows(t, pub.rows))
	require.Equal(t, codecs.PendingMessageTrackerFields{
		NextServiceSessionID:   (math.MinInt64 | 1<<56) + 1,
		LogServiceSessionID:    math.MinInt64 | 1<<56,
		PendingMessageCapacity: 0,
		ServiceID:              1,
	}, emptyPendingMessageTracker(1))

	require.Equal(t, SnapshotTakeResult{
		LogPosition:         actionPos,
		LeadershipTermID:    termID,
		Timestamp:           nowNs,
		RecordingID:         fakeSnapshotFirstRecordingID,
		ServiceRecordingIDs: []int64{77},
	}, result)
	require.Equal(t, 1, pub.closed)
	require.False(t, archive.watching)
}

// fakeTermBaseLogPosition is the TERM row base the fake recording log
// reports.
const fakeTermBaseLogPosition int64 = 256

// fakeSnapshotRecordingLog records each snapshot row append and each Force
// level. FindTermEntry returns a TERM row at fakeTermBaseLogPosition for
// every term unless missingTerm is set.
type fakeSnapshotRecordingLog struct {
	missingTerm bool

	// appendErr fails the append call at index appendErrAt, counting from 0.
	// The failed call does not record a row.
	appendErr   error
	appendErrAt int
	forceErr    error

	rows   []recordinglog.Entry
	forces []fakeSnapshotForce
}

// fakeSnapshotForce is one Force call. rows counts the rows appended before
// the call.
type fakeSnapshotForce struct {
	level recordinglog.FileSyncLevel
	rows  int
}

var _ SnapshotRecordingLog = (*fakeSnapshotRecordingLog)(nil)

func (l *fakeSnapshotRecordingLog) FindTermEntry(leadershipTermID int64) *recordinglog.Entry {
	if l.missingTerm {
		return nil
	}
	return &recordinglog.Entry{
		LeadershipTermID:    leadershipTermID,
		TermBaseLogPosition: fakeTermBaseLogPosition,
		Type:                recordinglog.EntryTypeTerm,
		Valid:               true,
	}
}

func (l *fakeSnapshotRecordingLog) AppendSnapshot(
	recordingID int64,
	leadershipTermID int64,
	termBaseLogPosition int64,
	logPosition int64,
	timestamp int64,
	serviceID int32,
) error {
	if l.appendErr != nil && len(l.rows) == l.appendErrAt {
		return l.appendErr
	}
	l.rows = append(l.rows, recordinglog.Entry{
		RecordingID:         recordingID,
		LeadershipTermID:    leadershipTermID,
		TermBaseLogPosition: termBaseLogPosition,
		LogPosition:         logPosition,
		Timestamp:           timestamp,
		ServiceID:           serviceID,
		Type:                recordinglog.EntryTypeSnapshot,
		Valid:               true,
	})
	return nil
}

func (l *fakeSnapshotRecordingLog) Force(level recordinglog.FileSyncLevel) error {
	l.forces = append(l.forces, fakeSnapshotForce{level: level, rows: len(l.rows)})
	return l.forceErr
}

// fakeSnapshotGroup returns the rows of one snapshot group in write order:
// the services from the highest id down to 0, then the module row.
func fakeSnapshotGroup(
	result SnapshotTakeResult,
	moduleRecordingID int64,
) []recordinglog.Entry {
	row := func(recordingID int64, serviceID int32) recordinglog.Entry {
		return recordinglog.Entry{
			RecordingID:         recordingID,
			LeadershipTermID:    result.LeadershipTermID,
			TermBaseLogPosition: fakeTermBaseLogPosition,
			LogPosition:         result.LogPosition,
			Timestamp:           result.Timestamp,
			ServiceID:           serviceID,
			Type:                recordinglog.EntryTypeSnapshot,
			Valid:               true,
		}
	}
	rows := make([]recordinglog.Entry, 0, len(result.ServiceRecordingIDs)+1)
	for serviceID, recordingID := range slices.Backward(result.ServiceRecordingIDs) {
		rows = append(rows, row(recordingID, int32(serviceID)))
	}
	return append(rows, row(moduleRecordingID, recordinglog.ConsensusModuleServiceID))
}

// anyRecordingLister lists every recording id with one fixed extent.
type anyRecordingLister struct{}

func (anyRecordingLister) ListRecording(int64) (recordinglog.LogRecordingExtents, error) {
	return recordinglog.LogRecordingExtents{
		StopPosition:     1 << 20,
		TermBufferLength: 64 << 10,
		MtuLength:        1408,
	}, nil
}

func TestSnapshotTake_writesRecordingLogRows(t *testing.T) {
	t.Parallel()

	const (
		termID   int64 = 7
		termBase int64 = 128
		nowNs    int64 = 1000
	)
	f := newTakeFixture(t, AgentConfig{
		Role:             RoleLeader,
		ServiceCount:     2,
		LeadershipTermID: termID,
	})
	dir := t.TempDir()
	log, err := recordinglog.Open(dir, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, log.Close()) })
	require.NoError(t, log.AppendTerm(1, termID, termBase, 10))
	f.agent.SetSnapshotRecordingLog(log, recordinglog.FileSyncData)

	position := f.request(t, nowNs)
	f.ack(t, position, 0, 77, 0)
	f.ack(t, position, 0, 78, 1)
	result, _ := f.tickUntilResolved(t, nowNs, 1)
	require.NoError(t, result.Err)

	snapshotRow := func(recordingID int64, serviceID, index int32) recordinglog.Entry {
		return recordinglog.Entry{
			RecordingID:         recordingID,
			LeadershipTermID:    termID,
			TermBaseLogPosition: termBase,
			LogPosition:         position,
			Timestamp:           nowNs,
			ServiceID:           serviceID,
			Type:                recordinglog.EntryTypeSnapshot,
			Valid:               true,
			Index:               index,
		}
	}
	entries := log.Entries()
	require.Len(t, entries, 4)
	require.Equal(t, recordinglog.EntryTypeTerm, entries[0].Type)
	require.Equal(t, termID, entries[0].LeadershipTermID)

	// The file order equals the sort order: each Index matches its slot.
	got := slices.Clone(entries[1:])
	for i := range got {
		got[i].Position = 0
	}
	require.Equal(t, []recordinglog.Entry{
		snapshotRow(78, 1, 1),
		snapshotRow(77, 0, 2),
		snapshotRow(fakeSnapshotFirstRecordingID, recordinglog.ConsensusModuleServiceID, 3),
	}, got)

	reloaded, err := recordinglog.Reload(filepath.Join(dir, "recording.log"))
	require.NoError(t, err)
	require.Equal(t, entries, reloaded)

	plan, err := log.PlanRecovery(anyRecordingLister{}, recordinglog.NewRecoveryPlanConfig(2))
	require.NoError(t, err)
	snapshot := func(recordingID int64, serviceID int32) recordinglog.Snapshot {
		return recordinglog.Snapshot{
			RecordingID:         recordingID,
			LeadershipTermID:    termID,
			TermBaseLogPosition: termBase,
			LogPosition:         position,
			Timestamp:           nowNs,
			ServiceID:           serviceID,
		}
	}
	require.Equal(t, []recordinglog.Snapshot{
		snapshot(fakeSnapshotFirstRecordingID, recordinglog.ConsensusModuleServiceID),
		snapshot(77, 0),
		snapshot(78, 1),
	}, plan.Snapshots)
}

// takeFixture is a settled leader with a bound log, a service control queue,
// a commit counter, and the fake snapshot archive and recording log wired.
type takeFixture struct {
	agent        *Agent
	commit       *atomic.Int64
	service      *QueueConsensusIngress
	archive      *fakeSnapshotArchive
	recordingLog *fakeSnapshotRecordingLog
}

// newTakeFixture builds the agent from cfg with a fresh commit counter. A
// leader also binds a log publisher.
func newTakeFixture(t testing.TB, cfg AgentConfig) *takeFixture {
	t.Helper()
	commit := new(atomic.Int64)
	cfg.Commit = commit
	a := NewAgent(cfg)
	q := attachQueueServiceIngress(a)
	if cfg.Role == RoleLeader {
		lp, _ := boundLogPublisher(t)
		a.SetLogPublisher(lp)
	}
	archive := newFakeSnapshotArchive()
	a.SetSnapshotRecordingControl(archive)
	a.SetSnapshotPublicationAdder(archive)
	recordingLog := &fakeSnapshotRecordingLog{}
	a.SetSnapshotRecordingLog(recordingLog, recordinglog.FileSyncData)
	a.SetSnapshotStream(testSnapshotChannel, testSnapshotStreamID)
	return &takeFixture{
		agent:        a,
		commit:       commit,
		service:      q,
		archive:      archive,
		recordingLog: recordingLog,
	}
}

func newTakeLeader(t testing.TB, serviceCount int) *takeFixture {
	t.Helper()
	return newTakeFixture(t, AgentConfig{
		Role:             RoleLeader,
		ServiceCount:     serviceCount,
		LeadershipTermID: 5,
	})
}

func newTakeFollower(t testing.TB, serviceCount int) *takeFixture {
	t.Helper()
	return newTakeFixture(t, AgentConfig{Role: RoleFollower, ServiceCount: serviceCount})
}

// request appends one snapshot action, raises commit to it and returns its
// position.
func (f *takeFixture) request(t testing.TB, nowNs int64) int64 {
	t.Helper()
	require.NoError(t, f.agent.RequestSnapshot(nowNs))
	position := f.agent.ExpectedAckPosition()
	require.Positive(t, position)
	f.commit.Store(position)
	return position
}

// ack queues one ServiceAck from serviceID with relevantID.
func (f *takeFixture) ack(t testing.TB, position, ackID, relevantID int64, serviceID int32) {
	t.Helper()
	f.service.Enqueue(encodeServiceAckWire(t, codecs.ServiceAckFields{
		LogPosition: position,
		AckID:       ackID,
		RelevantID:  relevantID,
		ServiceID:   serviceID,
	}))
}

// state returns the take state. The take must exist.
func (f *takeFixture) state(t testing.TB) snapshotTakeState {
	t.Helper()
	require.NotNil(t, f.agent.snapshotTake)
	return f.agent.snapshotTake.state
}

// tickUntilResolved runs DoWork from startNs in steps of stepNs until a take
// resolves and no snapshot action pends. Returns the result and the resolving
// tick.
func (f *takeFixture) tickUntilResolved(
	t testing.TB,
	startNs, stepNs int64,
) (SnapshotTakeResult, int64) {
	t.Helper()
	nowNs := startNs
	for range 64 {
		f.agent.DoWork(nowNs)
		requireNoPreProofID(t, f.agent)
		result, ok := f.agent.LastSnapshotTake()
		if ok && !f.agent.HasPendingSnapshotAction() {
			return result, nowNs
		}
		nowNs += stepNs
	}
	t.Fatal("take did not resolve")
	return SnapshotTakeResult{}, 0
}

// requireNoRecordingLogWrite asserts that the take did not call AppendSnapshot
// or Force.
func (f *takeFixture) requireNoRecordingLogWrite(t testing.TB) {
	t.Helper()
	require.Empty(t, f.recordingLog.rows)
	require.Empty(t, f.recordingLog.forces)
}

// requireCleanedUp asserts that the take cancelled its pending add, closed
// each start request and publication, and cleared the STOP watch.
func (f *takeFixture) requireCleanedUp(t testing.TB) {
	t.Helper()
	require.Nil(t, f.agent.snapshotTake)
	require.Empty(t, f.archive.pending)
	for i, start := range f.archive.starts {
		require.Equal(t, 1, start.closed, "start %d", i)
	}
	require.False(t, f.archive.watching)
	for i, pub := range f.archive.pubs {
		require.Equal(t, 1, pub.closed, "publication %d", i)
	}
}

func TestSnapshotTake_checkingOutcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "service acked minus one",
			run: func(t *testing.T) {
				f := newTakeLeader(t, 2)
				position := f.request(t, 100)
				f.ack(t, position, 0, 77, 0)
				f.ack(t, position, 0, nullValue, 1)
				result, _ := f.tickUntilResolved(t, 100, 1)
				require.ErrorIs(t, result.Err, errSnapshotServiceFailed)
				require.Equal(t, int64(nullValue), result.RecordingID)
				require.Equal(t, []int64{77, nullValue}, result.ServiceRecordingIDs)
				require.Empty(t, f.archive.addOffers)
				require.Empty(t, f.archive.starts)
				require.False(t, f.agent.HasPendingSnapshotAction())
				f.requireNoRecordingLogWrite(t)
			},
		},
		{
			name: "no recording control",
			run: func(t *testing.T) {
				f := newTakeLeader(t, 1)
				f.agent.SetSnapshotRecordingControl(nil)
				position := f.request(t, 100)
				f.ack(t, position, 0, 77, 0)
				result, _ := f.tickUntilResolved(t, 100, 1)
				require.ErrorIs(t, result.Err, errSnapshotNotConfigured)
				require.Equal(t, int64(nullValue), result.RecordingID)
				require.Equal(t, []int64{77}, result.ServiceRecordingIDs)
				require.Empty(t, f.archive.addOffers)
			},
		},
		{
			name: "no recording log",
			run: func(t *testing.T) {
				f := newTakeLeader(t, 1)
				f.agent.SetSnapshotRecordingLog(nil, recordinglog.FileSyncNone)
				position := f.request(t, 100)
				f.ack(t, position, 0, 77, 0)
				result, _ := f.tickUntilResolved(t, 100, 1)
				require.ErrorIs(t, result.Err, errSnapshotNotConfigured)
				require.Equal(t, int64(nullValue), result.RecordingID)
				require.Empty(t, f.archive.addOffers)
				require.Empty(t, f.archive.starts)
				f.requireNoRecordingLogWrite(t)
			},
		},
		{
			name: "no publication adder",
			run: func(t *testing.T) {
				f := newTakeLeader(t, 1)
				f.agent.SetSnapshotPublicationAdder(nil)
				position := f.request(t, 100)
				f.ack(t, position, 0, 77, 0)
				result, _ := f.tickUntilResolved(t, 100, 1)
				require.ErrorIs(t, result.Err, errSnapshotNotConfigured)
				require.Empty(t, f.archive.addOffers)
				require.Empty(t, f.archive.starts)
			},
		},
		{
			name: "zero services leader waits for commit",
			run: func(t *testing.T) {
				f := newTakeLeader(t, 0)
				require.NoError(t, f.agent.RequestSnapshot(100))
				position := f.agent.ExpectedAckPosition()
				f.commit.Store(position - 1)

				// Phase 1: the take waits in Checking below commit.
				for nowNs := range int64(3) {
					f.agent.DoWork(100 + nowNs)
					require.Equal(t, takeChecking, f.state(t))
					require.Empty(t, f.archive.addOffers)
					require.True(t, f.agent.HasPendingSnapshotAction())
				}

				// Phase 2: commit reaches the action. The take proceeds.
				f.commit.Store(position)
				result, _ := f.tickUntilResolved(t, 200, 1)
				require.NoError(t, result.Err)
				require.Empty(t, result.ServiceRecordingIDs)
				require.Equal(t, []any{
					snapshotMarkerRow(position, 5, 0, clusterwire.SnapshotMark_BEGIN),
					codecs.ConsensusModuleFields{NextSessionID: 1},
					snapshotMarkerRow(position, 5, 0, clusterwire.SnapshotMark_END),
				}, decodeSnapshotRows(t, f.archive.pubs[0].rows))
			},
		},
		{
			name: "zero services follower proceeds on its first tick",
			run: func(t *testing.T) {
				const position int64 = 4096
				f := newTakeFollower(t, 0)
				f.commit.Store(position)
				f.agent.queueSnapshotWait(position, 3, 9)

				f.agent.DoWork(100)
				require.Len(t, f.archive.addOffers, 1)
				require.Equal(t, takeAwaitingStart, f.state(t))

				result, _ := f.tickUntilResolved(t, 101, 1)
				require.NoError(t, result.Err)
				require.Equal(t, SnapshotTakeResult{
					LogPosition:      position,
					LeadershipTermID: 3,
					Timestamp:        9,
					RecordingID:      fakeSnapshotFirstRecordingID,
				}, result)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// heldLeader is a one-service leader with a queued client ingress connect, a
// pending session with a connected egress and an expired timer.
type heldLeader struct {
	*takeFixture
	ingress *QueueClientIngress
}

func newHeldLeader(t *testing.T, nowNs int64) heldLeader {
	t.Helper()
	f := newTakeLeader(t, 1)
	f.archive.offerRetryTimeoutNs = 10_000_000_000
	f.archive.responseTimeoutNs = 5_000_000_000
	a := f.agent
	wireImmediateEgress(t, a)
	ingress := &QueueClientIngress{}
	a.SetClientIngress(ingress)
	ingress.Enqueue(encodeSessionConnectWire(t, validConnectFields(61)))
	a.acceptSessionConnect(validConnectFields(60), nowNs)
	require.Equal(t, 1, a.PendingSessionCount())
	scheduleTimer(a, 11, 1)
	return heldLeader{takeFixture: f, ingress: ingress}
}

// requireUntouched asserts that the ingress frame, the pending session and the
// timer have not moved.
func (h heldLeader) requireUntouched(t testing.TB, label string) {
	t.Helper()
	require.Equal(t, 1, h.ingress.Len(), label)
	require.Equal(t, 1, h.agent.PendingSessionCount(), label)
	require.Equal(t, 0, h.agent.OpenSessionCount(), label)
	require.Equal(t, int64(1), h.agent.ScheduledTimerCount(), label)
}

// requireSessionAndTimerMoved asserts that the pending session opened and the
// timer fired, while the ingress frame waits.
func (h heldLeader) requireSessionAndTimerMoved(t testing.TB) {
	t.Helper()
	require.Equal(t, 1, h.agent.OpenSessionCount())
	require.Equal(t, int64(0), h.agent.ScheduledTimerCount())
	require.Equal(t, 1, h.ingress.Len())
}

func TestSnapshotTake_leaderHold(t *testing.T) {
	t.Parallel()

	const startNs int64 = 1_000_000_000
	h := newHeldLeader(t, startNs)
	h.archive.stopNever = true
	position := h.request(t, startNs)
	require.True(t, h.agent.isLeaderSnapshotPending())

	// Phase 1: held ticks before and after the pop.
	nowNs := startNs
	for range 4 {
		nowNs += 1_000_000
		h.agent.DoWork(nowNs)
		h.requireUntouched(t, "wait")
	}
	h.ack(t, position, 0, 77, 0)
	for range 4 {
		nowNs += 1_000_000
		h.agent.DoWork(nowNs)
		require.NotNil(t, h.agent.snapshotTake)
		h.requireUntouched(t, "take")
	}
	require.Equal(t, takeAwaitingStop, h.state(t))

	// Phase 2: the resolution tick opens the session and fires the timer.
	h.archive.stopNever = false
	h.archive.stopDelay = 0
	nowNs += 1_000_000
	h.agent.DoWork(nowNs)
	result, ok := h.agent.LastSnapshotTake()
	require.True(t, ok)
	require.NoError(t, result.Err)
	require.Equal(t, fakeSnapshotFirstRecordingID, result.RecordingID)
	require.False(t, h.agent.isLeaderSnapshotPending())
	h.requireSessionAndTimerMoved(t)

	// Phase 3: the ingress poll runs before the take step, so the frame
	// moves one tick later.
	nowNs += 1_000_000
	h.agent.DoWork(nowNs)
	require.Equal(t, 0, h.ingress.Len())
}

func TestSnapshotTake_refusals(t *testing.T) {
	t.Parallel()

	t.Run("take in flight", func(t *testing.T) {
		t.Parallel()

		f := newTakeLeader(t, 1)
		f.archive.stopNever = true
		proxy, _ := boundServiceProxy(t)
		f.agent.SetServiceProxy(proxy)
		position := f.request(t, 100)
		f.ack(t, position, 0, 77, 0)
		f.agent.DoWork(101)
		f.agent.DoWork(102)
		require.Equal(t, takeAwaitingStop, f.state(t))
		require.Equal(t, 0, f.agent.PendingServiceAckWaits())

		require.ErrorIs(t, f.agent.RequestSnapshot(103), errSnapshotActionPending)
		require.ErrorIs(t, f.agent.RequestServiceAck(position), errSnapshotActionPending)
		require.Equal(t, 0, f.agent.PendingServiceAckWaits())
	})

	t.Run("marked failure with no wait", func(t *testing.T) {
		t.Parallel()

		f := newTakeLeader(t, 1)
		f.agent.serviceAckFailure = ErrServiceAckOutOfSequence
		pre := f.agent.logPublisher.Position()
		require.ErrorIs(t, f.agent.RequestSnapshot(100), ErrServiceAckOutOfSequence)
		require.Equal(t, pre, f.agent.logPublisher.Position())
		require.Equal(t, 0, f.agent.PendingServiceAckWaits())
		require.False(t, f.agent.HasPendingSnapshotAction())
	})
}

func TestSnapshotTake_markedFailureWithWaitLiftsHold(t *testing.T) {
	t.Parallel()

	const startNs int64 = 1_000_000_000
	h := newHeldLeader(t, startNs)
	position := h.request(t, startNs)

	// Phase 1: the wait holds the leader.
	h.agent.DoWork(startNs + 1)
	h.requireUntouched(t, "wait")

	// Phase 2: an out-of-sequence ack marks the failure. The marking tick
	// opens the session and fires the timer.
	h.ack(t, position, 5, 77, 0)
	h.agent.DoWork(startNs + 2)
	require.ErrorIs(t, h.agent.ServiceAckFailure(), ErrServiceAckOutOfSequence)
	require.False(t, h.agent.isLeaderSnapshotPending())
	h.requireSessionAndTimerMoved(t)

	// Phase 3: the ingress frame moves one tick later. No take builds.
	h.agent.DoWork(startNs + 3)
	require.Equal(t, 0, h.ingress.Len())
	require.Nil(t, h.agent.snapshotTake)
	require.True(t, h.agent.HasPendingSnapshotAction())
	_, ok := h.agent.LastSnapshotTake()
	require.False(t, ok)

	// Phase 4: prepare retires the wait.
	_, prepared, err := h.agent.PrepareForNewLeadership(0, startNs+4)
	require.NoError(t, err)
	require.True(t, prepared)
	require.False(t, h.agent.HasPendingSnapshotAction())
}

func TestSnapshotTake_holdLiftKeepsOpenSessions(t *testing.T) {
	t.Parallel()

	const (
		timeoutNs int64 = 1000
		startNs   int64 = 100
		liftNs          = startNs + 10 + timeoutNs + 1
	)
	tests := []struct {
		name         string
		serviceCount int
		noControl    bool
		heldTicks    int
		// ackBeforeHeld queues the valid ack before the held ticks, so the
		// wait pops and a take holds through them.
		ackBeforeHeld bool
		// lift runs the lift tick at liftNs.
		lift func(t *testing.T, f *takeFixture, position int64)
	}{
		{
			name:          "take resolves after held ticks",
			serviceCount:  1,
			heldTicks:     3,
			ackBeforeHeld: true,
			lift: func(t *testing.T, f *takeFixture, _ int64) {
				f.archive.stopNever = false
				f.archive.stopDelay = 0
				f.agent.DoWork(liftNs)
				result, ok := f.agent.LastSnapshotTake()
				require.True(t, ok)
				require.NoError(t, result.Err)
				require.Equal(t, fakeSnapshotFirstRecordingID, result.RecordingID)
			},
		},
		{
			name:         "marked failure after held ticks",
			serviceCount: 1,
			heldTicks:    3,
			lift: func(t *testing.T, f *takeFixture, position int64) {
				f.ack(t, position, 5, 77, 0)
				f.agent.DoWork(liftNs)
				require.Error(t, f.agent.ServiceAckFailure())
				// The mark lifts the hold. The wait stays queued and no
				// take builds.
				require.Nil(t, f.agent.snapshotTake)
				_, ok := f.agent.LastSnapshotTake()
				require.False(t, ok)
				require.True(t, f.agent.HasPendingSnapshotAction())
			},
		},
		{
			name:         "marked failure in the first tick",
			serviceCount: 1,
			lift: func(t *testing.T, f *takeFixture, position int64) {
				f.ack(t, position, 5, 77, 0)
				f.agent.DoWork(liftNs)
				require.Error(t, f.agent.ServiceAckFailure())
			},
		},
		{
			name:      "zero services with no control in the first tick",
			noControl: true,
			lift: func(t *testing.T, f *takeFixture, _ int64) {
				f.agent.DoWork(liftNs)
				result, ok := f.agent.LastSnapshotTake()
				require.True(t, ok)
				require.ErrorIs(t, result.Err, errSnapshotNotConfigured)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newTakeLeader(t, tt.serviceCount)
			f.agent.sessionTimeoutNs = timeoutNs
			if tt.noControl {
				f.agent.SetSnapshotRecordingControl(nil)
			}
			f.archive.stopNever = true
			f.agent.sessions.learnSessionOpen(codecs.SessionOpenEventFields{
				ClusterSessionID: 1,
				ResponseChannel:  []byte("aeron:ipc"),
			}, 32, startNs)
			addPending(f.agent.sessions, validConnectFields(2), startNs)
			position := f.request(t, startNs)
			if tt.ackBeforeHeld {
				f.ack(t, position, 0, 77, 0)
			}
			for i := range tt.heldTicks {
				f.agent.DoWork(startNs + 1 + int64(i))
				require.True(t, f.agent.isLeaderSnapshotPending())
			}

			tt.lift(t, f, position)
			require.False(t, f.agent.isLeaderSnapshotPending())
			session, ok := f.agent.sessions.sessionByID(1)
			require.True(t, ok)
			require.Equal(t, sessionOpen, session.state)
			require.Equal(t, liftNs, session.lastActivityNs)
			require.Equal(t, 0, f.agent.PendingSessionCount())
			_, ok = f.agent.sessions.sessionByID(2)
			require.False(t, ok)
		})
	}
}

func TestSnapshotTake_markedFailureWhileWriting(t *testing.T) {
	t.Parallel()

	f := newTakeLeader(t, 1)
	f.archive.pubScript = func(pub *fakeSnapshotPublication) {
		pub.offerCodes = []int64{fakeOfferAccept, aeron.BackPressured}
	}
	position := f.request(t, 100)
	f.ack(t, position, 0, 77, 0)
	f.agent.DoWork(101)
	f.agent.DoWork(102)
	require.Equal(t, takeWriting, f.state(t))

	// An out-of-sequence ack marks the failure. The take keeps the hold.
	f.ack(t, position, 5, 77, 0)
	f.agent.DoWork(103)
	require.Error(t, f.agent.ServiceAckFailure())
	require.True(t, f.agent.isLeaderSnapshotPending())

	result, _ := f.tickUntilResolved(t, 104, 1)
	require.NoError(t, result.Err)
	require.False(t, f.agent.isLeaderSnapshotPending())
}

func TestSnapshotTake_copyCut(t *testing.T) {
	t.Parallel()

	const nowNs int64 = 100
	f := newTakeLeader(t, 1)
	a := f.agent
	f.archive.startScript = func(start *fakeSnapshotStart) { start.signalDelay = 3 }
	a.sessions.learnSessionOpen(codecs.SessionOpenEventFields{
		ClusterSessionID: 1,
		CorrelationID:    10,
		ResponseStreamID: 102,
		ResponseChannel:  []byte("aeron:ipc"),
	}, 32, nowNs)
	for _, id := range []int64{2, 3, 4} {
		require.NotNil(t, a.sessions.addPendingConnect(id, validConnectFields(id), nowNs))
	}
	require.True(t, a.sessions.markChallenged(3))
	require.True(t, a.sessions.markRejected(4))
	scheduleTimer(a, 5, 9_000_000_000)

	position := f.request(t, nowNs)
	f.ack(t, position, 0, 77, 0)
	a.DoWork(nowNs + 1)
	require.Equal(t, takeAwaitingStart, f.state(t))

	// Changes after the pop must not reach the copied rows.
	a.sessions.learnSessionOpen(codecs.SessionOpenEventFields{
		ClusterSessionID: 9,
		ResponseChannel:  []byte("aeron:ipc"),
	}, 64, nowNs)
	scheduleTimer(a, 6, 9_000_000_000)
	a.sessions.byID[1].responseChannel[0] = 'X'

	result, _ := f.tickUntilResolved(t, nowNs+2, 1)
	require.NoError(t, result.Err)
	require.Equal(t, []any{
		snapshotMarkerRow(position, 5, 0, clusterwire.SnapshotMark_BEGIN),
		codecs.ConsensusModuleFields{
			NextSessionID:        2,
			NextServiceSessionID: math.MinInt64 + 1,
			LogServiceSessionID:  math.MinInt64,
		},
		codecs.ClusterSessionFields{
			ClusterSessionID:   1,
			CorrelationID:      10,
			OpenedLogPosition:  32,
			TimeOfLastActivity: -1,
			CloseReason:        clusterwire.CloseReason_NULL_VALUE,
			ResponseStreamID:   102,
			ResponseChannel:    []byte("aeron:ipc"),
		},
		codecs.TimerFields{CorrelationID: 5, Deadline: 9_000_000_000},
		emptyPendingMessageTracker(0),
		snapshotMarkerRow(position, 5, 0, clusterwire.SnapshotMark_END),
	}, decodeSnapshotRows(t, f.archive.pubs[0].rows))
}

func TestSnapshotTake_backPressureClears(t *testing.T) {
	t.Parallel()

	f := newTakeLeader(t, 1)
	f.archive.pubScript = func(pub *fakeSnapshotPublication) {
		pub.offerCodes = []int64{
			fakeOfferAccept,
			aeron.BackPressured,
			aeron.BackPressured,
			aeron.AdminAction,
		}
	}
	position := f.request(t, 100)
	f.ack(t, position, 0, 77, 0)
	f.agent.DoWork(101)
	f.agent.DoWork(102)
	pub := f.archive.pubs[0]

	// Phase 1: the take stays at row 1 across ticks.
	for nowNs := range int64(2) {
		require.Equal(t, takeWriting, f.state(t))
		require.Equal(t, 1, f.agent.snapshotTake.nextRow)
		require.Len(t, pub.rows, 1)
		f.agent.DoWork(103 + nowNs)
	}

	// Phase 2: the accepted offer resumes at the same row.
	result, _ := f.tickUntilResolved(t, 105, 1)
	require.NoError(t, result.Err)
	require.Equal(t, []any{
		snapshotMarkerRow(position, 5, 0, clusterwire.SnapshotMark_BEGIN),
		codecs.ConsensusModuleFields{
			NextSessionID:        1,
			NextServiceSessionID: math.MinInt64 + 1,
			LogServiceSessionID:  math.MinInt64,
		},
		emptyPendingMessageTracker(0),
		snapshotMarkerRow(position, 5, 0, clusterwire.SnapshotMark_END),
	}, decodeSnapshotRows(t, pub.rows))
}

func TestSnapshotTake_failures(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	tests := []struct {
		name   string
		stepNs int64
		script func(archive *fakeSnapshotArchive)
		want   error
		// wantCancels lists the expected CancelPending ids.
		wantCancels []int64
	}{
		{
			name:   "add offer error",
			script: func(f *fakeSnapshotArchive) { f.offerAddErr = errBoom },
			want:   errSnapshotPublicationAdd,
		},
		{
			name:        "add poll error",
			script:      func(f *fakeSnapshotArchive) { f.pollAddErr = errBoom },
			want:        errSnapshotPublicationAdd,
			wantCancels: []int64{1},
		},
		{
			name: "add registration timeout",
			script: func(f *fakeSnapshotArchive) {
				f.pollAddErr = &aeron.RegistrationTimeoutError{Kind: "exclusive publication"}
				f.pollAddErrDone = true
			},
			want: errSnapshotPublicationAdd,
		},
		{
			name:   "adder returns no publication",
			script: func(f *fakeSnapshotArchive) { f.nilPublication = true },
			want:   errSnapshotPublicationAdd,
		},
		{
			name: "terminal start offer",
			script: func(f *fakeSnapshotArchive) {
				f.startScript = func(s *fakeSnapshotStart) { s.offerErr = errBoom }
			},
			want: errSnapshotRecordingStart,
		},
		{
			name:   "start offer deadline",
			stepNs: 400,
			script: func(f *fakeSnapshotArchive) {
				f.startScript = func(s *fakeSnapshotStart) { s.offerNever = true }
			},
			want: errSnapshotRecordingStart,
		},
		{
			name: "archive error before START",
			script: func(f *fakeSnapshotArchive) {
				f.startScript = func(s *fakeSnapshotStart) { s.signalErr = errBoom }
			},
			want: errSnapshotRecordingStart,
		},
		{
			name:   "START deadline",
			stepNs: 200,
			script: func(f *fakeSnapshotArchive) {
				f.startScript = func(s *fakeSnapshotStart) { s.signalNever = true }
			},
			want: errSnapshotRecordingStart,
		},
		{
			name: "terminal row offer",
			script: func(f *fakeSnapshotArchive) {
				f.pubScript = func(p *fakeSnapshotPublication) {
					p.offerCodes = []int64{fakeOfferAccept, aeron.Closed}
				}
			},
			want: errSnapshotWrite,
		},
		{
			name:   "row deadline",
			stepNs: 200,
			script: func(f *fakeSnapshotArchive) {
				f.pubScript = func(p *fakeSnapshotPublication) {
					p.offerCodes = slices.Repeat([]int64{aeron.BackPressured}, 32)
				}
			},
			want: errSnapshotWrite,
		},
		{
			name:   "early STOP while writing",
			script: func(f *fakeSnapshotArchive) { f.earlyStop = true },
			want:   errSnapshotWrite,
		},
		{
			name: "publication close error",
			script: func(f *fakeSnapshotArchive) {
				f.pubScript = func(p *fakeSnapshotPublication) { p.closeErr = errBoom }
			},
			want: errSnapshotWrite,
		},
		{
			name:   "STOP below the close position",
			script: func(f *fakeSnapshotArchive) { f.stopShortBy = 1 },
			want:   errSnapshotRecordingStop,
		},
		{
			name:   "STOP deadline",
			stepNs: 200,
			script: func(f *fakeSnapshotArchive) { f.stopNever = true },
			want:   errSnapshotRecordingStop,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newTakeLeader(t, 1)
			tt.script(f.archive)
			position := f.request(t, 100)
			f.ack(t, position, 0, 77, 0)
			stepNs := max(tt.stepNs, 1)
			result, _ := f.tickUntilResolved(t, 100, stepNs)

			require.ErrorIs(t, result.Err, tt.want)
			require.Equal(t, int64(nullValue), result.RecordingID)
			require.Equal(t, position, result.LogPosition)
			require.Equal(t, []int64{77}, result.ServiceRecordingIDs)
			require.False(t, f.agent.HasPendingSnapshotAction())
			require.Equal(t, tt.wantCancels, f.archive.cancels)
			f.requireCleanedUp(t)
			f.requireNoRecordingLogWrite(t)
		})
	}
}

func TestSnapshotTake_completionOnDeadlineTick(t *testing.T) {
	t.Parallel()

	f := newTakeLeader(t, 1)
	position := f.request(t, 100)
	f.ack(t, position, 0, 77, 0)

	// Tick 1: the start offer lands at 100. The START deadline is 600.
	f.agent.DoWork(100)
	require.Equal(t, takeAwaitingStart, f.state(t))

	// Tick 2: START arrives on the deadline tick. The rows go out and the
	// publication closes. The STOP deadline is 1100.
	f.agent.DoWork(100 + f.archive.responseTimeoutNs)
	require.Equal(t, takeAwaitingStop, f.state(t))

	// Tick 3: STOP arrives on the deadline tick.
	f.agent.DoWork(100 + 2*f.archive.responseTimeoutNs)
	result, ok := f.agent.LastSnapshotTake()
	require.True(t, ok)
	require.NoError(t, result.Err)
	require.Equal(t, fakeSnapshotFirstRecordingID, result.RecordingID)
}

func TestSnapshotTake_cancelAtPrepare(t *testing.T) {
	t.Parallel()

	addInFlight := func(f *takeFixture) { f.archive.addPendingPolls = 100 }
	tests := []struct {
		name          string
		state         snapshotTakeState
		script        func(f *takeFixture)
		beforePrepare func(f *takeFixture)
		wantCancels   []int64
	}{
		{
			name:   "checking",
			state:  takeChecking,
			script: func(f *takeFixture) { f.commit.Store(0) },
		},
		{
			// One not-done poll remains. A second cleanup poll would read
			// ready and skip the cancel.
			name:          "adding publication in flight",
			state:         takeAddingPublication,
			script:        addInFlight,
			beforePrepare: func(f *takeFixture) { f.archive.pending[1] = 1 },
			wantCancels:   []int64{1},
		},
		{
			// A conductor tick outside the take read the ready reply.
			name:          "adding publication ready",
			state:         takeAddingPublication,
			script:        addInFlight,
			beforePrepare: func(f *takeFixture) { f.archive.pending[1] = 0 },
		},
		{
			name:   "adding publication done without publication",
			state:  takeAddingPublication,
			script: addInFlight,
			beforePrepare: func(f *takeFixture) {
				f.archive.pending[1] = 0
				f.archive.nilPublication = true
			},
			wantCancels: []int64{1},
		},
		{
			name:   "adding publication terminal",
			state:  takeAddingPublication,
			script: addInFlight,
			beforePrepare: func(f *takeFixture) {
				f.archive.pollAddErr = &aeron.RegistrationTimeoutError{Kind: "exclusive publication"}
				f.archive.pollAddErrDone = true
			},
			wantCancels: []int64{1},
		},
		{
			name:   "adding publication vanished",
			state:  takeAddingPublication,
			script: addInFlight,
			beforePrepare: func(f *takeFixture) {
				f.archive.pollAddErr = aeron.ErrAwaitInterrupted
				f.archive.pollAddErrDone = false
			},
			wantCancels: []int64{1},
		},
		{
			name:  "offering start",
			state: takeOfferingStart,
			script: func(f *takeFixture) {
				f.archive.startScript = func(s *fakeSnapshotStart) { s.offerNever = true }
			},
		},
		{
			name:  "awaiting start",
			state: takeAwaitingStart,
			script: func(f *takeFixture) {
				f.archive.startScript = func(s *fakeSnapshotStart) { s.signalNever = true }
			},
		},
		{
			name:  "writing",
			state: takeWriting,
			script: func(f *takeFixture) {
				f.archive.pubScript = func(p *fakeSnapshotPublication) {
					p.offerCodes = slices.Repeat([]int64{aeron.BackPressured}, 32)
				}
			},
		},
		{
			name:   "awaiting stop",
			state:  takeAwaitingStop,
			script: func(f *takeFixture) { f.archive.stopNever = true },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newTakeLeader(t, 1)
			position := f.request(t, 100)
			tt.script(f)
			f.ack(t, position, 0, 77, 0)
			f.agent.DoWork(101)
			f.agent.DoWork(102)
			require.Equal(t, tt.state, f.state(t))
			if tt.beforePrepare != nil {
				tt.beforePrepare(f)
			}

			_, prepared, err := f.agent.PrepareForNewLeadership(0, 103)
			require.NoError(t, err)
			require.True(t, prepared)
			f.requireCleanedUp(t)
			f.requireNoRecordingLogWrite(t)
			require.Equal(t, tt.wantCancels, f.archive.cancels)
			_, ok := f.agent.LastSnapshotTake()
			require.False(t, ok)
			require.NotNil(t, f.agent.abandonedSnapshotWaitAt(position))
			require.False(t, f.agent.HasPendingSnapshotAction())
		})
	}
}

func TestSnapshotTake_cancelAtFollowerJoin(t *testing.T) {
	t.Parallel()

	const position int64 = 4096
	f := newTakeFollower(t, 0)
	a := f.agent
	f.commit.Store(position)
	f.archive.pubScript = func(p *fakeSnapshotPublication) {
		p.offerCodes = slices.Repeat([]int64{aeron.BackPressured}, 32)
	}
	a.queueSnapshotWait(position, 3, 9)
	a.DoWork(100)
	a.DoWork(101)
	require.Equal(t, takeWriting, f.state(t))

	adapter, _ := boundLogAdapter(t)
	a.SetLogAdapter(adapter)
	image := aeron.NewTestImageForHooks(t, aeron.NewTestSubscriptionForHooks(t), 0)
	a.SetLogJoinSource(&stubLogJoinSource{image: image})
	control := &fakeLogRecordingControl{nextRecordingID: 12}
	control.onStart = func() {
		f.requireCleanedUp(t)
	}
	a.SetLogRecordingControl(control)

	joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{LogSessionID: image.SessionID()}, 102)
	require.NoError(t, err)
	require.True(t, joined)
	require.Len(t, control.startCalls, 1)
	f.requireCleanedUp(t)
	f.requireNoRecordingLogWrite(t)
	_, ok := a.LastSnapshotTake()
	require.False(t, ok)
	require.NotNil(t, a.abandonedSnapshotWaitAt(position))
}

func TestCloseSnapshotTake_pendingAdd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		beforeClose func(f *takeFixture)
		wantCancels []int64
	}{
		{
			name:        "in flight",
			beforeClose: func(f *takeFixture) { f.archive.pending[1] = 1 },
			wantCancels: []int64{1},
		},
		{
			// A conductor tick outside the take read the ready reply.
			name:        "ready",
			beforeClose: func(f *takeFixture) { f.archive.pending[1] = 0 },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newTakeLeader(t, 1)
			f.archive.addPendingPolls = 100
			position := f.request(t, 100)
			f.ack(t, position, 0, 77, 0)
			f.agent.DoWork(101)
			require.Equal(t, takeAddingPublication, f.state(t))
			if tt.beforeClose != nil {
				tt.beforeClose(f)
			}

			f.agent.CloseSnapshotTake()
			f.agent.CloseSnapshotTake()
			require.Equal(t, tt.wantCancels, f.archive.cancels)
			f.requireCleanedUp(t)
			f.requireNoRecordingLogWrite(t)
			_, ok := f.agent.LastSnapshotTake()
			require.False(t, ok)
			require.False(t, f.agent.HasPendingSnapshotAction())
		})
	}
}

func TestSnapshotTake_repeatTake(t *testing.T) {
	t.Parallel()

	f := newTakeLeader(t, 1)
	first := f.request(t, 100)
	f.ack(t, first, 0, 77, 0)
	firstResult, nowNs := f.tickUntilResolved(t, 100, 1)
	require.NoError(t, firstResult.Err)
	require.Equal(t, fakeSnapshotFirstRecordingID, firstResult.RecordingID)

	second := f.request(t, nowNs+1)
	require.Greater(t, second, first)
	f.ack(t, second, 1, 78, 0)
	result, _ := f.tickUntilResolved(t, nowNs+1, 1)
	require.NoError(t, result.Err)
	require.Equal(t, second, result.LogPosition)
	require.Equal(t, fakeSnapshotFirstRecordingID+1, result.RecordingID)
	require.Equal(t, []int64{78}, result.ServiceRecordingIDs)
	require.Len(t, f.archive.pubs, 2)
	require.Len(t, f.archive.addOffers, 2)
	require.Equal(t, slices.Concat(
		fakeSnapshotGroup(firstResult, fakeSnapshotFirstRecordingID),
		fakeSnapshotGroup(result, fakeSnapshotFirstRecordingID+1),
	), f.recordingLog.rows)
	require.Len(t, f.recordingLog.forces, 2)
}

func TestSnapshotTake_followerHold(t *testing.T) {
	t.Parallel()

	adapter, image, spy := boundLogAdapterWithHandler(t)
	actionEnd, messageEnd := writeSnapshotActionThenMessage(t, image, 0)
	a, q := newFollowerActionAgent(messageEnd+100, adapter, 1)
	a.notifiedCommitPosition = messageEnd + 32
	archive := newFakeSnapshotArchive()
	archive.stopNever = true
	a.SetSnapshotRecordingControl(archive)
	a.SetSnapshotPublicationAdder(archive)
	recordingLog := &fakeSnapshotRecordingLog{}
	a.SetSnapshotRecordingLog(recordingLog, recordinglog.FileSyncData)
	a.SetSnapshotStream(testSnapshotChannel, testSnapshotStreamID)

	// Phase 1: consume the action and queue the wait.
	a.DoWork(1)
	require.Equal(t, actionEnd, adapter.Position())
	require.True(t, a.HasPendingSnapshotAction())

	// Phase 2: the ack pops the wait. The take holds the adapter at the
	// action.
	enqueueServiceAck(t, q, actionEnd, 0, 0)
	for nowNs := range int64(4) {
		a.DoWork(2 + nowNs)
		require.NotNil(t, a.snapshotTake)
		require.Equal(t, int64(0), a.snapshotWaitPosition)
		require.Equal(t, actionEnd, adapter.Position())
		require.Equal(t, actionEnd, a.CommitPosition())
		requireHandlerIdle(t, spy)
		require.Empty(t, recordingLog.rows)
	}

	// Phase 3: the STOP tick resolves the take. The message arrives in the
	// same tick, since consensusWork runs after the take step.
	archive.stopNever = false
	archive.stopDelay = 0
	a.DoWork(6)
	result, ok := a.LastSnapshotTake()
	require.True(t, ok)
	require.NoError(t, result.Err)
	require.Equal(t, fakeSnapshotFirstRecordingID, result.RecordingID)
	require.Equal(t, int64(1), result.LeadershipTermID)
	require.Equal(t, int64(5), result.Timestamp)
	require.Equal(t, []int64{1}, result.ServiceRecordingIDs)
	require.Equal(t, actionEnd, result.LogPosition)
	require.Equal(t, fakeSnapshotGroup(result, fakeSnapshotFirstRecordingID), recordingLog.rows)
	require.Equal(t, []fakeSnapshotForce{
		{level: recordinglog.FileSyncData, rows: 2},
	}, recordingLog.forces)
	require.Equal(t, 1, spy.sessionMessages)
	require.Equal(t, messageEnd, adapter.Position())
}

// resolveTakeWithServices requests one snapshot on a leader with
// serviceCount services. Service s acks RelevantID 77+s. script adjusts the
// fixture before the request.
func resolveTakeWithServices(
	t *testing.T,
	serviceCount int,
	script func(f *takeFixture),
) (*takeFixture, SnapshotTakeResult) {
	t.Helper()
	f := newTakeLeader(t, serviceCount)
	if script != nil {
		script(f)
	}
	position := f.request(t, 100)
	for serviceID := range serviceCount {
		f.ack(t, position, 0, 77+int64(serviceID), int32(serviceID))
	}
	result, _ := f.tickUntilResolved(t, 100, 1)
	return f, result
}

func TestSnapshotTake_recordingLogRows(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	syncData := func(rows int) []fakeSnapshotForce {
		return []fakeSnapshotForce{{level: recordinglog.FileSyncData, rows: rows}}
	}
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "zero services writes the module row only",
			run: func(t *testing.T) {
				f, result := resolveTakeWithServices(t, 0, nil)
				require.NoError(t, result.Err)
				require.NoError(t, result.ForceErr)
				require.Equal(t, fakeSnapshotGroup(result, fakeSnapshotFirstRecordingID), f.recordingLog.rows)
				require.Equal(t, syncData(1), f.recordingLog.forces)
			},
		},
		{
			name: "group order and force",
			run: func(t *testing.T) {
				f, result := resolveTakeWithServices(t, 3, nil)
				require.NoError(t, result.Err)
				require.NoError(t, result.ForceErr)
				require.Equal(t, fakeSnapshotFirstRecordingID, result.RecordingID)
				require.Equal(t, int64(5), result.LeadershipTermID)
				require.Equal(t, int64(100), result.Timestamp)
				rows := f.recordingLog.rows
				require.Equal(t, fakeSnapshotGroup(result, fakeSnapshotFirstRecordingID), rows)
				serviceIDs := make([]int32, 0, len(rows))
				recordingIDs := make([]int64, 0, len(rows))
				for _, row := range rows {
					serviceIDs = append(serviceIDs, row.ServiceID)
					recordingIDs = append(recordingIDs, row.RecordingID)
				}
				require.Equal(t, []int32{2, 1, 0, recordinglog.ConsensusModuleServiceID}, serviceIDs)
				require.Equal(t, []int64{79, 78, 77, fakeSnapshotFirstRecordingID}, recordingIDs)
				require.Equal(t, syncData(4), f.recordingLog.forces)
			},
		},
		{
			name: "force uses the configured level",
			run: func(t *testing.T) {
				f, result := resolveTakeWithServices(t, 2, func(f *takeFixture) {
					f.agent.SetSnapshotRecordingLog(f.recordingLog, recordinglog.FileSyncFull)
				})
				require.NoError(t, result.Err)
				require.NoError(t, result.ForceErr)
				require.Equal(t, []fakeSnapshotForce{
					{level: recordinglog.FileSyncFull, rows: 3},
				}, f.recordingLog.forces)
			},
		},
		{
			name: "missing term entry",
			run: func(t *testing.T) {
				f, result := resolveTakeWithServices(t, 1, func(f *takeFixture) {
					f.recordingLog.missingTerm = true
				})
				require.ErrorIs(t, result.Err, errSnapshotRecordingLog)
				require.NoError(t, result.ForceErr)
				require.Equal(t, int64(nullValue), result.RecordingID)
				require.Len(t, f.archive.starts, 1)
				require.False(t, f.agent.HasPendingSnapshotAction())
				f.requireNoRecordingLogWrite(t)
				f.requireCleanedUp(t)
			},
		},
		{
			name: "append error stops the group",
			run: func(t *testing.T) {
				// Append calls run service 1, service 0, then the module row.
				for _, failAt := range []int{0, 1, 2} {
					t.Run(fmt.Sprintf("fail at %d", failAt), func(t *testing.T) {
						f, result := resolveTakeWithServices(t, 2, func(f *takeFixture) {
							f.recordingLog.appendErr = errBoom
							f.recordingLog.appendErrAt = failAt
						})
						require.ErrorIs(t, result.Err, errSnapshotRecordingLog)
						require.ErrorIs(t, result.Err, errBoom)
						require.NoError(t, result.ForceErr)
						require.Equal(t, int64(nullValue), result.RecordingID)
						group := fakeSnapshotGroup(result, fakeSnapshotFirstRecordingID)
						require.Len(t, f.recordingLog.rows, failAt)
						for i, row := range f.recordingLog.rows {
							require.Equal(t, group[i], row, "row %d", i)
						}
						require.Empty(t, f.recordingLog.forces)
						f.requireCleanedUp(t)
					})
				}
			},
		},
		{
			name: "force error keeps the snapshot",
			run: func(t *testing.T) {
				f, result := resolveTakeWithServices(t, 2, func(f *takeFixture) {
					f.recordingLog.forceErr = errBoom
				})
				require.NoError(t, result.Err)
				require.Equal(t, fakeSnapshotFirstRecordingID, result.RecordingID)
				require.ErrorIs(t, result.ForceErr, errSnapshotRecordingLogForce)
				require.ErrorIs(t, result.ForceErr, errBoom)
				require.Equal(t, []int64{77, 78}, result.ServiceRecordingIDs)
				require.Equal(t, fakeSnapshotGroup(result, fakeSnapshotFirstRecordingID), f.recordingLog.rows)
				require.Equal(t, syncData(3), f.recordingLog.forces)
				require.False(t, f.agent.HasPendingSnapshotAction())
				f.requireCleanedUp(t)
			},
		},
		{
			name: "retake after a failed write",
			run: func(t *testing.T) {
				// Phase 1: appending the module row fails the first take. The
				// service row stays.
				f, first := resolveTakeWithServices(t, 1, func(f *takeFixture) {
					f.recordingLog.appendErr = errBoom
					f.recordingLog.appendErrAt = 1
				})
				require.ErrorIs(t, first.Err, errSnapshotRecordingLog)
				firstRows := fakeSnapshotGroup(first, fakeSnapshotFirstRecordingID)[:1]
				require.Equal(t, firstRows, f.recordingLog.rows)

				// Phase 2: a second action writes a full group at its own
				// position.
				f.recordingLog.appendErr = nil
				second := f.request(t, 200)
				require.Greater(t, second, first.LogPosition)
				f.ack(t, second, 1, 88, 0)
				result, _ := f.tickUntilResolved(t, 200, 1)
				require.NoError(t, result.Err)
				require.NoError(t, result.ForceErr)
				require.Equal(t, second, result.LogPosition)
				require.Equal(t, slices.Concat(
					firstRows,
					fakeSnapshotGroup(result, fakeSnapshotFirstRecordingID+1),
				), f.recordingLog.rows)
				require.Equal(t, []fakeSnapshotForce{
					{level: recordinglog.FileSyncData, rows: 3},
				}, f.recordingLog.forces)
			},
		},
		{
			name: "retake after a force error",
			run: func(t *testing.T) {
				// Phase 1: Force fails the first take after its full group.
				// The take keeps the group.
				f, first := resolveTakeWithServices(t, 1, func(f *takeFixture) {
					f.recordingLog.forceErr = errBoom
				})
				require.NoError(t, first.Err)
				require.ErrorIs(t, first.ForceErr, errSnapshotRecordingLogForce)
				firstGroup := fakeSnapshotGroup(first, fakeSnapshotFirstRecordingID)
				require.Equal(t, firstGroup, f.recordingLog.rows)

				// Phase 2: a second action appends a full group behind the
				// first, then forces the log again.
				f.recordingLog.forceErr = nil
				second := f.request(t, 200)
				require.Greater(t, second, first.LogPosition)
				f.ack(t, second, 1, 88, 0)
				result, _ := f.tickUntilResolved(t, 200, 1)
				require.NoError(t, result.Err)
				require.NoError(t, result.ForceErr)
				require.Equal(t, second, result.LogPosition)
				require.Equal(t, slices.Concat(
					firstGroup,
					fakeSnapshotGroup(result, fakeSnapshotFirstRecordingID+1),
				), f.recordingLog.rows)
				require.Equal(t, []fakeSnapshotForce{
					{level: recordinglog.FileSyncData, rows: 2},
					{level: recordinglog.FileSyncData, rows: 4},
				}, f.recordingLog.forces)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
