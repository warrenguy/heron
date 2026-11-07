package cluster

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/internal/testutil"
)

const testModuleErrorCounterID int32 = 2

var errStorageSpace = &archive.Exception{ErrorCode: archive.ErrorStorageSpace}

// newTestCounters returns a reader over four fresh counters and its values
// buffer.
func newTestCounters(t *testing.T) (*cnc.CountersReader, atomicbuffer.AtomicBuffer) {
	t.Helper()
	values := make([]byte, 4*cnc.CounterValueLength)
	meta := make([]byte, 4*cnc.CounterMetadataLength)
	counters, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)
	return counters, atomicbuffer.Wrap(values)
}

// newTestModuleErrorHandler builds a handler on counter
// testModuleErrorCounterID of fresh counters. It also returns the values
// buffer.
func newTestModuleErrorHandler(
	t *testing.T,
	handler func(error),
) (*moduleErrorHandler, atomicbuffer.AtomicBuffer) {
	t.Helper()
	counters, values := newTestCounters(t)
	h, err := newModuleErrorHandler(counters, testModuleErrorCounterID, handler)
	require.NoError(t, err)
	return h, values
}

func moduleErrorCount(buffer atomicbuffer.AtomicBuffer) int64 {
	return buffer.GetInt64Volatile(cnc.CounterOffset(testModuleErrorCounterID))
}

type errorRecorder struct {
	reported []error
}

func (r *errorRecorder) record(err error) {
	r.reported = append(r.reported, err)
}

func TestModuleErrorCounterLabel(t *testing.T) {
	t.Parallel()

	require.Equal(t, "Cluster Errors - clusterId=0", moduleErrorCounterLabel)
}

func TestNewModuleErrorHandler_rejectsOutOfRangeCounterID(t *testing.T) {
	t.Parallel()

	counters, _ := newTestCounters(t)

	h, err := newModuleErrorHandler(counters, 4, nil)
	require.ErrorContains(t, err, "cluster: module error counter: ")
	require.Nil(t, h)
}

func TestModuleErrorHandler_handleError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "one report counts before the handler call",
			run: func(t *testing.T) {
				var reported []error
				var countInCall []int64
				var buffer atomicbuffer.AtomicBuffer
				var h *moduleErrorHandler
				h, buffer = newTestModuleErrorHandler(t, func(err error) {
					reported = append(reported, err)
					countInCall = append(countInCall, moduleErrorCount(buffer))
				})

				h.handleError(errBoom)
				require.Equal(t, []error{errBoom}, reported)
				require.Equal(t, []int64{1}, countInCall)
				require.Equal(t, int64(1), moduleErrorCount(buffer))
			},
		},
		{
			name: "nil handler still counts",
			run: func(t *testing.T) {
				h, buffer := newTestModuleErrorHandler(t, nil)

				h.handleError(errBoom)
				h.handleError(errBoom)
				require.Equal(t, int64(2), moduleErrorCount(buffer))
			},
		},
		{
			name: "nil error does not count or call",
			run: func(t *testing.T) {
				var recorder errorRecorder
				h, buffer := newTestModuleErrorHandler(t, recorder.record)

				h.handleError(nil)
				require.Empty(t, recorder.reported)
				require.Zero(t, moduleErrorCount(buffer))
			},
		},
		{
			name: "report after close does not count or call",
			run: func(t *testing.T) {
				var recorder errorRecorder
				h, buffer := newTestModuleErrorHandler(t, recorder.record)

				// Phase 1: one report before close.
				h.handleError(errBoom)
				require.Equal(t, []error{errBoom}, recorder.reported)
				require.Equal(t, int64(1), moduleErrorCount(buffer))

				// Phase 2: close, then report.
				h.close()
				h.handleError(errBoom)
				require.Equal(t, []error{errBoom}, recorder.reported)
				require.Equal(t, int64(1), moduleErrorCount(buffer))
			},
		},
		{
			name: "second close keeps the count and the closed state",
			run: func(t *testing.T) {
				var recorder errorRecorder
				h, buffer := newTestModuleErrorHandler(t, recorder.record)
				h.handleError(errBoom)

				h.close()
				h.close()
				h.handleError(errBoom)
				require.Equal(t, []error{errBoom}, recorder.reported)
				require.Equal(t, int64(1), moduleErrorCount(buffer))
			},
		},
		{
			name: "bare storage space ends the member after the report",
			run: func(t *testing.T) {
				requireStorageSpaceEndsMember(t, errStorageSpace)
			},
		},
		{
			name: "wrapped storage space ends the member after the report",
			run: func(t *testing.T) {
				requireStorageSpaceEndsMember(t, fmt.Errorf("take: %w", errStorageSpace))
			},
		},
		{
			name: "storage space with a nil handler ends the member",
			run: func(t *testing.T) {
				var ended errorRecorder
				h, buffer := newTestModuleErrorHandler(t, nil)
				h.terminate = ended.record

				h.handleError(errStorageSpace)
				require.Equal(t, []error{errStorageSpace}, ended.reported)
				require.Equal(t, int64(1), moduleErrorCount(buffer))
			},
		},
		{
			name: "another archive code does not end the member",
			run: func(t *testing.T) {
				var recorder, ended errorRecorder
				h, buffer := newTestModuleErrorHandler(t, recorder.record)
				h.terminate = ended.record
				errUnknownReplay := &archive.Exception{ErrorCode: archive.ErrorUnknownReplay}

				h.handleError(errUnknownReplay)
				require.Equal(t, []error{errUnknownReplay}, recorder.reported)
				require.NotErrorIs(t, recorder.reported[0], election.ErrEvent)
				require.Empty(t, ended.reported)
				require.Equal(t, int64(1), moduleErrorCount(buffer))
			},
		},
		{
			name: "a non archive error does not end the member",
			run: func(t *testing.T) {
				var ended errorRecorder
				h, buffer := newTestModuleErrorHandler(t, nil)
				h.terminate = ended.record

				h.handleError(errBoom)
				require.Empty(t, ended.reported)
				require.Equal(t, int64(1), moduleErrorCount(buffer))
			},
		},
		{
			name: "nil terminate still counts and calls the handler",
			run: func(t *testing.T) {
				var recorder errorRecorder
				h, buffer := newTestModuleErrorHandler(t, recorder.record)

				h.handleError(errStorageSpace)
				require.Equal(t, []error{errStorageSpace}, recorder.reported)
				require.Equal(t, int64(1), moduleErrorCount(buffer))
			},
		},
		{
			name: "closed handler does not count or call the handler or terminate",
			run: func(t *testing.T) {
				var recorder, ended errorRecorder
				h, buffer := newTestModuleErrorHandler(t, recorder.record)
				h.terminate = ended.record

				h.close()
				h.handleError(errStorageSpace)
				require.Empty(t, recorder.reported)
				require.Empty(t, ended.reported)
				require.Zero(t, moduleErrorCount(buffer))
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

func requireStorageSpaceEndsMember(t *testing.T, err error) {
	t.Helper()
	var order []string
	h, buffer := newTestModuleErrorHandler(t, func(got error) {
		require.Same(t, err, got)
		order = append(order, "handler")
	})
	h.terminate = func(got error) {
		require.Same(t, err, got)
		order = append(order, "terminate")
	}

	h.handleError(err)
	require.Equal(t, []string{"handler", "terminate"}, order)
	require.Equal(t, int64(1), moduleErrorCount(buffer))
}

func TestModuleErrorHandler_handleError_zeroAllocs(t *testing.T) {
	errBoom := errors.New("boom")

	withoutHandler, _ := newTestModuleErrorHandler(t, nil)
	testutil.AssertZeroAllocs(t, "nil handler", 100, func() {
		withoutHandler.handleError(errBoom)
	})

	withHandler, _ := newTestModuleErrorHandler(t, func(error) {})
	testutil.AssertZeroAllocs(t, "no-op handler", 100, func() {
		withHandler.handleError(errBoom)
	})
}

func newTestModuleAgent(h *moduleErrorHandler) *consensus.Agent {
	return consensus.NewAgent(moduleAgentConfig(
		Config{},
		election.ElectionConfig{},
		new(atomic.Int64),
		h.handleError,
	))
}

type fakeArchiveFailureSource struct {
	connected bool
	handler   func(error)
}

func (s *fakeArchiveFailureSource) IsConnected() bool { return s.connected }

func (s *fakeArchiveFailureSource) SetErrorResponseHandler(handler func(error)) {
	s.handler = handler
}

func TestWireArchiveFailure(t *testing.T) {
	t.Parallel()

	t.Run("storage space report ends the member", func(t *testing.T) {
		t.Parallel()

		var recorder errorRecorder
		h, buffer := newTestModuleErrorHandler(t, recorder.record)
		agent := newTestModuleAgent(h)
		source := &fakeArchiveFailureSource{connected: true}

		wireArchiveFailure(h, agent, source)
		require.NotNil(t, source.handler)
		require.NoError(t, agent.TerminationError())

		source.handler(errStorageSpace)
		require.Equal(t, int64(1), moduleErrorCount(buffer))
		require.Equal(t, []error{errStorageSpace}, recorder.reported)
		require.ErrorIs(t, agent.TerminationError(), consensus.ErrTerminated)
		require.ErrorIs(t, agent.TerminationError(), errStorageSpace)
		require.Equal(t, consensus.ModuleStateClosed, agent.ModuleState())
	})

	t.Run("lost connection ends the member on the slow tick", func(t *testing.T) {
		t.Parallel()

		var recorder errorRecorder
		h, buffer := newTestModuleErrorHandler(t, recorder.record)
		agent := newTestModuleAgent(h)

		wireArchiveFailure(h, agent, &fakeArchiveFailureSource{connected: false})
		require.NoError(t, agent.TerminationError())

		require.Equal(t, 1, agent.DoWork(1))
		require.Equal(t, int64(1), moduleErrorCount(buffer))
		require.Equal(t, []error{consensus.ErrLocalArchiveDisconnected}, recorder.reported)
		require.ErrorIs(t, agent.TerminationError(), consensus.ErrTerminated)
		require.ErrorIs(t, agent.TerminationError(), consensus.ErrLocalArchiveDisconnected)
		require.Equal(t, consensus.ModuleStateClosed, agent.ModuleState())
	})
}

func TestModuleAgentConfig_carriesErrorHandler(t *testing.T) {
	t.Parallel()

	auth := &recordingConnectAuthenticator{}
	cfg := Config{
		MemberID:         3,
		SessionTimeoutNs: 7_000,
		Authenticator:    auth,
		ServiceCount:     2,
	}
	electionCfg := election.ElectionConfig{
		LeaderHeartbeatIntervalNs: 11_000,
		LeaderHeartbeatTimeoutNs:  13_000,
		AppVersion:                17,
	}
	commit := new(atomic.Int64)
	var recorder errorRecorder

	got := moduleAgentConfig(cfg, electionCfg, commit, recorder.record)
	require.NotNil(t, got.ErrorHandler)
	errBoom := errors.New("boom")
	got.ErrorHandler(errBoom)
	require.Equal(t, []error{errBoom}, recorder.reported)

	got.ErrorHandler = nil
	require.Equal(t, consensus.AgentConfig{
		Commit:                    commit,
		ThisMemberID:              3,
		LeaderMemberID:            election.NullMemberID,
		LeaderHeartbeatIntervalNs: 11_000,
		LeaderHeartbeatTimeoutNs:  13_000,
		SessionTimeoutNs:          7_000,
		AppVersion:                17,
		Authenticator:             auth,
		ServiceCount:              2,
	}, got)
	require.Same(t, commit, got.Commit)
}

func TestConsensusModule_Close_stopsErrorReports(t *testing.T) {
	t.Parallel()

	t.Run("reports stop after Close", func(t *testing.T) {
		t.Parallel()

		var recorder errorRecorder
		h, buffer := newTestModuleErrorHandler(t, recorder.record)
		agent := newTestModuleAgent(h)
		module := &ConsensusModule{agent: agent, errorHandler: h}
		errBoom := errors.New("boom")

		// Phase 1: report before Close.
		h.handleError(errBoom)
		require.Equal(t, []error{errBoom}, recorder.reported)
		require.Equal(t, int64(1), moduleErrorCount(buffer))

		// Phase 2: Close, then report.
		require.NoError(t, module.Close())
		h.handleError(errBoom)
		require.Equal(t, []error{errBoom}, recorder.reported)
		require.Equal(t, int64(1), moduleErrorCount(buffer))
	})

	t.Run("Close clears the archive connection", func(t *testing.T) {
		t.Parallel()

		h, _ := newTestModuleErrorHandler(t, nil)
		agent := newTestModuleAgent(h)
		wireArchiveFailure(h, agent, &fakeArchiveFailureSource{connected: false})
		module := &ConsensusModule{agent: agent, errorHandler: h}

		require.NoError(t, module.Close())
		require.Zero(t, agent.DoWork(1))
		require.NoError(t, agent.TerminationError())
	})

	t.Run("Close without an error handler", func(t *testing.T) {
		t.Parallel()

		module := &ConsensusModule{}
		require.NotPanics(t, func() {
			require.NoError(t, module.Close())
		})
	})
}
