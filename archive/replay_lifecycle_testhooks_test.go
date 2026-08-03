//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const (
	lifecycleReplaySessionID int64 = 8800
	lifecycleReplayStreamID  int32 = 1201
	lifecycleReplayChannel         = "aeron:ipc?term-length=64k"
)

func (h *lifecycleHarness) newClient() *Client {
	return &Client{
		controlSessionID:    controlResponseTestControlSessionID,
		archiveID:           lifecycleArchiveID,
		proxy:               h.proxy,
		controlPoller:       h.poller,
		descriptorPoller:    NewRecordingDescriptorPoller(),
		controlSubscription: h.sub,
		controlAssembler:    h.assembler,
		aeronClient:         h.client,
		cfg:                 h.cfg,
	}
}

// writeNextControlResponse pre-writes a ControlResponse for the correlation id the
// next Client method call allocates. The ring correlation counter is sequential
// and tests are single-threaded, so the id consumed here is one below that id.
func (h *lifecycleHarness) writeNextControlResponse(
	t *testing.T,
	relevantID int64,
	code archivewire.ControlResponseCode,
) {
	t.Helper()

	responseWire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    h.client.NextCorrelationID() + 1,
		RelevantID:       relevantID,
		Code:             code,
		Version:          520,
	})
	writeUnfragmentedWireToTerm(t, h.termBuffer, responseWire)
}

func TestStartReplay_awaitReturnsReplaySessionId(t *testing.T) {
	h := newLifecycleHarness(t)
	written := false
	targets := h.responseTargets()
	pollFn := h.respondControlResponseOnce(
		t,
		lifecycleReplaySessionID,
		archivewire.ControlResponseCode_OK,
		&written,
		targets,
	)

	got, err := h.controlOps(pollFn).startReplay(
		t.Context(),
		lifecycleRecordingID,
		4096,
		8192,
		lifecycleReplayStreamID,
		[]byte(lifecycleReplayChannel),
	)
	require.NoError(t, err)
	require.Equal(t, lifecycleReplaySessionID, got)
}

func TestStartReplay_errorResponseReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	written := false
	targets := h.responseTargets()
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		archivewire.ControlResponseCode_ERROR,
		&written,
		targets,
	)

	_, err := h.controlOps(pollFn).startReplay(
		t.Context(),
		lifecycleRecordingID,
		4096,
		8192,
		lifecycleReplayStreamID,
		[]byte(lifecycleReplayChannel),
	)
	require.Error(t, err)
}

func TestStopReplay_awaitOK(t *testing.T) {
	h := newLifecycleHarness(t)
	written := false
	targets := h.responseTargets()
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		archivewire.ControlResponseCode_OK,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).stopReplay(t.Context(), lifecycleReplaySessionID)
	require.NoError(t, err)
}

func TestStopReplay_errorResponseReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	written := false
	targets := h.responseTargets()
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		archivewire.ControlResponseCode_ERROR,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).stopReplay(t.Context(), lifecycleReplaySessionID)
	require.Error(t, err)
}

func TestClientStartReplay_methodReturnsReplaySessionId(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := h.newClient()
	h.writeNextControlResponse(t, lifecycleReplaySessionID, archivewire.ControlResponseCode_OK)

	got, err := archive.StartReplay(
		t.Context(),
		lifecycleRecordingID,
		4096,
		8192,
		lifecycleReplayStreamID,
		[]byte(lifecycleReplayChannel),
	)
	require.NoError(t, err)
	require.Equal(t, lifecycleReplaySessionID, got)
}

func TestStartReplay_timeout(t *testing.T) {
	h := newLifecycleHarness(t)
	targets := h.responseTargets()
	pollFn := func() int {
		h.nowNs.Add(int64(h.cfg.ControlResponseTimeout))
		return h.demuxPoll(targets)
	}

	_, err := h.controlOps(pollFn).startReplay(
		t.Context(),
		lifecycleRecordingID,
		4096,
		8192,
		lifecycleReplayStreamID,
		[]byte(lifecycleReplayChannel),
	)
	require.Error(t, err)
	var timeoutErr *ControlResponseTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.Equal(t, ControlResponseAwaitOperation, timeoutErr.Operation)
}

func TestStopReplay_timeout(t *testing.T) {
	h := newLifecycleHarness(t)
	targets := h.responseTargets()
	pollFn := func() int {
		h.nowNs.Add(int64(h.cfg.ControlResponseTimeout))
		return h.demuxPoll(targets)
	}

	err := h.controlOps(pollFn).stopReplay(t.Context(), lifecycleReplaySessionID)
	require.Error(t, err)
	var timeoutErr *ControlResponseTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.Equal(t, ControlResponseAwaitOperation, timeoutErr.Operation)
}

func TestClientStopReplay_methodOK(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := h.newClient()
	h.writeNextControlResponse(t, 0, archivewire.ControlResponseCode_OK)

	err := archive.StopReplay(t.Context(), lifecycleReplaySessionID)
	require.NoError(t, err)
}

func TestClientStopReplay_errorResponse(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := h.newClient()
	h.writeNextControlResponse(t, 0, archivewire.ControlResponseCode_ERROR)

	err := archive.StopReplay(t.Context(), lifecycleReplaySessionID)
	require.Error(t, err)
}

func TestTruncateRecording_awaitOK(t *testing.T) {
	h := newLifecycleHarness(t)
	written := false
	targets := h.responseTargets()
	const deletedSegmentCount int64 = 3
	pollFn := h.respondControlResponseOnce(
		t,
		deletedSegmentCount,
		archivewire.ControlResponseCode_OK,
		&written,
		targets,
	)

	got, err := h.controlOps(pollFn).truncateRecording(
		t.Context(),
		lifecycleRecordingID,
		4096,
	)
	require.NoError(t, err)
	require.Equal(t, deletedSegmentCount, got)
}

func TestTruncateRecording_errorResponseReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	written := false
	targets := h.responseTargets()
	pollFn := h.respondControlResponseOnce(
		t,
		int64(ErrorInvalidPosition),
		archivewire.ControlResponseCode_ERROR,
		&written,
		targets,
	)

	_, err := h.controlOps(pollFn).truncateRecording(
		t.Context(),
		lifecycleRecordingID,
		150,
	)
	require.Error(t, err)
	code, ok := ExceptionCode(err)
	require.True(t, ok)
	require.Equal(t, int32(ErrorInvalidPosition), code)
}

func TestTruncateRecording_timeout(t *testing.T) {
	h := newLifecycleHarness(t)
	targets := h.responseTargets()
	pollFn := func() int {
		h.nowNs.Add(int64(h.cfg.ControlResponseTimeout))
		return h.demuxPoll(targets)
	}

	_, err := h.controlOps(pollFn).truncateRecording(
		t.Context(),
		lifecycleRecordingID,
		4096,
	)
	require.Error(t, err)
	var timeoutErr *ControlResponseTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
	require.Equal(t, ControlResponseAwaitOperation, timeoutErr.Operation)
}

func TestClientTruncateRecording_methodReturnsDeletedSegmentCount(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := h.newClient()
	const deletedSegmentCount int64 = 2
	h.writeNextControlResponse(t, deletedSegmentCount, archivewire.ControlResponseCode_OK)

	got, err := archive.TruncateRecording(t.Context(), lifecycleRecordingID, 4096)
	require.NoError(t, err)
	require.Equal(t, deletedSegmentCount, got)
}

func TestClientTruncateRecording_errorResponse(t *testing.T) {
	tests := []struct {
		name string
		code int
	}{
		{name: "active recording", code: ErrorActiveRecording},
		{name: "invalid position", code: ErrorInvalidPosition},
		{name: "unknown recording", code: ErrorUnknownRecording},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newLifecycleHarness(t)
			archive := h.newClient()
			h.writeNextControlResponse(t, int64(tc.code), archivewire.ControlResponseCode_ERROR)

			_, err := archive.TruncateRecording(t.Context(), lifecycleRecordingID, 0)
			require.Error(t, err)
			code, ok := ExceptionCode(err)
			require.True(t, ok)
			require.Equal(t, int32(tc.code), code)
		})
	}
}
