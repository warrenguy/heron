//go:build testhooks

package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

const (
	lifecycleReplaySessionID int64 = 8800
	lifecycleReplayStreamID  int32 = 1201
	lifecycleReplayChannel         = "aeron:ipc?term-length=64k"
)

func (h *lifecycleHarness) newAeronArchive() *AeronArchive {
	return &AeronArchive{
		controlSessionID:    controlResponseTestControlSessionID,
		archiveID:           lifecycleArchiveID,
		proxy:               h.proxy,
		controlPoller:       h.poller,
		controlSubscription: h.sub,
		controlAssembler:    h.assembler,
		aeronClient:         h.client,
		ctx:                 h.ctx,
	}
}

// writeNextControlResponse pre-writes a ControlResponse for the correlation id the
// next AeronArchive method call allocates. The ring correlation counter is sequential
// and tests are single-threaded, so the id consumed here is one below that id.
func (h *lifecycleHarness) writeNextControlResponse(
	t *testing.T,
	relevantID int64,
	code gensbe.ControlResponseCode,
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
		gensbe.ControlResponseCode_OK,
		&written,
		targets,
	)

	got, err := h.controlOps(pollFn).startReplay(
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
		gensbe.ControlResponseCode_ERROR,
		&written,
		targets,
	)

	_, err := h.controlOps(pollFn).startReplay(
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
		gensbe.ControlResponseCode_OK,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).stopReplay(lifecycleReplaySessionID)
	require.NoError(t, err)
}

func TestStopReplay_errorResponseReturnsError(t *testing.T) {
	h := newLifecycleHarness(t)
	written := false
	targets := h.responseTargets()
	pollFn := h.respondControlResponseOnce(
		t,
		0,
		gensbe.ControlResponseCode_ERROR,
		&written,
		targets,
	)

	err := h.controlOps(pollFn).stopReplay(lifecycleReplaySessionID)
	require.Error(t, err)
}

func TestAeronArchiveStartReplay_methodReturnsReplaySessionId(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := h.newAeronArchive()
	h.writeNextControlResponse(t, lifecycleReplaySessionID, gensbe.ControlResponseCode_OK)

	got, err := archive.StartReplay(
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
		h.nowNs.Add(int64(h.ctx.ControlResponseTimeout))
		return h.demuxPoll(targets)
	}

	_, err := h.controlOps(pollFn).startReplay(
		lifecycleRecordingID,
		4096,
		8192,
		lifecycleReplayStreamID,
		[]byte(lifecycleReplayChannel),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout")
}

func TestStopReplay_timeout(t *testing.T) {
	h := newLifecycleHarness(t)
	targets := h.responseTargets()
	pollFn := func() int {
		h.nowNs.Add(int64(h.ctx.ControlResponseTimeout))
		return h.demuxPoll(targets)
	}

	err := h.controlOps(pollFn).stopReplay(lifecycleReplaySessionID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout")
}

func TestAeronArchiveStopReplay_methodOK(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := h.newAeronArchive()
	h.writeNextControlResponse(t, 0, gensbe.ControlResponseCode_OK)

	err := archive.StopReplay(lifecycleReplaySessionID)
	require.NoError(t, err)
}

func TestAeronArchiveStopReplay_errorResponse(t *testing.T) {
	h := newLifecycleHarness(t)
	archive := h.newAeronArchive()
	h.writeNextControlResponse(t, 0, gensbe.ControlResponseCode_ERROR)

	err := archive.StopReplay(lifecycleReplaySessionID)
	require.Error(t, err)
}
