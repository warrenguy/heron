//go:build testhooks

package archive

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func TestClientLifecycle_contextDone(t *testing.T) {
	t.Parallel()

	ops := []struct {
		name string
		run  func(client *Client, ctx context.Context) error
	}{
		{
			name: "startRecording",
			run: func(client *Client, ctx context.Context) error {
				_, err := client.StartRecording(
					ctx,
					archiveProxyTestRecordingStreamID,
					archivewire.SourceLocation_LOCAL,
					[]byte(lifecycleRecordingChannel),
				)
				return err
			},
		},
		{
			name: "startRecordingAwaitSignal",
			run: func(client *Client, ctx context.Context) error {
				tracker := NewStartSignalTracker(controlResponseTestControlSessionID)
				_, err := client.StartRecordingAwaitSignal(
					ctx,
					archiveProxyTestRecordingStreamID,
					archivewire.SourceLocation_LOCAL,
					[]byte(lifecycleRecordingChannel),
					tracker,
				)
				return err
			},
		},
		{
			name: "listRecording",
			run: func(client *Client, ctx context.Context) error {
				_, err := client.ListRecording(ctx, lifecycleRecordingID)
				return err
			},
		},
		{
			name: "stopRecordingAwaitSignal",
			run: func(client *Client, ctx context.Context) error {
				tracker := NewStopSignalTracker(controlResponseTestControlSessionID, -1)
				return client.StopRecordingAwaitSignal(
					ctx,
					archiveProxyTestRecordingStreamID,
					[]byte(lifecycleRecordingChannel),
					tracker,
				)
			},
		},
		{
			name: "extendRecordingAwaitSignal",
			run: func(client *Client, ctx context.Context) error {
				tracker := NewExtendSignalTracker(
					controlResponseTestControlSessionID,
					lifecycleRecordingID,
				)
				_, err := client.ExtendRecordingAwaitSignal(
					ctx,
					lifecycleRecordingID,
					archiveProxyTestRecordingStreamID,
					archivewire.SourceLocation_LOCAL,
					true,
					[]byte(lifecycleRecordingChannel),
					tracker,
				)
				return err
			},
		},
	}
	ctxModes := []struct {
		name     string
		buildCtx func(t *testing.T) context.Context
		wantErr  error
	}{
		{
			name: "preCanceled",
			buildCtx: func(t *testing.T) context.Context {
				cancelCtx, cancel := context.WithCancel(t.Context())
				cancel()
				return cancelCtx
			},
			wantErr: context.Canceled,
		},
		{
			name: "preExpired",
			buildCtx: func(t *testing.T) context.Context {
				deadlineCtx, cancel := context.WithDeadline(
					t.Context(),
					time.Now().Add(-time.Second),
				)
				t.Cleanup(cancel)
				return deadlineCtx
			},
			wantErr: context.DeadlineExceeded,
		},
	}

	for _, mode := range ctxModes {
		for _, op := range ops {
			t.Run(op.name+"_"+mode.name, func(t *testing.T) {
				t.Parallel()

				h := newLifecycleHarness(t)
				client := h.newClient()

				err := op.run(client, mode.buildCtx(t))
				require.ErrorIs(t, err, mode.wantErr)
			})
		}
	}
}

type cancelOnNthOfferPublication struct {
	stubControlPublication
	threshold int
	cancel    context.CancelFunc
	calls     int
}

func (p *cancelOnNthOfferPublication) Offer(buffer []byte) int64 {
	p.calls++
	if p.calls >= p.threshold {
		p.cancel()
	}
	return p.stubControlPublication.Offer(buffer)
}

func TestClientStartRecording_contextCanceledMidLoop(t *testing.T) {
	h := newLifecycleHarness(t)
	cancelCtx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	pub := &cancelOnNthOfferPublication{
		stubControlPublication: stubControlPublication{offerResult: aeron.BackPressured},
		threshold:              2,
		cancel:                 cancel,
	}
	client := h.newClient()
	client.proxy = NewProxy(pub, h.cfg)

	_, err := client.StartRecording(
		cancelCtx,
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_LOCAL,
		[]byte(lifecycleRecordingChannel),
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, pub.calls)
}

func TestNewLister_contextCanceled(t *testing.T) {
	h := newLifecycleHarness(t)
	client := h.newClient()

	cancelCtx, cancel := context.WithCancel(t.Context())
	cancel()

	lister := NewLister(cancelCtx, client)
	got, err := lister.ListRecording(lifecycleRecordingID)
	require.Error(t, err)
	require.Equal(t, recordinglog.LogRecordingExtents{}, got)
	require.ErrorIs(t, err, context.Canceled)
}

func TestClientListRecording_contextDeadlineSooner(t *testing.T) {
	h := newLifecycleHarness(t)
	client := h.newClient()
	h.pub.offerResult = aeron.BackPressured

	deadlineCtx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := client.ListRecording(deadlineCtx, lifecycleRecordingID)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, 50*time.Millisecond)
}
