package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// stopArchiveDouble scripts StopRecordingByIdentity and ListRecording.
type stopArchiveDouble struct {
	stopped bool
	stopErr error
	// listStops hands one stop position to each list. The last entry repeats.
	listStops []int64
	listErr   error

	stopIDs []int64
	listIDs []int64
}

func (d *stopArchiveDouble) StopRecordingByIdentity(
	ctx context.Context,
	recordingID int64,
) (bool, error) {
	d.stopIDs = append(d.stopIDs, recordingID)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return d.stopped, d.stopErr
}

func (d *stopArchiveDouble) ListRecording(
	ctx context.Context,
	recordingID int64,
) (codecs.RecordingDescriptorFields, error) {
	n := len(d.listIDs)
	d.listIDs = append(d.listIDs, recordingID)
	if err := ctx.Err(); err != nil {
		return codecs.RecordingDescriptorFields{}, err
	}
	if d.listErr != nil {
		return codecs.RecordingDescriptorFields{}, d.listErr
	}
	stop := d.listStops[min(n, len(d.listStops)-1)]
	return codecs.RecordingDescriptorFields{
		RecordingID:  recordingID,
		StopPosition: stop,
	}, nil
}

type countingIdle struct {
	idleCalls  int
	resetCalls int
	onIdle     func()
}

func (c *countingIdle) Idle(int) {
	c.idleCalls++
	if c.onIdle != nil {
		c.onIdle()
	}
}

func (c *countingIdle) Reset() {
	c.resetCalls++
}

func TestTermRecordingStop_StopTermRecording(t *testing.T) {
	t.Parallel()

	const (
		recordingID = int64(42)
		waitNs      = int64(100)
		clockStepNs = int64(60)
		stop        = int64(4096)
		null        = recordinglog.NullPosition
	)
	stopErr := &archive.Exception{ErrorCode: archive.ErrorGeneric}
	unknown := &archive.Exception{ErrorCode: archive.ErrorUnknownRecording}
	listErr := errors.New("list publication closed")

	cases := []struct {
		name         string
		double       stopArchiveDouble
		cancelOnIdle bool
		cause        error
		errText      string
		lists        int
		idles        int
		resets       int
	}{
		{
			name:   "not active and stop set",
			double: stopArchiveDouble{listStops: []int64{stop}},
			lists:  1,
			resets: 1,
		},
		{
			name:   "not active while an earlier stop closes the session",
			double: stopArchiveDouble{listStops: []int64{null, stop}},
			lists:  2,
			idles:  1,
			resets: 1,
		},
		{
			name:   "stopped and stop set on the second list",
			double: stopArchiveDouble{stopped: true, listStops: []int64{null, stop}},
			lists:  2,
			idles:  1,
			resets: 1,
		},
		{
			name:   "stopped and stop null past the wait",
			double: stopArchiveDouble{stopped: true, listStops: []int64{null}},
			lists:  2,
			idles:  1,
			resets: 1,
		},
		{
			name:   "stop error",
			double: stopArchiveDouble{stopErr: stopErr},
			cause:  stopErr,
		},
		{
			name:   "unknown recording on the stop",
			double: stopArchiveDouble{stopErr: unknown},
			cause:  recordinglog.ErrUnknownRecordingID,
		},
		{
			name:    "list error",
			double:  stopArchiveDouble{stopped: true, listErr: listErr},
			cause:   listErr,
			errText: "read catalog stop",
			lists:   1,
			resets:  1,
		},
		{
			name:         "ctx canceled during the wait",
			double:       stopArchiveDouble{stopped: true, listStops: []int64{null}},
			cancelOnIdle: true,
			cause:        context.Canceled,
			errText:      "read catalog stop",
			lists:        2,
			idles:        1,
			resets:       1,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			double := tt.double
			var nowNs int64
			clock := func() int64 {
				nowNs += clockStepNs
				return nowNs
			}

			s := newTermRecordingStop(ctx, &double, waitNs, clock)
			require.NotNil(t, s.idle)
			idle := &countingIdle{}
			if tt.cancelOnIdle {
				idle.onIdle = cancel
			}
			s.idle = idle

			err := s.StopTermRecording(recordingID)
			if tt.cause != nil {
				require.ErrorIs(t, err, tt.cause)
			} else {
				require.NoError(t, err)
			}
			if tt.errText != "" {
				require.ErrorContains(t, err, tt.errText)
			}
			require.Equal(t, []int64{recordingID}, double.stopIDs)
			require.Len(t, double.listIDs, tt.lists)
			for _, id := range double.listIDs {
				require.Equal(t, recordingID, id)
			}
			require.Equal(t, tt.idles, idle.idleCalls)
			require.Equal(t, tt.resets, idle.resetCalls)
		})
	}
}
