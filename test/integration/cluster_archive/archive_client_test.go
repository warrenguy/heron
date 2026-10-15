//go:build integration && archive_live

package cluster_archive_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/test/integration/harness"
)

const archiveClientRecordingChannel = "aeron:ipc"

// TestArchiveClient_startAutoStopReleasesSubscription asserts the archive
// releases the recording subscription after STOP only when autoStop is true.
func TestArchiveClient_startAutoStopReleasesSubscription(t *testing.T) {
	t.Parallel()

	md := harness.Start(t)
	harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: harness.ArchiveClientControl0,
	})
	aeronClient, hadClientError := harness.ConnectIPCClient(t, md)

	cfg := archive.Config{
		ControlRequestChannel:  archive.DefaultLocalControlChannel,
		ControlResponseChannel: archive.DefaultLocalControlChannel,
		Aeron:                  aeronClient,
	}.WithRemoteControl().Normalize()
	archiveClient, err := archive.Connect(t.Context(), cfg)
	require.NoError(t, err)
	harness.CleanupClose(t, "close archive client", archiveClient.Close)

	// Each case owns a stream id. The archive keys recording subscriptions on
	// channel and stream, so a kept subscription would refuse the next case.
	cases := []struct {
		name     string
		autoStop bool
		streamID int32
	}{
		{name: "autoStop_false", autoStop: false, streamID: 4001},
		{name: "autoStop_true", autoStop: true, streamID: 4002},
	}
	channel := []byte(archiveClientRecordingChannel)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Phase: start the first recording. Wait for START.
			pub, err := aeronClient.AddPublication(tc.streamID, archiveClientRecordingChannel)
			require.NoError(t, err)
			harness.CleanupClose(t, "close first record publication", pub.Close)
			startTracker := archive.NewStartSignalTracker(archiveClient.ControlSessionID())
			recordingID, err := archiveClient.StartRecordingAwaitSignal(
				t.Context(),
				tc.streamID,
				archivewire.SourceLocation_LOCAL,
				tc.autoStop,
				channel,
				startTracker,
			)
			require.NoError(t, err)

			// Phase: close the publication and wait for STOP.
			stopped := false
			archiveClient.SetRecordingSignalHandler(archive.RecordingSignalHandlerFunc(
				func(event codecs.RecordingSignalEventFields) {
					if event.Signal == archivewire.RecordingSignal_STOP &&
						event.RecordingID == recordingID {
						stopped = true
					}
				},
			))
			t.Cleanup(func() { archiveClient.SetRecordingSignalHandler(nil) })
			require.NoError(t, pub.Close())
			harness.PollClientUntilTransportClean(
				t,
				aeronClient,
				hadClientError,
				harness.ConnectedPollTimeout,
				"client transport error during recording stop signal poll",
				func() bool {
					_ = archiveClient.Poll()
					return stopped
				},
				func() string {
					return "archive recording STOP signal not received within " +
						harness.ConnectedPollTimeout.String()
				},
			)

			// Phase: start again on the same channel and stream.
			if !tc.autoStop {
				_, err = archiveClient.StartRecordingAwaitSignal(
					t.Context(),
					tc.streamID,
					archivewire.SourceLocation_LOCAL,
					false,
					channel,
					archive.NewStartSignalTracker(archiveClient.ControlSessionID()),
				)
				code, ok := archive.ExceptionCode(err)
				require.True(t, ok, "start error: %v", err)
				require.Equal(t, int32(archive.ErrorActiveSubscription), code)

				require.NoError(t, archiveClient.StopRecording(t.Context(), tc.streamID, channel))
				return
			}

			secondPub, err := aeronClient.AddPublication(tc.streamID, archiveClientRecordingChannel)
			require.NoError(t, err)
			harness.CleanupClose(t, "close second record publication", secondPub.Close)
			secondTracker := archive.NewStartSignalTracker(archiveClient.ControlSessionID())
			secondRecordingID, err := archiveClient.StartRecordingAwaitSignal(
				t.Context(),
				tc.streamID,
				archivewire.SourceLocation_LOCAL,
				true,
				channel,
				secondTracker,
			)
			require.NoError(t, err)
			require.True(t, secondTracker.Started())
			require.NotEqual(t, recordingID, secondRecordingID)
		})
	}
}
