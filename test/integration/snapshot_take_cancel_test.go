//go:build integration

package integration_test

import (
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/cluster/service"
	"github.com/warrenguy/heron/test/integration/harness"
)

// snapshotAddRecorder records the id of each add the embedded client starts.
// While pollsSkipped is set, a poll returns not done without a conductor tick.
type snapshotAddRecorder struct {
	*aeron.Client
	pollsSkipped bool
	adds         []int64
}

func (r *snapshotAddRecorder) OfferAddExclusivePublication(
	streamID int32,
	channel string,
) (int64, error) {
	id, err := r.Client.OfferAddExclusivePublication(streamID, channel)
	if err == nil {
		r.adds = append(r.adds, id)
	}
	return id, err
}

func (r *snapshotAddRecorder) PollAddExclusivePublication(
	correlationID int64,
) (*aeron.ExclusivePublication, bool, error) {
	if r.pollsSkipped {
		return nil, false, nil
	}
	return r.Client.PollAddExclusivePublication(correlationID)
}

// moduleSnapshotAdder adapts snapshotAddRecorder to consensus.SnapshotPublicationAdder.
type moduleSnapshotAdder struct {
	*snapshotAddRecorder
}

func (a moduleSnapshotAdder) PollAddExclusivePublication(
	correlationID int64,
) (consensus.SnapshotPublication, bool, error) {
	publication, done, err := a.snapshotAddRecorder.PollAddExclusivePublication(correlationID)
	if publication == nil {
		return nil, done, err
	}
	return publication, done, err
}

// idleSnapshotControl fails the test on a start request and never reaches
// an archive.
type idleSnapshotControl struct {
	t *testing.T
}

func (c *idleSnapshotControl) NewStartRecording(int32, string) consensus.SnapshotStartRequest {
	c.t.Errorf("unexpected start recording")
	return nil
}

func (*idleSnapshotControl) ControlTimeoutsNs() (offerRetryTimeoutNs, responseTimeoutNs int64) {
	return publicationReleaseTimeout.Nanoseconds(), publicationReleaseTimeout.Nanoseconds()
}

func (*idleSnapshotControl) WatchStop(int64) {}

func (*idleSnapshotControl) StopPosition() (stopPosition int64, stopped bool) {
	return 0, false
}

func (*idleSnapshotControl) ClearStop() {}

// encodeIPCJoinLog encodes one startup JoinLog for a follower whose log is
// IPC on logStreamID.
func encodeIPCJoinLog(t *testing.T, logSessionID, logStreamID int32) []byte {
	t.Helper()
	fields := codecs.JoinLogFields{
		LogPosition:    0,
		MaxLogPosition: math.MaxInt64,
		MemberID:       0,
		LogSessionID:   logSessionID,
		LogStreamID:    logStreamID,
		IsStartup:      clusterwire.BooleanType_TRUE,
		Role:           codecs.RoleFollower.Code(),
		IsStandby:      clusterwire.BooleanType_FALSE,
		LogChannel:     []byte(harness.IPCChannel),
	}
	actingVersion := uint16(clusterwire.JoinLogSbeSchemaVersion)
	wireLen, ok := codecs.ClusterWireMessageLen(
		codecs.JoinLogBlockLength(actingVersion),
		len(fields.LogChannel),
	)
	require.True(t, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeJoinLog(buf, 0, actingVersion, fields)
	require.True(t, ok)
	return buf[:end]
}

// snapshotTakeCancelCase names the log and snapshot streams of one take.
type snapshotTakeCancelCase struct {
	name             string
	logStreamID      int32
	snapshotStreamID int32
	// extraTicks counts client.DoWork calls after the ready reply. One
	// tick moves the ready mark off the add.
	extraTicks int
}

func snapshotTakeCancelCases(
	markedLogStreamID, markedStreamID int32,
	movedLogStreamID, movedStreamID int32,
) []snapshotTakeCancelCase {
	return []snapshotTakeCancelCase{
		{
			name:             "markNamesAdd",
			logStreamID:      markedLogStreamID,
			snapshotStreamID: markedStreamID,
		},
		{
			name:             "markMoved",
			logStreamID:      movedLogStreamID,
			snapshotStreamID: movedStreamID,
			extraTicks:       1,
		},
	}
}

// requireSnapshotAddReleased ends the take with polls enabled after the client
// reads the ready reply of the one recorded add.
func requireSnapshotAddReleased(
	t *testing.T,
	hadClientError *atomic.Bool,
	recorder *snapshotAddRecorder,
	extraTicks int,
	end func(),
) {
	t.Helper()
	require.Len(t, recorder.adds, 1)
	client := recorder.Client
	id := recorder.adds[0]
	requireReadyPublicationReleased(
		t,
		client,
		hadClientError,
		id,
		func() bool {
			_, ok := client.ExclusivePublication(id)
			return ok
		},
		extraTicks,
		func() {
			recorder.pollsSkipped = false
			end()
		},
	)
}

// TestConsensusSnapshotTake_closeAfterReadyReleasesPublication asserts the driver
// frees a module snapshot publication closed after its ready reply.
func TestConsensusSnapshotTake_closeAfterReadyReleasesPublication(t *testing.T) {
	t.Parallel()
	tests := snapshotTakeCancelCases(
		harness.ConsensusSnapshotCancelMarkedLogStreamID,
		harness.ConsensusSnapshotCancelMarkedStreamID,
		harness.ConsensusSnapshotCancelMovedLogStreamID,
		harness.ConsensusSnapshotCancelMovedStreamID,
	)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			md, client, hadClientError, logPub, _ := connectExclusiveIPC(t, tc.logStreamID)

			commit := &atomic.Int64{}
			agent := consensus.NewAgent(consensus.AgentConfig{
				Role:   consensus.RoleLeader,
				Commit: commit,
			})
			agent.SetLogPublisher(newLogPublisher(logPub))
			recorder := &snapshotAddRecorder{Client: client, pollsSkipped: true}
			agent.SetSnapshotPublicationAdder(moduleSnapshotAdder{recorder})
			agent.SetSnapshotStream(harness.IPCChannel, tc.snapshotStreamID)
			control := &idleSnapshotControl{t: t}
			agent.SetSnapshotRecordingControl(control)
			recordingLog, err := recordinglog.Open(t.TempDir(), true)
			require.NoError(t, err)
			harness.CleanupClose(t, "recording log", recordingLog.Close)
			agent.SetSnapshotRecordingLog(recordingLog, recordinglog.FileSyncData)

			// Phase 1: RequestSnapshot appends to the subscribed log. One DoWork
			// pops the snapshot wait and starts the add.
			nowNs := time.Now().UnixNano()
			require.NoError(t, agent.RequestSnapshot(nowNs))
			commit.Store(agent.ExpectedAckPosition())
			agent.DoWork(nowNs)
			require.True(t, agent.HasPendingSnapshotAction())

			// Phase 2: close the take after the ready reply.
			requireSnapshotAddReleased(t, hadClientError, recorder, tc.extraTicks, agent.CloseSnapshotTake)
			require.False(t, agent.HasPendingSnapshotAction())
			_, ok := agent.LastSnapshotTake()
			require.False(t, ok)
			finishPubSubIPCTest(t, md, hadClientError)
		})
	}
}

// TestServiceSnapshotTake_closeAfterReadyReleasesPublication asserts the driver
// frees a service snapshot publication closed after its ready reply.
func TestServiceSnapshotTake_closeAfterReadyReleasesPublication(t *testing.T) {
	t.Parallel()
	tests := snapshotTakeCancelCases(
		harness.ServiceSnapshotCancelMarkedLogStreamID,
		harness.ServiceSnapshotCancelMarkedStreamID,
		harness.ServiceSnapshotCancelMovedLogStreamID,
		harness.ServiceSnapshotCancelMovedStreamID,
	)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			md := harness.SharedParallel(t)
			client, hadClientError := harness.ConnectIPCClient(t, md)
			logPub, err := client.AddExclusivePublication(tc.logStreamID, harness.IPCChannel)
			require.NoError(t, err)
			harness.CleanupClose(t, "log publication", logPub.Close)

			control := &service.QueueControlIngress{}
			control.Enqueue(encodeIPCJoinLog(t, logPub.SessionID(), tc.logStreamID))
			recorder := &snapshotAddRecorder{Client: client, pollsSkipped: true}
			// The take ends before its first archive request. A zero
			// archive.Client is enough.
			agent := service.NewAgent(service.AgentConfig{
				Control:              control,
				Subscriptions:        client,
				CommittedPosition:    logPub.Position,
				Archive:              &archive.Client{},
				SnapshotPublications: recorder,
				SnapshotChannel:      harness.IPCChannel,
				SnapshotStreamID:     tc.snapshotStreamID,
			})

			// Phase 1: the agent binds the log. A SNAPSHOT action then starts
			// the add.
			harness.PollClientUntilTransportClean(
				t,
				client,
				hadClientError,
				publicationReleaseTimeout,
				"client error before log bind",
				func() bool {
					agent.DoWork(time.Now().UnixNano())
					return agent.IsLogBound() && logPub.IsConnected()
				},
				func() string {
					return fmt.Sprintf(
						"log not bound or publication not connected: bound %t, connected %t, failure %v",
						agent.IsLogBound(),
						logPub.IsConnected(),
						agent.ServiceFailure(),
					)
				},
			)
			logPublisher := newLogPublisher(logPub)
			nowNs := time.Now().UnixNano()
			require.True(t, logPublisher.AppendNewLeadershipTermEvent(
				0,
				nowNs,
				0,
				0,
				logPub.SessionID(),
				clusterwire.ClusterTimeUnit_NANOS,
				0,
			))
			_, ok := logPublisher.AppendClusterAction(0, nowNs, clusterwire.ClusterAction_SNAPSHOT, 0)
			require.True(t, ok)
			harness.PollClientUntilTransportClean(
				t,
				client,
				hadClientError,
				publicationReleaseTimeout,
				"client error before snapshot add",
				func() bool {
					agent.DoWork(time.Now().UnixNano())
					return len(recorder.adds) == 1
				},
				func() string {
					return fmt.Sprintf("snapshot add not started: %v", agent.ServiceFailure())
				},
			)
			require.NoError(t, agent.ServiceFailure())

			// Phase 2: close the agent after the ready reply.
			requireSnapshotAddReleased(t, hadClientError, recorder, tc.extraTicks, agent.Close)
			require.NoError(t, agent.ServiceFailure())
			finishPubSubIPCTest(t, md, hadClientError)
		})
	}
}
