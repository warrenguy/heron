//go:build testhooks

package service

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster/codecs"
)

type transportAdd struct {
	streamID int32
	channel  string
}

// fakeTransport records AddSubscription and AddPublication and returns hook
// handles. A configured error skips handle creation for that add. An
// exclusive publication add and a response publication add never complete.
type fakeTransport struct {
	tb testing.TB

	subCalls []transportAdd
	pubCalls []transportAdd

	sub    *aeron.Subscription
	pub    *aeron.Publication
	subErr error
	pubErr error

	returnNilSub bool
	returnNilPub bool
}

func (f *fakeTransport) AddSubscription(
	streamID int32,
	channel string,
) (*aeron.Subscription, error) {
	f.subCalls = append(f.subCalls, transportAdd{streamID: streamID, channel: channel})
	if f.subErr != nil {
		return nil, f.subErr
	}
	if f.returnNilSub {
		return nil, nil
	}
	if f.sub == nil {
		f.sub = aeron.NewTestSubscriptionForHooks(f.tb)
	}
	return f.sub, nil
}

func (f *fakeTransport) AddPublication(
	streamID int32,
	channel string,
) (*aeron.Publication, error) {
	f.pubCalls = append(f.pubCalls, transportAdd{streamID: streamID, channel: channel})
	if f.pubErr != nil {
		return nil, f.pubErr
	}
	if f.returnNilPub {
		return nil, nil
	}
	if f.pub == nil {
		f.pub = aeron.NewTestPublicationForHooksWithConnected(f.tb, true)
	}
	return f.pub, nil
}

func (*fakeTransport) OfferAddExclusivePublication(int32, string) (int64, error) {
	return 1, nil
}

func (*fakeTransport) PollAddExclusivePublication(
	int64,
) (*aeron.ExclusivePublication, bool, error) {
	return nil, false, nil
}

func (*fakeTransport) OfferAddPublication(int32, string) (int64, error) {
	return 1, nil
}

func (*fakeTransport) PollAddPublication(int64) (*aeron.Publication, bool, error) {
	return nil, false, nil
}

func (*fakeTransport) CancelPending(int64) {}

func newFakeTransport(tb testing.TB) *fakeTransport {
	tb.Helper()
	return &fakeTransport{tb: tb}
}

func openConfig(aeronClient TransportClient) Config {
	return Config{
		Aeron:             aeronClient,
		Service:           &recordingService{},
		CommittedPosition: func() int64 { return 0 },
	}
}

func archiveOpenConfig(t *testing.T, archiveCfg archive.Config) Config {
	t.Helper()
	cfg := openConfig(newFakeTransport(t))
	cfg.Archive = &archiveCfg
	return cfg
}

func recoveryTransportConfig(t *testing.T, recovery RecoveryConfig) Config {
	t.Helper()
	cfg := openConfig(newRecoveryFakeTransport(t))
	cfg.Recovery = &recovery
	return cfg
}

func openContainer(t *testing.T, cfg Config) *Container {
	t.Helper()
	c, err := Open(cfg)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.NotNil(t, c.Agent())
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func openWiredControl(t *testing.T) (*fakeTransport, *Container, *SubscriptionControlIngress) {
	t.Helper()
	client := newFakeTransport(t)
	c := openContainer(t, openConfig(client))
	require.NotNil(t, c.ingress)
	return client, c, c.ingress
}

func openRecordingContainer(t *testing.T) (*fakeTransport, *recordingService, *Container) {
	t.Helper()
	client := newFakeTransport(t)
	svc := &recordingService{}
	cfg := openConfig(client)
	cfg.Service = svc
	return client, svc, openContainer(t, cfg)
}

func TestOpen_refusesInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     func(t *testing.T) Config
		wantErr string
	}{
		{
			name: "nil aeron",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				cfg.Aeron = nil
				return cfg
			},
			wantErr: "service: transport client is required",
		},
		{
			name: "typed nil aeron",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				var client *aeron.Client
				cfg.Aeron = client
				return cfg
			},
			wantErr: "service: transport client is required",
		},
		{
			name: "nil service",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				cfg.Service = nil
				return cfg
			},
			wantErr: "service: clustered service is required",
		},
		{
			name: "typed nil service",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				var svc *recordingService
				cfg.Service = svc
				return cfg
			},
			wantErr: "service: clustered service is required",
		},
		{
			name: "nil committed position",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				cfg.CommittedPosition = nil
				return cfg
			},
			wantErr: "service: committed position is required",
		},
		{
			name: "negative service id",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				cfg.ServiceID = -1
				return cfg
			},
			wantErr: "service: service id must not be negative",
		},
		{
			name: "negative start ack log position",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				cfg.StartAck = &StartAckConfig{LogPosition: -1, RelevantID: 1}
				return cfg
			},
			wantErr: "service: start ack log position must not be negative",
		},
		{
			name: "colliding stream ids after defaulting module stream",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				cfg.ServiceStreamID = DefaultConsensusModuleStreamID
				return cfg
			},
			wantErr: "service: ServiceStreamID and ConsensusModuleStreamID must differ",
		},
		{
			name: "colliding stream ids after defaulting service stream",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				cfg.ConsensusModuleStreamID = DefaultServiceStreamID
				return cfg
			},
			wantErr: "service: ServiceStreamID and ConsensusModuleStreamID must differ",
		},
		{
			name: "colliding stream ids explicit",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				cfg.ServiceStreamID = 10
				cfg.ConsensusModuleStreamID = 10
				return cfg
			},
			wantErr: "service: ServiceStreamID and ConsensusModuleStreamID must differ",
		},
		{
			name: "archive with aeron set",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{Aeron: &aeron.Client{}})
			},
			wantErr: "service: Archive must not set Aeron",
		},
		{
			name: "archive with recording signal handler",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{
					RecordingSignalHandler: &archive.RecordingSignalHandlers{},
				})
			},
			wantErr: "service: Archive must not set RecordingSignalHandler",
		},
		{
			name: "archive with local control channel",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{
					LocalControlChannel: archive.DefaultLocalControlChannel,
				})
			},
			wantErr: "service: Archive must not set LocalControlChannel",
		},
		{
			name: "archive with local control stream",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{
					LocalControlStreamID: archive.DefaultLocalControlStreamID,
				})
			},
			wantErr: "service: Archive must not set LocalControlStreamID",
		},
		{
			name: "archive udp request channel",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{
					ControlRequestChannel: archive.DefaultControlRequestChannel,
				})
			},
			wantErr: "service: archive ControlRequestChannel must be IPC",
		},
		{
			name: "archive udp response channel",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{
					ControlResponseChannel: archive.DefaultControlResponseChannel,
				})
			},
			wantErr: "service: archive ControlResponseChannel must be IPC",
		},
		{
			name: "archive response stream equals request stream",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{
					ControlResponseStreamID: archive.DefaultLocalControlStreamID,
				})
			},
			wantErr: "service: archive ControlResponseStreamID and ControlRequestStreamID must differ",
		},
		{
			name: "archive response stream equals explicit request stream",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{
					ControlRequestStreamID:  300,
					ControlResponseStreamID: 300,
				})
			},
			wantErr: "service: archive ControlResponseStreamID and ControlRequestStreamID must differ",
		},
		{
			name: "archive request stream equals service stream",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{
					ControlRequestStreamID: DefaultServiceStreamID,
				})
			},
			wantErr: "service: archive ControlRequestStreamID and ServiceStreamID must differ",
		},
		{
			name: "archive request stream equals consensus module stream",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{
					ControlRequestStreamID: DefaultConsensusModuleStreamID,
				})
			},
			wantErr: "service: archive ControlRequestStreamID and ConsensusModuleStreamID must differ",
		},
		{
			name: "archive request stream equals snapshot stream",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{
					ControlRequestStreamID: DefaultSnapshotStreamID,
				})
			},
			wantErr: "service: archive ControlRequestStreamID and SnapshotStreamID must differ",
		},
		{
			name: "archive response stream equals service stream",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{
					ControlResponseStreamID: DefaultServiceStreamID,
				})
			},
			wantErr: "service: archive ControlResponseStreamID and ServiceStreamID must differ",
		},
		{
			name: "archive response stream equals consensus module stream",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{
					ControlResponseStreamID: DefaultConsensusModuleStreamID,
				})
			},
			wantErr: "service: archive ControlResponseStreamID and ConsensusModuleStreamID must differ",
		},
		{
			name: "archive response stream equals snapshot stream",
			cfg: func(t *testing.T) Config {
				return archiveOpenConfig(t, archive.Config{
					ControlResponseStreamID: DefaultSnapshotStreamID,
				})
			},
			wantErr: "service: archive ControlResponseStreamID and SnapshotStreamID must differ",
		},
		{
			name: "archive derived response stream equals explicit service stream",
			cfg: func(t *testing.T) Config {
				cfg := archiveOpenConfig(t, archive.Config{})
				cfg.ServiceStreamID = 121
				return cfg
			},
			wantErr: "service: archive ControlResponseStreamID and ServiceStreamID must differ",
		},
		{
			name: "archive zero response stream with max service id",
			cfg: func(t *testing.T) Config {
				cfg := archiveOpenConfig(t, archive.Config{})
				cfg.ServiceID = math.MaxInt32
				return cfg
			},
			wantErr: "service: ServiceID too large to derive archive ControlResponseStreamID",
		},
		{
			name: "archive request stream equals replay stream",
			cfg: func(t *testing.T) Config {
				cfg := recoveryTransportConfig(t, RecoveryConfig{})
				cfg.Archive = &archive.Config{ControlRequestStreamID: DefaultReplayStreamID}
				return cfg
			},
			wantErr: "service: archive ControlRequestStreamID and ReplayStreamID must differ",
		},
		{
			name: "archive response stream equals replay stream",
			cfg: func(t *testing.T) Config {
				cfg := recoveryTransportConfig(t, RecoveryConfig{ReplayStreamID: 300})
				cfg.Archive = &archive.Config{ControlResponseStreamID: 300}
				return cfg
			},
			wantErr: "service: archive ControlResponseStreamID and ReplayStreamID must differ",
		},
		{
			name: "snapshot channel with session id",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				cfg.SnapshotChannel = "aeron:ipc?alias=snapshot|session-id=7"
				return cfg
			},
			wantErr: "service: SnapshotChannel must not set session-id=",
		},
		{
			name: "snapshot stream equals service stream",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				cfg.SnapshotStreamID = DefaultServiceStreamID
				return cfg
			},
			wantErr: "service: SnapshotStreamID and ServiceStreamID must differ",
		},
		{
			name: "snapshot stream equals consensus module stream",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				cfg.SnapshotStreamID = DefaultConsensusModuleStreamID
				return cfg
			},
			wantErr: "service: SnapshotStreamID and ConsensusModuleStreamID must differ",
		},
		{
			name: "explicit service stream equals default snapshot stream",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				cfg.ServiceStreamID = DefaultSnapshotStreamID
				return cfg
			},
			wantErr: "service: SnapshotStreamID and ServiceStreamID must differ",
		},
		{
			name: "start ack with recovery",
			cfg: func(t *testing.T) Config {
				cfg := recoveryTransportConfig(t, RecoveryConfig{})
				cfg.StartAck = &StartAckConfig{RelevantID: 1}
				return cfg
			},
			wantErr: "service: StartAck and Recovery must not both be set",
		},
		{
			name: "negative recovery timeout",
			cfg: func(t *testing.T) Config {
				return recoveryTransportConfig(t, RecoveryConfig{Timeout: -time.Millisecond})
			},
			wantErr: "service: Recovery Timeout must not be negative",
		},
		{
			name: "replay channel with session id",
			cfg: func(t *testing.T) Config {
				return recoveryTransportConfig(t, RecoveryConfig{ReplayChannel: "aeron:ipc?session-id=7"})
			},
			wantErr: "service: Recovery ReplayChannel must not set session-id=",
		},
		{
			name: "replay stream equals service stream",
			cfg: func(t *testing.T) Config {
				return recoveryTransportConfig(t, RecoveryConfig{ReplayStreamID: DefaultServiceStreamID})
			},
			wantErr: "service: Recovery ReplayStreamID and ServiceStreamID must differ",
		},
		{
			name: "replay stream equals consensus module stream",
			cfg: func(t *testing.T) Config {
				return recoveryTransportConfig(t, RecoveryConfig{
					ReplayStreamID: DefaultConsensusModuleStreamID,
				})
			},
			wantErr: "service: Recovery ReplayStreamID and ConsensusModuleStreamID must differ",
		},
		{
			name: "recovery on a transport without the recovery surface",
			cfg: func(t *testing.T) Config {
				cfg := openConfig(newFakeTransport(t))
				cfg.Recovery = &RecoveryConfig{}
				return cfg
			},
			wantErr: errRecoveryTransport.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := tt.cfg(t)
			c, err := Open(cfg)
			require.EqualError(t, err, tt.wantErr)
			require.Nil(t, c)
			client, _ := cfg.Aeron.(*fakeTransport)
			if recovery, ok := cfg.Aeron.(*recoveryFakeTransport); ok {
				require.Zero(t, recovery.pumps, "recovery pumps")
				client = recovery.fakeTransport
			}
			if client != nil {
				require.Empty(t, client.subCalls)
				require.Empty(t, client.pubCalls)
				require.Nil(t, client.sub)
				require.Nil(t, client.pub)
			}
		})
	}
}

func TestOpen_defaultsControlChannelAndStreamIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		channel             string
		serviceID           int32
		moduleID            int32
		snapshotChannel     string
		snapshotStreamID    int32
		wantChannel         string
		wantService         int32
		wantModule          int32
		wantSnapshotChannel string
		wantSnapshotStream  int32
	}{
		{
			name:                "empty channel and zero ids",
			wantChannel:         DefaultControlChannel,
			wantService:         DefaultServiceStreamID,
			wantModule:          DefaultConsensusModuleStreamID,
			wantSnapshotChannel: "aeron:ipc?alias=snapshot",
			wantSnapshotStream:  106,
		},
		{
			name:                "explicit values",
			channel:             "aeron:ipc?alias=ctrl",
			serviceID:           200,
			moduleID:            201,
			snapshotChannel:     "aeron:ipc?alias=snap",
			snapshotStreamID:    202,
			wantChannel:         "aeron:ipc?alias=ctrl",
			wantService:         200,
			wantModule:          201,
			wantSnapshotChannel: "aeron:ipc?alias=snap",
			wantSnapshotStream:  202,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := newFakeTransport(t)
			cfg := openConfig(client)
			cfg.ControlChannel = tt.channel
			cfg.ServiceStreamID = tt.serviceID
			cfg.ConsensusModuleStreamID = tt.moduleID
			cfg.SnapshotChannel = tt.snapshotChannel
			cfg.SnapshotStreamID = tt.snapshotStreamID

			defaulted := defaultConfig(cfg)
			require.Equal(t, tt.wantSnapshotChannel, defaulted.SnapshotChannel)
			require.Equal(t, tt.wantSnapshotStream, defaulted.SnapshotStreamID)

			_ = openContainer(t, cfg)

			require.Equal(t, []transportAdd{
				{streamID: tt.wantService, channel: tt.wantChannel},
			}, client.subCalls)
			require.Equal(t, []transportAdd{
				{streamID: tt.wantModule, channel: tt.wantChannel},
			}, client.pubCalls)
		})
	}
}

func TestOpen_wiresControlIngressAndProxy(t *testing.T) {
	t.Parallel()

	client := newFakeTransport(t)
	cfg := openConfig(client)
	cfg.ServiceID = 3
	c := openContainer(t, cfg)

	agent := c.Agent()
	require.NotNil(t, agent.control)
	require.NotNil(t, agent.moduleProxy)
	require.Same(t, client, agent.subscriptions)
	require.Same(t, client, agent.responsePublications)
	require.Same(t, client.pub, agent.moduleProxy.Publication())
	require.Nil(t, c.ArchiveClient())
	require.Equal(t, snapshotArchiveControl(nil), agent.archive)

	image := aeron.NewTestImageForHooks(t, client.sub, 0)
	require.True(t, aeron.SubscriptionAddImageForHooks(client.sub, image))
	writeUnfragmentedFrame(t, image, 0, encodeRequestServiceAckControl(t, 0))

	work := agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.GreaterOrEqual(t, work, 3, "Start + control fragment + drain")
	require.Equal(t, int64(1), agent.NextAckID())
	require.Equal(t, 0, agent.PendingCoordinationAcks())

	acks := decodeServiceAcksFromPub(t, client.pub)
	require.Equal(t, []codecs.ServiceAckFields{{
		LogPosition: 0,
		Timestamp:   0,
		AckID:       0,
		RelevantID:  -1,
		ServiceID:   3,
	}}, acks)
}

func TestOpen_StartAckPassthrough(t *testing.T) {
	t.Parallel()

	const (
		relevantID int64 = 77
		serviceID  int32 = 3
	)
	tests := []struct {
		name     string
		startPos int64
	}{
		{name: "nonzero position", startPos: 42},
		{name: "position zero", startPos: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := newFakeTransport(t)
			cfg := openConfig(client)
			cfg.ServiceID = serviceID
			cfg.StartAck = &StartAckConfig{
				LogPosition: tt.startPos,
				RelevantID:  relevantID,
			}
			c := openContainer(t, cfg)
			agent := c.Agent()

			require.Equal(t, 1, agent.PendingCoordinationAcks())
			require.Equal(t, 0, agent.ReadyCoordinationAcks())
			require.Equal(t, int64(0), agent.NextAckID())

			work := agent.DoWork(0)
			require.NoError(t, agent.ServiceFailure())
			require.GreaterOrEqual(t, work, 2, "Start + drain")
			require.Equal(t, int64(1), agent.NextAckID())
			require.Equal(t, 0, agent.PendingCoordinationAcks())
			require.Equal(t, []codecs.ServiceAckFields{{
				LogPosition: tt.startPos,
				Timestamp:   0,
				AckID:       0,
				RelevantID:  relevantID,
				ServiceID:   serviceID,
			}}, decodeServiceAcksFromPub(t, client.pub))
		})
	}
}

func TestOpen_addFailure(t *testing.T) {
	t.Parallel()

	addSubErr := errors.New("add subscription failed")
	addPubErr := errors.New("add publication failed")

	tests := []struct {
		name          string
		setup         func(*fakeTransport)
		archive       *archive.Config
		wantErr       error
		wantPubCalls  int
		wantSubClosed bool
		wantPubClosed bool
	}{
		{
			name: "subscription add error",
			setup: func(c *fakeTransport) {
				c.subErr = addSubErr
			},
			wantErr: addSubErr,
		},
		{
			name: "subscription add returns nil-nil",
			setup: func(c *fakeTransport) {
				c.returnNilSub = true
			},
			wantErr: errAdderReturnedNoSubscription,
		},
		{
			name: "publication add returns nil-nil",
			setup: func(c *fakeTransport) {
				c.returnNilPub = true
			},
			wantErr:       errAdderReturnedNoPublication,
			wantPubCalls:  1,
			wantSubClosed: true,
		},
		{
			name: "publication add error closes subscription",
			setup: func(c *fakeTransport) {
				c.pubErr = addPubErr
			},
			wantErr:       addPubErr,
			wantPubCalls:  1,
			wantSubClosed: true,
		},
		{
			name:          "archive on a non-client transport",
			setup:         func(*fakeTransport) {},
			archive:       &archive.Config{},
			wantErr:       errArchiveTransportNotClient,
			wantPubCalls:  1,
			wantSubClosed: true,
			wantPubClosed: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := newFakeTransport(t)
			tt.setup(client)
			cfg := openConfig(client)
			cfg.Archive = tt.archive

			c, err := Open(cfg)
			require.ErrorIs(t, err, tt.wantErr)
			require.Nil(t, c)
			require.Len(t, client.subCalls, 1)
			require.Len(t, client.pubCalls, tt.wantPubCalls)
			if tt.wantSubClosed {
				require.NotNil(t, client.sub)
				require.True(t, client.sub.IsClosed())
			} else {
				require.Nil(t, client.sub)
			}
			if tt.wantPubClosed {
				require.NotNil(t, client.pub)
				require.True(t, client.pub.IsClosed())
			} else {
				require.Nil(t, client.pub)
			}
		})
	}
}

func TestServiceArchiveConfig_defaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		serviceID          int32
		responseStreamID   int32
		wantResponseStream int32
	}{
		{name: "service 0 derives 121", serviceID: 0, wantResponseStream: 121},
		{name: "service 3 derives 124", serviceID: 3, wantResponseStream: 124},
		{
			name:               "set response stream stays",
			serviceID:          3,
			responseStreamID:   300,
			wantResponseStream: 300,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := archiveOpenConfig(t, archive.Config{
				ControlResponseStreamID: tt.responseStreamID,
			})
			cfg.ServiceID = tt.serviceID

			got, err := serviceArchiveConfig(defaultConfig(cfg))
			require.NoError(t, err)
			require.Equal(t, archive.DefaultLocalControlChannel, got.ControlRequestChannel)
			require.Equal(t, archive.DefaultLocalControlChannel, got.ControlResponseChannel)
			require.Equal(t, archive.DefaultLocalControlStreamID, got.ControlRequestStreamID)
			require.Equal(t, tt.wantResponseStream, got.ControlResponseStreamID)
			require.Nil(t, got.Aeron)
			require.Empty(t, got.Normalize().LocalControlChannel)
			require.Empty(t, cfg.Archive.ControlRequestChannel, "caller config stays unchanged")
		})
	}
}

func TestContainer_Close(t *testing.T) {
	t.Parallel()

	t.Run("after start", func(t *testing.T) {
		t.Parallel()
		client, svc, c := openRecordingContainer(t)

		require.Equal(t, 1, c.Agent().DoWork(0))
		require.Equal(t, 1, svc.startCount)
		require.False(t, client.sub.IsClosed())
		require.False(t, client.pub.IsClosed())

		require.NoError(t, c.Close())
		require.True(t, client.sub.IsClosed())
		require.True(t, client.pub.IsClosed())
		require.True(t, c.Agent().closed)
		require.Nil(t, c.controlSub)
		require.Nil(t, c.controlPub)
		require.Equal(t, 0, c.Agent().DoWork(0))
		require.Equal(t, 1, svc.startCount)

		require.NoError(t, c.Close())
		require.Nil(t, c.controlSub)
		require.Nil(t, c.controlPub)
		require.NoError(t, (*Container)(nil).Close())
	})

	t.Run("before start", func(t *testing.T) {
		t.Parallel()
		_, svc, c := openRecordingContainer(t)

		require.NoError(t, c.Close())
		require.True(t, c.Agent().closed)
		require.Equal(t, 0, c.Agent().DoWork(0))
		require.Equal(t, 0, svc.startCount)
	})
}

// seedControlPendingBegin attaches one image with a BEGIN-only frame and polls
// so the open-wired assembler holds a session builder.
func seedControlPendingBegin(
	t *testing.T,
	sub *aeron.Subscription,
	ingress *SubscriptionControlIngress,
) *aeron.Image {
	t.Helper()
	image := aeron.NewTestImageForHooks(t, sub, 0)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	writeControlFragmented(t, image, 0, []aeron.FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: []byte("pending-begin-reassembly-bytes")},
	})
	require.Equal(t, 1, ingress.Poll(1, func([]byte, int) {
		t.Fatal("BEGIN must not reach onFragment")
	}))
	return image
}

func TestOpen_unavailableImageFreesAssembler(t *testing.T) {
	t.Parallel()

	client, _, ingress := openWiredControl(t)

	handler := aeron.SubscriptionUnavailableImageHandlerForHooks(client.sub)
	require.NotNil(t, handler)

	image := seedControlPendingBegin(t, client.sub, ingress)
	handler(image)
	require.False(t, ingress.FreeSessionBuffer(image.SessionID()))
	require.NotPanics(t, func() { handler(nil) })

	// Same session id, new image: END only must not splice onto the old BEGIN.
	endImage := aeron.NewTestImageForSubscriptionHooksWithSessionID(
		t,
		client.sub,
		image.SessionID(),
		image.CorrelationID()+1,
		0,
	)
	require.True(t, aeron.SubscriptionAddImageForHooks(client.sub, endImage))
	writeControlFragmented(t, endImage, 0, []aeron.FragmentedFrameForTest{
		{Flags: logbuffer.EndFlag, Payload: []byte("end-only-must-not-splice")},
	})
	var got []byte
	n := ingress.Poll(10, func(buf []byte, _ int) {
		got = append([]byte(nil), buf...)
	})
	require.Equal(t, 1, n)
	require.Empty(t, got)
}

func TestContainer_Close_clearsAssembler(t *testing.T) {
	t.Parallel()

	client, c, ingress := openWiredControl(t)
	image := seedControlPendingBegin(t, client.sub, ingress)
	require.NoError(t, c.Close())

	require.False(t, ingress.FreeSessionBuffer(image.SessionID()))
	require.True(t, c.Agent().closed)
	require.Nil(t, c.controlSub)
	require.Nil(t, c.controlPub)
}
