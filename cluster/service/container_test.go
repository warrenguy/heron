//go:build testhooks

package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
)

type transportAdd struct {
	streamID int32
	channel  string
}

// fakeTransport records AddSubscription and AddPublication and returns hook
// handles. A configured error skips handle creation for that add.
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := tt.cfg(t)
			c, err := Open(cfg)
			require.EqualError(t, err, tt.wantErr)
			require.Nil(t, c)
			if client, ok := cfg.Aeron.(*fakeTransport); ok {
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
		name        string
		channel     string
		serviceID   int32
		moduleID    int32
		wantChannel string
		wantService int32
		wantModule  int32
	}{
		{
			name:        "empty channel and zero ids",
			wantChannel: DefaultControlChannel,
			wantService: DefaultServiceStreamID,
			wantModule:  DefaultConsensusModuleStreamID,
		},
		{
			name:        "explicit values",
			channel:     "aeron:ipc?alias=ctrl",
			serviceID:   200,
			moduleID:    201,
			wantChannel: "aeron:ipc?alias=ctrl",
			wantService: 200,
			wantModule:  201,
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
	require.Same(t, client.pub, agent.moduleProxy.Publication())

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
		wantErr       error
		wantPubCalls  int
		wantSubClosed bool
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := newFakeTransport(t)
			tt.setup(client)

			c, err := Open(openConfig(client))
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
			require.Nil(t, client.pub)
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
