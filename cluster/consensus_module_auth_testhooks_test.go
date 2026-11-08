//go:build testhooks

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
	"github.com/warrenguy/luas/cluster/consensus"
)

type immediateEgressAdder struct {
	pub *aeron.Publication
}

func (a *immediateEgressAdder) OfferAddPublication(int32, string) (int64, error) {
	return 1, nil
}

func (a *immediateEgressAdder) PollAddPublication(int64) (*aeron.Publication, bool, error) {
	return a.pub, true, nil
}

func (a *immediateEgressAdder) CancelPending(int64) {}

type casToggle struct {
	value int64
}

func (c *casToggle) Load() int64 { return c.value }

func (c *casToggle) Store(value int64) { c.value = value }

func (c *casToggle) CompareAndSet(expect, update int64) bool {
	if c.value != expect {
		return false
	}

	c.value = update
	return true
}

func TestConfigAuthoriser_openWireStyle(t *testing.T) {
	t.Parallel()

	t.Run("nil refuses", func(t *testing.T) {
		t.Parallel()
		requireConfigAuthoriser(
			t,
			nil,
			clusterwire.AdminResponseCode_UNAUTHORISED_ACCESS,
			"Execution of the SNAPSHOT request was not authorised",
			consensus.ControlToggleNeutral,
		)
	})

	t.Run("allow all", func(t *testing.T) {
		t.Parallel()
		requireConfigAuthoriser(
			t,
			consensus.AllowAllAuthoriser{},
			clusterwire.AdminResponseCode_OK,
			"",
			consensus.ControlToggleSnapshot,
		)
	})
}

func requireConfigAuthoriser(
	t testing.TB,
	authoriser consensus.Authoriser,
	wantCode clusterwire.AdminResponseCode,
	wantMessage string,
	wantToggle consensus.ControlToggleState,
) {
	t.Helper()

	const (
		nowNs         int64 = 1_000
		correlationID int64 = 42
	)
	agent := openWireStyleAgent(Config{
		MemberID:   0,
		Authoriser: authoriser,
	})
	logPublisher := consensus.NewLogPublisher()
	logPublisher.SetPublication(aeron.NewTestExclusivePublicationForHooks(t))
	agent.SetLogPublisher(logPublisher)
	egress := aeron.NewTestPublicationForHooksWithConnected(t, true)
	agent.SetEgressPublicationAdder(&immediateEgressAdder{pub: egress})
	toggle := &casToggle{value: int64(consensus.ControlToggleNeutral)}
	agent.SetClusterControlToggle(toggle)
	ingress := &consensus.QueueClientIngress{}
	agent.SetClientIngress(ingress)
	ingress.Enqueue(encodeSessionConnectForAuthTest(t, minimalConnectFields()))

	for range 4 {
		if agent.OpenSessionCount() == 1 {
			break
		}

		agent.DoWork(nowNs)
	}

	require.Equal(t, 1, agent.OpenSessionCount())

	ingress.Enqueue(encodeAdminRequestForAuthTest(t, codecs.AdminRequestFields{
		LeadershipTermID: 0,
		ClusterSessionID: 1,
		CorrelationID:    correlationID,
		RequestType:      clusterwire.AdminRequestType_SNAPSHOT,
	}))
	agent.DoWork(nowNs)

	got := findAdminResponse(t, egress)
	require.Equal(t, int64(1), got.ClusterSessionID)
	require.Equal(t, correlationID, got.CorrelationID)
	require.Equal(t, clusterwire.AdminRequestType_SNAPSHOT, got.RequestType)
	require.Equal(t, wantCode, got.ResponseCode)
	require.Empty(t, got.Payload)
	require.Equal(t, wantMessage, string(got.Message))
	require.Equal(t, int64(wantToggle), toggle.Load())
}

func encodeAdminRequestForAuthTest(tb testing.TB, fields codecs.AdminRequestFields) []byte {
	tb.Helper()

	actingVersion := uint16(clusterwire.AdminRequestSbeSchemaVersion)
	wireLen, ok := codecs.ClusterWireMessageLen(
		clusterwire.AdminRequestSbeBlockLength,
		len(fields.Payload),
	)
	require.True(tb, ok)
	buf := make([]byte, wireLen)
	end, ok := codecs.EncodeAdminRequest(buf, 0, actingVersion, fields)
	require.True(tb, ok)
	return buf[:end]
}

func findAdminResponse(tb testing.TB, pub *aeron.Publication) codecs.AdminResponseFields {
	tb.Helper()

	var found []codecs.AdminResponseFields
	var off int
	bridgePub(tb, pub, &off, func(wire []byte) {
		decoded, ok := codecs.TryDecodeByTemplate(wire, 0, codecs.ClusterDecodeMaxVarLen)
		if !ok || decoded.TemplateID != clusterwire.AdminResponseSbeTemplateID {
			return
		}

		fields, typeOK := decoded.Fields.(codecs.AdminResponseFields)
		require.True(tb, typeOK)
		found = append(found, fields)
	})

	require.Len(tb, found, 1)
	return found[0]
}
