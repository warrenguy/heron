//go:build testhooks

package consensus

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func encodeAdminRequestWire(tb testing.TB, fields codecs.AdminRequestFields) []byte {
	tb.Helper()

	actingVersion := uint16(clusterwire.AdminRequestSbeSchemaVersion)
	return mustConsensus(tb, func(buf []byte) (int, bool) {
		return codecs.EncodeAdminRequest(buf, 0, actingVersion, fields)
	})
}

func TestHandleAdminRequest(t *testing.T) {
	t.Parallel()

	neutral := int64(ControlToggleNeutral)
	replyCases := []struct {
		name        string
		allow       bool
		toggle      *fakeToggle
		requestType clusterwire.AdminRequestType
		wantCode    clusterwire.AdminResponseCode
		wantMessage string
		wantToggle  int64
	}{
		{
			name:        "nil authoriser keeps neutral",
			toggle:      newFakeToggle(neutral),
			requestType: clusterwire.AdminRequestType_SNAPSHOT,
			wantCode:    clusterwire.AdminResponseCode_UNAUTHORISED_ACCESS,
			wantMessage: "Execution of the SNAPSHOT request was not authorised",
			wantToggle:  neutral,
		},
		{
			name:        "allowed snapshot",
			allow:       true,
			toggle:      newFakeToggle(neutral),
			requestType: clusterwire.AdminRequestType_SNAPSHOT,
			wantCode:    clusterwire.AdminResponseCode_OK,
			wantToggle:  int64(ControlToggleSnapshot),
		},
		{
			name:        "toggle not neutral",
			allow:       true,
			toggle:      newFakeToggle(int64(ControlToggleSuspend)),
			requestType: clusterwire.AdminRequestType_SNAPSHOT,
			wantCode:    clusterwire.AdminResponseCode_ERROR,
			wantMessage: "Failed to switch ClusterControl to the ToggleState.SNAPSHOT state",
			wantToggle:  int64(ControlToggleSuspend),
		},
		{
			name:        "nil toggle",
			allow:       true,
			requestType: clusterwire.AdminRequestType_SNAPSHOT,
			wantCode:    clusterwire.AdminResponseCode_ERROR,
			wantMessage: "Failed to switch ClusterControl to the ToggleState.SNAPSHOT state",
		},
		{
			name:        "null type allowed",
			allow:       true,
			toggle:      newFakeToggle(neutral),
			requestType: clusterwire.AdminRequestType_NULL_VALUE,
			wantCode:    clusterwire.AdminResponseCode_ERROR,
			wantMessage: "Unknown request type: NULL_VAL",
			wantToggle:  neutral,
		},
		{
			name:        "null type nil authoriser",
			toggle:      newFakeToggle(neutral),
			requestType: clusterwire.AdminRequestType_NULL_VALUE,
			wantCode:    clusterwire.AdminResponseCode_UNAUTHORISED_ACCESS,
			wantMessage: "Execution of the NULL_VAL request was not authorised",
			wantToggle:  neutral,
		},
	}

	for _, tc := range replyCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opened := openAdminSession(t, nil)

			if tc.allow {
				opened.agent.authoriser = AllowAllAuthoriser{}
			}

			if tc.toggle != nil {
				opened.agent.SetClusterControlToggle(tc.toggle)
			}

			logBefore := opened.logPub.Position()
			require.Positive(t, logBefore)
			pollAdminRequest(t, opened, adminRequestFields(opened.session.id, tc.requestType))

			got, _ := decodeClusterFieldsAtTermOffset[codecs.AdminResponseFields](
				t,
				opened.egress,
				opened.replyOff,
				clusterwire.AdminResponseSbeTemplateID,
			)
			require.Equal(t, opened.session.id, got.ClusterSessionID)
			require.Equal(t, adminTestCorrelationID, got.CorrelationID)
			require.Equal(t, tc.requestType, got.RequestType)
			require.Equal(t, tc.wantCode, got.ResponseCode)
			require.Equal(t, tc.wantMessage, string(got.Message))
			require.Empty(t, got.Payload)
			require.Equal(t, logBefore, opened.logPub.Position())
			require.Equal(t, ModuleStateActive, opened.agent.ModuleState())

			if tc.toggle != nil {
				require.Equal(t, tc.wantToggle, tc.toggle.Load())
			}
		})
	}

	t.Run("unknown type code", func(t *testing.T) {
		t.Parallel()

		opened := openAdminSession(t, nil)
		var reported []error
		opened.agent.errorHandler = func(err error) {
			reported = append(reported, err)
		}
		authoriser := &capturingAuthoriser{}
		opened.agent.authoriser = authoriser
		toggle := newFakeToggle(neutral)
		opened.agent.SetClusterControlToggle(toggle)
		before := opened.egress.Position()
		pollAdminRequest(
			t,
			opened,
			adminRequestFields(opened.session.id, clusterwire.AdminRequestType(7)),
		)

		require.Equal(t, before, opened.egress.Position())
		require.Equal(t, neutral, toggle.Load())
		require.Empty(t, authoriser.calls)
		require.Len(t, reported, 1)
		require.ErrorIs(t, reported[0], ErrUnknownAdminRequestType)
		require.EqualError(t, reported[0], "consensus: unknown admin request type: 7")
	})

	dropCases := []struct {
		name    string
		prepare func(agent *Agent, fields *codecs.AdminRequestFields)
	}{
		{
			name: "wrong term",
			prepare: func(_ *Agent, fields *codecs.AdminRequestFields) {
				fields.LeadershipTermID++
			},
		},
		{
			name: "follower role",
			prepare: func(agent *Agent, _ *codecs.AdminRequestFields) {
				agent.RoleSet(RoleFollower)
			},
		},
		{
			name: "unknown session",
			prepare: func(_ *Agent, fields *codecs.AdminRequestFields) {
				fields.ClusterSessionID = 99
			},
		},
	}

	for _, tc := range dropCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opened := openAdminSession(t, nil)
			opened.agent.authoriser = AllowAllAuthoriser{}
			toggle := newFakeToggle(neutral)
			opened.agent.SetClusterControlToggle(toggle)
			before := opened.egress.Position()
			fields := adminRequestFields(opened.session.id, clusterwire.AdminRequestType_SNAPSHOT)
			tc.prepare(opened.agent, &fields)
			pollAdminRequest(t, opened, fields)

			require.Equal(t, before, opened.egress.Position())
			require.Equal(t, neutral, toggle.Load())
		})
	}

	t.Run("pending session", func(t *testing.T) {
		t.Parallel()

		a, _, _, egress := leaderWithAuthEgress(
			t,
			acceptAllAuthenticator{},
			adminTestTermID,
			adminTestMemberID,
		)
		a.authoriser = AllowAllAuthoriser{}
		toggle := newFakeToggle(neutral)
		a.SetClusterControlToggle(toggle)
		a.acceptSessionConnect(validConnectFields(11), adminTestNowNs)
		session, ok := a.sessions.sessionByID(1)
		require.True(t, ok)
		require.Equal(t, sessionPending, session.state)
		ingress := attachQueueClientIngress(a)
		before := egress.Position()
		ingress.Enqueue(encodeAdminRequestWire(
			t,
			adminRequestFields(session.id, clusterwire.AdminRequestType_SNAPSHOT),
		))
		require.Equal(t, 1, a.pollClientIngress(adminTestNowNs))
		require.Zero(t, ingress.Len())
		require.Equal(t, before, egress.Position())
		require.Equal(t, neutral, toggle.Load())
		require.Equal(t, sessionPending, session.state)
	})

	staleCases := []struct {
		name       string
		allow      bool
		wantCode   clusterwire.AdminResponseCode
		wantOpen   int
		wantToggle int64
	}{
		{
			name:       "accepted snapshot keeps stale session",
			allow:      true,
			wantCode:   clusterwire.AdminResponseCode_OK,
			wantOpen:   1,
			wantToggle: int64(ControlToggleSnapshot),
		},
		{
			name:       "refused snapshot closes stale session",
			wantCode:   clusterwire.AdminResponseCode_UNAUTHORISED_ACCESS,
			wantToggle: neutral,
		},
	}

	for _, tc := range staleCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opened := openAdminSession(t, nil)

			if tc.allow {
				opened.agent.authoriser = AllowAllAuthoriser{}
			}

			toggle := newFakeToggle(neutral)
			opened.agent.SetClusterControlToggle(toggle)
			staleNs := opened.session.lastActivityNs + opened.agent.sessionTimeoutNs + 1
			opened.ingress.Enqueue(encodeAdminRequestWire(
				t,
				adminRequestFields(opened.session.id, clusterwire.AdminRequestType_SNAPSHOT),
			))
			opened.agent.DoWork(staleNs)

			got, _ := decodeClusterFieldsAtTermOffset[codecs.AdminResponseFields](
				t,
				opened.egress,
				opened.replyOff,
				clusterwire.AdminResponseSbeTemplateID,
			)
			require.Equal(t, tc.wantCode, got.ResponseCode)
			require.Equal(t, tc.wantOpen, opened.agent.OpenSessionCount())
			require.Equal(t, tc.wantToggle, toggle.Load())
		})
	}

	t.Run("capturing authoriser", func(t *testing.T) {
		t.Parallel()

		auth := newScriptedAuthenticator()
		auth.enqueue(1, authAction{
			kind:      authActionAuthenticate,
			principal: []byte("prin"),
		})
		opened := openAdminSession(t, auth)
		require.Equal(t, []byte("prin"), opened.session.encodedPrincipal)
		authoriser := &capturingAuthoriser{}
		opened.agent.authoriser = authoriser
		toggle := newFakeToggle(neutral)
		opened.agent.SetClusterControlToggle(toggle)
		pollAdminRequest(
			t,
			opened,
			adminRequestFields(opened.session.id, clusterwire.AdminRequestType_SNAPSHOT),
		)

		require.Equal(t, int32(111), int32(clusterwire.AdminRequestSbeSchemaID))
		require.Equal(t, int32(26), int32(clusterwire.AdminRequestSbeTemplateID))
		require.Equal(t, []authoriserCall{{
			protocolID: int32(clusterwire.AdminRequestSbeSchemaID),
			actionID:   int32(clusterwire.AdminRequestSbeTemplateID),
			actionType: int32(clusterwire.AdminRequestType_SNAPSHOT),
			principal:  []byte("prin"),
		}}, authoriser.calls)
		require.Equal(t, int64(ControlToggleSnapshot), toggle.Load())
	})
}

const (
	adminTestNowNs         int64 = 1_000
	adminTestTermID        int64 = 4
	adminTestMemberID      int32 = 1
	adminTestCorrelationID int64 = 77
)

type openedAdminSession struct {
	agent    *Agent
	logPub   *aeron.ExclusivePublication
	egress   *aeron.Publication
	ingress  *QueueClientIngress
	session  *clientSession
	replyOff int
}

func openAdminSession(t testing.TB, auth Authenticator) openedAdminSession {
	t.Helper()

	if auth == nil {
		auth = acceptAllAuthenticator{}
	}

	a, logPub, _, egress := leaderWithAuthEgress(
		t,
		auth,
		adminTestTermID,
		adminTestMemberID,
	)
	a.acceptSessionConnect(validConnectFields(11), adminTestNowNs)
	require.Positive(t, a.sessionWork(adminTestNowNs))
	session, ok := a.sessions.sessionByID(1)
	require.True(t, ok)
	require.Equal(t, sessionOpen, session.state)
	_, next := decodeSessionEventAtTermOffset(t, egress, 0)
	return openedAdminSession{
		agent:    a,
		logPub:   logPub,
		egress:   egress,
		ingress:  attachQueueClientIngress(a),
		session:  session,
		replyOff: next,
	}
}

func adminRequestFields(
	sessionID int64,
	requestType clusterwire.AdminRequestType,
) codecs.AdminRequestFields {
	return codecs.AdminRequestFields{
		LeadershipTermID: adminTestTermID,
		ClusterSessionID: sessionID,
		CorrelationID:    adminTestCorrelationID,
		RequestType:      requestType,
	}
}

func pollAdminRequest(
	t testing.TB,
	opened openedAdminSession,
	fields codecs.AdminRequestFields,
) {
	t.Helper()

	opened.ingress.Enqueue(encodeAdminRequestWire(t, fields))
	require.Equal(t, 1, opened.agent.pollClientIngress(adminTestNowNs))
	require.Zero(t, opened.ingress.Len())
}

type authoriserCall struct {
	protocolID int32
	actionID   int32
	actionType int32
	principal  []byte
}

type capturingAuthoriser struct {
	calls []authoriserCall
}

func (c *capturingAuthoriser) IsAuthorised(
	protocolID, actionID, actionType int32,
	encodedPrincipal []byte,
) bool {
	c.calls = append(c.calls, authoriserCall{
		protocolID: protocolID,
		actionID:   actionID,
		actionType: actionType,
		principal:  bytes.Clone(encodedPrincipal),
	})
	return true
}
