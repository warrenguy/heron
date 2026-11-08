package consensus

import (
	"errors"
	"fmt"

	"luas.fast/cluster/codecs"
	"luas.fast/cluster/codecs/clusterwire"
)

// ErrUnknownAdminRequestType reports an admin request with an unknown type
// code. The agent drops the frame without a reply. The error is not fatal.
var ErrUnknownAdminRequestType = errors.New("consensus: unknown admin request type")

const adminResponseActingVersion = uint16(clusterwire.AdminResponseSbeSchemaVersion)

// Authoriser decides whether a session principal may run a privileged
// request.
//
// IsAuthorised receives:
//   - protocolID: the SBE schema id of the request.
//   - actionID: the SBE template id of the request.
//   - actionType: the request type code. The null type arrives as
//     [clusterwire.AdminRequestType_NULL_VALUE].
//   - encodedPrincipal: the principal from the [Authenticator]. A session
//     learned from the log or a snapshot has an empty principal.
//
// Concurrency: methods run inside [Agent.DoWork]. An implementation must
// not block. An implementation must not call [Agent.DoWork] or another
// method that polls, sends, or changes agent state.
//
// Lifetime: encodedPrincipal is valid only during the call.
type Authoriser interface {
	IsAuthorised(protocolID, actionID, actionType int32, encodedPrincipal []byte) bool
}

type AllowAllAuthoriser struct{}

func (AllowAllAuthoriser) IsAuthorised(int32, int32, int32, []byte) bool { return true }

type denyAllAuthoriser struct{}

func (denyAllAuthoriser) IsAuthorised(int32, int32, int32, []byte) bool { return false }

var (
	_ Authoriser = AllowAllAuthoriser{}
	_ Authoriser = denyAllAuthoriser{}
)

// handleAdminRequest reports an unknown type code with
// [ErrUnknownAdminRequestType] before the session checks. An OK reply
// refreshes session activity. The idle sweep can run before a slow tick
// consumes the toggle.
func (a *Agent) handleAdminRequest(fields codecs.AdminRequestFields, nowNs int64) {
	requestType, err := clusterwire.LookupAdminRequestType(int32(fields.RequestType))
	if err != nil {
		a.reportError(fmt.Errorf("%w: %d", ErrUnknownAdminRequestType, int32(fields.RequestType)))
		return
	}

	session, ok := a.openLeaderSession(fields.ClusterSessionID, fields.LeadershipTermID)
	if !ok {
		return
	}

	code, message := a.runAdminRequest(requestType, session.encodedPrincipal)
	if code == clusterwire.AdminResponseCode_OK {
		session.lastActivityNs = nowNs
	}

	a.sendAdminResponse(session, fields, code, message)
}

// runAdminRequest sets [ControlToggleSnapshot] for an authorised snapshot
// request.
func (a *Agent) runAdminRequest(
	requestType clusterwire.AdminRequestType,
	encodedPrincipal []byte,
) (
	clusterwire.AdminResponseCode,
	string,
) {
	typeName := adminRequestTypeName(requestType)

	authorised := a.authoriser.IsAuthorised(
		int32(clusterwire.AdminRequestSbeSchemaID),
		int32(clusterwire.AdminRequestSbeTemplateID),
		int32(requestType),
		encodedPrincipal,
	)
	if !authorised {
		return clusterwire.AdminResponseCode_UNAUTHORISED_ACCESS,
			"Execution of the " + typeName + " request was not authorised"
	}

	if requestType != clusterwire.AdminRequestType_SNAPSHOT {
		return clusterwire.AdminResponseCode_ERROR, "Unknown request type: " + typeName
	}

	if !ControlToggleSnapshot.Toggle(a.controlToggle) {
		return clusterwire.AdminResponseCode_ERROR,
			"Failed to switch ClusterControl to the ToggleState.SNAPSHOT state"
	}

	return clusterwire.AdminResponseCode_OK, ""
}

// adminRequestTypeName returns the Java enum name. Generated
// AdminRequestType.String returns NULL_VALUE where Java returns NULL_VAL.
func adminRequestTypeName(requestType clusterwire.AdminRequestType) string {
	if requestType == clusterwire.AdminRequestType_NULL_VALUE {
		return "NULL_VAL"
	}
	return requestType.String()
}

// sendAdminResponse offers one AdminResponse on the session egress under
// aeron.SendAttempts. sendAdminResponse drops the reply on a nil egress
// publication, an oversize frame, an encode failure, or a failed offer.
func (a *Agent) sendAdminResponse(
	session *clientSession,
	fields codecs.AdminRequestFields,
	code clusterwire.AdminResponseCode,
	message string,
) {
	pub := session.egressPublication
	if pub == nil {
		return
	}

	messageBytes := []byte(message)
	wireLen, ok := codecs.ClusterWireMessageLen(
		clusterwire.AdminResponseSbeBlockLength,
		len(messageBytes),
		0,
	)
	if !ok || wireLen > pub.MaxPayloadLength() {
		return
	}

	a.ensureEgressScratch(wireLen)
	end, ok := codecs.EncodeAdminResponse(
		a.egressScratch,
		0,
		adminResponseActingVersion,
		codecs.AdminResponseFields{
			ClusterSessionID: fields.ClusterSessionID,
			CorrelationID:    fields.CorrelationID,
			RequestType:      fields.RequestType,
			ResponseCode:     code,
			Message:          messageBytes,
		},
	)
	if !ok {
		return
	}

	_ = a.offerEgressScratch(pub, end)
}
