package client

import (
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// EventCode is an SBE enum alias from clusterwire for cluster session events.
type EventCode = clusterwire.EventCode

// AdminRequestType is an SBE enum alias from clusterwire for admin request types.
type AdminRequestType = clusterwire.AdminRequestType

// AdminResponseCode is an SBE enum alias from clusterwire for admin response codes.
type AdminResponseCode = clusterwire.AdminResponseCode

// Cluster egress and admin SBE enum wire values re-exported from clusterwire.
const (
	EventCodeOK                         = clusterwire.EventCode_OK
	EventCodeError                      = clusterwire.EventCode_ERROR
	EventCodeRedirect                   = clusterwire.EventCode_REDIRECT
	EventCodeAuthenticationRejected     = clusterwire.EventCode_AUTHENTICATION_REJECTED
	EventCodeClosed                     = clusterwire.EventCode_CLOSED
	EventCodeNullValue                  = clusterwire.EventCode_NULL_VALUE
	AdminRequestTypeSnapshot            = clusterwire.AdminRequestType_SNAPSHOT
	AdminResponseCodeOK                 = clusterwire.AdminResponseCode_OK
	AdminResponseCodeError              = clusterwire.AdminResponseCode_ERROR
	AdminResponseCodeUnauthorisedAccess = clusterwire.AdminResponseCode_UNAUTHORISED_ACCESS
)
