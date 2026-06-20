package client

import (
	gensbe "github.com/warrenguy/heron/aeron/sbe/generated/sbe"
)

// EventCode is an SBE enum alias from gensbe for cluster session events.
type EventCode = gensbe.EventCode

// AdminRequestType is an SBE enum alias from gensbe for admin request types.
type AdminRequestType = gensbe.AdminRequestType

// AdminResponseCode is an SBE enum alias from gensbe for admin response codes.
type AdminResponseCode = gensbe.AdminResponseCode

// Cluster egress and admin SBE enum wire values re-exported from gensbe.
const (
	EventCodeOK                         = gensbe.EventCode_OK
	EventCodeError                      = gensbe.EventCode_ERROR
	EventCodeRedirect                   = gensbe.EventCode_REDIRECT
	EventCodeAuthenticationRejected     = gensbe.EventCode_AUTHENTICATION_REJECTED
	EventCodeClosed                     = gensbe.EventCode_CLOSED
	AdminRequestTypeSnapshot            = gensbe.AdminRequestType_SNAPSHOT
	AdminResponseCodeOK                 = gensbe.AdminResponseCode_OK
	AdminResponseCodeError              = gensbe.AdminResponseCode_ERROR
	AdminResponseCodeUnauthorisedAccess = gensbe.AdminResponseCode_UNAUTHORISED_ACCESS
)
