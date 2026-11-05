package service

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/cluster/codecs"
)

// ErrLifecycleRefused reports a module request or session offer that the
// agent refuses inside Start, RoleChange, or DoBackgroundWork. The error is
// not fatal.
var ErrLifecycleRefused = errors.New("service: refused inside lifecycle call")

// lifecycleCall names the service call inside which the agent refuses module
// requests and session offers.
type lifecycleCall uint8

const (
	lifecycleCallNone lifecycleCall = iota
	lifecycleCallStart
	lifecycleCallRoleChange
	lifecycleCallBackgroundWork

	lifecycleCallCount
)

type refusedCall uint8

const (
	refusedScheduleTimer refusedCall = iota
	refusedCancelTimer
	refusedCloseClientSession
	refusedOffer

	refusedCallCount
)

// lifecycleRefusals holds one refusal error per lifecycle call and refused
// call. A refusal reports the stored value, so the report does not allocate.
var lifecycleRefusals = newLifecycleRefusals()

func newLifecycleRefusals() (table [lifecycleCallCount][refusedCallCount]error) {
	lifecycleCallNames := [lifecycleCallCount]string{
		lifecycleCallStart:          "Start",
		lifecycleCallRoleChange:     "RoleChange",
		lifecycleCallBackgroundWork: "DoBackgroundWork",
	}
	refusedCallNames := [refusedCallCount]string{
		refusedScheduleTimer:      "ScheduleTimer",
		refusedCancelTimer:        "CancelTimer",
		refusedCloseClientSession: "CloseClientSession",
		refusedOffer:              "ClientSession.Offer",
	}
	for active := lifecycleCallStart; active < lifecycleCallCount; active++ {
		for call := range refusedCallCount {
			table[active][call] = fmt.Errorf(
				"%w: %s in %s",
				ErrLifecycleRefused,
				refusedCallNames[call],
				lifecycleCallNames[active],
			)
		}
	}
	return table
}

func (a *Agent) callStart() error {
	if a.service == nil {
		return nil
	}
	a.activeLifecycleCall = lifecycleCallStart
	defer a.endLifecycleCall()
	return a.service.Start(a)
}

func (a *Agent) changeRole(role codecs.Role) {
	if role == a.role {
		return
	}
	a.role = role
	if a.roleChangeHandler == nil {
		return
	}
	a.activeLifecycleCall = lifecycleCallRoleChange
	defer a.endLifecycleCall()
	a.roleChangeHandler.RoleChange(role)
}

func (a *Agent) backgroundWork(nowNs int64) int {
	if a.backgroundWorkHandler == nil {
		return 0
	}
	a.activeLifecycleCall = lifecycleCallBackgroundWork
	defer a.endLifecycleCall()
	return max(a.backgroundWorkHandler.DoBackgroundWork(nowNs), 0)
}

func (a *Agent) endLifecycleCall() {
	a.activeLifecycleCall = lifecycleCallNone
}

// refuseInsideLifecycleCall reports whether a lifecycle call runs. When one
// runs, the agent passes the refusal of call to reportError.
func (a *Agent) refuseInsideLifecycleCall(call refusedCall) bool {
	active := a.activeLifecycleCall
	if active == lifecycleCallNone {
		return false
	}
	a.reportError(lifecycleRefusals[active][call])
	return true
}
