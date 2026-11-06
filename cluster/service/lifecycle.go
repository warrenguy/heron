package service

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/cluster/codecs"
)

// ErrLifecycleRefused reports a module request or session offer that the
// agent refuses inside Start, RoleChange, DoBackgroundWork, or Terminate. The
// error is not fatal.
var ErrLifecycleRefused = errors.New("service: refused inside lifecycle call")

// lifecycleCall names the service call inside which the agent refuses module
// requests and session offers.
type lifecycleCall uint8

const (
	lifecycleCallNone lifecycleCall = iota
	lifecycleCallStart
	lifecycleCallRoleChange
	lifecycleCallBackgroundWork
	lifecycleCallTerminate

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
		lifecycleCallTerminate:      "Terminate",
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

// callTerminate runs Terminate under the lifecycle tag when serviceActive
// holds. It clears serviceActive first, so a panic does not rerun Terminate.
// It reports a Terminate error after the tag clears.
func (a *Agent) callTerminate() {
	if !a.serviceActive {
		return
	}

	a.serviceActive = false

	if a.terminateHandler == nil {
		return
	}

	if err := a.runTerminate(); err != nil {
		a.reportError(fmt.Errorf("%w: %w", ErrTerminate, err))
	}
}

func (a *Agent) runTerminate() error {
	a.activeLifecycleCall = lifecycleCallTerminate
	defer a.endLifecycleCall()
	return a.terminateHandler.Terminate(a)
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
