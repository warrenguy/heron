package client

import "github.com/warrenguy/heron/agrona/idlestrategy"

// SpinOnce runs one duty-cycle iteration: DoWork, PollEgress, then idle pacing via
// the session context IdleStrategy. Returns the PollEgress work count.
//
// DoWork runs before PollEgress so conductor work is pumped before egress is polled.
//
// Idle pacing uses the PollEgress work count. Connect's await loop calls Idle(0)
// instead. SpinOnce does not call idle.Reset().
//
// Nil receiver returns 0. Nil Aeron skips DoWork; PollEgress still runs (0 when
// closed). Injected-transport sessions may keep a non-nil Aeron after Close; DoWork
// still runs in that case.
//
// Not safe for concurrent use on the same session.
func (ac *AeronCluster) SpinOnce() int {
	if ac == nil {
		return 0
	}
	return ac.spinOnce(ac.ctx.IdleStrategy)
}

func (ac *AeronCluster) spinOnce(idle idlestrategy.IdleStrategy) int {
	if client := ac.aeron; client != nil {
		client.DoWork()
		recordSpinOnceOp("DoWork")
	}
	recordSpinOnceOp("PollEgress")
	work := ac.PollEgress()
	if idle != nil {
		idle.Idle(work)
	}
	return work
}
