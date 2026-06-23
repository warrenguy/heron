//go:build testhooks

package aeron

import "github.com/warrenguy/heron/internal/testhook"

var awaitPollSleepingHooks = testhook.NewRegistry[*Client, func()]()

func invokeAwaitPollSleepingHook(client *Client) {
	if client == nil {
		return
	}
	hook := awaitPollSleepingHooks.Load(client)
	if hook != nil {
		hook()
	}
}

func setAwaitPollSleepingHook(client *Client, hook func()) {
	if client == nil {
		return
	}
	if hook == nil {
		awaitPollSleepingHooks.Delete(client)
		return
	}
	awaitPollSleepingHooks.Store(client, hook)
}
