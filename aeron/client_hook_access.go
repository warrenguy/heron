//go:build testhooks

package aeron

import "sync"

var (
	awaitPollSleepingHooks   = make(map[*Client]func())
	awaitPollSleepingHooksMu sync.Mutex
)

func invokeAwaitPollSleepingHook(client *Client) {
	if client == nil {
		return
	}
	awaitPollSleepingHooksMu.Lock()
	hook := awaitPollSleepingHooks[client]
	awaitPollSleepingHooksMu.Unlock()
	if hook != nil {
		hook()
	}
}

func setAwaitPollSleepingHook(client *Client, hook func()) {
	if client == nil {
		return
	}
	awaitPollSleepingHooksMu.Lock()
	defer awaitPollSleepingHooksMu.Unlock()
	if hook == nil {
		delete(awaitPollSleepingHooks, client)
		return
	}
	awaitPollSleepingHooks[client] = hook
}
