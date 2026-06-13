//go:build testhooks

package aeron

func invokeAwaitPollSleepingHook(client *Client) {
	if client == nil {
		return
	}
	client.awaitPollSleepingHookMu.Lock()
	hook := client.awaitPollSleepingHook
	client.awaitPollSleepingHookMu.Unlock()
	if hook != nil {
		hook()
	}
}

func setAwaitPollSleepingHook(client *Client, hook func()) {
	if client == nil {
		return
	}
	client.awaitPollSleepingHookMu.Lock()
	client.awaitPollSleepingHook = hook
	client.awaitPollSleepingHookMu.Unlock()
}
