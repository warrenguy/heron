//go:build testhooks

package aeron

func invokeAwaitPollSleepingHook(client *Client) {
	if client == nil {
		return
	}
	if hook := client.awaitPollSleepingHook; hook != nil {
		hook()
	}
}

func setAwaitPollSleepingHook(client *Client, hook func()) {
	if client == nil {
		return
	}
	client.awaitPollSleepingHook = hook
}
