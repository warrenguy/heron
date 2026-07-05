//go:build testhooks

package aeron

import "github.com/warrenguy/heron/internal/testhook"

var (
	awaitPollSleepingHooks = testhook.NewRegistry[*Client, func()]()
	doWorkAfterHooks       = testhook.NewRegistry[*Client, func()]()
)

func invokeClientHook(registry *testhook.Registry[*Client, func()], client *Client) {
	if client == nil {
		return
	}
	if hook := registry.Load(client); hook != nil {
		hook()
	}
}

func setClientHook(registry *testhook.Registry[*Client, func()], client *Client, hook func()) {
	if client == nil {
		return
	}
	if hook == nil {
		registry.Delete(client)
		return
	}
	registry.Store(client, hook)
}

func invokeDoWorkAfterHook(client *Client) {
	invokeClientHook(doWorkAfterHooks, client)
}

func setDoWorkAfterHook(client *Client, hook func()) {
	setClientHook(doWorkAfterHooks, client, hook)
}

func invokeAwaitPollSleepingHook(client *Client) {
	invokeClientHook(awaitPollSleepingHooks, client)
}

func setAwaitPollSleepingHook(client *Client, hook func()) {
	setClientHook(awaitPollSleepingHooks, client, hook)
}
