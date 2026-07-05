//go:build !testhooks

package aeron

func invokeAwaitPollSleepingHook(client *Client) {}

func invokeDoWorkAfterHook(client *Client) {}
