//go:build !testhooks

package client

func notifyConnectAsyncConnectObserver(*AsyncConnect) {}

func notifyAwaitArchiveConnectStepObserver(*AsyncConnect) {}
