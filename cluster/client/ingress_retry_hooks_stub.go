//go:build !testhooks

package client

// recordIngressRetryDoWork is a no-op in production builds; testhooks replaces it
// with a recorder-backed implementation (see ingress_retry_hooks.go).
func recordIngressRetryDoWork() {}
