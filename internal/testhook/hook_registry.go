//go:build testhooks

// Package testhook holds a mutex-protected keyed registry for test-only hooks.
//
// Builds only with the testhooks tag. Store keeps the key when the value is
// nil or zero. Call Delete when nil must mean "unset".
package testhook

import (
	"sync"
	"testing"
)

// Registry is a mutex-protected map of test hooks keyed by K.
//
// Concurrency: the mutex serializes every map access. Load concurrent with an
// unfinished Store may still return the zero value for that key. Callers must
// establish happens-before from a completed Store or SetForTest to the Load.
// Load then reads the stored value until Delete removes the key.
type Registry[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]V
}

// NewRegistry returns an empty Registry.
func NewRegistry[K comparable, V any]() *Registry[K, V] {
	return &Registry[K, V]{m: make(map[K]V)}
}

// Store assigns value to key without removing the entry.
func (r *Registry[K, V]) Store(key K, value V) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = make(map[K]V)
	}
	r.m[key] = value
}

// Load returns the value for key, or the zero value of V when the key is absent.
func (r *Registry[K, V]) Load(key K) V {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m[key]
}

// Delete removes key from the registry.
func (r *Registry[K, V]) Delete(key K) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, key)
}

// SetForTest stores hook under key and deletes the entry when tb finishes.
//
// Lifetime: the entry stays for the rest of tb, including subtests.
func SetForTest[K comparable, V any](
	tb testing.TB,
	registry *Registry[K, V],
	key K,
	hook V,
) {
	tb.Helper()
	registry.Store(key, hook)
	tb.Cleanup(func() { registry.Delete(key) })
}
