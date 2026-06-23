//go:build testhooks

// Package testhook provides a mutex-protected keyed registry for test-only hook
// storage. It is compiled only with the testhooks build tag. Store assigns the
// value without removing the key (nil/zero values are stored). Call Delete to
// remove an entry when nil should mean "unset".
package testhook

import "sync"

// Registry is a mutex-protected map for test hook storage keyed by K.
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
