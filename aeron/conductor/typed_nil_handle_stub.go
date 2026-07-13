//go:build !testhooks

package conductor

// Production ResourceLifecycle implementations never box a nil concrete
// pointer into a handle, so untyped nil detection suffices; reflect-based
// typed-nil probing exists only in testhooks builds.
func isTypedNilHandle[T any](T) bool {
	return false
}
