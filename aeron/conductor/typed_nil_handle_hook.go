//go:build testhooks

package conductor

import "reflect"

// Test fakes and ForTest hooks may inject typed-nil handles; reflect detects
// them so map absence checks and the ready-handler guard reject the violation.
func isTypedNilHandle[T any](value T) bool {
	v := reflect.ValueOf(value)

	//exhaustive:ignore non-nillable kinds fall to default
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.Interface:
		return v.IsNil()
	default:
		return false
	}
}
