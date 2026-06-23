//go:build testhooks

package testhook

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistry(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, r *Registry[string, *int])
	}{
		{
			name: "store_load",
			run: func(t *testing.T, r *Registry[string, *int]) {
				v := 42
				r.Store("key", &v)
				got := r.Load("key")
				require.NotNil(t, got)
				require.Equal(t, 42, *got)
			},
		},
		{
			name: "store_overwrite",
			run: func(t *testing.T, r *Registry[string, *int]) {
				v1 := 1
				v2 := 2
				r.Store("key", &v1)
				r.Store("key", &v2)
				got := r.Load("key")
				require.NotNil(t, got)
				require.Equal(t, 2, *got)
			},
		},
		{
			name: "load_absent",
			run: func(t *testing.T, r *Registry[string, *int]) {
				require.Nil(t, r.Load("absent"))
			},
		},
		{
			name: "delete_then_absent",
			run: func(t *testing.T, r *Registry[string, *int]) {
				v := 42
				r.Store("key", &v)
				r.Delete("key")
				require.Nil(t, r.Load("key"))
			},
		},
		{
			name: "store_nil_retains_entry",
			run: func(t *testing.T, r *Registry[string, *int]) {
				r.Store("key", nil)
				_, retained := r.m["key"]
				require.True(t, retained, "Store must retain key when value is nil")
				require.Nil(t, r.Load("key"))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRegistry[string, *int]()
			tc.run(t, r)
		})
	}
}

func TestRegistryZeroValue(t *testing.T) {
	var r Registry[string, *int]
	v := 42
	require.NotPanics(t, func() { r.Store("key", &v) })
	got := r.Load("key")
	require.NotNil(t, got)
	require.Equal(t, 42, *got)
}

func TestRegistryConcurrent(t *testing.T) {
	r := NewRegistry[int, int]()
	const goroutines = 8
	const iterations = 100

	keyFor := func(g, i int) int {
		return g*iterations + i
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range iterations {
				key := keyFor(id, i)
				r.Store(key, key)
				_ = r.Load(key)
				if i%3 == 0 {
					r.Delete(key)
				}
			}
		}(g)
	}
	wg.Wait()

	for g := range goroutines {
		for i := range iterations {
			key := keyFor(g, i)
			if i%3 == 0 {
				require.Equal(t, 0, r.Load(key))
				continue
			}
			require.Equal(t, key, r.Load(key))
		}
	}
}
