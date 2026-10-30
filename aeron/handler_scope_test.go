package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCurrentGoroutineID(t *testing.T) {
	t.Parallel()

	first := currentGoroutineID()
	require.Positive(t, first)
	require.Equal(t, first, currentGoroutineID())

	const goroutines = 8
	ids := make(chan int64, goroutines)
	for range goroutines {
		go func() {
			ids <- currentGoroutineID()
		}()
	}

	seen := map[int64]struct{}{first: {}}
	for range goroutines {
		id := <-ids
		require.Positive(t, id)
		_, dup := seen[id]
		require.False(t, dup)
		seen[id] = struct{}{}
	}
}

func TestHandlerScope_enterExit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "nestedEnterExit", run: testHandlerScopeNestedEnterExit},
		{name: "otherGoroutine", run: testHandlerScopeOtherGoroutine},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func testHandlerScopeNestedEnterExit(t *testing.T) {
	var scope handlerScope
	require.False(t, scope.isHandlerGoroutine())

	require.True(t, scope.EnterHandler())
	require.True(t, scope.isHandlerGoroutine())
	require.False(t, scope.EnterHandler())
	require.True(t, scope.isHandlerGoroutine())

	scope.ExitHandler()
	require.False(t, scope.isHandlerGoroutine())
	require.Zero(t, scope.goroutine.Load())

	require.True(t, scope.EnterHandler())
	require.True(t, scope.isHandlerGoroutine())
	scope.ExitHandler()
	require.False(t, scope.isHandlerGoroutine())
	require.Zero(t, scope.goroutine.Load())
}

func testHandlerScopeOtherGoroutine(t *testing.T) {
	var scope handlerScope
	require.True(t, scope.EnterHandler())

	saw := make(chan bool, 1)
	go func() {
		saw <- scope.isHandlerGoroutine()
	}()
	require.False(t, <-saw)
	require.True(t, scope.isHandlerGoroutine())

	scope.ExitHandler()
	require.False(t, scope.isHandlerGoroutine())
}
