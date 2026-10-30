//go:build rapid

package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestHandlerScope_model_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		var scope handlerScope

		// Only the outermost EnterHandler returns true. The conductor calls
		// ExitHandler only for that call.
		var depth int
		exitTop := func() {
			if depth == 0 {
				return
			}
			depth--
			if depth == 0 {
				scope.ExitHandler()
			}
		}

		steps := rapid.IntRange(0, 32).Draw(rt, "steps")
		for range steps {
			switch rapid.IntRange(0, 3).Draw(rt, "action") {
			case 0:
				require.Equal(rt, depth == 0, scope.EnterHandler())
				depth++
			case 1:
				exitTop()
			case 2:
				require.Equal(rt, depth > 0, scope.isHandlerGoroutine())
			case 3:
				saw := make(chan bool, 1)
				go func() {
					saw <- scope.isHandlerGoroutine()
				}()
				require.False(rt, <-saw)
			}
		}

		for depth > 0 {
			exitTop()
		}
		require.Zero(rt, scope.goroutine.Load())
	})
}
