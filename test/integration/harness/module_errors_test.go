//go:build integration

package harness

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/cluster"
)

// TestModuleErrors_countAndMatchAt asserts the store keeps reports per member
// and matches them with errors.Is.
func TestModuleErrors_countAndMatchAt(t *testing.T) {
	t.Parallel()

	errTarget := errors.New("target")
	errOther := errors.New("other")

	t.Run("member ids stay apart", func(t *testing.T) {
		t.Parallel()

		store := NewModuleErrors()
		first := store.AmendConfig(cluster.Config{MemberID: 0})
		second := store.AmendConfig(cluster.Config{MemberID: 1})

		first.ErrorHandler(errTarget)
		second.ErrorHandler(errTarget)
		second.ErrorHandler(errTarget)

		require.Equal(t, 1, store.Count(0, errTarget))
		require.Equal(t, 2, store.Count(1, errTarget))
	})

	t.Run("config copy reports into the same member", func(t *testing.T) {
		t.Parallel()

		store := NewModuleErrors()
		amended := store.AmendConfig(cluster.Config{MemberID: 2})
		restarted := amended

		amended.ErrorHandler(errTarget)
		restarted.ErrorHandler(errTarget)

		require.Equal(t, 2, store.Count(2, errTarget))
	})

	t.Run("count matches a wrap and skips other errors", func(t *testing.T) {
		t.Parallel()

		store := NewModuleErrors()
		cfg := store.AmendConfig(cluster.Config{MemberID: 0})

		cfg.ErrorHandler(fmt.Errorf("member step: %w", errTarget))
		cfg.ErrorHandler(errOther)

		require.Equal(t, 1, store.Count(0, errTarget))
		require.Equal(t, 1, store.Count(0, errOther))
	})

	t.Run("match index follows report order", func(t *testing.T) {
		t.Parallel()

		store := NewModuleErrors()
		cfg := store.AmendConfig(cluster.Config{MemberID: 0})
		firstWrap := fmt.Errorf("first: %w", errTarget)
		secondWrap := fmt.Errorf("second: %w", errTarget)

		cfg.ErrorHandler(firstWrap)
		cfg.ErrorHandler(errOther)
		cfg.ErrorHandler(secondWrap)

		require.Equal(t, firstWrap, store.MatchAt(0, errTarget, 0))
		require.Equal(t, secondWrap, store.MatchAt(0, errTarget, 1))
		require.NoError(t, store.MatchAt(0, errTarget, 2))
	})

	t.Run("unknown member", func(t *testing.T) {
		t.Parallel()

		store := NewModuleErrors()
		cfg := store.AmendConfig(cluster.Config{MemberID: 0})
		cfg.ErrorHandler(errTarget)

		require.Zero(t, store.Count(7, errTarget))
		require.NoError(t, store.MatchAt(7, errTarget, 0))
	})
}
