package testutil

import (
	"context"
	"os"
	"testing"

	"pgregory.net/rapid"
)

// IterationTB returns tb with its per-check methods sent to rt. Pass the result
// to testing.TB helpers inside a rapid.Check property.
//
// The result sends these methods to rt:
//   - Cleanup and TempDir: the cleanup and the directory end with the check.
//   - Context: rt cancels the context before the check cleanups run.
//   - Error, Fail, Fatal, Skip, and their variants: rapid reports the failure
//     with its seed and shrinks the input.
//
// Other methods call tb.
//
// Behaviour: rapid checks Error and Fail before the check cleanups run. A
// cleanup that must fail the check calls Fatal.
func IterationTB(tb testing.TB, rt *rapid.T) testing.TB {
	return iterationTB{TB: tb, rt: rt}
}

type iterationTB struct {
	testing.TB
	rt *rapid.T
}

func (tb iterationTB) Cleanup(f func()) {
	tb.rt.Cleanup(f)
}

func (tb iterationTB) Context() context.Context {
	return tb.rt.Context()
}

func (tb iterationTB) Error(args ...any) {
	tb.rt.Helper()
	tb.rt.Error(args...)
}

func (tb iterationTB) Errorf(format string, args ...any) {
	tb.rt.Helper()
	tb.rt.Errorf(format, args...)
}

func (tb iterationTB) Fail() {
	tb.rt.Fail()
}

func (tb iterationTB) FailNow() {
	tb.rt.FailNow()
}

func (tb iterationTB) Failed() bool {
	return tb.rt.Failed()
}

func (tb iterationTB) Fatal(args ...any) {
	tb.rt.Helper()
	tb.rt.Fatal(args...)
}

func (tb iterationTB) Fatalf(format string, args ...any) {
	tb.rt.Helper()
	tb.rt.Fatalf(format, args...)
}

func (tb iterationTB) Skip(args ...any) {
	tb.rt.Helper()
	tb.rt.Skip(args...)
}

func (tb iterationTB) Skipf(format string, args ...any) {
	tb.rt.Helper()
	tb.rt.Skipf(format, args...)
}

func (tb iterationTB) SkipNow() {
	tb.rt.SkipNow()
}

// TempDir returns a new directory that RemoveAll deletes when the check ends.
func (tb iterationTB) TempDir() string {
	tb.rt.Helper()
	dir, err := os.MkdirTemp("", "rapid-check-")
	if err != nil {
		tb.rt.Fatalf("TempDir: %v", err)
	}

	tb.rt.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			tb.rt.Fatalf("TempDir RemoveAll: %v", err)
		}
	})

	return dir
}
