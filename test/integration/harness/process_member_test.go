//go:build integration

package harness

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// idleScript writes READY_LINE to the status pipe, then idles on a sleep
// child. The sleep child closes fd 3 so that status EOF follows the shell
// exit. A trapped signal interrupts wait, so the loop calls wait again.
const idleScript = `
on_term() {
  kill "$child" 2>/dev/null
  [ -n "$EXIT_LINE" ] && printf '%s\n' "$EXIT_LINE" >&3
  exit 0
}
trap on_term TERM
trap '[ -n "$STATUS_LINE" ] && printf "%s\n" "$STATUS_LINE" >&3' USR1
sleep 60 3>&- &
child=$!
printf '%s\n' "$READY_LINE" >&3
until wait; do :; done
`

func TestProcessMember_killDropsPartialRecord(t *testing.T) {
	const script = `
printf '%s\n' "$READY_LINE" >&3
printf '%s' '{"Status":{"Started":true' >&3
echo armed
exec sleep 60
`
	want := MemberReady{ClientID: 9, TermRecordingID: 11, LogRecordingStopPosition: 13}
	member := startShellMember(t, script, recordEnv(t, "READY_LINE", memberRecord{Ready: &want}))
	_, err := PollUntilFileWhen(member.LogPath(), 5*time.Second, func(data []byte) bool {
		return bytes.Contains(data, []byte("armed"))
	})
	require.NoError(t, err)
	require.NoError(t, member.Kill())

	ready, ok := member.Ready()
	require.True(t, ok)
	require.Equal(t, want, ready)
	_, hasStatus := member.Status()
	require.False(t, hasStatus)
	require.Empty(t, member.Applied())
	_, hasExit := member.ExitRecord()
	require.False(t, hasExit)

	state := member.ProcessState()
	require.NotNil(t, state)
	ws, ok := state.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.True(t, ws.Signaled())
	require.Equal(t, syscall.SIGKILL, ws.Signal())
}

func TestProcessMember_stopWaitsForExitRecord(t *testing.T) {
	readyEnv := recordEnv(t, "READY_LINE", memberRecord{Ready: &MemberReady{}})
	exitEnv := recordEnv(t, "EXIT_LINE", memberRecord{Exit: &MemberExit{}})

	member := startShellMember(t, idleScript, readyEnv, exitEnv)
	waitRecord(t, member, member.Ready)
	require.NoError(t, member.Stop(5*time.Second))

	missing := startShellMember(t, idleScript, readyEnv)
	waitRecord(t, missing, missing.Ready)
	require.ErrorContains(t, missing.Stop(5*time.Second), "exit record")
}

func TestProcessMember_signalStartDeliversUSR1(t *testing.T) {
	want := MemberStatus{Started: true, MemberID: 1}
	member := startShellMember(t, idleScript,
		recordEnv(t, "READY_LINE", memberRecord{Ready: &MemberReady{}}),
		recordEnv(t, "EXIT_LINE", memberRecord{Exit: &MemberExit{}}),
		recordEnv(t, "STATUS_LINE", memberRecord{Status: &want}),
	)
	waitRecord(t, member, member.Ready)
	require.NoError(t, member.SignalStart())

	status := waitRecord(t, member, member.Status)
	require.NoError(t, member.Stop(5*time.Second))
	require.Equal(t, want, status)
}

func TestWatchLifeline_exitsOnEOF(t *testing.T) {
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	got := make(chan int, 1)
	go watchLifeline(reader, func(code int) { got <- code })
	require.NoError(t, writer.Close())

	select {
	case code := <-got:
		require.Equal(t, 2, code)
	case <-time.After(5 * time.Second):
		require.Fail(t, "lifeline did not exit")
	}
}

func TestProcessMember_raceReports(t *testing.T) {
	path := filepath.Join(t.TempDir(), "child.log")
	body := "ok\n" + raceReportPrefix + "\nmore\n"
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))

	member := &ProcessMember{logPath: path}
	require.Equal(t, []string{raceReportPrefix}, member.RaceReports())
}

func startShellMember(t testing.TB, script string, env ...string) *ProcessMember {
	t.Helper()

	logPath := filepath.Join(t.TempDir(), "child.log")
	cmd := exec.Command("/bin/sh", "-c", script)
	cmd.Env = append(os.Environ(), env...)
	member, err := startProcessMember(cmd, logPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = member.Kill() })
	return member
}

func recordEnv(t testing.TB, name string, rec memberRecord) string {
	t.Helper()

	data, err := json.Marshal(rec)
	require.NoError(t, err)
	return name + "=" + string(data)
}

func waitRecord[T any](t testing.TB, member *ProcessMember, fetch func() (T, bool)) T {
	t.Helper()

	var rec T
	ok := PollUntilBool(5*time.Second, time.Millisecond, func() bool {
		var has bool
		rec, has = fetch()
		return has
	})
	require.True(t, ok, "%T record missing\n%s", rec, lastLogLines(member.LogPath(), 20))
	return rec
}
