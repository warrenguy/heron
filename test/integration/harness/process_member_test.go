//go:build integration

package harness

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"luas.fast/cluster"
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

func TestProcessMember_storesSoakFields(t *testing.T) {
	status := MemberStatus{
		ApplyDigest:         42,
		AppliedCount:        3,
		LogPublicationBound: true,
		LogSessionID:        0,
		LogImageSessions:    []int32{0},
	}
	exitRec := MemberExit{Error: "stopped", Terminated: true}
	const script = `
printf '%s\n' "$STATUS_LINE" >&3
printf '%s\n' "$EXIT_LINE" >&3
exit 0
`
	member := startShellMember(t, script,
		recordEnv(t, "STATUS_LINE", memberRecord{Status: &status}),
		recordEnv(t, "EXIT_LINE", memberRecord{Exit: &exitRec}),
	)
	require.NoError(t, member.AwaitExit(5*time.Second))

	gotStatus, ok := member.Status()
	require.True(t, ok)
	require.Equal(t, status, gotStatus)

	gotExit, ok := member.ExitRecord()
	require.True(t, ok)
	require.Equal(t, exitRec, gotExit)
}

func TestProcessMember_awaitExit(t *testing.T) {
	const exitScript = `
sleep 0.2
exit 4
`
	member := startShellMember(t, exitScript)
	require.NoError(t, member.AwaitExit(5*time.Second))
	state := member.ProcessState()
	require.NotNil(t, state)
	require.Equal(t, 4, state.ExitCode())

	flag := filepath.Join(t.TempDir(), "flag")
	const lateScript = `
(
  while [ ! -f "$FLAG" ]; do sleep 0.01; done
  printf '%s\n' "$EXIT_LINE" >&3
) &
exit 4
`
	exitRec := MemberExit{Terminated: true, Error: "late"}
	late := startShellMember(t, lateScript,
		"FLAG="+flag,
		recordEnv(t, "EXIT_LINE", memberRecord{Exit: &exitRec}),
	)
	t.Cleanup(func() {
		_ = os.WriteFile(flag, []byte("x"), 0o644)
	})

	require.Eventually(t, func() bool {
		return late.ProcessState() != nil
	}, 5*time.Second, time.Millisecond)
	require.False(t, late.HasExited())

	require.NoError(t, os.WriteFile(flag, []byte("x"), 0o644))
	require.NoError(t, late.AwaitExit(5*time.Second))
	got, ok := late.ExitRecord()
	require.True(t, ok)
	require.Equal(t, exitRec, got)
	require.True(t, late.HasExited())
}

func TestProcessMember_statusHandlerGetsEveryRecord(t *testing.T) {
	records := []MemberStatus{
		{Started: true, AppliedCount: 1},
		{Started: true, AppliedCount: 2},
		{Started: true, AppliedCount: 3},
	}
	const script = `
printf '%s\n' "$S1" >&3
printf '%s\n' "$S2" >&3
printf '%s\n' "$S3" >&3
exit 0
`
	// AwaitExit joins the reader goroutine, so got does not need a lock.
	var got []MemberStatus
	member := startShellMemberWithHandler(
		t,
		func(status MemberStatus) { got = append(got, status) },
		script,
		recordEnv(t, "S1", memberRecord{Status: &records[0]}),
		recordEnv(t, "S2", memberRecord{Status: &records[1]}),
		recordEnv(t, "S3", memberRecord{Status: &records[2]}),
	)

	require.NoError(t, member.AwaitExit(5*time.Second))
	require.Equal(t, records, got)

	last, ok := member.Status()
	require.True(t, ok)
	require.Equal(t, records[2], last)
}

func TestChooseDutyLoopExit(t *testing.T) {
	t.Parallel()

	termination := errors.New(
		"consensus: member terminated: consensus: local archive is not connected",
	)
	failure := errors.New("service: boom")
	decode := errors.New("short payload")

	transportExit := &MemberExit{Error: cluster.ErrTransportTerminating.Error()}
	terminationExit := &MemberExit{Error: termination.Error(), Terminated: true}
	failureExit := &MemberExit{Error: failure.Error()}
	decodeExit := &MemberExit{Error: ExitTextDecodeRequest + ": " + decode.Error()}

	tests := []struct {
		name        string
		transport   bool
		termination error
		failure     error
		decode      error
		want        *MemberExit
	}{
		{name: "no input"},
		{name: "transport alone", transport: true, want: transportExit},
		{
			name:        "transport beside termination",
			transport:   true,
			termination: termination,
			want:        transportExit,
		},
		{name: "termination alone", termination: termination, want: terminationExit},
		{
			name:        "termination beside failure and decode",
			termination: termination,
			failure:     failure,
			decode:      decode,
			want:        terminationExit,
		},
		{name: "service failure alone", failure: failure, want: failureExit},
		{name: "service failure beside decode", failure: failure, decode: decode, want: failureExit},
		{name: "decode alone", decode: decode, want: decodeExit},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := chooseDutyLoopExit(
				test.transport,
				test.termination,
				test.failure,
				test.decode,
			)
			require.Equal(t, test.want, got)
		})
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

	return startShellMemberWithHandler(t, nil, script, env...)
}

func startShellMemberWithHandler(
	t testing.TB,
	handler func(MemberStatus),
	script string,
	env ...string,
) *ProcessMember {
	t.Helper()

	logPath := filepath.Join(t.TempDir(), "child.log")
	cmd := exec.Command("/bin/sh", "-c", script)
	cmd.Env = append(os.Environ(), env...)
	member, err := startProcessMember(cmd, logPath, handler)
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
