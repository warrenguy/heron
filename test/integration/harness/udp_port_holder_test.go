//go:build integration || testhooks

package harness

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUDPPortHolders_findsOwnSocket(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("holder lookup requires linux /proc")
	}
	port := listenLoopbackUDP(t)

	holders, _, err := udpPortHolders(port)
	require.NoError(t, err)

	own := udpPortHolder{pid: os.Getpid(), cmdline: strings.Join(os.Args, " ")}
	require.Contains(t, holders, own, "holders of port %d", port)
}

func TestParseUDPSocketLine_parsesProcNetUDPLine(t *testing.T) {
	const (
		ipv4Line = "  123: 0100007F:499F 00000000:0000 07 00000000:00000000 " +
			"00:00000000 00000000  1000        0 4242 2 0000000000000000 0"
		ipv6Line = "  7: 00000000000000000000000001000000:499F " +
			"00000000000000000000000000000000:0000 07 00000000:00000000 " +
			"00:00000000 00000000     0        0 5151 2 0000000000000000 0"
		header = "  sl  local_address rem_address   st tx_queue rx_queue tr " +
			"tm->when retrnsmt   uid  timeout inode ref pointer drops"
	)

	tests := []struct {
		name   string
		line   string
		port   int
		want   udpPortSocket
		wantOK bool
	}{
		{
			name:   "IPv4 match",
			line:   ipv4Line,
			port:   18847,
			want:   udpPortSocket{inode: 4242, uid: 1000},
			wantOK: true,
		},
		{
			name:   "IPv6 match",
			line:   ipv6Line,
			port:   18847,
			want:   udpPortSocket{inode: 5151},
			wantOK: true,
		},
		{name: "other port", line: ipv4Line, port: 18848},
		{name: "header", line: header, port: 18847},
		{name: "short line", line: "  1: 0100007F:499F", port: 18847},
		{name: "bad port hex", line: "  1: 0100007F:ZZZZ 0 0 0 0 0 0 0 4242", port: 18847},
		{name: "port over 16 bits", line: "  1: 0100007F:1499F 0 0 0 0 0 0 0 4242", port: 18847},
		{name: "bad uid", line: "  1: 0100007F:499F 0 0 0 0 0 x 0 4242", port: 18847},
		{name: "bad inode", line: "  1: 0100007F:499F 0 0 0 0 0 0 0 x", port: 18847},
		{name: "zero inode", line: "  1: 0100007F:499F 0 0 0 0 0 0 0 0", port: 18847, wantOK: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			socket, ok := parseUDPSocketLine(tt.line, tt.port)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.want, socket)
		})
	}
}

func TestUDPHolderNote_namesWhatProcLists(t *testing.T) {
	tests := []struct {
		name    string
		holders []udpPortHolder
		unheld  []udpPortSocket
		want    string
	}{
		{
			name: "no socket",
			want: "UDP port 18847: /proc does not list a holder",
		},
		{
			name:   "holders unreadable",
			unheld: []udpPortSocket{{inode: 4242, uid: 0}, {inode: 5151, uid: 0}},
			want: "UDP port 18847: /proc does not show a readable fd link for " +
				"socket inode 4242 (uid 0), socket inode 5151 (uid 0)",
		},
		{
			name:    "named holder",
			holders: []udpPortHolder{{pid: 7, cmdline: "driver -d /tmp/aeron"}},
			want:    "UDP port 18847: held by pid 7 (driver -d /tmp/aeron)",
		},
		{
			name:    "named holder and unreadable holder",
			holders: []udpPortHolder{{pid: 7, cmdline: "driver -d /tmp/aeron"}},
			unheld:  []udpPortSocket{{inode: 5151, uid: 0}},
			want: "UDP port 18847: held by pid 7 (driver -d /tmp/aeron); " +
				"/proc does not show a readable fd link for socket inode 5151 (uid 0)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, udpHolderNote(18847, tt.holders, tt.unheld))
		})
	}
}

func TestRequireNoUDPBindError_namesHolderOnAddressInUse(t *testing.T) {
	port := listenLoopbackUDP(t)
	addressInUse := errors.New("bind: Address already in use")

	t.Run("nil error", func(t *testing.T) {
		capture := NewCaptureFailTB(t)
		RequireNoUDPBindError(capture, nil, port)
		require.False(t, capture.Failed())
	})

	t.Run("address in use", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("holder lookup requires linux /proc")
		}
		capture := NewCaptureFailTB(t)
		RequireNoUDPBindError(capture, addressInUse, port)
		require.True(t, capture.Failed())
		require.Contains(t, capture.lastMsg, addressInUse.Error())
		require.Contains(t, capture.lastMsg, "pid "+strconv.Itoa(os.Getpid()))
		require.Contains(t, capture.lastMsg, filepath.Base(os.Args[0]))
	})

	t.Run("other error", func(t *testing.T) {
		capture := NewCaptureFailTB(t)
		RequireNoUDPBindError(capture, errors.New("driver timeout"), port)
		require.True(t, capture.Failed())
		require.Contains(t, capture.lastMsg, "driver timeout")
		require.NotContains(t, capture.lastMsg, "UDP port")
	})

	t.Run("lookup error", func(t *testing.T) {
		saved := procNetUDPFiles
		t.Cleanup(func() { procNetUDPFiles = saved })
		procNetUDPFiles = []string{filepath.Join(t.TempDir(), "udp")}

		capture := NewCaptureFailTB(t)
		RequireNoUDPBindError(capture, addressInUse, port)
		require.True(t, capture.Failed())
		require.Contains(t, capture.lastMsg, "UDP port "+strconv.Itoa(port)+": find holder: ")
	})
}

func listenLoopbackUDP(t *testing.T) int {
	t.Helper()
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn.LocalAddr().(*net.UDPAddr).Port
}
