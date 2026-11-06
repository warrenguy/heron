package harness

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// addressInUseText is the strerror text for EADDRINUSE in driver errors.
const addressInUseText = "Address already in use"

const (
	procNetUDP = "/proc/net/udp"
	// procNetUDP6 does not exist on a kernel without IPv6.
	procNetUDP6 = "/proc/net/udp6"
)

var procNetUDPFiles = []string{procNetUDP, procNetUDP6}

type udpPortSocket struct {
	inode uint64
	uid   uint32
}

type udpPortHolder struct {
	pid     int
	cmdline string
}

// RequireNoUDPBindError fails t when err is not nil.
//
// Behaviour: when err contains "Address already in use", the failure names
// each process that /proc shows holding port. The lookup sees only the
// caller's network namespace.
func RequireNoUDPBindError(t testing.TB, err error, port int) {
	t.Helper()
	if err == nil {
		return
	}
	if note := udpBindFailureNote(err, port); note != "" {
		err = fmt.Errorf("%w; %s", err, note)
	}
	require.NoError(t, err)
}

func udpBindFailureNote(err error, port int) string {
	if !strings.Contains(err.Error(), addressInUseText) {
		return ""
	}
	holders, unheld, lookupErr := udpPortHolders(port)
	if lookupErr != nil {
		return fmt.Sprintf("UDP port %d: find holder: %v", port, lookupErr)
	}
	return udpHolderNote(port, holders, unheld)
}

func udpHolderNote(port int, holders []udpPortHolder, unheld []udpPortSocket) string {
	if len(holders) == 0 && len(unheld) == 0 {
		return fmt.Sprintf("UDP port %d: /proc does not list a holder", port)
	}
	var notes []string
	if len(holders) > 0 {
		parts := make([]string, 0, len(holders))
		for _, holder := range holders {
			parts = append(parts, fmt.Sprintf("pid %d (%s)", holder.pid, holder.cmdline))
		}
		notes = append(notes, "held by "+strings.Join(parts, ", "))
	}
	if len(unheld) > 0 {
		parts := make([]string, 0, len(unheld))
		for _, socket := range unheld {
			parts = append(parts, fmt.Sprintf("socket inode %d (uid %d)", socket.inode, socket.uid))
		}
		notes = append(notes, "/proc does not show a readable fd link for "+strings.Join(parts, ", "))
	}
	return fmt.Sprintf("UDP port %d: %s", port, strings.Join(notes, "; "))
}

// udpPortHolders returns each process with an open fd on a socket bound to
// port. It also returns each such socket that it cannot map to a process.
//
// The /proc walk skips a process whose fd directory it cannot read. A holder
// keeps an empty cmdline when /proc/<pid>/cmdline is unreadable.
func udpPortHolders(port int) ([]udpPortHolder, []udpPortSocket, error) {
	sockets, err := udpPortSockets(port)
	if err != nil {
		return nil, nil, err
	}
	if len(sockets) == 0 {
		return nil, nil, nil
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, nil, err
	}
	links := make(map[string]uint64, len(sockets))
	for _, socket := range sockets {
		links[fmt.Sprintf("socket:[%d]", socket.inode)] = socket.inode
	}
	held := make(map[uint64]struct{}, len(sockets))
	var holders []udpPortHolder
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		if !markSocketLinks(pid, links, held) {
			continue
		}
		holders = append(holders, udpPortHolder{pid: pid, cmdline: processCmdline(pid)})
	}
	var unheld []udpPortSocket
	for _, socket := range sockets {
		if _, ok := held[socket.inode]; !ok {
			unheld = append(unheld, socket)
		}
	}
	return holders, unheld, nil
}

func udpPortSockets(port int) ([]udpPortSocket, error) {
	var sockets []udpPortSocket
	for _, path := range procNetUDPFiles {
		data, err := os.ReadFile(path)
		if path == procNetUDP6 && errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for line := range strings.Lines(string(data)) {
			if socket, ok := parseUDPSocketLine(line, port); ok {
				sockets = append(sockets, socket)
			}
		}
	}
	return sockets, nil
}

// parseUDPSocketLine returns the socket on one /proc/net/udp or /proc/net/udp6
// line. Returns false for the header, a malformed line, and any other port.
func parseUDPSocketLine(line string, port int) (udpPortSocket, bool) {
	fields := strings.Fields(line)
	if len(fields) < 10 {
		return udpPortSocket{}, false
	}
	_, portHex, found := strings.Cut(fields[1], ":")
	if !found {
		return udpPortSocket{}, false
	}
	localPort, err := strconv.ParseUint(portHex, 16, 16)
	if err != nil || int(localPort) != port {
		return udpPortSocket{}, false
	}
	uid, err := strconv.ParseUint(fields[7], 10, 32)
	if err != nil {
		return udpPortSocket{}, false
	}
	inode, err := strconv.ParseUint(fields[9], 10, 64)
	if err != nil {
		return udpPortSocket{}, false
	}
	return udpPortSocket{inode: inode, uid: uint32(uid)}, true
}

// markSocketLinks adds to held the inode of each fd of pid whose link target
// is in links. Reports whether it found such an fd.
func markSocketLinks(pid int, links map[string]uint64, held map[uint64]struct{}) bool {
	fdDir := filepath.Join("/proc", strconv.Itoa(pid), "fd")
	fds, err := os.ReadDir(fdDir)
	if err != nil {
		return false
	}
	found := false
	for _, fd := range fds {
		target, err := os.Readlink(filepath.Join(fdDir, fd.Name()))
		if err != nil {
			continue
		}
		if inode, ok := links[target]; ok {
			held[inode] = struct{}{}
			found = true
		}
	}
	return found
}

func processCmdline(pid int) string {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err != nil {
		return ""
	}
	return strings.ReplaceAll(strings.TrimRight(string(data), "\x00"), "\x00", " ")
}
