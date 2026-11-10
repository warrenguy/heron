//go:build integration && cluster_soak

package cluster_soak_test

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/warrenguy/luas/test/integration/harness"
	"github.com/warrenguy/luas/test/integration/internal/soakrun"
)

const (
	kvSessions       = 3
	faultUnicastPort = 16809
	faultFrames      = 20
	faultLossPackets = 400
	faultLossLow     = 100
	faultLossHigh    = 300
)

var (
	faultLogEndpoint   = netip.MustParseAddrPort(harness.LiveSoakLogEndpoint)
	faultMulticastPort = int(faultLogEndpoint.Port())
)

// TestFaultNetwork_cutsAndHeals checks UDP delivery across member namespaces through fault rules.
func TestFaultNetwork_cutsAndHeals(t *testing.T) {
	netw := harness.StartFaultNetwork(t, kvSessions)
	groups := [2]net.IP{
		faultLogEndpoint.Addr().AsSlice(),
		faultLogEndpoint.Addr().Next().AsSlice(),
	}

	var joined [kvSessions][2]net.PacketConn
	var unicast [kvSessions]net.PacketConn

	for i := range kvSessions {
		for g, group := range groups {
			conn, err := listenJoined(netw, i, group)
			require.NoError(t, err)
			joined[i][g] = conn
			t.Cleanup(func() { _ = conn.Close() })
		}

		u, err := listenUnicast(netw, i)
		require.NoError(t, err)
		unicast[i] = u
		t.Cleanup(func() { _ = u.Close() })
	}

	requireUnicast(t, unicast)
	requireMulticast(t, joined, groups)
	cutMember(t, netw, 1)
	requireQuiet(t, unicast[0], unicast[1])
	requireQuiet(t, unicast[1], unicast[0])
	requireQuiet(t, unicast[1], unicast[2])
	requireFlow(t, unicast[0], unicast[2])
	require.NoError(t, netw.Heal())
	requireUnicast(t, unicast)
	requireLoss(t, netw, unicast[0], unicast[2])
}

func listenJoined(n *harness.FaultNetwork, index int, group net.IP) (net.PacketConn, error) {
	var conn net.PacketConn
	err := n.InMemberNamespace(index, func() error {
		lc := net.ListenConfig{
			Control: func(_, _ string, raw syscall.RawConn) error {
				var sockErr error
				ctrlErr := raw.Control(func(fd uintptr) {
					sockErr = setMemberSocket(int(fd), group, net.ParseIP(n.MemberHost(index)))
				})

				if ctrlErr != nil {
					return ctrlErr
				}

				return sockErr
			},
		}
		var listenErr error
		conn, listenErr = lc.ListenPacket(
			context.Background(),
			"udp4",
			net.JoinHostPort("0.0.0.0", strconv.Itoa(faultMulticastPort)),
		)

		return listenErr
	})

	return conn, err
}

func listenUnicast(n *harness.FaultNetwork, index int) (net.PacketConn, error) {
	var conn net.PacketConn
	err := n.InMemberNamespace(index, func() error {
		var listenErr error
		conn, listenErr = net.ListenPacket(
			"udp4",
			net.JoinHostPort(n.MemberHost(index), strconv.Itoa(faultUnicastPort)),
		)

		return listenErr
	})

	return conn, err
}

func setMemberSocket(fd int, group, iface net.IP) error {
	for _, opt := range []struct{ level, name, value int }{
		{unix.SOL_SOCKET, unix.SO_REUSEADDR, 1},
		{unix.SOL_SOCKET, unix.SO_REUSEPORT, 1},
		{unix.IPPROTO_IP, unix.IP_MULTICAST_ALL, 0},
		{unix.IPPROTO_IP, unix.IP_MULTICAST_TTL, 1},
		{unix.IPPROTO_IP, unix.IP_MULTICAST_LOOP, 1},
	} {
		if err := unix.SetsockoptInt(fd, opt.level, opt.name, opt.value); err != nil {
			return err
		}
	}

	mreq := &unix.IPMreq{Multiaddr: ipv4Bytes(group), Interface: ipv4Bytes(iface)}

	if err := unix.SetsockoptIPMreq(fd, unix.IPPROTO_IP, unix.IP_ADD_MEMBERSHIP, mreq); err != nil {
		return err
	}

	return unix.SetsockoptInet4Addr(fd, unix.IPPROTO_IP, unix.IP_MULTICAST_IF, ipv4Bytes(iface))
}

func ipv4Bytes(ip net.IP) [4]byte {
	return [4]byte(ip.To4())
}

func requireUnicast(t testing.TB, sockets [kvSessions]net.PacketConn) {
	t.Helper()

	for from := range kvSessions {
		for to := range kvSessions {
			if from == to {
				continue
			}

			requireFlow(t, sockets[from], sockets[to])
		}
	}
}

func requireFlow(t testing.TB, from, to net.PacketConn) {
	t.Helper()
	drain(to)
	dst := udpAddr(to)
	require.NoError(t, sendN(from, dst, 8, []byte("u")))
	require.Len(t, collect(to, 8, 2*time.Second), 8)
}

func requireQuiet(t testing.TB, from, to net.PacketConn) {
	t.Helper()
	drain(to)
	require.NoError(t, sendN(from, udpAddr(to), 8, []byte("q")))
	require.Empty(t, collect(to, 1, 400*time.Millisecond))
}

func requireMulticast(t testing.TB, joined [kvSessions][2]net.PacketConn, groups [2]net.IP) {
	t.Helper()

	for i := range kvSessions {
		for _, conn := range joined[i] {
			drain(conn)
		}
	}

	for sender := range kvSessions {
		for g, group := range groups {
			require.NoError(t, sendGroup(joined[sender][g], group, sender))
		}
	}

	for i := range kvSessions {
		for g, group := range groups {
			requireGroup(t, joined[i][g], group)
		}
	}
}

func sendGroup(conn net.PacketConn, group net.IP, sender int) error {
	dst := &net.UDPAddr{IP: group, Port: faultMulticastPort}
	octet := group.To4()[3]

	for seq := range faultFrames {
		if _, err := conn.WriteTo([]byte(groupFrame(octet, sender, seq)), dst); err != nil {
			return err
		}
	}

	return nil
}

func groupFrame(octet byte, sender, seq int) string {
	return fmt.Sprintf("%d %d %d", octet, sender, seq)
}

func requireGroup(t testing.TB, conn net.PacketConn, group net.IP) {
	t.Helper()
	octet := group.To4()[3]
	want := make([]string, 0, kvSessions*faultFrames)

	for sender := range kvSessions {
		for seq := range faultFrames {
			want = append(want, groupFrame(octet, sender, seq))
		}
	}

	require.ElementsMatch(t, want, collect(conn, len(want), 5*time.Second))
}

func cutMember(t testing.TB, n *harness.FaultNetwork, member int) {
	t.Helper()
	step := soakrun.Step{Kind: soakrun.Partition}
	require.NoError(t, n.Apply(soakrun.Rules(step, member)))
}

func requireLoss(t testing.TB, n *harness.FaultNetwork, from, to net.PacketConn) {
	t.Helper()
	step := soakrun.Step{
		Kind:        soakrun.PacketLoss,
		LossPercent: 50,
		LossScope:   soakrun.AllMembers,
	}
	require.NoError(t, n.Apply(soakrun.Rules(step, 0)))
	drain(to)

	received := make(chan int, 1)

	go func() { received <- len(collect(to, faultLossPackets, time.Second)) }()

	require.NoError(t, sendN(from, udpAddr(to), faultLossPackets, []byte("loss")))
	got := <-received
	require.GreaterOrEqual(t, got, faultLossLow)
	require.LessOrEqual(t, got, faultLossHigh)
}

func udpAddr(conn net.PacketConn) *net.UDPAddr {
	return conn.LocalAddr().(*net.UDPAddr)
}

func sendN(conn net.PacketConn, dst *net.UDPAddr, n int, payload []byte) error {
	for range n {
		if _, err := conn.WriteTo(payload, dst); err != nil {
			return err
		}
	}

	return nil
}

func drain(conn net.PacketConn) {
	_ = conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	buf := make([]byte, 256)

	for {
		if _, _, err := conn.ReadFrom(buf); err != nil {
			return
		}
	}
}

func collect(conn net.PacketConn, want int, bound time.Duration) []string {
	deadline := time.Now().Add(bound)
	buf := make([]byte, 256)
	var out []string

	for len(out) < want && time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			continue
		}

		out = append(out, string(buf[:n]))
	}

	return out
}
