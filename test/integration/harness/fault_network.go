//go:build integration

package harness

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/warrenguy/luas/test/integration/internal/soakrun"
)

const (
	faultBridgeNS = "luas-bridge"
	faultClientNS = "luas-client"
	faultClientIP = "10.77.0.10"
)

// FaultNetwork is one bridge and one namespace per member and client.
//
// ProcessCluster launches only media drivers in a member namespace.
type FaultNetwork struct {
	memberNS []string
}

// StartFaultNetwork builds the bridge and one namespace per member and client.
//
// A test cleanup deletes the namespaces.
func StartFaultNetwork(t testing.TB, memberCount int) *FaultNetwork {
	t.Helper()
	require.Positive(t, memberCount)
	require.Less(t, memberCount, 10)

	n := &FaultNetwork{memberNS: make([]string, memberCount)}

	for i := range n.memberNS {
		n.memberNS[i] = fmt.Sprintf("luas-m%d", i)
	}

	t.Cleanup(n.cleanup)

	if err := n.build(); err != nil {
		t.Fatalf("fault network: %v\nrun make test-soak", err)
	}

	return n
}

// MemberHost returns the IPv4 address of member index.
func (n *FaultNetwork) MemberHost(index int) string {
	return fmt.Sprintf("10.77.0.%d", index+1)
}

// ClientHost returns the IPv4 address of the client namespace.
func (n *FaultNetwork) ClientHost() string {
	return faultClientIP
}

// InMemberNamespace runs start on a thread in the member namespace.
//
// Behaviour:
//   - A process that start launches stays in that namespace.
//   - A nil network runs start in the caller's namespace.
//   - A failed restore panics. The thread must not return to the scheduler in
//     the wrong namespace.
//
// Concurrency: safe from any goroutine.
func (n *FaultNetwork) InMemberNamespace(index int, start func() error) error {
	if n == nil {
		return start()
	}

	if index < 0 || index >= len(n.memberNS) {
		return fmt.Errorf("fault network: member %d is absent", index)
	}

	return inNetNS(n.memberNS[index], start)
}

// StartClientDriver starts the client media driver in the client namespace.
//
// It registers MediaDriver.Stop as a cleanup.
func (n *FaultNetwork) StartClientDriver(t testing.TB) *MediaDriver {
	t.Helper()

	aeronmdPath := resolveAeronmdOrSkip(t)
	var md *MediaDriver
	err := inNetNS(faultClientNS, func() error {
		var startErr error
		md, startErr = startEphemeralMediaDriver(aeronmdPath, nil)
		return startErr
	})
	if err != nil {
		t.Fatalf("fault network: start client driver: %v", err)
	}

	t.Cleanup(func() { md.Stop() })
	return md
}

// Apply runs each nftables rule in the bridge namespace.
func (n *FaultNetwork) Apply(rules []string) error {
	for _, rule := range rules {
		cmd := nsCommand(faultBridgeNS, append([]string{"nft"}, strings.Fields(rule)...)...)

		if err := runCmd(cmd); err != nil {
			return err
		}
	}

	return nil
}

// Heal removes every rule from the fault chain.
func (n *FaultNetwork) Heal() error {
	return n.Apply(soakrun.HealRules())
}

// Rules returns the bridge fault chain text.
func (n *FaultNetwork) Rules() (string, error) {
	cmd := nsCommand(faultBridgeNS, "nft", "list", "chain", "bridge", "faults", "forward")
	return combinedOutput(cmd)
}

func (n *FaultNetwork) build() error {
	if err := addNS(faultBridgeNS); err != nil {
		return err
	}

	if err := runCmd(nsCommand(
		faultBridgeNS,
		"ip", "link", "add", "br0", "type", "bridge", "mcast_snooping", "0",
	)); err != nil {
		return err
	}

	if err := runCmd(nsCommand(faultBridgeNS, "ip", "link", "set", "br0", "up")); err != nil {
		return err
	}

	for i, ns := range n.memberNS {
		if err := addNS(ns); err != nil {
			return err
		}

		if err := addLink(
			n.MemberHost(i),
			ns,
			fmt.Sprintf("m%d-eth", i),
			soakrun.BridgePort(i),
		); err != nil {
			return err
		}
	}

	if err := addNS(faultClientNS); err != nil {
		return err
	}

	if err := addLink(n.ClientHost(), faultClientNS, "c-eth", "c-br"); err != nil {
		return err
	}

	return addFaultTable()
}

func addNS(name string) error {
	if err := runCmd(exec.Command("ip", "netns", "add", name)); err != nil {
		return err
	}

	return runCmd(nsCommand(name, "ip", "link", "set", "lo", "up"))
}

func addLink(host, nsName, tempName, brPort string) error {
	steps := []*exec.Cmd{
		exec.Command("ip", "link", "add", tempName, "type", "veth", "peer", "name", brPort),
		exec.Command("ip", "link", "set", tempName, "netns", nsName),
		exec.Command("ip", "link", "set", brPort, "netns", faultBridgeNS),
		nsCommand(nsName, "ip", "link", "set", tempName, "name", "eth0"),
		nsCommand(nsName, "ip", "addr", "add", host+"/24", "dev", "eth0"),
		nsCommand(nsName, "ip", "link", "set", "eth0", "up"),
		nsCommand(nsName, "ip", "route", "add", "224.0.0.0/4", "dev", "eth0"),
		nsCommand(nsName, "sysctl", "-w", "net.ipv4.ip_early_demux=0"),
		nsCommand(faultBridgeNS, "ip", "link", "set", brPort, "master", "br0"),
		nsCommand(faultBridgeNS, "ip", "link", "set", brPort, "up"),
	}

	for _, step := range steps {
		if err := runCmd(step); err != nil {
			return err
		}
	}

	return nil
}

func addFaultTable() error {
	cmd := nsCommand(faultBridgeNS, "nft", "-f", "-")
	cmd.Stdin = strings.NewReader(
		"table bridge faults {\n" +
			"\tchain forward {\n" +
			"\t\ttype filter hook forward priority 0; policy accept;\n" +
			"\t}\n" +
			"}\n",
	)

	return runCmd(cmd)
}

func (n *FaultNetwork) cleanup() {
	for _, name := range slices.Concat(n.memberNS, []string{faultClientNS, faultBridgeNS}) {
		_ = runCmd(exec.Command("ip", "netns", "del", name))
	}
}

func inNetNS(name string, start func() error) error {
	errCh := make(chan error, 1)

	go func() { errCh <- runInNetNS(name, start) }()

	return <-errCh
}

func runInNetNS(name string, start func() error) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	saved, err := os.Open("/proc/thread-self/ns/net")
	if err != nil {
		return err
	}

	defer func() { _ = saved.Close() }()

	target, err := os.Open("/run/netns/" + name)
	if err != nil {
		return err
	}

	defer func() { _ = target.Close() }()

	if err := unix.Setns(int(target.Fd()), unix.CLONE_NEWNET); err != nil {
		return err
	}

	defer func() {
		if restoreErr := unix.Setns(int(saved.Fd()), unix.CLONE_NEWNET); restoreErr != nil {
			panic("fault network: restore network namespace: " + restoreErr.Error())
		}
	}()

	return start()
}

func nsCommand(ns string, args ...string) *exec.Cmd {
	return exec.Command("ip", slices.Concat([]string{"netns", "exec", ns}, args)...)
}

func runCmd(cmd *exec.Cmd) error {
	_, err := combinedOutput(cmd)

	return err
}

func combinedOutput(cmd *exec.Cmd) (string, error) {
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s: %w: %s", cmd.String(), err, strings.TrimSpace(string(out)))
	}

	return string(out), nil
}
