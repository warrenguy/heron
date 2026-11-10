//go:build integration

package harness

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/warrenguy/luas/test/integration/internal/soakrun"
)

const archiveRestartBound = 30 * time.Second

// KillArchive stops the Archive for index.
func (pc *ProcessCluster) KillArchive(index int) error {
	slot := pc.slotAt(index)

	if slot == nil || slot.archive == nil {
		return fmt.Errorf("process cluster: member %d archive is absent", index)
	}

	slot.archive.Stop()
	return nil
}

// KillDriver stops the media driver for index.
func (pc *ProcessCluster) KillDriver(index int) error {
	slot := pc.slotAt(index)

	if slot == nil || slot.driver == nil {
		return fmt.Errorf("process cluster: member %d driver is absent", index)
	}

	return slot.driver.Kill()
}

// RestartArchive starts a new Archive in the member directory.
//
// It retries a start that exits before ready until archiveRestartBound
// elapses.
func (pc *ProcessCluster) RestartArchive(index int) error {
	slot := pc.slotAt(index)

	if slot == nil {
		return fmt.Errorf("process cluster: member %d is absent", index)
	}

	slot.archive.Stop()

	marker := filepath.Join(slot.archiveDir, ArchiveReadyMarker)
	deadline := time.Now().Add(archiveRestartBound)
	remain := archiveRestartBound

	for {
		_ = os.Remove(marker)
		slot.archiveInc++
		logPath := archiveIncLog(pc.logDir, index, slot.archiveInc)
		node, err := startArchiveNodeLogged(
			slot.spec.AeronDir,
			slot.archiveDir,
			slot.controlChannel,
			pc.javaPath,
			pc.jarPath,
			logPath,
			remain,
		)

		if err == nil {
			archiveID, parseErr := strconv.ParseInt(node.ArchiveID, 10, 64)

			if parseErr != nil {
				node.Stop()
				return fmt.Errorf("process cluster: member %d: %w", index, parseErr)
			}

			slot.archive = node
			slot.archiveID = archiveID
			return nil
		}

		remain = time.Until(deadline)

		if remain <= 0 || !errors.Is(err, errArchiveExitedBeforeReady) {
			return fmt.Errorf("process cluster: member %d: %w", index, err)
		}
	}
}

// RestartDriver starts a media driver in the member's existing directory.
//
// RecordingPosition reports false until the next Archive registers its
// counters.
func (pc *ProcessCluster) RestartDriver(index int) error {
	slot := pc.slotAt(index)

	if slot == nil || slot.driver == nil {
		return fmt.Errorf("process cluster: member %d driver is absent", index)
	}

	dir := slot.driver.Dir
	env, ipcTermDataLength, err := buildDriverEnv(dir, nil)
	if err != nil {
		return fmt.Errorf("process cluster: member %d: %w", index, err)
	}

	if err := slot.counters.Close(); err != nil {
		return fmt.Errorf("process cluster: member %d: %w", index, err)
	}

	slot.counters = nil
	releaseDriverProcess(slot.driver)

	if err := os.Remove(driverCncPath(dir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("process cluster: member %d: remove cnc: %w", index, err)
	}

	err = pc.network.InMemberNamespace(index, func() error {
		md, launchErr := launchMediaDriver(pc.aeronmdPath, dir, env, ipcTermDataLength)
		if launchErr != nil {
			return launchErr
		}

		slot.driver = md
		return nil
	})
	if err != nil {
		return fmt.Errorf("process cluster: member %d: %w", index, err)
	}

	counters, err := OpenDriverCounters(dir)
	if err != nil {
		return fmt.Errorf("process cluster: member %d: %w", index, err)
	}

	slot.counters = counters
	return nil
}

// RestartReturning starts a child on the stored spec.
//
// It sends the start signal once the new child's log images hold every live
// member's log session. The wait lasts at most processClusterImageBound.
// Returns the [soakrun.CheckAlive] error when the child exits during the wait.
func (pc *ProcessCluster) RestartReturning(index int) error {
	slot := pc.slotAt(index)

	if slot == nil {
		return fmt.Errorf("process cluster: member %d is absent", index)
	}

	if slot.current().isLive() {
		return fmt.Errorf("process cluster: member %d is still running", index)
	}

	child, err := pc.startChild(index)
	if err != nil {
		return err
	}

	settled := func() bool {
		return child.HasExited() || pc.returnerHasImages(child)
	}

	if !PollUntilBool(processClusterImageBound, time.Millisecond, settled) {
		return fmt.Errorf(
			"process cluster: returning image wait elapsed\n%s",
			pc.State(),
		)
	}

	if child.HasExited() {
		return soakrun.CheckAlive([]soakrun.ChildState{pc.childState(index)}, -1)
	}

	return child.SignalStart()
}

// ChildStates returns one exit state per member, in member order.
func (pc *ProcessCluster) ChildStates() []soakrun.ChildState {
	out := make([]soakrun.ChildState, len(pc.slots))

	for i := range pc.slots {
		out[i] = pc.childState(i)
	}

	return out
}

func (pc *ProcessCluster) childState(index int) soakrun.ChildState {
	state := soakrun.ChildState{Slot: index}
	member := pc.Member(index)

	if member == nil || !member.HasExited() {
		return state
	}

	state.Exited = true

	if rec, ok := member.ExitRecord(); ok {
		state.ExitText = rec.Error
	}

	return state
}

// returnerHasImages reports whether child's images hold the log session of
// every live member, child included.
func (pc *ProcessCluster) returnerHasImages(child *ProcessMember) bool {
	status, ok := child.Status()

	if !ok {
		return false
	}

	for i := range pc.slots {
		member := pc.slots[i].current()

		if member == nil || !member.isLive() {
			continue
		}

		peer, peerOK := member.Status()

		if !peerOK {
			return false
		}

		if !peer.LogPublicationBound {
			return false
		}

		if !slices.Contains(status.LogImageSessions, peer.LogSessionID) {
			return false
		}
	}

	return true
}

// releaseDriverProcess cancels the driver process. It keeps the directory.
func releaseDriverProcess(md *MediaDriver) {
	if md.external {
		return
	}

	if md.cancel != nil {
		md.cancel()
		md.cancel = nil
	}

	md.waitExited()
	md.cmd = nil
}
