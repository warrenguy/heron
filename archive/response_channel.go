package archive

import (
	"fmt"
	"strings"

	"github.com/warrenguy/heron/aeron"
)

const (
	aeronSchemePrefix = "aeron:"
	aeronSpyPrefix    = "aeron-spy:"
)

// ingressChannelMedia strips aeron-spy: and returns the media token (e.g. "ipc",
// "ipcx", "udp").
func ingressChannelMedia(channel string) (media string, ok bool) {
	channel = strings.TrimPrefix(channel, aeronSpyPrefix)
	if !strings.HasPrefix(channel, aeronSchemePrefix) {
		return "", false
	}
	media, _, _ = strings.Cut(channel[len(aeronSchemePrefix):], "?")
	if media == "" {
		return "", false
	}
	return media, true
}

func ingressPassthroughMedia(media string) bool {
	return media == "ipc" || media == "ipcx"
}

// applyUDPRejoinFalse returns channel with rejoin=false on UDP media.
//
// Called from concludeConfig after ValidateForConnect. Idempotent when
// rejoin=false is already set. IPC, ipcx, and other valid aeron: media pass
// through unchanged. Malformed aeron: URIs return archive: invalid control
// response channel.
func applyUDPRejoinFalse(channel string) (string, error) {
	media, ok := ingressChannelMedia(channel)
	if !ok {
		return "", fmt.Errorf("archive: invalid control response channel: %s", channel)
	}
	if media != "udp" {
		return channel, nil
	}
	return aeron.SetChannelParam(channel, aeron.ChannelRejoinParam, "false"), nil
}

func validateArchiveIngressChannel(channel, role string) error {
	media, ok := ingressChannelMedia(channel)
	if !ok {
		return fmt.Errorf("archive: invalid control %s channel: %s", role, channel)
	}
	if ingressPassthroughMedia(media) || media == "udp" {
		return nil
	}
	return fmt.Errorf(
		"archive: %s control %s channel not supported; use ipc, ipcx, or udp",
		media,
		role,
	)
}

// validateResponseMode rejects control channel pairs that cannot connect in
// response mode. Call it after validateArchiveIngressChannel accepts both
// channels.
func validateResponseMode(requestChannel, responseChannel string) error {
	requestMedia, _ := ingressChannelMedia(requestChannel)
	if requestMedia == "udp" && aeron.IsControlModeResponse(requestChannel) {
		return fmt.Errorf(
			"archive: udp control request channel must not set control-mode=response: %s",
			requestChannel,
		)
	}
	if !aeron.IsControlModeResponse(responseChannel) {
		return nil
	}
	responseMedia, _ := ingressChannelMedia(responseChannel)
	if ingressPassthroughMedia(requestMedia) != ingressPassthroughMedia(responseMedia) {
		return fmt.Errorf(
			"archive: control response media %s differs from request media %s; "+
				"control-mode=response needs one media",
			responseMedia,
			requestMedia,
		)
	}
	return nil
}

type responseChannelResolver interface {
	Channel() string
	TryResolveChannelEndpointPort() (string, bool)
}

// resolveResponseChannel returns the wire response channel URI for archive control connect.
//
// Returns the normalized aeron:ipc/ipcx URI (spy prefix stripped) or a resolved UDP
// endpoint when sub is non-nil. False when sub is nil or UDP wildcard endpoint=:0 is
// not yet active (Java tryResolveChannelEndpointPort parity). AsyncConnect calls this
// during the connect handshake.
func resolveResponseChannel(sub responseChannelResolver) (string, bool) {
	if sub == nil {
		return "", false
	}
	channel := sub.Channel()
	media, ok := ingressChannelMedia(channel)
	if !ok {
		return "", false
	}

	var wire string
	switch {
	case ingressPassthroughMedia(media):
		wire = channel
	case media == "udp":
		resolved, ok := sub.TryResolveChannelEndpointPort()
		if !ok {
			return "", false
		}
		wire = resolved
	default:
		return "", false
	}
	return strings.TrimPrefix(wire, aeronSpyPrefix), true
}
