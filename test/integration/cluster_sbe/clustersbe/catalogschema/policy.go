package catalogschema

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/sbe"
)

const (
	templateSessionEvent          = "sessionEvent"
	templateSessionConnectRequest = "sessionConnectRequest"
	SessionEventHeartbeatField    = "leaderHeartbeatTimeoutNs"

	connectVersionWireActingVersion          = 2
	sessionEventOmitBlockLengthActingVersion = 6
)

func ConnectVersionRequired(actingVersion uint16) bool {
	return actingVersion < connectVersionWireActingVersion
}

func ValidateConnectVersion(template string, actingVersion uint16, fields map[string]any) error {
	if template != templateSessionConnectRequest || !ConnectVersionRequired(actingVersion) {
		return nil
	}
	if _, ok := fields["version"]; !ok {
		return errors.New("missing version for actingVersion < 2")
	}
	return nil
}

// SessionEventLegacyBlockLength reports the catalog blockLength for sessionEvent when
// actingVersion < 6; the bool is false when blockLength must be omitted.
func SessionEventLegacyBlockLength(actingVersion uint16) (uint16, bool) {
	if actingVersion >= sessionEventOmitBlockLengthActingVersion {
		return 0, false
	}
	return sbe.SessionEventBlockLength(actingVersion), true
}

func ValidateEntryBlockLength(entry FixtureEntry) error {
	if entry.BlockLength == nil {
		return nil
	}
	if entry.Template != templateSessionEvent {
		return fmt.Errorf("fixture %q: blockLength only allowed for sessionEvent", entry.Name)
	}
	want, ok := SessionEventLegacyBlockLength(entry.ActingVersion)
	if !ok {
		return fmt.Errorf(
			"fixture %q: blockLength not allowed for actingVersion %d",
			entry.Name,
			entry.ActingVersion,
		)
	}
	if *entry.BlockLength != want {
		return fmt.Errorf(
			"fixture %q: blockLength %d does not match actingVersion %d (want %d)",
			entry.Name,
			*entry.BlockLength,
			entry.ActingVersion,
			want,
		)
	}
	return nil
}
