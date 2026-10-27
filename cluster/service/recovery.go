package service

import (
	"errors"
	"fmt"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/idlestrategy"
)

// DefaultRecoveryTimeout is the default [RecoveryConfig.Timeout]. Matches
// the archive DefaultConnectTimeout.
const DefaultRecoveryTimeout = 5 * time.Second

// DefaultReplayChannel is the snapshot replay channel. Matches upstream
// REPLAY_CHANNEL_DEFAULT.
const DefaultReplayChannel = "aeron:ipc"

// DefaultReplayStreamID is the snapshot replay stream. Matches upstream
// REPLAY_STREAM_ID_DEFAULT.
const DefaultReplayStreamID int32 = 103

// Recovery failure causes. Counter wait, path, and load errors wrap one of
// these with the failure detail. A counters reader failure wraps the reader
// error instead.
var (
	errRecoveryTransport = errors.New(
		"service: Recovery requires a transport client with CountersReader, DoWork and ClientId",
	)
	errRecoveryStateMissing = errors.New("service: recovery state counter not found")
	errRecoveryStateInvalid = errors.New("service: recovery state invalid")
	errSnapshotLoad         = errors.New("service: load snapshot")
)

// RecoveryConfig configures the recovery load in [Open].
//
// Behaviour: Open reads the first allocated recovery state counter (type
// 204). The counter key holds the log position, the cluster time, the
// leadership term, and one snapshot recording id per service.
//   - NULL_VALUE term: Open skips the load. The agent starts at the key log
//     position.
//   - any other term: Open loads this service's snapshot recording first.
//
// Open builds the start ack from the key: ack id 0, the key log position,
// and the transport client id as RelevantID. The consensus module must
// consume that ack.
type RecoveryConfig struct {
	// Timeout caps each recovery wait: the counter, the replay image, and
	// each loader gap between fragments. Zero takes DefaultRecoveryTimeout.
	// Must not be negative.
	//
	// The module must allocate the recovery state counter within Timeout
	// of Open. Otherwise open the container later, or raise Timeout.
	Timeout time.Duration

	// ReplayChannel is the snapshot replay channel. Empty takes
	// DefaultReplayChannel. Must not set session-id=. Open appends the
	// replay session id.
	ReplayChannel string

	// ReplayStreamID is the snapshot replay stream. Zero takes
	// DefaultReplayStreamID. Must differ from ServiceStreamID and
	// ConsensusModuleStreamID after defaulting.
	ReplayStreamID int32
}

// recoveryTransport is the transport client surface the recovery load
// uses.
type recoveryTransport interface {
	CountersReader() (*cnc.CountersReader, error)
	DoWork() int
	//nolint:staticcheck // Aeron Java API uses ClientId.
	ClientId() int64
}

var _ recoveryTransport = (*aeron.Client)(nil)

// defaultRecoveryConfig returns a copy of recovery with the defaults set.
func defaultRecoveryConfig(recovery RecoveryConfig) *RecoveryConfig {
	if recovery.Timeout == 0 {
		recovery.Timeout = DefaultRecoveryTimeout
	}
	if recovery.ReplayChannel == "" {
		recovery.ReplayChannel = DefaultReplayChannel
	}
	if recovery.ReplayStreamID == 0 {
		recovery.ReplayStreamID = DefaultReplayStreamID
	}
	return &recovery
}

// validateRecoveryConfig checks cfg.Recovery. cfg must hold its defaults.
func validateRecoveryConfig(cfg Config) error {
	recovery := cfg.Recovery
	switch {
	case cfg.StartAck != nil:
		return errors.New("service: StartAck and Recovery must not both be set")
	case recovery.Timeout < 0:
		return errors.New("service: Recovery Timeout must not be negative")
	case aeron.ChannelParamIndex(recovery.ReplayChannel, aeron.ChannelSessionIDParam, true) >= 0:
		return errors.New("service: Recovery ReplayChannel must not set session-id=")
	case recovery.ReplayStreamID == cfg.ServiceStreamID:
		return errors.New("service: Recovery ReplayStreamID and ServiceStreamID must differ")
	case recovery.ReplayStreamID == cfg.ConsensusModuleStreamID:
		return errors.New("service: Recovery ReplayStreamID and ConsensusModuleStreamID must differ")
	}
	if _, ok := cfg.Aeron.(recoveryTransport); !ok {
		return errRecoveryTransport
	}
	return nil
}

// awaitRecoveryState returns the key of the first allocated recovery state
// counter. It pumps transport between reads.
//
// Returns an error when:
//   - the counter does not appear within timeout
//   - the counters reader fails
//   - the key does not decode
//   - the cluster id is not 0. Config does not set a cluster id.
//   - the log position is negative
func awaitRecoveryState(
	transport recoveryTransport,
	timeout time.Duration,
) (codecs.RecoveryStateKeyFields, error) {
	deadline := time.Now().Add(timeout)
	idle := idlestrategy.NewDefaultBackoff()
	for {
		reader, err := transport.CountersReader()
		if err != nil {
			return codecs.RecoveryStateKeyFields{}, fmt.Errorf(
				"service: read recovery state counter: %w", err,
			)
		}
		if counterID, found := reader.FindCounterOfType(codecs.RecoveryStateCounterTypeID); found {
			// allocated is false when the driver frees the counter between
			// the two reads.
			if key, allocated := reader.CounterKey(counterID); allocated {
				return checkRecoveryState(key[:])
			}
		}
		idle.Idle(transport.DoWork())
		if !time.Now().Before(deadline) {
			return codecs.RecoveryStateKeyFields{}, fmt.Errorf(
				"%w after %s", errRecoveryStateMissing, timeout,
			)
		}
	}
}

// checkRecoveryState decodes key and applies the key checks of
// [awaitRecoveryState].
func checkRecoveryState(key []byte) (codecs.RecoveryStateKeyFields, error) {
	state, ok := codecs.TryDecodeRecoveryStateKeyAt(key, 0)
	switch {
	case !ok:
		return codecs.RecoveryStateKeyFields{}, fmt.Errorf(
			"%w: key does not decode", errRecoveryStateInvalid,
		)
	case state.ClusterID != 0:
		return codecs.RecoveryStateKeyFields{}, fmt.Errorf(
			"%w: cluster id %d, want 0", errRecoveryStateInvalid, state.ClusterID,
		)
	case state.LogPosition < 0:
		return codecs.RecoveryStateKeyFields{}, fmt.Errorf(
			"%w: log position %d", errRecoveryStateInvalid, state.LogPosition,
		)
	}
	return state, nil
}

// recoveryPath is the resolved recovery state of one service.
type recoveryPath struct {
	state       codecs.RecoveryStateKeyFields
	recordingID int64 // nullValue on the term-only path
}

// isLoad reports whether the path loads a snapshot recording.
func (p recoveryPath) isLoad() bool {
	return p.recordingID != nullValue
}

// resolveRecoveryPath picks the recovery path for serviceID.
//
// Returns:
//   - the term-only path when the key term is NULL_VALUE
//   - the load path with the key recording id for serviceID otherwise
//   - an error when the key does not hold a recording id for serviceID
//   - an error when that recording id is negative
func resolveRecoveryPath(
	state codecs.RecoveryStateKeyFields,
	serviceID int32,
) (recoveryPath, error) {
	if state.LeadershipTermID == nullValue {
		return recoveryPath{state: state, recordingID: nullValue}, nil
	}
	recordingID, ok := state.SnapshotRecordingID(serviceID)
	if !ok {
		return recoveryPath{}, fmt.Errorf(
			"%w: service id %d outside service count %d",
			errRecoveryStateInvalid,
			serviceID,
			len(state.SnapshotRecordingIDs),
		)
	}
	if recordingID < 0 {
		return recoveryPath{}, fmt.Errorf(
			"%w: snapshot recording id %d", errRecoveryStateInvalid, recordingID,
		)
	}
	return recoveryPath{state: state, recordingID: recordingID}, nil
}

// awaitRecoveryPath waits for the recovery state counter on transport. It
// then resolves the path for cfg.ServiceID. cfg must hold its defaults and
// pass validateConfig. A load path also needs cfg.Archive.
func awaitRecoveryPath(cfg Config, transport recoveryTransport) (recoveryPath, error) {
	state, err := awaitRecoveryState(transport, cfg.Recovery.Timeout)
	if err != nil {
		return recoveryPath{}, err
	}
	path, err := resolveRecoveryPath(state, cfg.ServiceID)
	if err != nil {
		return recoveryPath{}, err
	}
	if path.isLoad() && cfg.Archive == nil {
		return recoveryPath{}, fmt.Errorf(
			"%w: recording %d needs Archive", errSnapshotLoad, path.recordingID,
		)
	}
	return path, nil
}
