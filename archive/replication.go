package archive

import "context"

// Replicate offers a ReplicateRequest2 and blocks until the archive replies.
// The archive this client connects to replicates sourceRecordingID from the
// archive at sourceControlChannel and sourceControlStreamID. Build params
// with NewReplicationParams.
//
// Returns the replication id from the OK RelevantID. Stop the replication
// with StopReplication or NewAsyncStopReplication.
//
// Signals: the wait polls through Client.Poll. The session handler sees
// signals during the wait, including those behind the OK in the same poll.
// Record them in the handler and match on the returned id afterwards.
//
// Replication failure: the archive reports a failed replication as an ERROR
// keyed on the replication id. Replicate does not return that ERROR. The
// session handler sees REPLICATE_END.
//
// Concurrency: invoker-only. Blocks the invoker. Drives the transport client
// while it waits. Use NewAsyncReplicate on the duty cycle.
//
// Returns an error when:
//   - params fail validation. The error wraps ErrInvalidReplicationParams.
//     Replicate does not offer
//   - the request cannot be offered within the offer bound
//   - the archive refuses the request: ErrorUnknownRecording for an unknown
//     DestinationRecordingID, ErrorActiveRecording for an active destination
//     recording, ErrorGeneric for a response-mode ReplicationChannel with a
//     LiveDestination or tags
//   - the response does not arrive within ControlResponseTimeout
//   - ctx is done before the response
func (client *Client) Replicate(
	ctx context.Context,
	sourceRecordingID int64,
	sourceControlStreamID int32,
	sourceControlChannel []byte,
	params ReplicationParams,
) (replicationID int64, err error) {
	return client.sessionControlOps(client.Poll).replicate(
		ctx,
		sourceRecordingID,
		sourceControlStreamID,
		sourceControlChannel,
		params,
	)
}

// StopReplication offers a StopReplicationRequest for replicationID and
// blocks until the archive replies.
//
// Concurrency: invoker-only. Blocks the invoker. Drives the transport client
// while it waits. Use NewAsyncStopReplication on the duty cycle.
//
// Returns:
//   - true, nil: the archive stopped an active replication
//   - false, nil: the archive replied ErrorUnknownReplication. The
//     replication already ended, or the id never existed
//   - false, err: the offer or the reply failed. ctx.Err() when the context
//     is done first
func (client *Client) StopReplication(
	ctx context.Context,
	replicationID int64,
) (stopped bool, err error) {
	return client.sessionControlOps(client.Poll).stopReplication(ctx, replicationID)
}

// replicate backs Client.Replicate.
func (ops archiveControlOps) replicate(
	ctx context.Context,
	sourceRecordingID int64,
	sourceControlStreamID int32,
	sourceControlChannel []byte,
	params ReplicationParams,
) (int64, error) {
	if err := params.validate(); err != nil {
		return 0, err
	}
	fields, err := ops.offerAndAwait(ctx, func(correlationID int64) (int64, bool) {
		return ops.proxy.TryReplicate(
			ops.controlSessionID,
			correlationID,
			sourceRecordingID,
			sourceControlStreamID,
			sourceControlChannel,
			params,
		)
	})
	if err != nil {
		return 0, err
	}
	return fields.RelevantID, nil
}

// stopReplication backs Client.StopReplication. The stop OK carries
// RelevantID 0, so the OK code alone means stopped.
func (ops archiveControlOps) stopReplication(
	ctx context.Context,
	replicationID int64,
) (bool, error) {
	_, err := ops.offerAndAwait(ctx, func(correlationID int64) (int64, bool) {
		return ops.proxy.TryStopReplication(
			ops.controlSessionID,
			correlationID,
			replicationID,
		)
	})
	if err == nil {
		return true, nil
	}
	if isUnknownReplication(err) {
		return false, nil
	}
	return false, err
}

// isUnknownReplication reports whether err is an archive ERROR with code
// ErrorUnknownReplication.
func isUnknownReplication(err error) bool {
	code, ok := ExceptionCode(err)
	return ok && code == ErrorUnknownReplication
}
