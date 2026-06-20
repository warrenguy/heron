package testutil

import (
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/atomic"
)

func ClientTimeoutPayload(clientID int64) []byte {
	payload := make([]byte, conductor.ClientTimeoutBodyLength)
	atomic.Wrap(payload).PutInt64(0, clientID)
	return payload
}
