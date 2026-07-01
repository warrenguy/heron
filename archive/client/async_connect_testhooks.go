//go:build testhooks

package client

import (
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
)

func (ac *AsyncConnect) pollAddControlResponseSubscription(
	registrationID int64,
) (*aeron.Subscription, bool, error) {
	return ac.client.PollAddSubscription(registrationID)
}

func (ac *AsyncConnect) pollAddControlRequestPublication(
	registrationID int64,
) (ControlPublication, bool, error) {
	return ac.client.PollAddExclusivePublication(registrationID)
}

func (ac *AsyncConnect) shouldRetryControlResponseSubscription() bool {
	return false
}

// CorrelationIDForTest exposes the active connect correlation id during connect tests.
func (ac *AsyncConnect) CorrelationIDForTest() int64 {
	if ac == nil {
		return conductor.NullCorrelationID
	}
	return ac.session.correlationID
}

// ControlResponseSubscriptionForTest exposes the control response subscription during
// connect tests.
func (ac *AsyncConnect) ControlResponseSubscriptionForTest() *aeron.Subscription {
	if ac == nil {
		return nil
	}
	return ac.transport.responseSubscription
}

// ArchiveContextForTest exposes the concluded archive context during connect tests.
func (ac *AsyncConnect) ArchiveContextForTest() ArchiveContext {
	if ac == nil {
		return ArchiveContext{}
	}
	return ac.ctx
}

// ControlResponseRegIDForTest exposes the control response async add registration id.
func (ac *AsyncConnect) ControlResponseRegIDForTest() int64 {
	if ac == nil {
		return conductor.NullCorrelationID
	}
	return ac.transport.responseRegID
}

// ControlRequestRegIDForTest exposes the control request async add registration id.
func (ac *AsyncConnect) ControlRequestRegIDForTest() int64 {
	if ac == nil {
		return conductor.NullCorrelationID
	}
	return ac.transport.requestRegID
}

func (ac *AsyncConnect) runPrepareForTest(fn func(*AsyncConnect) error) error {
	if ac == nil {
		return errors.New("archive client: nil async connect")
	}
	return fn(ac)
}

// PrepareConnectRequestForTest exercises connect wire encoding during tests.
func (ac *AsyncConnect) PrepareConnectRequestForTest() error {
	return ac.runPrepareForTest((*AsyncConnect).prepareConnectRequest)
}

// PrepareChallengeResponseForTest exercises challenge wire encoding during tests.
func (ac *AsyncConnect) PrepareChallengeResponseForTest() error {
	return ac.runPrepareForTest((*AsyncConnect).prepareChallengeResponse)
}

// ConnectWireLengthForTest exposes the encoded connect wire length during tests.
func (ac *AsyncConnect) ConnectWireLengthForTest() int {
	if ac == nil {
		return 0
	}
	return ac.wire.length
}

// ConnectWireBufferForTest exposes the connect wire buffer during tests.
func (ac *AsyncConnect) ConnectWireBufferForTest() []byte {
	if ac == nil {
		return nil
	}
	return ac.wire.buffer
}

// PollSendMessageForTest exercises pollSendMessage during connect tests.
func (ac *AsyncConnect) PollSendMessageForTest() (*ConnectResult, error) {
	return ac.pollSendMessage()
}
