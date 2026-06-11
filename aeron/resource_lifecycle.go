package aeron

import (
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

type conductorResourceLifecycle struct{}

// ConductorResourceLifecycle wires conductor ready events to aeron types.
var ConductorResourceLifecycle conductor.ResourceLifecycle = conductorResourceLifecycle{}

func (conductorResourceLifecycle) NewPublication(
	event conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (conductor.PublicationHandle, error) {
	return newPublicationFromReady(event, logBuffers.MappedLogBuffer(), countersReader, channel)
}

func (conductorResourceLifecycle) NewPreSubscription(
	registrationID int64,
	streamID int32,
	channel string,
) conductor.SubscriptionHandle {
	return newSubscription(registrationID, streamID, channel)
}

func (conductorResourceLifecycle) ApplySubscriptionReady(
	sub conductor.SubscriptionHandle,
	event conductor.SubscriptionReady,
) error {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return err
	}
	subscription.applySubscriptionReady(event.ChannelStatusCounterID)
	return nil
}

func (conductorResourceLifecycle) RetireSubscription(
	sub conductor.SubscriptionHandle,
) ([]string, error) {
	subscription, err := subscriptionFromHandle(sub)
	if err != nil {
		return nil, err
	}
	images := subscription.images
	subscription.images = nil

	registryKeys := make([]string, 0, len(images))
	for _, image := range images {
		if image == nil {
			continue
		}
		registryKeys = append(registryKeys, image.LogFileName())
		image.internalClose()
	}
	return registryKeys, subscription.Close()
}

func (conductorResourceLifecycle) RetirePublication(
	pub conductor.PublicationHandle,
) (string, error) {
	publication, err := publicationFromHandle(pub)
	if err != nil {
		return "", err
	}
	var registryKey string
	if publication.log != nil {
		registryKey = publication.log.Path()
	}
	return registryKey, publication.Close()
}
