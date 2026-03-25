package nats

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	go_nats "github.com/nats-io/nats.go"
)

type NatsSubscriber struct {
	subscriber *nats.Subscriber
}

func NewNatsSubscriber(url string, queueGroupPrefix string, maxAckPending int, ackWait time.Duration) (*NatsSubscriber, error) {
	if ackWait == 0 {
		ackWait = 30 * time.Second
	}
	sub, err := nats.NewSubscriber(
		nats.SubscriberConfig{
			URL:              url,
			QueueGroupPrefix: queueGroupPrefix,
			JetStream: nats.JetStreamConfig{
				Disabled:      false,
				AutoProvision: true,
				DurablePrefix: queueGroupPrefix,
				TrackMsgId:    true,
				SubscribeOptions: []go_nats.SubOpt{
					go_nats.MaxAckPending(maxAckPending),
					go_nats.AckWait(ackWait),
				},
			},
			NatsOptions: []go_nats.Option{
				go_nats.Name("daya-data-pipeline-subscriber"),
			},
		},
		nil, // Logger
	)
	if err != nil {
		return nil, err
	}
	return &NatsSubscriber{subscriber: sub}, nil
}

func (s *NatsSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return s.subscriber.Subscribe(ctx, topic)
}

func (s *NatsSubscriber) Close() error {
	return s.subscriber.Close()
}
