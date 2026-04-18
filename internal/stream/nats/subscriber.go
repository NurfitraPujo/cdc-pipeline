package nats

import (
	"context"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/logger"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	go_nats "github.com/nats-io/nats.go"
)

type NatsSubscriber struct {
	subscriber *nats.Subscriber
}

func NewNatsSubscriber(url string, queueGroupPrefix string, streamName string, maxAckPending int, ackWait time.Duration) (*NatsSubscriber, error) {
	if ackWait == 0 {
		ackWait = 30 * time.Second
	}

	subscribeOptions := []go_nats.SubOpt{
		go_nats.MaxAckPending(maxAckPending),
		go_nats.AckWait(ackWait),
	}
	if streamName != "" {
		subscribeOptions = append(subscribeOptions, go_nats.BindStream(streamName))
	}

	sub, err := nats.NewSubscriber(
		nats.SubscriberConfig{
			URL:              url,
			QueueGroupPrefix: queueGroupPrefix,
			JetStream: nats.JetStreamConfig{
				Disabled:         false,
				AutoProvision:    true,
				DurablePrefix:    queueGroupPrefix,
				TrackMsgId:       true,
				SubscribeOptions: subscribeOptions,
			},
			NatsOptions: []go_nats.Option{
				go_nats.Name("cdc-data-pipeline-subscriber-" + queueGroupPrefix),
			},
		},
		logger.NewWatermillLogger(),
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
