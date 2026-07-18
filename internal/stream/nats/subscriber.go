package nats

import (
	"context"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/logger"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	go_nats "github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// subscriberReconnectOpts returns NATS client options that guarantee
// auto-recovery after transient network outages (see T0-6 in
// docs/todos/holistic_review_remediation.md). Extracted so they can be
// unit-tested without spinning up a NATS container.
func subscriberReconnectOpts() []go_nats.Option {
	return []go_nats.Option{
		go_nats.MaxReconnects(-1),
		go_nats.ReconnectWait(2 * time.Second),
		go_nats.Timeout(5 * time.Second),
		go_nats.PingInterval(20 * time.Second),
		go_nats.MaxPingsOutstanding(2),
		go_nats.ReconnectHandler(func(_ *go_nats.Conn) {
			log.Info().Msg("NatsSubscriber: reconnected to NATS")
		}),
		go_nats.DisconnectErrHandler(func(_ *go_nats.Conn, err error) {
			if err != nil {
				log.Warn().Err(err).Msg("NatsSubscriber: disconnected from NATS")
			} else {
				log.Warn().Msg("NatsSubscriber: disconnected from NATS")
			}
		}),
		go_nats.ClosedHandler(func(_ *go_nats.Conn) {
			log.Warn().Msg("NatsSubscriber: NATS connection closed")
		}),
		go_nats.ErrorHandler(func(_ *go_nats.Conn, sub *go_nats.Subscription, err error) {
			log.Error().Err(err).Bool("has_subscription", sub != nil).Msg("NatsSubscriber: NATS async error")
		}),
	}
}

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
			NatsOptions: append([]go_nats.Option{
				go_nats.Name("cdc-data-pipeline-subscriber-" + queueGroupPrefix),
			}, subscriberReconnectOpts()...),
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
