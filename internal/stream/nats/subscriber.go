package nats

import (
	"context"
	"fmt"
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

	// js is derived from the SAME *go_nats.Conn the watermill subscriber
	// uses (see NewNatsSubscriber), not a second connection. watermill's
	// *nats.Subscriber doesn't expose its internal JetStreamContext, so we
	// build the raw conn ourselves via nats.NewSubscriberWithNatsConn and
	// keep our own JetStreamContext handle from it for ConsumerInfo/
	// DeleteConsumer calls. This avoids doubling the fleet's NATS connection
	// count (every subscriber is created per-sink, per-pipeline, per-worker
	// replica; a second connection each would have meaningfully increased
	// broker-side connection load for no operational benefit).
	js          go_nats.JetStreamContext
	streamName  string
	durableName string
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

	natsOpts := append([]go_nats.Option{
		go_nats.Name("cdc-data-pipeline-subscriber-" + queueGroupPrefix),
	}, subscriberReconnectOpts()...)

	conn, err := go_nats.Connect(url, natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("obtaining JetStream context: %w", err)
	}

	cfg := nats.SubscriberConfig{
		URL:              url,
		QueueGroupPrefix: queueGroupPrefix,
		JetStream: nats.JetStreamConfig{
			Disabled:         false,
			AutoProvision:    true,
			DurablePrefix:    queueGroupPrefix,
			TrackMsgId:       true,
			SubscribeOptions: subscribeOptions,
		},
		NatsOptions: natsOpts,
	}

	sub, err := nats.NewSubscriberWithNatsConn(conn, cfg.GetSubscriberSubscriptionConfig(), logger.NewWatermillLogger())
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &NatsSubscriber{
		subscriber:  sub,
		js:          js,
		streamName:  streamName,
		durableName: queueGroupPrefix,
	}, nil
}

func (s *NatsSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return s.subscriber.Subscribe(ctx, topic)
}

func (s *NatsSubscriber) Close() error {
	// s.subscriber.Close() closes the single underlying *go_nats.Conn we
	// handed it in NewNatsSubscriber; there is no separate connection of
	// ours left to close.
	return s.subscriber.Close()
}

// PendingCount reports whether this durable JetStream consumer's backlog is
// truly empty. A naive NumPending==0 check is NOT sufficient: NumPending
// only counts messages not yet delivered to this consumer, and excludes
// NumAckPending — everything already delivered and awaiting ack, which
// includes both prefetched-but-unprocessed messages (bounded by
// MaxAckPending) and anything previously Nacked and awaiting redelivery.
// Treating NumPending==0 alone as "empty" can declare a drain complete while
// up to MaxAckPending messages are still in flight or awaiting redelivery —
// exactly the silent-stranding failure mode this replaces a client-side idle
// timer to avoid (plan 01a WI-9). Callers must therefore require BOTH
// counts to be zero; PendingCount enforces that itself and returns the sum
// so a caller checking `== 0` gets the correct answer either way. The call
// is bound by ctx so a NATS outage surfaces as an error rather than
// blocking forever.
func (s *NatsSubscriber) PendingCount(ctx context.Context) (uint64, error) {
	if s.js == nil {
		return 0, fmt.Errorf("PendingCount unavailable: no JetStream context for durable %s", s.durableName)
	}
	info, err := s.js.ConsumerInfo(s.streamName, s.durableName, go_nats.Context(ctx))
	if err != nil {
		return 0, fmt.Errorf("fetching consumer info for stream %s durable %s: %w", s.streamName, s.durableName, err)
	}
	return info.NumPending + uint64(info.NumAckPending), nil
}

// DeleteConsumer removes this subscriber's durable JetStream consumer. Used
// by short-lived, uniquely-named subscribers (e.g. the schema-evolution
// buffer drainer, plan 01a WI-9) to avoid leaking a durable consumer
// definition per drain cycle now that the durable name is stable rather than
// UUID-suffixed.
func (s *NatsSubscriber) DeleteConsumer(ctx context.Context) error {
	if s.js == nil {
		return fmt.Errorf("DeleteConsumer unavailable: no JetStream context for durable %s", s.durableName)
	}
	return s.js.DeleteConsumer(s.streamName, s.durableName, go_nats.Context(ctx))
}
