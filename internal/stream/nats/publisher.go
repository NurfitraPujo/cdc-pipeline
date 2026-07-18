package nats

import (
	"context"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/logger"
	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	go_nats "github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// PendingAcksTimeout is the default timeout for waiting on pending ACKs during batch publishing.
const PendingAcksTimeout = 30 * time.Second

type NatsPublisher struct {
	publisher           *nats.Publisher
	conn                *go_nats.Conn
	js                  go_nats.JetStreamContext
	pendingAcksTimeout  time.Duration
}

func NewNatsPublisher(url string) (*NatsPublisher, error) {
	return NewNatsPublisherWithTimeout(url, PendingAcksTimeout)
}

func NewNatsPublisherWithTimeout(url string, pendingAcksTimeout time.Duration) (*NatsPublisher, error) {
	pub, err := nats.NewPublisher(
		nats.PublisherConfig{
			URL: url,
			JetStream: nats.JetStreamConfig{
				Disabled:      false,
				TrackMsgId:    true,
				AutoProvision: true,
			},
			NatsOptions: []go_nats.Option{
				go_nats.Name("cdc-data-pipeline-publisher"),
				go_nats.MaxReconnects(-1),
				go_nats.ReconnectWait(1 * time.Second),
			},
		},
		logger.NewWatermillLogger(),
	)
	if err != nil {
		return nil, err
	}

	// Open an independent connection to drive batch (async) publishes.
	// This is used only by PublishBatch; per-message Publish() goes through
	// the Watermill publisher above. Keeping the connections separate avoids
	// conflicting usage of the same JetStream context across publishers.
	conn, err := go_nats.Connect(url,
		go_nats.Name("cdc-data-pipeline-publisher-batch"),
		go_nats.MaxReconnects(-1),
		go_nats.ReconnectWait(1*time.Second),
	)
	if err != nil {
		_ = pub.Close()
		return nil, err
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		_ = pub.Close()
		return nil, err
	}

	return &NatsPublisher{
		publisher:          pub,
		conn:               conn,
		js:                 js,
		pendingAcksTimeout: pendingAcksTimeout,
	}, nil
}

func (p *NatsPublisher) Publish(topic string, messages ...*message.Message) error {
	err := p.publisher.Publish(topic, messages...)
	if err != nil {
		log.Error().Err(err).Str("topic", topic).Msg("NatsPublisher: Publish failed")
	}
	return err
}

// PublishBatch publishes multiple messages asynchronously using JetStream PublishMsgAsync
// and waits for all pending ACKs at the end. This is more efficient than sequential
// Publish calls because it allows the server to batch acknowledgments.
func (p *NatsPublisher) PublishBatch(topic string, msgs ...*message.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	// Update metric before publishing
	metrics.NatsPublisherPendingAcks.Set(float64(len(msgs)))

	var lastErr error
	marshaler := &nats.NATSMarshaler{}

	for _, msg := range msgs {
		natsMsg, err := marshaler.Marshal(topic, msg)
		if err != nil {
			log.Error().Err(err).Str("topic", topic).Msg("NatsPublisher: PublishBatch marshal failed")
			lastErr = err
			continue
		}

		_, err = p.js.PublishMsgAsync(natsMsg)
		if err != nil {
			log.Error().Err(err).Str("topic", topic).Msg("NatsPublisher: PublishMsgAsync failed")
			lastErr = err
		}
	}

	// Wait for all pending ACKs with timeout using PublishAsyncComplete channel
	ctx, cancel := context.WithTimeout(context.Background(), p.pendingAcksTimeout)
	defer cancel()

	select {
	case <-p.js.PublishAsyncComplete():
		// All pending messages have been acknowledged
	case <-ctx.Done():
		log.Error().Err(ctx.Err()).Msg("NatsPublisher: PublishAsyncComplete timed out")
		lastErr = ctx.Err()
	}

	// Update metric after batch completes
	metrics.NatsPublisherPendingAcks.Set(0)

	return lastErr
}

func (p *NatsPublisher) Close() error {
	return p.publisher.Close()
}
