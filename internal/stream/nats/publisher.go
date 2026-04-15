package nats

import (
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/logger"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	go_nats "github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

type NatsPublisher struct {
	publisher *nats.Publisher
}

func NewNatsPublisher(url string) (*NatsPublisher, error) {
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
	return &NatsPublisher{publisher: pub}, nil
}

func (p *NatsPublisher) Publish(topic string, messages ...*message.Message) error {
	err := p.publisher.Publish(topic, messages...)
	if err != nil {
		log.Error().Err(err).Str("topic", topic).Msg("NatsPublisher: Publish failed")
	}
	return err
}

func (p *NatsPublisher) Close() error {
	return p.publisher.Close()
}
