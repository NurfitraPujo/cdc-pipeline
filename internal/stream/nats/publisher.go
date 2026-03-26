package nats

import (
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	go_nats "github.com/nats-io/nats.go"
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
			},
		},
		nil, // Logger
	)
	if err != nil {
		return nil, err
	}
	return &NatsPublisher{publisher: pub}, nil
}

func (p *NatsPublisher) Publish(topic string, messages ...*message.Message) error {
	return p.publisher.Publish(topic, messages...)
}

func (p *NatsPublisher) Close() error {
	return p.publisher.Close()
}
