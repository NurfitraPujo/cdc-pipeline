package nats

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	tc_nats "github.com/testcontainers/testcontainers-go/modules/nats"
)

func TestNatsStreaming(t *testing.T) {
	ctx := context.Background()
	natsC, err := tc_nats.Run(ctx,
		"nats:2.10-alpine",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{"-js"},
			},
		}),
	)
	if err != nil {
		t.Fatalf("Failed to start NATS: %v", err)
	}
	defer natsC.Terminate(ctx)

	url, _ := natsC.ConnectionString(ctx)

	pub, err := NewNatsPublisher(url)
	assert.NoError(t, err)
	defer pub.Close()

	sub, err := NewNatsSubscriber(url, "test-sub", 10, time.Second)
	assert.NoError(t, err)
	defer sub.Close()

	t.Run("Publish and Subscribe", func(t *testing.T) {
		topic := "test-topic"
		msgs, err := sub.Subscribe(ctx, topic)
		assert.NoError(t, err)

		msg := message.NewMessage("1", []byte("hello"))
		err = pub.Publish(topic, msg)
		assert.NoError(t, err)

		select {
		case received := <-msgs:
			assert.Equal(t, "hello", string(received.Payload))
			received.Ack()
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for message")
		}
	})
}
