# Internal Stream: PnP Messaging

The `internal/stream` package provides a **Plug-and-Play (PnP)** messaging infrastructure. By abstracting message publishing and subscription, the CDC Data Pipeline can adapt to various message brokers without changing the data processing logic.

## The Messaging Interfaces

Located in `provider.go`, the interfaces define the contract for messaging backends:

### `Publisher`
- `Publish(topic, messages ...*message.Message)`: Sends messages to a specific topic.
- `Close()`: Gracefully disconnects from the broker.

### `Subscriber`
- `Subscribe(ctx, topic)`: Returns a channel of messages for consumption.
- `Close()`: Stops consumption and disconnects.

## Supported Backends

- **NATS JetStream Implementation (`nats/`)**:
    - Leverages **Watermill NATS** for reliable patterns.
    - Uses **JetStream** for persistence and durability.
    - Implements **Queue Groups** for consumer scaling.

## Plugging in New Backends

To add a new messaging backend (e.g., Kafka, RabbitMQ, Redis Streams):
1.  Create a new subdirectory (e.g., `internal/stream/kafka`).
2.  Implement the `Publisher` and `Subscriber` interfaces.
3.  Utilize **Watermill** providers when possible to leverage standard integration patterns.

## Conventions

- **Auto-Provisioning**: PnP implementations should automatically create streams/topics and durable consumers if the configuration allows.
- **Header Metadata**: Use message headers to propagate trace IDs or other operational metadata alongside the payload.
- **Graceful Shutdown**: Always ensure all pending acknowledgments are processed before closing the connection.
