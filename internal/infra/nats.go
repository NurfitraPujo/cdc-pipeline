package infra

import (
	"fmt"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
)

// NATSConfig holds connection and bucket details
type NATSConfig struct {
	URL        string
	BucketName string
}

// InitNATS sets up the NATS connection, JetStream context, and returns the KV store.
func InitNATS(cfg NATSConfig) (*nats.Conn, nats.KeyValue, error) {
	if cfg.BucketName == "" {
		cfg.BucketName = protocol.KVBucketName
	}

	nc, err := nats.Connect(cfg.URL)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}

	kv, err := js.KeyValue(cfg.BucketName)
	if err != nil {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: cfg.BucketName,
		})
		if err != nil {
			nc.Close()
			return nil, nil, fmt.Errorf("failed to get or create KV bucket: %w", err)
		}
	}

	return nc, kv, nil
}
