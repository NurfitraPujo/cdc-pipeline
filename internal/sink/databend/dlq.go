package databend

import (
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/ThreeDotsLabs/watermill/message"
)

// DefaultSinkDeadLetterSubject returns the default NATS subject used to publish
// sink dead letter events for the given sink instance.
func DefaultSinkDeadLetterSubject(sinkID string) string {
	if sinkID == "" {
		return "cdc.sink.dlq"
	}
	return "cdc.sink." + sinkID + ".dlq"
}

// SinkDeadLetterEvent is the structured payload published when the Databend
// sink cannot process a CDC record. The struct is intentionally narrow so it
// can be inspected by an external operator dashboard or replay tooling without
// pulling in the full protocol.Message schema.
type SinkDeadLetterEvent struct {
	SinkID    string                 `json:"sink_id"`
	Table     string                 `json:"table"`
	UUID      string                 `json:"uuid"`
	LSN       uint64                 `json:"lsn"`
	Op        protocol.OperationType `json:"op"`
	SourceID  string                 `json:"source_id"`
	Reason    string                 `json:"reason"`
	Payload   []byte                 `json:"payload"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

// DLQPublisher is the minimal publisher contract used by the Databend sink to
// emit dead letter events. We declare it here (rather than importing the
// stream package) to avoid an unnecessary back-edge in the dependency graph
// while keeping the existing sink.Sink interface unchanged.
type DLQPublisher interface {
	Publish(topic string, messages ...*message.Message) error
}

// buildDLQMessage wraps a marshalled SinkDeadLetterEvent in a Watermill
// message. The UUID is used as the Watermill message UUID so consumers can
// deduplicate re-deliveries. The function lives next to the DLQ types so
// tests can construct identical envelopes when mocking the publisher.
func buildDLQMessage(uuid string, payload []byte) *message.Message {
	msg := message.NewMessage(uuid, payload)
	msg.Metadata.Set("sink-dlq", "true")
	return msg
}
