package e2e

// WI-9 gap (plan 01a §"gap flagged during review"): PendingCount = NumPending
// + NumAckPending (internal/stream/nats/subscriber.go) has no non-vacuous
// unit test -- the existing fake-based test would pass against the old,
// broken NumPending-only implementation too. Only a real JetStream server
// can prove the fix, because NumAckPending is server-side state that a fake
// can't meaningfully fabricate as a regression guard. This test drives a
// real JetStream stream/consumer directly (no full pipeline needed) and
// asserts PendingCount stays > 0 while messages are delivered-but-unacked
// (NumPending == 0, NumAckPending > 0) -- the exact case the fix targets.

import (
	"context"
	"testing"
	"time"

	natsstream "github.com/NurfitraPujo/cdc-pipeline/internal/stream/nats"
	go_nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestPendingCount_CountsDeliveredButUnacked(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	ctx := context.Background()
	SetTestContainerProvider()
	natsC, err := StartNats(ctx)
	require.NoError(t, err)
	defer natsC.Terminate(ctx)

	natsURL, err := natsC.ConnectionString(ctx)
	require.NoError(t, err)

	// watermill-nats' topicInterpreter.ensureStream looks up/creates a
	// stream by the SAME name as the topic/subject passed to Subscribe (see
	// topic.go: js.StreamInfo(topic) / AddStream{Name: topic}). If we create
	// a stream under a different name bound to this subject, ensureStream's
	// own AddStream(topic) attempt collides ("subjects overlap with an
	// existing stream"). So streamName and subject must be identical here.
	const streamName = "wi9_pending_stream"
	const subject = streamName
	const durable = "wi9-pending-durable"

	// 1. Create the stream directly via a plain client.
	nc, err := go_nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := nc.JetStream()
	require.NoError(t, err)

	_, err = js.AddStream(&go_nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
	})
	require.NoError(t, err)

	// 2. Publish 5 messages.
	for i := 0; i < 5; i++ {
		_, err := js.Publish(subject, []byte("payload"))
		require.NoError(t, err)
	}

	// 3. Deliver all 5 messages to a durable consumer WITHOUT acking them.
	//
	// NatsSubscriber.Subscribe can't be used for this half of the setup:
	// watermill-nats' processMessage callback (subscriber.go) blocks inside
	// its own per-message select, waiting for msg.Ack()/Nack() or its own
	// AckWaitTimeout, before the underlying NATS client invokes the
	// callback again for the next message -- so consuming without acking
	// stalls delivery after the first message instead of draining all 5.
	// PendingCount itself (see subscriber.go) only calls
	// js.ConsumerInfo(streamName, durableName) -- it doesn't care how
	// messages were delivered. So we drive the delivery directly via a
	// pull consumer sharing the SAME durable name, Fetch all 5 without
	// acking (this alone drives NumPending to 0 / NumAckPending to 5), and
	// then use a separate NatsSubscriber solely for its PendingCount
	// method -- it is never told to Subscribe, so it never creates or
	// competes for the consumer.
	pullSub, err := js.PullSubscribe(subject, durable, go_nats.MaxAckPending(100), go_nats.AckWait(30*time.Second))
	require.NoError(t, err)

	fetchCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	msgs, err := pullSub.Fetch(5, go_nats.Context(fetchCtx))
	require.NoError(t, err)
	require.Equal(t, 5, len(msgs), "expected to receive all 5 messages before proving the pending-vs-ack-pending distinction")
	// Deliberately do NOT ack/nack any of them -- they must stay
	// "delivered but unacked" for the rest of the test.

	sub, err := natsstream.NewNatsSubscriber(natsURL, durable, streamName, 100, 30*time.Second)
	require.NoError(t, err)
	defer sub.Close()

	// 4. Confirm server-side state actually matches the scenario the fix
	// targets: NumPending == 0 (nothing left undelivered) but
	// NumAckPending == 5 (delivered, unacked) -- the case where the old
	// NumPending-only implementation would have wrongly reported "empty".
	require.Eventually(t, func() bool {
		info, err := js.ConsumerInfo(streamName, durable)
		if err != nil {
			return false
		}
		t.Logf("ConsumerInfo: NumPending=%d NumAckPending=%d", info.NumPending, info.NumAckPending)
		return info.NumPending == 0 && info.NumAckPending == 5
	}, 15*time.Second, 500*time.Millisecond, "expected NumPending==0 and NumAckPending==5 (messages delivered but not yet acked)")

	// 5. This is the actual regression assertion: PendingCount must report
	// nonzero (5) here. Against the pre-fix implementation (NumPending
	// alone), this would incorrectly return 0 and a caller like
	// checkDrained/drainBufferedUntilIdle would wrongly conclude the backlog
	// is empty and terminate a drain while 5 messages are still in flight.
	pc, err := sub.PendingCount(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 5, pc,
		"PendingCount must count delivered-but-unacked messages (NumAckPending), not just NumPending -- "+
			"a drain relying on PendingCount==0 here would wrongly declare the backlog empty and strand these 5 messages")

	// 6. Purge the stream (clears both NumPending and NumAckPending) and
	// confirm PendingCount correctly falls to 0 -- proves this isn't just
	// permanently nonzero/broken in the other direction.
	require.NoError(t, js.PurgeStream(streamName))
	require.Eventually(t, func() bool {
		pc, err := sub.PendingCount(ctx)
		if err != nil {
			return false
		}
		t.Logf("PendingCount after purge: %d", pc)
		return pc == 0
	}, 15*time.Second, 500*time.Millisecond, "PendingCount should fall to 0 once the stream is purged and ack-pending entries expire")
}
