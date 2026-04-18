package e2e

import (
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/source/postgres"
	"github.com/NurfitraPujo/cdc-pipeline/internal/sink/databend"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream/nats"
	"github.com/stretchr/testify/assert"
)

func TestProviderLifecycle(t *testing.T) {
	env := Setup(t)
	defer env.Close()

	t.Run("PostgresSource Lifecycle", func(t *testing.T) {
		src := postgres.NewPostgresSource("s1")
		assert.Equal(t, "s1", src.Name())
		// Stop is safe even if not started
		assert.Nil(t, src.Stop())
	})

	t.Run("DatabendSink Lifecycle", func(t *testing.T) {
		snk, _ := databend.NewDatabendSink("snk1", env.DbConfig.DSN)
		assert.Equal(t, "snk1", snk.Name())
		assert.Nil(t, snk.Stop())
	})

	t.Run("NATS Publisher Lifecycle", func(t *testing.T) {
		pub, _ := nats.NewNatsPublisher(env.NatsURL)
		assert.Nil(t, pub.Close())
	})

	t.Run("NATS Subscriber Lifecycle", func(t *testing.T) {
		sub, _ := nats.NewNatsSubscriber(env.NatsURL, "q1", "cdc_pipeline_p1_ingest", 100, 30*time.Second)
		assert.Nil(t, sub.Close())
	})
}
