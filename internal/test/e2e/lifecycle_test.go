package e2e

import (
	"testing"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/source/postgres"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/sink/databend"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/stream/nats"
	"github.com/stretchr/testify/assert"
)

func TestProviderLifecycle(t *testing.T) {
	env := Setup(t)
	defer env.Close()

	t.Run("PostgresSource Lifecycle", func(t *testing.T) {
		src := postgres.NewPostgresSource("s1")
		assert.Equal(t, "postgres", src.Name())
		// Stop is safe even if not started
		assert.Nil(t, src.Stop())
	})

	t.Run("DatabendSink Lifecycle", func(t *testing.T) {
		snk, _ := databend.NewDatabendSink("snk1", env.DbConfig.DSN)
		assert.Equal(t, "databend", snk.Name())
		assert.Nil(t, snk.Stop())
	})

	t.Run("NATS Publisher Lifecycle", func(t *testing.T) {
		pub, _ := nats.NewNatsPublisher(env.NatsURL)
		assert.Nil(t, pub.Close())
	})

	t.Run("NATS Subscriber Lifecycle", func(t *testing.T) {
		sub, _ := nats.NewNatsSubscriber(env.NatsURL, "q1", 100, 30*time.Second)
		assert.Nil(t, sub.Close())
	})
}
