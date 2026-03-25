package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestMetrics(t *testing.T) {
	t.Run("Increment Counters", func(t *testing.T) {
		// Just exercise the calls to ensure no panics and registration works
		RecordsSynced.WithLabelValues("p1", "s1", "t1").Inc()
		SyncErrors.WithLabelValues("p1", "s1", "t1").Inc()
		PipelineLag.WithLabelValues("p1", "s1", "t1").Set(100)
		WorkerHeartbeat.WithLabelValues("w1").Set(123456)
		
		assert.NotNil(t, RecordsSynced)
	})

	t.Run("Registry Check", func(t *testing.T) {
		// Verify metrics are registered in global registry implicitly
		err := prometheus.Register(RecordsSynced)
		// Should error because already registered by init()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate metrics")
	})
}
