package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// Registry groups metric collectors for the Diamante monitoring system.
type Registry struct {
	Consensus   *ConsensusCollector
	Network     *NetworkCollector
	Performance *PerformanceCollector
	Business    *BusinessCollector
	logger      *logrus.Logger
}

// NewRegistry creates a Registry and registers all collectors with Prometheus.
func NewRegistry(logger *logrus.Logger) *Registry {
	if logger == nil {
		logger = logrus.New()
	}

	r := &Registry{
		Consensus:   NewConsensusCollector(),
		Network:     NewNetworkCollector(),
		Performance: NewPerformanceCollector(),
		Business:    NewBusinessCollector(logger),
		logger:      logger,
	}

	// Register all collectors with error handling
	if err := r.RegisterAll(prometheus.DefaultRegisterer); err != nil {
		// Log error but continue - metrics may already be registered
		r.logger.WithError(err).Warn("failed to register some metrics collectors, they may already be registered")
	}

	return r
}

// RegisterAll registers all collectors with the provided Prometheus registry
func (r *Registry) RegisterAll(registry prometheus.Registerer) error {
	collectors := []struct {
		name      string
		collector interface {
			Register(prometheus.Registerer) error
		}
	}{
		{"consensus", r.Consensus},
		{"network", r.Network},
		{"performance", r.Performance},
		{"business", r.Business},
	}

	var firstErr error
	for _, c := range collectors {
		if err := c.collector.Register(registry); err != nil {
			// Log the error but continue trying to register other collectors
			r.logger.WithError(err).WithField("collector", c.name).Error("failed to register collector")
			if firstErr == nil {
				firstErr = err
			}
		} else {
			r.logger.WithField("collector", c.name).Debug("successfully registered collector")
		}
	}

	return firstErr
}
