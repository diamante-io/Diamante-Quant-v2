// storage/metrics_exporter.go

package storage

import "github.com/prometheus/client_golang/prometheus"

// MetricsExporter interface for exporting metrics
type MetricsExporter interface {
	RegisterCollector(name string, collector prometheus.Collector) error
}
