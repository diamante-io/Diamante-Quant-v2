# Diamante Monitoring

Centralized monitoring configuration for Diamante blockchain.

## Structure

- `configs/` - All monitoring service configurations
  - `prometheus/` - Prometheus configuration and rules
  - `grafana/` - Grafana dashboards and provisioning
  - `alertmanager/` - Alert manager configuration
- `docker/` - Docker deployment files for monitoring stack
- `scripts/` - Utility scripts for monitoring
- `dashboards/` - Legacy dashboard location (use configs/grafana/dashboards)

## Quick Start

```bash
# Deploy monitoring stack
cd monitoring/docker
docker-compose -f docker-compose.monitoring.yml up -d

# Access services
# Prometheus: http://localhost:9090
# Grafana: http://localhost:3000 (admin/admin)
# AlertManager: http://localhost:9093
# Node Exporter: http://localhost:9100/metrics
```

## Integration

The main Diamante application exposes metrics on the following endpoints:
- Validator metrics: Port 9090-9093 (configurable via METRICS_PORT)
- API health: `/api/v1/health`
- Metrics endpoint: `/api/v1/metrics`

## Available Dashboards

- `diamante_overview.json` - Main overview dashboard
- `diamante-consensus.json` - Consensus mechanism metrics
- `diamante-network.json` - P2P network statistics
- `diamante-transactions.json` - Transaction processing metrics
- `diamante-storage.json` - Storage layer metrics
- `diamante-system.json` - System resource usage
- `diamante-security.json` - Security and validator metrics
- `operator_dashboard.json` - Operator-focused metrics
- `validator_dashboard.json` - Validator node metrics

## Configuration

### Prometheus

The main Prometheus configuration is in `configs/prometheus/prometheus.yml`. It includes:
- Validator node scraping (ports 9090-9093)
- Self-monitoring
- Alert rules from `configs/prometheus/rules/`

### Grafana

Grafana is pre-configured with:
- Prometheus as the default datasource
- Auto-provisioned dashboards from `configs/grafana/dashboards/`
- Default home dashboard set to `diamante_overview.json`

### Alert Manager

Alert configuration is in `configs/alertmanager/alertmanager.yml` (create if needed).

## Extending Monitoring

### Adding New Metrics

1. Expose metrics in your Go code using the monitoring package
2. Metrics will automatically be scraped by Prometheus

### Adding New Dashboards

1. Create dashboard in Grafana UI
2. Export as JSON
3. Save to `configs/grafana/dashboards/`
4. Restart Grafana container to auto-provision

### Adding Alert Rules

1. Create rule file in `configs/prometheus/rules/`
2. Restart Prometheus to reload rules

## Monitoring Package Usage

The monitoring package provides:
- Metrics collection and registration
- Health monitoring endpoints
- Alert management
- Dashboard generation utilities

See individual Go files for implementation details.

## Troubleshooting

### No metrics showing
- Check if application is exposing metrics on configured ports
- Verify Prometheus targets at http://localhost:9090/targets
- Check container logs: `docker logs diamante-prometheus`

### Dashboards not loading
- Ensure dashboard JSON files are valid
- Check Grafana logs: `docker logs diamante-grafana`
- Verify datasource configuration

### High memory usage
- Adjust Prometheus retention: `--storage.tsdb.retention.time`
- Increase scrape intervals in prometheus.yml
- Enable downsampling for long-term storage