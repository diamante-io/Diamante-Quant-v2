#!/bin/bash
set -e

echo "=== Setting up Production Monitoring ==="

# 1. Create log directories with proper permissions
sudo mkdir -p /var/log/diamante/{validator1,validator2,validator3}
sudo chown -R $USER:docker /var/log/diamante

# 2. Setup logrotate for blockchain logs
sudo tee /etc/logrotate.d/diamante << 'EOF'
/var/log/diamante/*/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 0644 $USER docker
    sharedscripts
    postrotate
        # Signal containers to reopen log files
        docker kill -s USR1 diamante-validator1 2>/dev/null || true
        docker kill -s USR1 diamante-validator2 2>/dev/null || true
        docker kill -s USR1 diamante-validator3 2>/dev/null || true
    endscript
}
EOF

# 3. Create monitoring docker-compose
cat > docker-compose-monitoring.yml << 'EOF'
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:latest
    container_name: diamante-prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus-data:/prometheus
    ports:
      - "9090:9090"
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.retention.time=30d'
    networks:
      - diamante-network

  grafana:
    image: grafana/grafana:latest
    container_name: diamante-grafana
    ports:
      - "3000:3000"
    volumes:
      - grafana-data:/var/lib/grafana
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=changeme
      - GF_USERS_ALLOW_SIGN_UP=false
    networks:
      - diamante-network

  node-exporter:
    image: prom/node-exporter:latest
    container_name: diamante-node-exporter
    ports:
      - "9100:9100"
    networks:
      - diamante-network

volumes:
  prometheus-data:
  grafana-data:

networks:
  diamante-network:
    external: true
EOF

# 4. Create Prometheus configuration
cat > prometheus.yml << 'EOF'
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'diamante-validators'
    static_configs:
      - targets: 
        - 'validator1:8080'
        - 'validator2:8080'
        - 'validator3:8080'
    metrics_path: '/api/v1/metrics'

  - job_name: 'node-exporter'
    static_configs:
      - targets: ['node-exporter:9100']
EOF

echo "Monitoring setup complete!"
echo "To start monitoring: docker-compose -f docker-compose-monitoring.yml up -d"
echo "Access Grafana at: http://localhost:3000 (admin/changeme)"