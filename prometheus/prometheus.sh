#!/usr/bin/env bash
#
# This init action install prometheus on dataproc cluster.
# Prometheus is time series datatabase that allows to easily monitor
# cluster parameters, JMX properties and other data during job execution.
#

set -euxo pipefail

readonly PROMETHEUS_VER='2.5.0'
readonly STATSD_EXPORTER_VER='0.8.0'

function install_prometheus {
  mkdir -p /etc/prometheus /var/lib/prometheus 
  wget -q "https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VER}/prometheus-${PROMETHEUS_VER}.linux-amd64.tar.gz" && \
  tar -xf "prometheus-${PROMETHEUS_VER}.linux-amd64.tar.gz" && \
  cp "prometheus-${PROMETHEUS_VER}.linux-amd64/prometheus" /usr/local/bin/ && \
  cp "prometheus-${PROMETHEUS_VER}.linux-amd64/promtool" /usr/local/bin/ && \
  cp -r "prometheus-${PROMETHEUS_VER}.linux-amd64/consoles" /etc/prometheus && \
  cp -r "prometheus-${PROMETHEUS_VER}.linux-amd64/console_libraries" /etc/prometheus

  rm -rf "prometheus-${PROMETHEUS_VER}.linux-amd64.tar.gz" "prometheus-${PROMETHEUS_VER}.linux-amd64"

cat << EOF > /etc/systemd/system/prometheus.service
[Unit]
Description=Prometheus
Wants=network-online.target
After=network-online.target

[Service]
User=root
Group=root
Type=simple
ExecStart=/usr/local/bin/prometheus \
    --config.file /etc/prometheus/prometheus.yml \
    --storage.tsdb.path /var/lib/prometheus/ \
    --web.console.templates=/etc/prometheus/consoles \
    --web.console.libraries=/etc/prometheus/console_libraries

[Install]
WantedBy=multi-user.target
EOF
}

function configure_prometheus {
cat << EOF > /etc/prometheus/prometheus.yml
global:
  scrape_interval: 10s

scrape_configs:
  - job_name: 'statsd'
    scrape_interval: 10s
    static_configs:
      - targets: ['localhost:9102']
EOF
}

function install_statsd_exporter {
  mkdir -p /var/lib/statsd
  wget -q "https://github.com/prometheus/statsd_exporter/releases/download/v${STATSD_EXPORTER_VER}/statsd_exporter-${STATSD_EXPORTER_VER}.linux-amd64.tar.gz" && \
    tar -xf "statsd_exporter-${STATSD_EXPORTER_VER}.linux-amd64.tar.gz" && \
    cp "statsd_exporter-${STATSD_EXPORTER_VER}.linux-amd64/statsd_exporter" /var/lib/statsd/statsd_exporter

  rm -rf "statsd_exporter-${STATSD_EXPORTER_VER}.linux-amd64.tar.gz" "statsd_exporter-${STATSD_EXPORTER_VER}.linux-amd64"
 
cat << EOF > /etc/systemd/system/statsd-exporter.service
[Unit]
Description=Statsd
Wants=network-online.target
After=network-online.target

[Service]
User=root
Group=root
Type=simple
ExecStart=/var/lib/statsd/statsd_exporter \
    --web.listen-address=:9102 

[Install]
WantedBy=multi-user.target
EOF
}

function start_services {
  systemctl daemon-reload
  systemctl start statsd-exporter
  systemctl start prometheus
}

function configure_metrics {
cat << EOF > /etc/spark/conf/metrics.properties
*.sink.statsd.class=org.apache.spark.metrics.sink.StatsdSink
*.sink.statsd.prefix=spark
*.sink.statsd.port=9125
EOF

cat << EOF > /etc/hadoop/conf/hadoop-metrics2.properties
resourcemanager.sink.statsd.class=org.apache.hadoop.metrics2.sink.StatsDSink
resourcemanager.sink.statsd.server.host=${hostname}
resourcemanager.sink.statsd.server.port=9125
resourcemanager.sink.statsd.skip.hostname=true
resourcemanager.sink.statsd.service.name=RM

mrappmaster.sink.statsd.class=org.apache.hadoop.metrics2.sink.StatsDSink
mrappmaster.sink.statsd.server.host=${hostname}
mrappmaster.sink.statsd.server.port=9125
mrappmaster.sink.statsd.skip.hostname=true
mrappmaster.sink.statsd.service.name=MRAPP
EOF
}

function restart_service_gracefully {
  while true; do
    if systemctl status "$1"| grep -q 'Active: active (running)'; then
      systemctl restart "$1"
      break;
    fi
    sleep 5;
  done
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  local hostname
  hostname="$(hostname)"

  install_prometheus
  configure_prometheus
  install_statsd_exporter
  start_services
  configure_metrics

  if [[ "${role}" == 'Master' ]]; then
    restart_service_gracefully hadoop-yarn-resourcemanager.service
  else
    restart_service_gracefully hadoop-yarn-nodemanager.service
  fi

}

main