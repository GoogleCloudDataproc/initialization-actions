#!/usr/bin/env bash
#
# This init action install prometheus on dataproc cluster.
# Prometheus is time series datatabase that allows to easily monitor
# cluster parameters, JMX properties and other data during job execution.
#

# This init actions installs Prometheus (https://prometheus.io) in a Dataproc
# cluster, performs necessary configurations and pulls metrics from Hadoop,
# Spark and Kafka if installed.
#
# By default, Prometheus server listens on HTTP port 9090.

set -euxo pipefail

readonly PROMETHEUS_VER='2.5.0'
readonly STATSD_EXPORTER_VER='0.8.0'

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly MONITOR_HADOOP="$(/usr/share/google/get_metadata_value attributes/monitor-hadoop || echo true)"
readonly MONITOR_SPARK="$(/usr/share/google/get_metadata_value attributes/monitor-spark || echo true)"
readonly MONITOR_KAFKA="$(/usr/share/google/get_metadata_value attributes/monitor-kafka || echo true)"
readonly PROMETHEUS_HTTP_PORT="$(/usr/share/google/get_metadata_value attributes/prometheus-http-port || echo 9090)"
readonly KAFKA_JMX_EXPORTER_PORT="$(/usr/share/google/get_metadata_value attributes/kafka-jmx-exporter-port || echo 7071)"

readonly KAFKA_CONFIG_DIR=/etc/kafka/conf
readonly KAFKA_CONFIG_FILE=${KAFKA_CONFIG_DIR}/server.properties
readonly KAFKA_LIBS_DIR=/usr/lib/kafka/libs
readonly KAFKA_JMX_JAVAAGENT_VERSION='0.11.0'
readonly KAFKA_JMX_JAVAAGENT_NAME="jmx_prometheus_javaagent-${KAFKA_JMX_JAVAAGENT_VERSION}.jar"
readonly KAFKA_JMX_JAVAAGENT_URI="http://central.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${KAFKA_JMX_JAVAAGENT_VERSION}/${KAFKA_JMX_JAVAAGENT_NAME}"
readonly KAFKA_JMX_EXPORTER_CONFIG_NAME="kafka-0-8-2.yml"
readonly KAFKA_JMX_EXPORTER_CONFIG_URI="https://raw.githubusercontent.com/prometheus/jmx_exporter/master/example_configs/${KAFKA_JMX_EXPORTER_CONFIG_NAME}"

function is_kafka_installed() {
  local result="$(cat ${KAFKA_CONFIG_FILE} | grep broker.id | tail -1)"
  if [[ "${result}" == "broker.id=0" ]]; then
    return 1
  else
    return 0
  fi
}

function install_prometheus() {
  mkdir -p /etc/prometheus /var/lib/prometheus
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VER}/prometheus-${PROMETHEUS_VER}.linux-amd64.tar.gz"
  tar -xf "prometheus-${PROMETHEUS_VER}.linux-amd64.tar.gz"
  cp "prometheus-${PROMETHEUS_VER}.linux-amd64/prometheus" /usr/local/bin/
  cp "prometheus-${PROMETHEUS_VER}.linux-amd64/promtool" /usr/local/bin/
  cp -r "prometheus-${PROMETHEUS_VER}.linux-amd64/consoles" /etc/prometheus
  cp -r "prometheus-${PROMETHEUS_VER}.linux-amd64/console_libraries" /etc/prometheus

  rm -rf "prometheus-${PROMETHEUS_VER}.linux-amd64.tar.gz" "prometheus-${PROMETHEUS_VER}.linux-amd64"

  cat <<EOF >/etc/systemd/system/prometheus.service
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
    --web.console.libraries=/etc/prometheus/console_libraries \
    --web.listen-address=:${PROMETHEUS_HTTP_PORT}

[Install]
WantedBy=multi-user.target
EOF
}

function configure_prometheus() {
  # Statsd for Hadoop and Spark.
  cat <<EOF >/etc/prometheus/prometheus.yml
global:
  scrape_interval: 10s
  evaluation_interval: 10s

scrape_configs:
  - job_name: 'statsd'
    scrape_interval: 10s
    static_configs:
      - targets: ['localhost:9102']
EOF

  # Kafka JMX exporter.
  if [[ "${MONITOR_KAFKA}" == "true" ]] && is_kafka_installed; then
    cat <<EOF >>/etc/prometheus/prometheus.yml
  - job_name: 'kafka'
    static_configs:
      - targets: ['localhost:${KAFKA_JMX_EXPORTER_PORT}']
EOF
  fi
}

function install_statsd_exporter() {
  mkdir -p /var/lib/statsd
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "https://github.com/prometheus/statsd_exporter/releases/download/v${STATSD_EXPORTER_VER}/statsd_exporter-${STATSD_EXPORTER_VER}.linux-amd64.tar.gz"
  tar -xf "statsd_exporter-${STATSD_EXPORTER_VER}.linux-amd64.tar.gz"
  cp "statsd_exporter-${STATSD_EXPORTER_VER}.linux-amd64/statsd_exporter" /var/lib/statsd/statsd_exporter

  rm -rf "statsd_exporter-${STATSD_EXPORTER_VER}.linux-amd64.tar.gz" "statsd_exporter-${STATSD_EXPORTER_VER}.linux-amd64"

  cat <<EOF >/etc/systemd/system/statsd-exporter.service
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

function install_jmx_exporter() {
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${KAFKA_JMX_JAVAAGENT_URI}" -P "${KAFKA_LIBS_DIR}"
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${KAFKA_JMX_EXPORTER_CONFIG_URI}" -P "${KAFKA_CONFIG_DIR}"
  sed -i "/kafka-run-class.sh/i export KAFKA_OPTS=\"\${KAFKA_OPTS} -javaagent:${KAFKA_LIBS_DIR}/${KAFKA_JMX_JAVAAGENT_NAME}=${KAFKA_JMX_EXPORTER_PORT}:${KAFKA_CONFIG_DIR}/${KAFKA_JMX_EXPORTER_CONFIG_NAME}\"" \
    /usr/lib/kafka/bin/kafka-server-start.sh
}

function start_services() {
  systemctl daemon-reload
  systemctl start statsd-exporter
  systemctl start prometheus
}

function configure_hadoop() {
  cat <<EOF >/etc/hadoop/conf/hadoop-metrics2.properties
resourcemanager.sink.statsd.class=org.apache.hadoop.metrics2.sink.StatsDSink
resourcemanager.sink.statsd.server.host=${HOSTNAME}
resourcemanager.sink.statsd.server.port=9125
resourcemanager.sink.statsd.skip.hostname=true
resourcemanager.sink.statsd.service.name=RM

mrappmaster.sink.statsd.class=org.apache.hadoop.metrics2.sink.StatsDSink
mrappmaster.sink.statsd.server.host=${HOSTNAME}
mrappmaster.sink.statsd.server.port=9125
mrappmaster.sink.statsd.skip.hostname=true
mrappmaster.sink.statsd.service.name=MRAPP
EOF

  if [[ "${ROLE}" == 'Master' ]]; then
    restart_service_gracefully hadoop-yarn-resourcemanager.service
  else
    restart_service_gracefully hadoop-yarn-nodemanager.service
  fi
}

function configure_spark() {
  cat <<EOF >/etc/spark/conf/metrics.properties
*.sink.statsd.class=org.apache.spark.metrics.sink.StatsdSink
*.sink.statsd.prefix=spark
*.sink.statsd.port=9125
EOF
}

function configure_kafka() {
  install_jmx_exporter
  systemctl restart kafka-server.service
}

function configure_components() {
  if [[ "${MONITOR_HADOOP}" == "true" || "${MONITOR_SPARK}" == "true" ]]; then
    install_statsd_exporter

    if [[ "${MONITOR_HADOOP}" == "true" ]]; then
      configure_hadoop
    fi

    if [[ "${MONITOR_SPARK}" == "true" ]]; then
      configure_spark
    fi
  fi

  if [[ "${MONITOR_KAFKA}" == "true" ]] && is_kafka_installed; then
    configure_kafka
  fi
}

function restart_service_gracefully() {
  while true; do
    if systemctl status "$1" | grep -q 'Active: active (running)'; then
      systemctl restart "$1"
      break
    fi
    sleep 5
  done
}

function main() {
  install_prometheus
  configure_prometheus
  configure_components
  start_services
}

main
