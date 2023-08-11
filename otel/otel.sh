#!/usr/bin/env bash

# This initialization action installs OpenTelemetry Collector Contrib (otelcol-contrib) in a Dataproc cluster
# configures OTel and pulls metrics from Prometheus endpoints provided by the customer. 
# The metrics are then exported to Google Cloud Monitoring (GCM).

readonly version="0.81.0"
readonly OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly PROMETHEUS_ENDPOINTS="$(/usr/share/google/get_metadata_value attributes/prometheus-scrape-endpoints || '')"
readonly MASTER_ONLY="$(/usr/share/google/get_metadata_value attributes/master-only || false)"

function install_otel() {
  # Install otelcol-contrib as package (https://github.com/open-telemetry/opentelemetry-collector-contrib)
  local -r download_url="https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v${version}/otelcol-contrib_${version}_linux_amd64"
  if [[ "${OS_NAME}" == "rocky" ]]; then
    wget ${download_url}.rpm
    yum install -y "otelcol-contrib_${version}_linux_amd64.rpm"
  else
    wget ${download_url}.deb
    dpkg -i "otelcol-contrib_${version}_linux_amd64.deb"
  fi
}

function configure_endpoints() {
  endpoints_array=($PROMETHEUS_ENDPOINTS)
  prometheus_scrape_endpoints=''

  # Storing the list of endpoints in a comma separated string
  for element in "${endpoints_array[@]}"
  do
  prometheus_scrape_endpoints+="'$element',"
  done

  # Remove the extra comma if endpoints passed
  length=${#prometheus_scrape_endpoints}
  if [ ${length} -gt 0 ]; then
      prometheus_scrape_endpoints="${prometheus_scrape_endpoints: : -1}"
  fi
}

function configure_otel(){
  # OTel reads config from /etc/otelcol-contrib/config.yaml
  # Configuring prometheus_receiver scraping endpoints if passed, default: empty

  cat >/etc/otelcol-contrib/config.yaml<<EOF
receivers:
  prometheus:
    config:
      scrape_configs:
      - job_name: 'otel-collector'
        scrape_interval: 10s
        static_configs:
        - targets: [${prometheus_scrape_endpoints}]

processors:
  resourcedetection:
    detectors: [gcp]
    timeout: 10s

exporters:
  googlecloud:
    metric:
      prefix: "custom.googleapis.com"
      instrumentation_library_labels: "false"
      service_resource_labels: "false"
service:
  pipelines:
    metrics:
      receivers: [prometheus]
      processors: [resourcedetection]
      exporters: [googlecloud]
EOF
}

function start_services(){
  systemctl daemon-reload
  systemctl start otelcol-contrib
}

function main(){

  if [ "${ROLE}" != 'Master' ] && [ $MASTER_ONLY == true ]; then
    return
  fi

  install_otel
  configure_endpoints
  configure_otel
  start_services
}

main
