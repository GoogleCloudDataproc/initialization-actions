#!/bin/bash
#    Copyright 2019 Google, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

# This script installs Cruise Control (https://github.com/linkedin/cruise-control)
# on a Dataproc Kafka cluster.
#
# Every node running Kafka broker will be updated to use the Cruise Control
# metric reporter, and Cruise Control server will be running on the first
# master node (port 9090 by default). By default, self healing is enabled for
# broker failure, goal violation and metric anomaly.

set -euxo pipefail

readonly CRUISE_CONTROL_HOME="/opt/cruise-control"
readonly CRUISE_CONTROL_CONFIG_FILE="${CRUISE_CONTROL_HOME}/config/cruisecontrol.properties"
readonly KAFKA_HOME=/usr/lib/kafka
readonly KAFKA_CONFIG_FILE='/etc/kafka/conf/server.properties'

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly CRUISE_CONTROL_VERSION="$(/usr/share/google/get_metadata_value attributes/cruise-control-version || echo 2.0.37)"
readonly CRUISE_CONTROL_HTTP_PORT="$(/usr/share/google/get_metadata_value attributes/cruise-control-http-port || echo 9090)"
readonly SELF_HEALING_BROKER_FAILURE_ENABLED="$(/usr/share/google/get_metadata_value attributes/self-healing-broker-failure-enabled || echo true)"
readonly SELF_HEALING_GOAL_VIOLATION_ENABLED="$(/usr/share/google/get_metadata_value attributes/self-healing-goal-violation-enabled || echo true)"
readonly SELF_HEALING_METRIC_ANOMALY_ENABLED="$(/usr/share/google/get_metadata_value attributes/self-healing-metric-anomaly-enabled || echo false)"
readonly BROKER_FAILURE_ALERT_THRESHOLD_MS="$(/usr/share/google/get_metadata_value attributes/broker-failure-alert-threshold-ms || echo 120000)"
readonly BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS="$(/usr/share/google/get_metadata_value attributes/broker-failure-alert-threshold-ms || echo 300000)"

function download_cruise_control() {
  mkdir -p /opt
  pushd /opt
  git clone --branch ${CRUISE_CONTROL_VERSION} --depth 1 https://github.com/linkedin/cruise-control.git
  popd
}

function build_cruise_control() {
  pushd ${CRUISE_CONTROL_HOME}
  ./gradlew jar copyDependantLibs
  popd
}

function update_kafka_metrics_reporter() {
  if [[ ! -d "${CRUISE_CONTROL_HOME}" ]]; then
    echo "Kafka is not installed on this node ${HOSTNAME}, skip configuring Cruise Control."
    return 0
  fi

  cp ${CRUISE_CONTROL_HOME}/cruise-control-metrics-reporter/build/libs/cruise-control-metrics-reporter-${CRUISE_CONTROL_VERSION}.jar \
    ${KAFKA_HOME}/libs
  cat >>${KAFKA_CONFIG_FILE} <<EOF

# Properties added by Cruise Control init action.
metric.reporters=com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter
EOF

  systemctl restart kafka-server
}

function configure_cruise_control_server() {
  echo "Configuring cruise control server."
  cat >>"${CRUISE_CONTROL_CONFIG_FILE}" <<EOF

# Properties from the Cruise Control init action.
webserver.http.port=${CRUISE_CONTROL_HTTP_PORT}
self.healing.broker.failure.enabled=${SELF_HEALING_BROKER_FAILURE_ENABLED}
self.healing.goal.violation.enabled=${SELF_HEALING_GOAL_VIOLATION_ENABLED}
self.healing.metric.anomaly.enabled=${SELF_HEALING_METRIC_ANOMALY_ENABLED}
broker.failure.alert.threshold.ms=${BROKER_FAILURE_ALERT_THRESHOLD_MS}
broker.failure.self.healing.threshold.ms=${BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS}
anomaly.detection.goals=com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal,com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal,com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal,com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal
metric.anomaly.finder.class=com.linkedin.kafka.cruisecontrol.detector.NoopMetricAnomalyFinder

EOF
}

function start_cruise_control_server() {
  # Wait for the metrics topic to be created.
  for ((i = 1; i <= 20; i++)); do
    local metrics_topic=$(/usr/lib/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181 | grep __CruiseControlMetrics)
    if [[ -n "${metrics_topic}" ]]; then
      break
    else
      echo "Metrics topic __CruiseControlMetrics is not created yet, retry $i ..."
      sleep 5
    fi
  done

  if [[ -z "${metrics_topic}" ]]; then
    err "Metrics topic __CruiseControlMetrics was not found in the cluster."
  fi

  echo "Start Cruise Control server on ${HOSTNAME}."
  pushd ${CRUISE_CONTROL_HOME}
  ./kafka-cruise-control-start.sh config/cruisecontrol.properties &
  popd
}

function main() {
  download_cruise_control
  build_cruise_control
  update_kafka_metrics_reporter
  # Run CC on the first master node.
  if [[ "${HOSTNAME}" == *-m || "${HOSTNAME}" == *-m-0 ]]; then
    configure_cruise_control_server
    start_cruise_control_server
  fi
}

main
