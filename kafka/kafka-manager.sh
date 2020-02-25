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

# This script installs Kafka Manager (https://github.com/yahoo/kafka-manager)
# on a Dataproc cluster.
#
# Kafka Manager server will be running on the first master node (port 9000 by default).

set -euxo pipefail

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly KAFKA_MANAGER_VERSION="$(/usr/share/google/get_metadata_value attributes/kafka-manager-version || echo 2.0.0.2)"
readonly KAFKA_MANAGER_HTTP_PORT="$(/usr/share/google/get_metadata_value attributes/kafka-manager-http-port || echo 9000)"
readonly KAFKA_VERSION="$(/usr/share/google/get_metadata_value attributes/kafka-version || echo 1.1.1)"
readonly KAFKA_ENABLE_JMX="$(/usr/share/google/get_metadata_value attributes/kafka-enable-jmx || echo false)"

readonly KAFKA_MANAGER_HOME="/opt/kafka-manager-${KAFKA_MANAGER_VERSION}"
readonly KAFKA_MANAGER_CONFIG="${KAFKA_MANAGER_HOME}/conf/application.conf"
readonly KAFKA_MANAGER_GIT_URI=https://github.com/yahoo/kafka-manager.git
readonly KAFKA_MANAGER_GIT_DIR=/tmp/kafka-manager
readonly ZOOKEEPER_HOME=/usr/lib/zookeeper
readonly ZOOKEEPER_CONFIG=/etc/zookeeper/conf/zoo.cfg

function download_kafka_manager() {
  git clone --branch "${KAFKA_MANAGER_VERSION}" --depth 1 "${KAFKA_MANAGER_GIT_URI}" "${KAFKA_MANAGER_GIT_DIR}"
}

function build_kafka_manager() {
  pushd ${KAFKA_MANAGER_GIT_DIR}
  ./sbt clean dist
  mv target/universal/kafka-manager-${KAFKA_MANAGER_VERSION}.zip /opt/
  rm -rf "${KAFKA_MANAGER_GIT_DIR}"
  popd

  pushd /opt/
  unzip -q /opt/kafka-manager-${KAFKA_MANAGER_VERSION}.zip
  rm -f /opt/kafka-manager-${KAFKA_MANAGER_VERSION}.zip
  popd
}

# Returns list of zookeeper servers configured in the zoo.cfg file.
function get_zookeeper_list() {
  local zookeeper_client_port=$(grep 'clientPort' "${ZOOKEEPER_CONFIG}" |
    tail -n 1 |
    cut -d '=' -f 2)
  local zookeeper_list=$(grep '^server.' "${ZOOKEEPER_CONFIG}" |
    tac |
    sort -u -t '=' -k1,1 |
    cut -d '=' -f 2 |
    cut -d ':' -f 1 |
    sed "s/$/:${zookeeper_client_port}/" |
    xargs echo |
    sed "s/ /,/g")
  echo "${zookeeper_list}"
}

# Waits until a ZNode exists in Zookeeper or time out.
function wait_for_zookeeper() {
  local path="$1"
  local zookeeper_home=/usr/lib/zookeeper
  local zookeeper_list=$(get_zookeeper_list)
  local zookeer_address="${zookeeper_list%%,*}"
  for i in {1..20}; do
    local result=$("${ZOOKEEPER_HOME}/bin/zkCli.sh" -server "${zookeer_address}" ls "${path}" 2>&1 1>/dev/null | grep "Node does not exist")
    if [[ -z "${result}" ]]; then
      return 0
    else
      echo "ZNode ${path} was not found in ZooKeeper ${zookeer_address}, retry ${i}..."
      sleep 5
    fi
  done
  echo "ZNode ${path} was not found in ZooKeeper ${zookeer_address}" >&2
  exit 1
}

function configure_kafka_manager() {
  local zookeeper_list="$(get_zookeeper_list)"
  local zookeeper_address="${zookeeper_list%%,*}"

  cat >>${KAFKA_MANAGER_CONFIG} <<EOF

# Properties from Kafka manager init action.
kafka-manager.zkhosts="${zookeeper_list}"
kafka-manager.zkhosts=\${?ZK_HOSTS}
EOF
}

function add_cluster_to_zookeeper() {
  local zookeeper_list="$(get_zookeeper_list)"
  local zookeeper_address="${zookeeper_list%%,*}"
  local cluster_name=$(hostname | sed 's/\(.*\)-m-0$/\1/g')
  local cluster_config="{\
    \"name\":\"${cluster_name}\",\
    \"curatorConfig\": {\
        \"zkConnect\":\"${zookeeper_list}\",\
        \"zkMaxRetry\":100,\
        \"baseSleepTimeMs\":100,\
        \"maxSleepTimeMs\":1000},\
        \"enabled\":true,\
        \"kafkaVersion\":\"${KAFKA_VERSION}\",\
        \"jmxEnabled\":${KAFKA_ENABLE_JMX},\
        \"jmxUser\":null,\
        \"jmxPass\":null,\
        \"jmxSsl\":false,\
        \"pollConsumers\":false,\
        \"filterConsumers\":false,\
        \"logkafkaEnabled\":false,\
        \"activeOffsetCacheEnabled\":false,\
        \"displaySizeEnabled\":false,\
        \"tuning\":{
             \"brokerViewUpdatePeriodSeconds\":30,\
               \"clusterManagerThreadPoolSize\":2,\
               \"clusterManagerThreadPoolQueueSize\":100,\
               \"kafkaCommandThreadPoolSize\":2,\
               \"kafkaCommandThreadPoolQueueSize\":100,\
               \"logkafkaCommandThreadPoolSize\":2,\
               \"logkafkaCommandThreadPoolQueueSize\":100,\
               \"logkafkaUpdatePeriodSeconds\":30,\
               \"partitionOffsetCacheTimeoutSecs\":5,\
               \"brokerViewThreadPoolSize\":2,\
               \"brokerViewThreadPoolQueueSize\":1000,\
               \"offsetCacheThreadPoolSize\":2,\
               \"offsetCacheThreadPoolQueueSize\":1000,\
               \"kafkaAdminClientThreadPoolSize\":2,\
               \"kafkaAdminClientThreadPoolQueueSize\":1000,\
               \"kafkaManagedOffsetMetadataCheckMillis\":30000,\
               \"kafkaManagedOffsetGroupCacheSize\":1000000,\
               \"kafkaManagedOffsetGroupExpireDays\":7},\
        \"securityProtocol\":\"PLAINTEXT\",\
        \"saslMechanism\":null,\
        \"jaasConfig\":null}"
  ${ZOOKEEPER_HOME}/bin/zkCli.sh -server "${zookeeper_address}" create "/kafka-manager/configs/${cluster_name}" "${cluster_config}"
}

function start_kafka_manager() {
  wait_for_zookeeper /

  echo "Starting Kafka Manager server on ${HOSTNAME}:${KAFKA_MANAGER_HTTP_PORT}."
  ${KAFKA_MANAGER_HOME}/bin/kafka-manager \
    -Dconfig.file=${KAFKA_MANAGER_CONFIG} \
    -Dapplication.home=${KAFKA_MANAGER_HOME} \
    -Dhttp.port=${KAFKA_MANAGER_HTTP_PORT} &

  # Wait until znode /kafka-manager/configs is created by Kafka Manager.
  wait_for_zookeeper /kafka-manager/configs
  add_cluster_to_zookeeper
}

function main() {
  # Run Kafka Manager on the first master node.
  if [[ "${HOSTNAME}" == *-m || "${HOSTNAME}" == *-m-0 ]]; then
    download_kafka_manager
    build_kafka_manager
    configure_kafka_manager
    start_kafka_manager
  fi
}

main
