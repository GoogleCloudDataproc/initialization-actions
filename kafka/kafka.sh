#!/bin/bash
#    Copyright 2015 Google, Inc.
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
#
# This script installs Apache Kafka (http://kafka.apache.org) on a Google Cloud
# Dataproc cluster.

set -euxo pipefail

readonly ZOOKEEPER_HOME=/usr/lib/zookeeper
readonly KAFKA_HOME=/usr/lib/kafka
readonly KAFKA_PROP_FILE='/etc/kafka/conf/server.properties'
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly RUN_ON_MASTER="$(/usr/share/google/get_metadata_value attributes/run-on-master || echo false)"
readonly KAFKA_ENABLE_JMX="$(/usr/share/google/get_metadata_value attributes/kafka-enable-jmx || echo false)"
readonly KAFKA_JMX_PORT="$(/usr/share/google/get_metadata_value attributes/kafka-jmx-port || echo 9999)"

# The first ZooKeeper server address, e.g., "cluster1-m-0:2181".
ZOOKEEPER_ADDRESS=''
# Integer broker ID of this node, e.g., 0
BROKER_ID=''

function retry_apt_command() {
  cmd="$1"
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function update_apt_get() {
  retry_apt_command "apt-get update"
}

function install_apt_get() {
  pkgs="$@"
  retry_apt_command "apt-get install -y $pkgs"
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

# Returns the list of broker IDs registered in ZooKeeper, e.g., " 0, 2, 1,".
function get_broker_list() {
  ${KAFKA_HOME}/bin/zookeeper-shell.sh "${ZOOKEEPER_ADDRESS}" \
    <<<"ls /brokers/ids" |
    grep '\[.*\]' |
    sed 's/\[/ /' |
    sed 's/\]/,/'
}

# Waits for zookeeper to be up or time out.
function wait_for_zookeeper() {
  for i in {1..20}; do
    if "${ZOOKEEPER_HOME}/bin/zkCli.sh" -server "${ZOOKEEPER_ADDRESS}" ls /; then
      return 0
    else
      echo "Failed to connect to ZooKeeper ${ZOOKEEPER_ADDRESS}, retry ${i}..."
      sleep 5
    fi
  done
  echo "Failed to connect to ZooKeeper ${ZOOKEEPER_ADDRESS}" >&2
  exit 1
}

# Wait until the current broker is registered or time out.
function wait_for_kafka() {
  for i in {1..20}; do
    local broker_list=$(get_broker_list || true)
    if [[ "${broker_list}" == *" ${BROKER_ID},"* ]]; then
      return 0
    else
      echo "Kafka broker ${BROKER_ID} is not registered yet, retry ${i}..."
      sleep 5
    fi
  done
  echo "Failed to start Kafka broker ${BROKER_ID}." >&2
  exit 1
}

function install_and_configure_kafka_server() {
  # Find zookeeper list first, before attempting any installation.
  local zookeeper_client_port
  zookeeper_client_port=$(grep 'clientPort' /etc/zookeeper/conf/zoo.cfg |
    tail -n 1 |
    cut -d '=' -f 2)

  local zookeeper_list
  zookeeper_list=$(grep '^server\.' /etc/zookeeper/conf/zoo.cfg |
    cut -d '=' -f 2 |
    cut -d ':' -f 1 |
    sort |
    uniq |
    sed "s/$/:${zookeeper_client_port}/" |
    xargs echo |
    sed "s/ /,/g")

  if [[ -z "${zookeeper_list}" ]]; then
    # Didn't find zookeeper quorum in zoo.cfg, but possibly workers just didn't
    # bother to populate it. Check if YARN HA is configured.
    zookeeper_list=$(bdconfig get_property_value --configuration_file \
      /etc/hadoop/conf/yarn-site.xml \
      --name yarn.resourcemanager.zk-address 2>/dev/null)
  fi

  # If all attempts failed, error out.
  if [[ -z "${zookeeper_list}" ]]; then
    err 'Failed to find configured Zookeeper list; try "--num-masters=3" for HA'
  fi

  ZOOKEEPER_ADDRESS="${zookeeper_list%%,*}"

  # Install Kafka from Dataproc distro.
  install_apt_get kafka-server || dpkg -l kafka-server ||
    err 'Unable to install and find kafka-server.'

  mkdir -p /var/lib/kafka-logs
  chown kafka:kafka -R /var/lib/kafka-logs

  if [[ "${ROLE}" == "Master" ]]; then
    # For master nodes, broker ID starts from 10,000.
    if [[ "$(hostname)" == *-m ]]; then
      # non-HA
      BROKER_ID=10000
    else
      # HA
      BROKER_ID=$((10000 + $(hostname | sed 's/.*-m-\([0-9]*\)$/\1/g')))
    fi
  else
    # For worker nodes, broker ID is the worker ID.
    BROKER_ID=$(hostname | sed 's/.*-w-\([0-9]*\)$/\1/g')
  fi
  sed -i 's|log.dirs=/tmp/kafka-logs|log.dirs=/var/lib/kafka-logs|' \
    "${KAFKA_PROP_FILE}"
  sed -i 's|^\(zookeeper\.connect=\).*|\1'${zookeeper_list}'|' \
    "${KAFKA_PROP_FILE}"
  sed -i 's,^\(broker\.id=\).*,\1'${BROKER_ID}',' \
    "${KAFKA_PROP_FILE}"
  echo -e '\nreserved.broker.max.id=100000' >>"${KAFKA_PROP_FILE}"
  echo -e '\ndelete.topic.enable=true' >>"${KAFKA_PROP_FILE}"

  if [[ "${KAFKA_ENABLE_JMX}" == "true" ]]; then
    sed -i '/kafka-run-class.sh/i export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=localhost -Djava.net.preferIPv4Stack=true"' /usr/lib/kafka/bin/kafka-server-start.sh
    sed -i "/kafka-run-class.sh/i export JMX_PORT=${KAFKA_JMX_PORT}" /usr/lib/kafka/bin/kafka-server-start.sh
  fi

  wait_for_zookeeper

  # Start Kafka.
  service kafka-server restart

  wait_for_kafka
}

function main() {
  update_apt_get || err 'Unable to update packages lists.'

  # Only run the installation on workers; verify zookeeper on master(s).
  if [[ "${ROLE}" == 'Master' ]]; then
    service zookeeper-server status ||
      err 'Required zookeeper-server not running on master!'
    if [[ "${RUN_ON_MASTER}" == "true" ]]; then
      # Run installation on masters.
      install_and_configure_kafka_server
    else
      # On master nodes, just install kafka command-line tools and libs but not
      # kafka-server.
      install_apt_get kafka ||
        err 'Unable to install kafka libraries on master!'
    fi
  else
    # Run installation on workers.
    install_and_configure_kafka_server
  fi
}

main
