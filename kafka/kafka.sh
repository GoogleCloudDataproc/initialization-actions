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

readonly KAFKA_PROP_FILE='/etc/kafka/conf/server.properties'
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly RUN_ON_MASTER="$(/usr/share/google/get_metadata_value attributes/run-on-master)"

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

function install_and_configure_kafka_server() {
  # Find zookeeper list first, before attempting any installation.
  local zookeeper_client_port
  zookeeper_client_port=$(grep 'clientPort' /etc/zookeeper/conf/zoo.cfg \
    | tail -n 1 \
    | cut -d '=' -f 2)

  local zookeeper_list
  zookeeper_list=$(grep '^server\.' /etc/zookeeper/conf/zoo.cfg \
    | cut -d '=' -f 2 \
    | cut -d ':' -f 1 \
    | sort \
    | uniq \
    | sed "s/$/:${zookeeper_client_port}/" \
    | xargs echo  \
    | sed "s/ /,/g")

  if [[ -z "${zookeeper_list}" ]]; then
    # Didn't find zookeeper quorum in zoo.cfg, but possibly workers just didn't
    # bother to populate it. Check if YARN HA is configured.
    zookeeper_list=$(bdconfig get_property_value --configuration_file \
      /etc/hadoop/conf/yarn-site.xml \
      --name yarn.resourcemanager.zk-address 2>/dev/null)
  fi

  # If all attempts failed, error out.
  if [[ -z "${zookeeper_list}" ]]; then
    err 'Failed to find configured Zookeeper list; try --num-masters=3 for HA'
  fi

  # Install Kafka from Dataproc distro.
  install_apt_get kafka-server || dpkg -l kafka-server \
    || err 'Unable to install and find kafka-server on worker node.'

  mkdir -p /var/lib/kafka-logs
  chown kafka:kafka -R /var/lib/kafka-logs

  local broker_id
  if [[ "${ROLE}" == "Master" ]]; then
    # For master nodes, broker ID starts from 10,000.
    if [[ "$(hostname)" == *-m ]]; then
      # non-HA
      broker_id=10000
    else
      # HA
      broker_id=$((10000 + $(hostname | sed 's/.*-m-\([0-9]*\)$/\1/g')))
    fi
  else
    # For worker nodes, broker ID is the worker ID.
    broker_id=$(hostname | sed 's/.*-w-\([0-9]*\)$/\1/g')
  fi
  sed -i 's|log.dirs=/tmp/kafka-logs|log.dirs=/var/lib/kafka-logs|' \
    "${KAFKA_PROP_FILE}"
  sed -i 's|^\(zookeeper\.connect=\).*|\1'${zookeeper_list}'|' \
    "${KAFKA_PROP_FILE}"
  sed -i 's,^\(broker\.id=\).*,\1'${broker_id}',' \
    "${KAFKA_PROP_FILE}"
  echo -e '\nreserved.broker.max.id=100000' >> "${KAFKA_PROP_FILE}"
  echo -e '\ndelete.topic.enable=true' >> "${KAFKA_PROP_FILE}"

  # Start Kafka.
  service kafka-server restart
}

function main() {
  update_apt_get || err 'Unable to update packages lists.'

  # Only run the installation on workers; verify zookeeper on master(s).
  if [[ "${ROLE}" == 'Master' ]]; then
    service zookeeper-server status \
      || err 'Required zookeeper-server not running on master!'
    if [[ "${RUN_ON_MASTER}" == "true" ]]; then
      # Run installation on masters.
      install_and_configure_kafka_server
    else
      # On master nodes, just install kafka command-line tools and libs but not
      # kafka-server.
      install_apt_get kafka \
        || err 'Unable to install kafka libraries on master!'
    fi
  else
    # Run installation on workers.
    install_and_configure_kafka_server
  fi
}

main
