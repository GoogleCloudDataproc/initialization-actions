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

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
}

function install_and_configure_kafka_server() {
  # Find zookeeper list first, before attempting any installation.
  local ZOOKEEPER_CLIENT_PORT
  local ZOOKEEPER_LIST
  readonly KAFKA_PROP_FILE='/etc/kafka/conf/server.properties'

  ZOOKEEPER_CLIENT_PORT=$(grep 'clientPort' /etc/zookeeper/conf/zoo.cfg \
    | cut -d '=' -f 2)
  ZOOKEEPER_LIST=$(grep '^server\.' /etc/zookeeper/conf/zoo.cfg \
    | cut -d '=' -f 2 \
    | cut -d ':' -f 1 \
    | sed "s/$/:${ZOOKEEPER_CLIENT_PORT}/" \
    | xargs echo  \
    | sed "s/ /,/g")

  if [[ -z "${ZOOKEEPER_LIST}" ]]; then
    # Didn't find zookeeper quorum in zoo.cfg, but possibly workers just didn't
    # bother to populate it. Check if YARN HA is configured.
    ZOOKEEPER_LIST=$(bdconfig get_property_value --configuration_file \
      /etc/hadoop/conf/yarn-site.xml \
      --name yarn.resourcemanager.zk-address 2>/dev/null)
  fi

  # If all attempts failed, error out.
  if [[ -z "${ZOOKEEPER_LIST}" ]]; then
    err 'Failed to find configured Zookeeper list; try --num-masters=3 for HA'
    exit 1
  fi

  # Install Kafka from Dataproc distro.
  apt-get install -y kafka-server \
    || (err 'Unable to install kafka-server' && exit 1)

  mkdir -p /var/lib/kafka-logs
  chown kafka:kafka -R /var/lib/kafka-logs

  # Note: If modified to also run brokers on master nodes, this logic for
  # generating BROKER_ID will need to be changed.
  BROKER_ID=$(hostname | sed 's/.*-w-\([0-9]\)*.*/\1/g')
  sed -i 's|log.dirs=/tmp/kafka-logs|log.dirs=/var/lib/kafka-logs|' \
    "${KAFKA_PROP_FILE}"
  sed -i 's|^\(zookeeper\.connect=\).*|\1'${ZOOKEEPER_LIST}'|' \
    "${KAFKA_PROP_FILE}"
  sed -i 's,^\(broker\.id=\).*,\1'${BROKER_ID}',' \
    "${KAFKA_PROP_FILE}"
  echo 'delete.topic.enable = true' >> "${KAFKA_PROP_FILE}"

  # Start Kafka.
  service kafka-server restart
}

function main() {
  local ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  update_apt_get || (err 'Unable to update packages lists.' && exit 1)

  # Only run the installation on workers; verify zookeeper on master(s).
  if [[ "${ROLE}" == 'Master' ]]; then
    service zookeeper-server status \
      || (err 'Required zookeeper-server not running on master!' && exit 1)
    # On master nodes, just install kafka libs but not kafka-server.
    apt-get install -y kafka \
      || (err 'Unable to install kafka libraries on master!' && exit 1)
    exit 0
  fi

  # Run installation on workers.
  install_and_configure_kafka_server

}

main
