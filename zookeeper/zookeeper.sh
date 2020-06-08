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
set -o errexit
set -o xtrace

readonly NOT_SUPPORTED_MESSAGE="Zookeeper initialization action is not supported on Dataproc 2.0+.
Use Zookeeper Component instead: https://cloud.google.com/dataproc/docs/concepts/components/zookeeper"
[[ $DATAPROC_VERSION = 2.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

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

function write_config() {
  cat >>/etc/zookeeper/conf/zoo.cfg <<EOF

# Properties from Zookeeper init action.
tickTime=2000
dataDir=/var/lib/zookeeper
clientPort=2181
initLimit=5
syncLimit=2
server.0=${CLUSTER_NAME}-m:2888:3888
EOF

  if [[ ${WORKER_COUNT} -gt 0 ]]; then
    cat >>/etc/zookeeper/conf/zoo.cfg <<EOF
server.1=${CLUSTER_NAME}-w-0:2888:3888
server.2=${CLUSTER_NAME}-w-1:2888:3888
EOF
  fi
}

# Variables for this script
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
CLUSTER_NAME=$(hostname | sed -r 's/(.*)-[w|m](-[0-9]+)?$/\1/')
WORKER_COUNT=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)

# Validate the cluster mode and worker count.
ADDITIONAL_MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)
if [[ -n "$ADDITIONAL_MASTER" ]]; then
  echo "ZooKeeper init action cannot be used in HA clusters which already have ZooKeeper running."
  exit 1
fi

# Configure ZooKeeper node ID, master has ID 0, workers start from 1.
if [[ "${ROLE}" == 'Worker' ]]; then
  NODE_NUMBER=$(($(hostname | sed 's/.*-w-\([0-9]\)*.*/\1/g') + 1))
else
  NODE_NUMBER=0
fi

if (($NODE_NUMBER > 2)); then
  write_config
  echo "Skip running ZooKeeper on this node."
  exit 0
fi

# Download and extract ZooKeeper Server
update_apt_get
install_apt_get zookeeper-server

# Write ZooKeeper node ID.
mkdir -p /var/lib/zookeeper
echo ${NODE_NUMBER} >|/var/lib/zookeeper/myid

# Write ZooKeeper configuration file
write_config

# Restart ZooKeeper
systemctl restart zookeeper-server
