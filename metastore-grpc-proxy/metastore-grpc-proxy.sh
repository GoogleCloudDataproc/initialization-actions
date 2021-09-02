#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script installs a proxy that will allow a Google Cloud Dataproc cluster
# to use a Hive Metastore that exposes a gRPC endpoint.

set -euxo pipefail

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly DATAPROC_MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly CONTAINER_NAME=$(/usr/share/google/get_metadata_value attributes/container-name || echo "hms-proxy")
readonly TAG=$(/usr/share/google/get_metadata_value attributes/tag || echo "v0.0.2")
readonly PROXY_MODE=$(/usr/share/google/get_metadata_value attributes/proxy-mode || echo "thrift")
# This is what the T2G proxy connects to. Set this to the DPMS gRPC endpoint.
readonly PROXY_URI=$(/usr/share/google/get_metadata_value attributes/proxy-uri | sed -e 's/^https:\/\///')
readonly GRPC_LISTENING_PORT=$(/usr/share/google/get_metadata_value attributes/grpc-listening-port || echo "9085")
# Point Dataproc components at this port.
readonly THRIFT_LISTENING_PORT=$(/usr/share/google/get_metadata_value attributes/thrift-listening-port || echo "9083")
# This is what the G2T proxy connects to. Set this to the DPMS Thrift endpoint.
readonly HIVE_METASTORE_URI=$(/usr/share/google/get_metadata_value attributes/hive-metastore-uri || echo "$(hostname):9083")
readonly HIVE_VERSION=$(/usr/share/google/get_metadata_value attributes/hive-version)
readonly DOCKER_IMAGE=$(/usr/share/google/get_metadata_value attributes/docker-image || echo "gcr.io/cloud-metastore-public/hms-grpc-proxy/${HIVE_VERSION}")

COMMON_PROXY_ARG="proxy.uri=${PROXY_URI} grpc.listening.port=${GRPC_LISTENING_PORT} thrift.listening.port=${THRIFT_LISTENING_PORT} hive.metastore.uri=${HIVE_METASTORE_URI} hive.version=${HIVE_VERSION} google.credentials.applicationdefault.enabled=true proxy.grpc.ssl.upstream.enabled=true"
ADDITIONAL_DOCKER_VOLUMES=""

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  exit 1
}

function is_docker_installed() {
  if [[ "$(systemctl is-active docker)" = "active" ]]; then
    return 0
  fi
  return 1
}

function pull_docker_image() {
  echo "DEBUG: Pulling docker image..."
  if is_docker_installed; then
    sudo docker pull "${DOCKER_IMAGE}:${TAG}"
  else
    err 'Docker command not available. Please install Docker and try again.'
  fi
}

function run_proxy() {
  echo "DEBUG: Running proxy..."
  docker run -dt \
    -p "${THRIFT_LISTENING_PORT}:${THRIFT_LISTENING_PORT}" \
    -p "${GRPC_LISTENING_PORT}:${GRPC_LISTENING_PORT}" \
    -p 5005:5005 \
    -e HIVE_HOME=/etc/hive \
    -v "/etc/hive/conf/hive-site.xml:/etc/hive/conf/hive-site.xml:ro" \
    -v "/etc/hadoop/conf/core-site.xml:/etc/hadoop/conf/core-site.xml:ro" \
    ${ADDITIONAL_DOCKER_VOLUMES} \
    --name="${CONTAINER_NAME}" "${DOCKER_IMAGE}:${TAG}" \
    --conf proxy.mode=${PROXY_MODE} ${COMMON_PROXY_ARG}
}

function stop_master_hms() {
  systemctl stop hive-metastore.service
}

function restart_hive_server() {
  systemctl restart hive-server2.service
}

function wait_for_hive_server_init() {
  # Timeout 5 min
  for ((i = 0; i < 60; i++)); do
    if `docker logs thrift | grep -q 'GetTablesRequest'`; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function main() {
  if [[ "${ROLE}" == 'Master' ]]; then
    stop_master_hms
  fi
  pull_docker_image
  run_proxy
  if [[ "${ROLE}" == 'Master' ]]; then
    restart_hive_server
  fi
}

main "$@"
