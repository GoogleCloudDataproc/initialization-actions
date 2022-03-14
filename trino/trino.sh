#!/bin/bash

#    Copyright 2022 Google, Inc.
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

set -euxo pipefail


# Use Python from /usr/bin instead of /opt/conda.
export PATH=/usr/bin:$PATH

# Variables for running this script
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly TRINO_MASTER_FQDN="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
readonly WORKER_COUNT=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
if [[ -d /usr/local/share/google/dataproc/lib ]]; then
  readonly CONNECTOR_JAR="$(find /usr/local/share/google/dataproc/lib -name 'gcs-connector-*.jar')"
else
  readonly CONNECTOR_JAR="$(find /usr/lib/hadoop/lib -name 'gcs-connector-*.jar')"
fi
# readonly TRINO_BASE_URL=https://repo1.maven.org/maven2/com/facebook/trino

readonly TRINO_BASE_URL=https://repo1.maven.org/maven2/io/trino
readonly TRINO_VERSION='369'
readonly HTTP_PORT="$(/usr/share/google/get_metadata_value attributes/trino-port || echo 8080)"
readonly INIT_SCRIPT='/usr/lib/systemd/system/trino.service'
readonly BQ_PROJECT_ID="$(/usr/share/google/get_metadata_value project-id)"
TRINO_JVM_MB=0
TRINO_QUERY_NODE_MB=0
TRINO_RESERVED_SYSTEM_MB=0
# Allocate some headroom for untracked memory usage (in the heap and to help GC).
TRINO_HEADROOM_NODE_MB=256

# Trino requires Java 11+. Below 2 lines updates Java:
sudo apt --assume-yes update
sudo apt --assume-yes install default-jre

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  return 1
}

function wait_for_trino_cluster_ready() {
  # wait up to 120s for trino being able to run query
  for ((i = 0; i < 12; i++)); do
    if trino "--server=localhost:${HTTP_PORT}" --execute='select * from system.runtime.nodes;'; then
      return 0
    fi
    sleep 10
  done
  return 1
}

# Download and unpack Trino Server
function get_trino() {
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    ${TRINO_BASE_URL}/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz -O - |
    tar -xzf - -C /opt
  ln -s /opt/trino-server-${TRINO_VERSION} /opt/trino-server
  mkdir -p /var/trino/data
}

function calculate_memory() {
  # Compute memory settings based on Spark's settings.
  # We use "tail -n 1" since overrides are applied just by order of appearance.
  local spark_executor_mb
  spark_executor_mb=$(grep spark.executor.memory \
    /etc/spark/conf/spark-defaults.conf |
    tail -n 1 |
    sed 's/.*[[:space:]=]\+\([[:digit:]]\+\).*/\1/')

  local spark_executor_cores
  spark_executor_cores=$(grep spark.executor.cores \
    /etc/spark/conf/spark-defaults.conf |
    tail -n 1 |
    sed 's/.*[[:space:]=]\+\([[:digit:]]\+\).*/\1/')

  local spark_executor_overhead_mb
  if (grep spark.yarn.executor.memoryOverhead /etc/spark/conf/spark-defaults.conf); then
    spark_executor_overhead_mb=$(grep spark.yarn.executor.memoryOverhead \
      /etc/spark/conf/spark-defaults.conf |
      tail -n 1 |
      sed 's/.*[[:space:]=]\+\([[:digit:]]\+\).*/\1/')
  else
    # When spark.yarn.executor.memoryOverhead couldn't be found in
    # spark-defaults.conf, use Spark default properties:
    # executorMemory * 0.10, with minimum of 384
    local min_executor_overhead=384
    spark_executor_overhead_mb=$((spark_executor_mb / 10))
    spark_executor_overhead_mb=$((spark_executor_overhead_mb > min_executor_overhead ? spark_executor_overhead_mb : min_executor_overhead))
  fi
  local spark_executor_count
  spark_executor_count=$(($(nproc) / spark_executor_cores))

  # Add up overhead and allocated executor MB for container size.
  local spark_container_mb
  spark_container_mb=$((spark_executor_mb + spark_executor_overhead_mb))
  TRINO_JVM_MB=$((spark_container_mb * spark_executor_count))
  readonly TRINO_JVM_MB

  # Give query.max-memory-per-node 60% of Xmx; this more-or-less assumes a
  # single-tenant use case rather than trying to allow many concurrent queries
  # against a shared cluster.
  # Subtract out spark_executor_overhead_mb in both the query MB and reserved
  # system MB as a crude approximation of other unaccounted overhead that we need
  # to leave betweenused bytes and Xmx bytes. Rounding down by integer division
  # here also effectively places round-down bytes in the "general" pool.
  TRINO_QUERY_NODE_MB=$((TRINO_JVM_MB * 6 / 10 - spark_executor_overhead_mb))
  TRINO_RESERVED_SYSTEM_MB=$((TRINO_JVM_MB * 4 / 10 - spark_executor_overhead_mb))
  readonly TRINO_QUERY_NODE_MB
  readonly TRINO_RESERVED_SYSTEM_MB
}

function configure_node_properties() {
  cat >/opt/trino-server/etc/node.properties <<EOF
node.environment=production
node.id=$(uuidgen)
node.data-dir=/var/trino/data
EOF
}

function configure_hive() {
  local metastore_uri
  metastore_uri=$(bdconfig get_property_value \
    --configuration_file /etc/hive/conf/hive-site.xml \
    --name hive.metastore.uris 2>/dev/null)

  cat >/opt/trino-server/etc/catalog/hive.properties <<EOF
connector.name=hive-hadoop2
hive.metastore.uri=${metastore_uri}
EOF
}

function configure_connectors() {
  cat >/opt/trino-server/etc/catalog/tpch.properties <<EOF
connector.name=tpch
EOF

  cat >/opt/trino-server/etc/catalog/tpcds.properties <<EOF
connector.name=tpcds
EOF

  cat >/opt/trino-server/etc/catalog/jmx.properties <<EOF
connector.name=jmx
EOF

  cat >/opt/trino-server/etc/catalog/memory.properties <<EOF
connector.name=memory
EOF
  cat >/opt/trino-server/etc/catalog/bigquery.properties <<EOF
connector.name=bigquery
bigquery.project-id=${BQ_PROJECT_ID}
bigquery.views-enabled=true
EOF
}

function configure_jvm() {
  cat >/opt/trino-server/etc/jvm.config <<EOF
-server
-Xmx${TRINO_JVM_MB}m
-Xmn512m
-XX:+UseConcMarkSweepGC
-XX:+ExplicitGCInvokesConcurrent
-XX:ReservedCodeCacheSize=150M
-XX:+ExplicitGCInvokesConcurrent
-XX:+CMSClassUnloadingEnabled
-XX:+AggressiveOpts
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p
-Dhive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml
-Djava.library.path=/usr/lib/hadoop/lib/native/:/usr/lib/
EOF
}

# Configure master properties
function configure_master() {
  if [[ ${WORKER_COUNT} == 0 ]]; then
    # master on single-node is also worker
    include_coordinator='true'
  else
    include_coordinator='false'
  fi
  cat >/opt/trino-server/etc/config.properties <<EOF
coordinator=true
node-scheduler.include-coordinator=${include_coordinator}
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${TRINO_QUERY_NODE_MB}MB
memory.heap-headroom-per-node=${TRINO_HEADROOM_NODE_MB}MB
discovery.uri=http://${TRINO_MASTER_FQDN}:${HTTP_PORT}
EOF

  # Install CLI
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    ${TRINO_BASE_URL}/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar -O /usr/bin/trino
  chmod a+x /usr/bin/trino
}

function configure_worker() {
  cat >/opt/trino-server/etc/config.properties <<EOF
coordinator=false
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${TRINO_QUERY_NODE_MB}MB
memory.heap-headroom-per-node=${TRINO_HEADROOM_NODE_MB}MB
discovery.uri=http://${TRINO_MASTER_FQDN}:${HTTP_PORT}
EOF
}

# Start Trino as SystemD service
function start_trino() {
  cat <<EOF >${INIT_SCRIPT}
[Unit]
Description=Trino DB

[Service]
Type=forking
ExecStart=/opt/trino-server/bin/launcher start
ExecStop=/opt/trino-server/bin/launcher stop
Restart=always

[Install]
WantedBy=multi-user.target
EOF

  chmod a+rw ${INIT_SCRIPT}

  systemctl daemon-reload
  systemctl enable trino
  systemctl start trino
  systemctl status trino
}

function configure_and_start_trino() {
  # Copy required Jars
  cp "${CONNECTOR_JAR}" /opt/trino-server/plugin/hive-hadoop2

  # Configure Trino
  mkdir -p /opt/trino-server/etc/catalog

  configure_node_properties
  configure_hive
  configure_connectors
  configure_jvm

  if [[ "${HOSTNAME}" == "${TRINO_MASTER_FQDN}" ]]; then
    configure_master
    start_trino
    wait_for_trino_cluster_ready
  fi

  if [[ "${ROLE}" == 'Worker' ]]; then
    configure_worker
    start_trino
  fi
}

function main() {
  if [[ -d /opt/trino-server ]]; then
    echo "Trino already installed in the '/opt/trino-server' directory"
    exit 1
  fi

  get_trino
  calculate_memory
  configure_and_start_trino
}

main
