#!/bin/bash
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
readonly PRESTO_MASTER_FQDN="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
readonly WORKER_COUNT=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
readonly PRESTO_BASE_URL=https://storage.googleapis.com/starburstdata/presto
readonly PRESTO_MAJOR_VERSION="332"
readonly PRESTO_VERSION="${PRESTO_MAJOR_VERSION}-e.0"
readonly HTTP_PORT="$(/usr/share/google/get_metadata_value attributes/presto-port || echo 8080)"
readonly INIT_SCRIPT="/usr/lib/systemd/system/presto.service"
PRESTO_JVM_MB=0
PRESTO_QUERY_NODE_MB=0
# Allocate some headroom for untracked memory usage (in the heap and to help GC).
PRESTO_HEADROOM_NODE_MB=256

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  return 1
}

function wait_for_presto_cluster_ready() {
  # wait up to 120s for presto being able to run query
  for ((i = 0; i < 12; i++)); do
    if presto --server="localhost:${HTTP_PORT}" --execute='select * from system.runtime.nodes;'; then
      return 0
    fi
    sleep 10
  done
  return 1
}

# Download and unpack Presto Server
function get_presto() {
  wget -nv --timeout=30 --tries=5 --retry-connrefused -O - \
    "${PRESTO_BASE_URL}/${PRESTO_MAJOR_VERSION}e/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz" |
    tar -xzf - -C /opt
  ln -s "/opt/presto-server-${PRESTO_VERSION}" "/opt/presto-server"
  mkdir -p /var/presto/data
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
  PRESTO_JVM_MB=$((spark_container_mb * spark_executor_count))
  readonly PRESTO_JVM_MB

  # Give query.max-memory-per-node 60% of Xmx; this more-or-less assumes a
  # single-tenant use case rather than trying to allow many concurrent queries
  # against a shared cluster.
  # Subtract out spark_executor_overhead_mb in both the query MB and reserved
  # system MB as a crude approximation of other unaccounted overhead that we need
  # to leave betweenused bytes and Xmx bytes. Rounding down by integer division
  # here also effectively places round-down bytes in the "general" pool.
  PRESTO_QUERY_NODE_MB=$((PRESTO_JVM_MB * 6 / 10 - spark_executor_overhead_mb))
  readonly PRESTO_QUERY_NODE_MB
  readonly PRESTO_RESERVED_SYSTEM_MB
}

function configure_node_properties() {
  cat >/opt/presto-server/etc/node.properties <<EOF
node.environment=production
node.id=$(uuidgen)
node.data-dir=/var/presto/data
EOF
}

function configure_hive() {
  local metastore_uri
  metastore_uri=$(bdconfig get_property_value \
    --configuration_file /etc/hive/conf/hive-site.xml \
    --name hive.metastore.uris 2>/dev/null)

  cat >/opt/presto-server/etc/catalog/hive.properties <<EOF
connector.name=hive-hadoop2
hive.metastore.uri=${metastore_uri}
EOF
}

function configure_connectors() {
  cat >/opt/presto-server/etc/catalog/tpch.properties <<EOF
connector.name=tpch
EOF

  cat >/opt/presto-server/etc/catalog/tpcds.properties <<EOF
connector.name=tpcds
EOF

  cat >/opt/presto-server/etc/catalog/jmx.properties <<EOF
connector.name=jmx
EOF

  cat >/opt/presto-server/etc/catalog/memory.properties <<EOF
connector.name=memory
EOF
}

function configure_jvm() {
  cat >/opt/presto-server/etc/jvm.config <<EOF
-server
-Xmx${PRESTO_JVM_MB}m
-XX:-UseBiasedLocking
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+UseGCOverheadLimit
-XX:+HeapDumpOnOutOfMemoryError
-XX:ReservedCodeCacheSize=512M
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
-Dhive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml
-Djava.library.path=/usr/lib/hadoop/lib/native/:/usr/lib/
-Dpresto-temporarily-allow-java8=true
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
  cat >/opt/presto-server/etc/config.properties <<EOF
coordinator=true
node-scheduler.include-coordinator=${include_coordinator}
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
query.max-total-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
memory.heap-headroom-per-node=${PRESTO_HEADROOM_NODE_MB}MB
discovery-server.enabled=true
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
EOF

  # Install CLI
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${PRESTO_BASE_URL}/${PRESTO_MAJOR_VERSION}e/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar" -O /usr/bin/presto
  chmod a+x /usr/bin/presto
}

function configure_worker() {
  cat >/opt/presto-server/etc/config.properties <<EOF
coordinator=false
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
query.max-total-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
memory.heap-headroom-per-node=${PRESTO_HEADROOM_NODE_MB}MB
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
EOF
}

# Start Presto as SystemD service
function start_presto() {
  cat <<EOF >${INIT_SCRIPT}
[Unit]
Description=Presto SQL

[Service]
Type=forking
ExecStart=/opt/presto-server/bin/launcher.py start
ExecStop=/opt/presto-server/bin/launcher.py stop
Restart=always

[Install]
WantedBy=multi-user.target
EOF

  chmod a+rw ${INIT_SCRIPT}

  systemctl daemon-reload
  systemctl enable presto
  systemctl start presto
  systemctl status presto
}

# Configure Presto
function configure_and_start_presto() {
  mkdir -p /opt/presto-server/etc/catalog

  configure_node_properties
  configure_hive
  configure_connectors
  configure_jvm

  if [[ "${HOSTNAME}" == "${PRESTO_MASTER_FQDN}" ]]; then
    configure_master
    start_presto
    wait_for_presto_cluster_ready
  fi

  if [[ "${ROLE}" == 'Worker' ]]; then
    configure_worker
    start_presto
  fi
}

function main() {
  if [[ -d /opt/presto-server ]]; then
    echo "Presto already installed in the '/opt/presto-server' directory"
    exit 1
  fi

  get_presto
  calculate_memory
  configure_and_start_presto
}

main
