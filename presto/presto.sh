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
set -x -e

# Variables for running this script
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
HOSTNAME=$(hostname)
DNSNAME=$(dnsdomainname)
FQDN=${HOSTNAME}.$DNSNAME
CONNECTOR_JAR=$(find /usr/lib/hadoop/lib -name 'gcs-connector-*.jar')
PRESTO_VERSION="0.144.1"
HTTP_PORT="8080"

# Download and unpack Presto server
wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz
tar -zxvf presto-server-${PRESTO_VERSION}.tar.gz
mkdir /var/presto
mkdir /var/presto/data

# Copy required Jars
cp ${CONNECTOR_JAR} presto-server-${PRESTO_VERSION}/plugin/hive-hadoop2

# Configure Presto
mkdir presto-server-${PRESTO_VERSION}/etc
mkdir presto-server-${PRESTO_VERSION}/etc/catalog

cat > presto-server-${PRESTO_VERSION}/etc/node.properties <<EOF
node.environment=production
node.id=$(uuidgen)
node.data-dir=/var/presto/data
EOF

# TODO - Inspect /etc/hive/conf/hite-site.xml to pull this uri
cat > presto-server-${PRESTO_VERSION}/etc/catalog/hive.properties <<EOF
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:9083
EOF

# Compute memory settings based on Spark's settings.
# We use "tail -n 1" since overrides are applied just by order of appearance.
SPARK_EXECUTOR_MB=$(grep spark.executor.memory /etc/spark/conf/spark-defaults.conf | tail -n 1 | cut -d '=' -f 2 | cut -d 'm' -f 1)
SPARK_EXECUTOR_CORES=$(grep spark.executor.cores /etc/spark/conf/spark-defaults.conf | tail -n 1 | cut -d '=' -f 2)
SPARK_EXECUTOR_OVERHEAD_MB=$(grep spark.yarn.executor.memoryOverhead /etc/spark/conf/spark-defaults.conf | tail -n 1 | cut -d '=' -f 2)
SPARK_EXECUTOR_COUNT=$(( $(nproc) / ${SPARK_EXECUTOR_CORES} ))

# Add up overhead and allocated executor MB for container size.
SPARK_CONTAINER_MB=$(( ${SPARK_EXECUTOR_MB} + ${SPARK_EXECUTOR_OVERHEAD_MB} ))
PRESTO_JVM_MB=$(( ${SPARK_CONTAINER_MB} * ${SPARK_EXECUTOR_COUNT} ))

# Give query.max-memorr-per-node 60% of Xmx; this more-or-less assumes a
# single-tenant use case rather than trying to allow many concurrent queries
# against a shared cluster.
# Subtract out SPARK_EXECUTOR_OVERHEAD_MB in both the query MB and reserved
# system MB as a crude approximation of other unaccounted overhead that we need
# to leave betweenused bytes and Xmx bytes. Rounding down by integer division
# here also effectively places round-down bytes in the "general" pool.
PRESTO_QUERY_NODE_MB=$(( ${PRESTO_JVM_MB} * 6 / 10 - ${SPARK_EXECUTOR_OVERHEAD_MB} ))
PRESTO_RESERVED_SYSTEM_MB=$(( ${PRESTO_JVM_MB} * 4 / 10 - ${SPARK_EXECUTOR_OVERHEAD_MB} ))

cat > presto-server-${PRESTO_VERSION}/etc/jvm.config <<EOF
-server
-Xmx${PRESTO_JVM_MB}m
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

if [[ "${ROLE}" == 'Master' ]]; then
	# Configure master properties
	PRESTO_MASTER_FQDN=${FQDN}
	cat > presto-server-${PRESTO_VERSION}/etc/config.properties <<EOF
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
resources.reserved-system-memory=${PRESTO_RESERVED_SYSTEM_MB}MB
discovery-server.enabled=true
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
EOF

	# Install cli
	$(wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar -O /usr/bin/presto)
	$(chmod a+x /usr/bin/presto)
else
	MASTER_NODE_NAME=$(hostname | sed 's/\(.*\)-s\?w-[a-z0-9]*.*/\1/g')
	PRESTO_MASTER_FQDN=${MASTER_NODE_NAME}-m.${DNSNAME}
	cat > presto-server-${PRESTO_VERSION}/etc/config.properties <<EOF
coordinator=false
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
resources.reserved-system-memory=${PRESTO_RESERVED_SYSTEM_MB}MB
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
EOF
fi

# Start presto
presto-server-${PRESTO_VERSION}/bin/launcher start
