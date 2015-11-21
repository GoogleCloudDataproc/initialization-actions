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
ROLE=$(/usr/share/google/get_metadata_value attributes/role)
HOSTNAME=$(hostname)
DNSNAME=$(dnsdomainname)
FQDN=${HOSTNAME}.$DNSNAME
CONNECTOR_JAR=$(find /usr/lib/hadoop/lib -name 'gcs-connector-*.jar')
PRESTO_VERSION="0.126"
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

cat > presto-server-${PRESTO_VERSION}/etc/catalog/hive.properties <<EOF
connector.name=hive-hadoop2
# TODO - Inspect /etc/hive/conf/hite-site.xml to pull this uri
hive.metastore.uri=thrift://localhost:9083
EOF

cat > presto-server-${PRESTO_VERSION}/etc/jvm.config <<EOF
-server
-Xmx$(grep spark.executor.memory /etc/spark/conf/spark-defaults.conf | awk '{print $2}')
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
-Djava.library.path=/usr/lib/hadoop/lib/
EOF

if [[ "${ROLE}" == 'Master' ]]; then
	# Configure master properties
	PRESTO_MASTER_FQDN=${FQDN}
	cat > presto-server-${PRESTO_VERSION}/etc/config.properties <<EOF
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=${HTTP_PORT}
query.max-memory=50GB
query.max-memory-per-node=1GB
discovery-server.enabled=true
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
EOF

	# Install cli
	$(wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar -O /usr/bin/presto)
	$(chmod a+x /usr/bin/presto)
else
	MASTER_NODE_NAME=$(hostname | sed 's/\(.*\)-w-[0-9]*.*/\1/g')
	PRESTO_MASTER_FQDN=${MASTER_NODE_NAME}-m.${DNSNAME}
	cat > presto-server-${PRESTO_VERSION}/etc/config.properties <<EOF
coordinator=false
http-server.http.port=8080
query.max-memory=50GB
query.max-memory-per-node=1GB
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
EOF
fi

# Start presto
presto-server-${PRESTO_VERSION}/bin/launcher start
