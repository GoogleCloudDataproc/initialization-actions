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

# This script installs Druid on a Google Cloud
# Dataproc cluster.
#
# To use this script, you will need to configure the following variables to
# match your cluster. For information about which software components
# (and their version) are included in Cloud Dataproc clusters, see the
# Cloud Dataproc Image Version information:
# https://cloud.google.com/dataproc/concepts/dataproc-versions

set -euxo pipefail

export DRUID_VERSION='0.12.3'
export DRUID_HOME="/$(whoami)"
export DRUID_DIR="${DRUID_HOME}/druid-${DRUID_VERSION}"

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function download_druid() {
  cd ${DRUID_HOME}
  curl -O http://static.druid.io/artifacts/releases/druid-${DRUID_VERSION}-bin.tar.gz
  tar -xzf druid-${DRUID_VERSION}-bin.tar.gz
}

function download_samples() {
  cd ${DRUID_DIR}
  curl -O http://druid.io/docs/${DRUID_VERSION}/tutorials/tutorial-examples.tar.gz
  tar zxvf tutorial-examples.tar.gz
}

function prepare_environment_services(){
  local metastore_instance
  local password_argument
  metastore_instance="$(/usr/share/google/get_metadata_value attributes/hive-metastore-instance)"
  if [ -z "${metastore_instance}" ];then
    systemctl start mysql
    password_argument='-proot-password'
  else
    password_argument=''
  fi
  echo | mysql -u root ${password_argument} -e "CREATE DATABASE druid DEFAULT CHARACTER SET utf8;"
  echo | mysql -u root ${password_argument} -e "GRANT ALL ON druid.* TO 'druid' IDENTIFIED BY 'diurd';"
  bash /usr/lib/zookeeper/bin/zkServer.sh restart
}

function configure_druid() {
  local java_home=$(echo ${JAVA_HOME})
  local hadoop_conf_dir="/etc/hadoop/conf"

  cd ${DRUID_DIR}
  bash bin/init

  java \
    -cp "lib/*" \
    -Ddruid.extensions.directory="extensions" \
    -Ddruid.extensions.hadoopDependenciesDir="hadoop-dependencies" \
    io.druid.cli.Main tools pull-deps \
    --no-default-hadoop \
    -c "io.druid.extensions:mysql-metadata-storage:${DRUID_VERSION}"

  java \
    -cp "lib/*" \
    -Ddruid.extensions.directory="extensions" \
    -Ddruid.extensions.hadoopDependenciesDir="hadoop-dependencies" \
    io.druid.cli.Main tools pull-deps \
    --no-default-hadoop \
    -c "io.druid.extensions:druid-hdfs-storage:${DRUID_VERSION}"

  cat <<EOF > ${DRUID_DIR}/conf/druid/_common/common.runtime.properties
#
# Licensed to Metamarkets Group Inc. (Metamarkets) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. Metamarkets licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# Extensions
#

# This is not the full list of Druid extensions, but common ones that people often use. You may need to change this list
# based on your particular setup.
druid.extensions.loadList=["druid-kafka-eight", "druid-histogram", "druid-datasketches", "druid-lookups-cached-global", "mysql-metadata-storage", "druid-hdfs-storage"]

# If you have a different version of Hadoop, place your Hadoop client jar files in your hadoop-dependencies directory
# and uncomment the line below to point to your directory.
#druid.extensions.hadoopDependenciesDir=/my/dir/hadoop-dependencies

#
# Logging
#

# Log all runtime properties on startup. Disable to avoid logging properties on startup:
druid.startup.logging.logProperties=true

#
# Zookeeper
#

druid.zk.service.host=localhost:2181
druid.zk.paths.base=/druid

#
# Metadata storage
#


# For MySQL:
druid.metadata.storage.type=mysql
druid.metadata.storage.connector.connectURI=jdbc:mysql://127.0.0.1:3306/druid
druid.metadata.storage.connector.user=druid
druid.metadata.storage.connector.password=diurd

# For PostgreSQL (make sure to additionally include the Postgres extension):
#druid.metadata.storage.type=postgresql
#druid.metadata.storage.connector.connectURI=jdbc:postgresql://db.example.com:5432/druid
#druid.metadata.storage.connector.user=...
#druid.metadata.storage.connector.password=...

#
# Deep storage
#

# For local disk (only viable in a cluster if this is a network mount):
#druid.storage.type=local
#druid.storage.storageDirectory=var/druid/segments

# For HDFS (make sure to include the HDFS extension and that your Hadoop config files in the cp):
druid.storage.type=hdfs
druid.storage.storageDirectory=/druid/segments

# For S3:
#druid.storage.type=s3
#druid.storage.bucket=your-bucket
#druid.storage.baseKey=druid/segments
#druid.s3.accessKey=...
#druid.s3.secretKey=...

#
# Indexing service logs
#

# For local disk (only viable in a cluster if this is a network mount):
#druid.indexer.logs.type=file
#druid.indexer.logs.directory=var/druid/indexing-logs

# For HDFS (make sure to include the HDFS extension and that your Hadoop config files in the cp):
druid.indexer.logs.type=hdfs
druid.indexer.logs.directory=/druid/indexing-logs

# For S3:
#druid.indexer.logs.type=s3
#druid.indexer.logs.s3Bucket=your-bucket
#druid.indexer.logs.s3Prefix=druid/indexing-logs

#
# Service discovery
#

druid.selectors.indexing.serviceName=druid/overlord
druid.selectors.coordinator.serviceName=druid/coordinator

#
# Monitoring
#

druid.monitoring.monitors=["io.druid.java.util.metrics.JvmMonitor"]
druid.emitter=logging
druid.emitter.logging.logLevel=info

# Storage type of double columns
# ommiting this will lead to index double as float at the storage layer

druid.indexing.doubleStorage=double
EOF

  cp /usr/lib/hadoop/etc/hadoop/core-site.xml conf/druid/_common/core-site.xml
  cp /usr/lib/hadoop/etc/hadoop/hdfs-site.xml conf/druid/_common/hdfs-site.xml
  cp /usr/lib/hadoop/etc/hadoop/yarn-site.xml conf/druid/_common/yarn-site.xml
  cp /usr/lib/hadoop/etc/hadoop/mapred-site.xml conf/druid/_common/mapred-site.xml

  cat << EOF > /etc/systemd/system/druid-coordinator.service
[Unit]
Description=Coordinator
Wants=network-online.target
After=network-online.target
[Service]
WorkingDirectory=${DRUID_DIR}
Environment="JAVA_HOME=${java_home}"
Environment="HADOOP_CONF_DIR=${hadoop_conf_dir}"
User=root
Group=root
Type=simple
ExecStart=/bin/bash -c "java `cat conf/druid/coordinator/jvm.config | xargs` -cp "${DRUID_DIR}/conf/druid/_common:${DRUID_DIR}/conf/druid/_common/hadoop-xml:${DRUID_DIR}/conf/druid/coordinator:lib/*" io.druid.cli.Main server coordinator"
[Install]
WantedBy=multi-user.target
EOF

  cat << EOF > /etc/systemd/system/druid-overlord.service
[Unit]
Description=Overlord
Wants=network-online.target
After=network-online.target
[Service]
WorkingDirectory=${DRUID_DIR}
Environment="JAVA_HOME=${java_home}"
Environment="HADOOP_CONF_DIR=${hadoop_conf_dir}"
User=root
Group=root
Type=simple
ExecStart=/bin/bash -c "java `cat conf/druid/overlord/jvm.config | xargs` -cp "${DRUID_DIR}/conf/druid/_common:${DRUID_DIR}/conf/druid/_common/hadoop-xml:${DRUID_DIR}/conf/druid/overlord:lib/*" io.druid.cli.Main server overlord"
[Install]
WantedBy=multi-user.target
EOF

  cat << EOF > /etc/systemd/system/druid-broker.service
[Unit]
Description=Broker
Wants=network-online.target
After=network-online.target
[Service]
WorkingDirectory=${DRUID_DIR}
Environment="JAVA_HOME=${java_home}"
Environment="HADOOP_CONF_DIR=${hadoop_conf_dir}"

User=root
Group=root
Type=simple
ExecStart=/bin/bash -c "java `cat examples/conf/druid/broker/jvm.config | xargs` -cp "${DRUID_DIR}/examples/conf/druid/_common:${DRUID_DIR}/examples/conf/druid/_common/hadoop-xml:${DRUID_DIR}/examples/conf/druid/broker:lib/*" io.druid.cli.Main server broker"
[Install]
WantedBy=multi-user.target
EOF

  cat << EOF > /etc/systemd/system/druid-historical.service
[Unit]
Description=Historical
Wants=network-online.target
After=network-online.target
[Service]
WorkingDirectory=${DRUID_DIR}
Environment="JAVA_HOME=${java_home}"
Environment="HADOOP_CONF_DIR=${hadoop_conf_dir}"

User=root
Group=root
Type=simple
ExecStart=/bin/bash -c "java `cat conf/druid/historical/jvm.config | xargs` -cp "${DRUID_DIR}/conf/druid/_common:${DRUID_DIR}/conf/druid/_common/hadoop-xml:${DRUID_DIR}/conf/druid/historical:lib/*" io.druid.cli.Main server historical"

[Install]
WantedBy=multi-user.target
EOF

  cat << EOF > /etc/systemd/system/druid-middle-manager.service
[Unit]
Description=MiddleManager
Wants=network-online.target
After=network-online.target
[Service]
WorkingDirectory=${DRUID_DIR}
Environment="JAVA_HOME=${java_home}"
Environment="HADOOP_CONF_DIR=${hadoop_conf_dir}"

User=root
Group=root
Type=simple
ExecStart=/bin/bash -c "java `cat examples/conf/druid/middleManager/jvm.config | xargs` -cp "${DRUID_DIR}/examples/conf/druid/_common:${DRUID_DIR}/examples/conf/druid/_common/hadoop-xml:${DRUID_DIR}/examples/conf/druid/middleManager:lib/*" io.druid.cli.Main server middleManager"
[Install]
WantedBy=multi-user.target
EOF
}

function start_druid(){
  cd ${DRUID_DIR}
  systemctl daemon-reload
  systemctl enable druid-coordinator druid-overlord druid-broker druid-historical druid-middle-manager
  systemctl start druid-coordinator druid-overlord druid-broker druid-middle-manager druid-historical
}

function main() {
  readonly enable_cloud_sql_metastore="$(/usr/share/google/get_metadata_value \
    attributes/enable-cloud-sql-hive-metastore || echo 'true')"
  local role=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
  if [[ ${role} == 'Master' ]]; then
    download_druid || err 'Druid download failed'
    download_samples || err 'Druid samples download failed'
    prepare_environment_services || err 'Zookeeper or mysql preparation failed'
    configure_druid || err 'Druid configuration failed'
    start_druid || err 'Starting Druid services failed'
  fi
}

main