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

export DRUID_VERSION='0.13.0-incubating'
export DRUID_HOME="/opt/druid"
export DRUID_DIR="${DRUID_HOME}/apache-druid-${DRUID_VERSION}"

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function make_druid_home() {
  mkdir -p ${DRUID_HOME}
}

function download_druid() {
  cd ${DRUID_HOME}
  wget -nc http://ftp.man.poznan.pl/apache/incubator/druid/${DRUID_VERSION}/apache-druid-${DRUID_VERSION}-bin.tar.gz
  tar -xzf apache-druid-${DRUID_VERSION}-bin.tar.gz
}

function extract_runtime_property(){
  local file="${1}"
  local parameter="${2}"
  echo $(cat ${file} | grep ${parameter} | grep -o -E '[0-9]+')
}

function calculate_xmx(){
  local processing_buffer=${1}
  local num_threads=${2}
  local num_merge_buffers=${3}

  echo $(( ${processing_buffer} * (${num_merge_buffers} + ${num_threads}) + 1 ))
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
  mysql -u root ${password_argument} -e "CREATE DATABASE druid DEFAULT CHARACTER SET utf8;" \
    || echo "Database already exists"
  mysql -u root ${password_argument} -e "GRANT ALL ON druid.* TO 'druid' IDENTIFIED BY 'diurd';" \
    || echo "Cannot grant priviliges to druid user"
}

function max() {
   [ "$1" -gt "$2" ] && echo $1 || echo $2
}

function modify_jvm_config(){
  local service_name="${1}"
  local cores
  local processing_buffer
  local num_threads
  local num_merge_buff
  local xmx
  local xx
  processing_buffer=$(extract_runtime_property \
    ${DRUID_DIR}/conf/druid/${service_name}/runtime.properties druid.processing.buffer.sizeBytes)
  if [[ ${processing_buffer} == "" ]];then
    # Default value 1GB
    processing_buffer=1073741824
  fi
  cores=$(grep -c ^processor /proc/cpuinfo)
  cores=$((${cores}-1))
  num_threads=$(max 1 ${cores})
  if [[ ${num_threads} < 4 ]];then
    num_merge_buff=2
  else
    num_merge_buff=$(max 2 ${num_threads}/4)
  fi
  xmx=$(calculate_xmx ${processing_buffer} ${num_threads} ${num_merge_buff})
  xx=$((${num_threads} * ${processing_buffer} * 4))
  cat << EOF > conf/druid/${service_name}/jvm.config
-XX:MaxDirectMemorySize=${xx}
-Xmx${xmx}
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.io.tmpdir=var/tmp
-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager
-Dderby.stream.error.file=var/druid/derby.log
EOF
}

function configure_druid() {
  local java_home
  local hadoop_conf_dir
  local druid_overlord_port
  local druid_coordinator_port
  local zookeeper_client_port
  local zookeeper_list
  local tmp_dir
  local cluster_name
  local sql_host
  local master_additional
  local hive_metastore_instance
  local gcs_bucket
  local base_extensions
  local gcs_enabled_extensions

  base_extensions="[\"druid-kafka-eight\", \"druid-histogram\", \"druid-datasketches\", \"druid-lookups-cached-global\", \"mysql-metadata-storage\", \"druid-hdfs-storage\",\"druid-kafka-indexing-service\"]"
  gcs_enabled_extensions="[\"druid-kafka-eight\", \"druid-histogram\", \"druid-datasketches\", \"druid-lookups-cached-global\", \"mysql-metadata-storage\", \"druid-hdfs-storage\", \"druid-kafka-indexing-service\", \"druid-google-extensions\"]"
  master_additional=$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)
  cluster_name=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
  hive_metastore_instance=$(/usr/share/google/get_metadata_value attributes/hive-metastore-instance)
  gcs_bucket=$(/usr/share/google/get_metadata_value attributes/gcs-bucket)

  # Set localhost as sql host for cloud-sql-proxy or master node for other configurations
  if [[ ${hive_metastore_instance} == "" ]]; then
    if [ "${master_additional}" == '' ]; then
      sql_host="${cluster_name}-m"
    else
      sql_host="${cluster_name}-m-0"
    fi
  else
    sql_host='localhost'
  fi

  druid_overlord_port=$(/usr/share/google/get_metadata_value attributes/druid-overlord-port)
  druid_coordinator_port=$(/usr/share/google/get_metadata_value attributes/druid-coordinator-port)
  java_home=$(echo ${JAVA_HOME})
  hadoop_conf_dir="/etc/hadoop/conf"

  zookeeper_client_port=$(grep 'clientPort' /etc/zookeeper/conf/zoo.cfg \
    | tail -n 1 \
    | cut -d '=' -f 2)

  zookeeper_list=$(grep '^server\.' /etc/zookeeper/conf/zoo.cfg \
    | cut -d '=' -f 2 \
    | cut -d ':' -f 1 \
    | sort \
    | uniq \
    | sed "s/$/:${zookeeper_client_port}/" \
    | xargs echo  \
    | sed "s/ /,/g")

  cd ${DRUID_DIR}

  ln -sf /usr/share/java/mysql-connector-java.jar extensions/mysql-metadata-storage/mysql-connector-java-5.1.38.jar

  java \
    -cp "lib/*" \
    -Ddruid.extensions.directory="extensions" \
    -Ddruid.extensions.hadoopDependenciesDir="hadoop-dependencies" \
    org.apache.druid.cli.Main tools pull-deps \
    --no-default-hadoop \
    -c "org.apache.druid.extensions:mysql-metadata-storage:${DRUID_VERSION}"

  java \
    -cp "lib/*" \
    -Ddruid.extensions.directory="extensions" \
    -Ddruid.extensions.hadoopDependenciesDir="hadoop-dependencies" \
    org.apache.druid.cli.Main tools pull-deps \
    --no-default-hadoop \
    -c "org.apache.druid.extensions:druid-hdfs-storage:${DRUID_VERSION}"

  cat <<EOF > ${DRUID_DIR}/conf/druid/_common/common.runtime.properties
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

druid.zk.service.host=${zookeeper_list}
druid.zk.paths.base=/druid

#
# Metadata storage
#


# For MySQL:
druid.metadata.storage.type=mysql
druid.metadata.storage.connector.connectURI=jdbc:mysql://${sql_host}:3306/druid
druid.metadata.storage.connector.user=druid
druid.metadata.storage.connector.password=diurd
druid.sql.enable = true

#
# Deep storage
#

# For local disk (only viable in a cluster if this is a network mount):
#druid.storage.type=local
#druid.storage.storageDirectory=var/druid/segments

# For HDFS (make sure to include the HDFS extension and that your Hadoop config files in the cp):
druid.storage.type=hdfs
druid.storage.storageDirectory=/druid/segments

#
# Indexing service logs
#

# For local disk (only viable in a cluster if this is a network mount):
#druid.indexer.logs.type=file
#druid.indexer.logs.directory=var/druid/indexing-logs

# For HDFS (make sure to include the HDFS extension and that your Hadoop config files in the cp):
druid.indexer.logs.type=hdfs
druid.indexer.logs.directory=/druid/indexing-logs
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

  if [ "${gcs_bucket}" != "" ];then
    java \
      -cp "lib/*" \
      -Ddruid.extensions.directory="extensions" \
      -Ddruid.extensions.hadoopDependenciesDir="hadoop-dependencies" \
      org.apache.druid.cli.Main tools pull-deps \
      --no-default-hadoop \
      -c "org.apache.druid.extensions.contrib:druid-google-extensions:${DRUID_VERSION}"

    sed -i -- "s/druid.indexer.logs.type=hdfs/druid.indexer.logs.type=google/g" ${DRUID_DIR}/conf/druid/_common/common.runtime.properties
    sed -i -- "s/druid.storage.type=hdfs/druid.storage.type=google/g" ${DRUID_DIR}/conf/druid/_common/common.runtime.properties
    cat << EOF >> ${DRUID_DIR}/conf/druid/_common/common.runtime.properties
druid.extensions.loadList=${gcs_enabled_extensions}
druid.google.bucket=${gcs_bucket}
druid.google.prefix=druid/segments
druid.indexer.logs.bucket=${gcs_bucket}
druid.indexer.logs.prefix=druid/indexing-logs
mapreduce.job.classloader=true
hadoopDependencyCoordinates="org.apache.hadoop:hadoop-client:2.8.3"
EOF
  else
    echo "druid.extensions.loadList=${base_extensions}" >> ${DRUID_DIR}/conf/druid/_common/common.runtime.properties
  fi


  ln -sf /usr/lib/hadoop/etc/hadoop/core-site.xml conf/druid/_common/core-site.xml
  ln -sf /usr/lib/hadoop/etc/hadoop/hdfs-site.xml conf/druid/_common/hdfs-site.xml
  ln -sf /usr/lib/hadoop/etc/hadoop/yarn-site.xml conf/druid/_common/yarn-site.xml
  ln -sf /usr/lib/hadoop/etc/hadoop/mapred-site.xml conf/druid/_common/mapred-site.xml

  chown root conf/druid/
  chmod +xr -R conf/druid/
  tmp_dir=var/tmp
  mkdir -p ${tmp_dir}

  for service in coordinator overlord broker historical middleManager; do
    modify_jvm_config ${service}
  done

  cat << 'EOF' > conf/druid/broker/runtime.properties
druid.service=druid/broker
druid.plaintextPort=8082

# HTTP server threads
druid.broker.http.numConnections=5
druid.server.http.numThreads=25

# Processing threads and buffers
druid.processing.buffer.sizeBytes=268435456
druid.processing.numThreads=7

# Query cache
druid.broker.cache.useCache=true
druid.broker.cache.populateCache=true
druid.cache.type=local
druid.cache.sizeInBytes=2000000000
EOF

  cat << 'EOF' > conf/druid/overlord/runtime.properties
druid.service=druid/overlord
druid.plaintextPort=8090

druid.indexer.queue.startDelay=PT30S
druid.processing.buffer.sizeBytes=268435456

druid.indexer.runner.type=remote
druid.indexer.storage.type=metadata
EOF

  cat << 'EOF' > conf/druid/middleManager/runtime.properties

druid.service=druid/middleManager
druid.plaintextPort=8091

# Number of tasks per middleManager
druid.worker.capacity=5

# Task launch parameters
druid.indexer.runner.javaOpts=-server -Xmx2g -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager
druid.indexer.task.baseTaskDir=var/druid/task

# HTTP server threads
druid.server.http.numThreads=25

# Processing threads and buffers on Peons

druid.indexer.fork.property.druid.processing.buffer.sizeBytes=268435456
druid.indexer.fork.property.druid.processing.numThreads=2

# Hadoop indexing
druid.indexer.task.hadoopWorkingPath=var/druid/hadoop-tmp
EOF

  cat << 'EOF' > conf/druid/historical/runtime.properties
druid.service=druid/historical
druid.plaintextPort=8083

# HTTP server threads
druid.server.http.numThreads=25

# Processing threads and buffers
druid.processing.buffer.sizeBytes=268435456
druid.processing.numThreads=7

# Segment storage
druid.segmentCache.locations=[{"path":"var/druid/segment-cache","maxSize":130000000000}]
druid.server.maxSize=130000000000

EOF

  cat << 'EOF' > conf/druid/coordinator/runtime.properties
druid.service=druid/coordinator
druid.plaintextPort=8081

druid.coordinator.startDelay=PT30S
druid.coordinator.period=PT30S
druid.processing.buffer.sizeBytes=268435456
EOF

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
ExecStart=/bin/bash -c "java `cat conf/druid/coordinator/jvm.config | xargs` -cp "${DRUID_DIR}/conf/druid/_common:${DRUID_DIR}/conf/druid/_common/hadoop-xml:${DRUID_DIR}/conf/druid/coordinator:lib/*" org.apache.druid.cli.Main server coordinator > coordinator.txt"
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
ExecStart=/bin/bash -c "java `cat conf/druid/overlord/jvm.config | xargs` -cp "${DRUID_DIR}/conf/druid/_common:${DRUID_DIR}/conf/druid/_common/hadoop-xml:${DRUID_DIR}/conf/druid/overlord:lib/*" org.apache.druid.cli.Main server overlord > overlord.txt"
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
ExecStart=/bin/bash -c "java `cat conf/druid/broker/jvm.config | xargs` -cp "${DRUID_DIR}/conf/druid/_common:${DRUID_DIR}/conf/druid/_common/hadoop-xml:${DRUID_DIR}/conf/druid/broker:lib/*" org.apache.druid.cli.Main server broker > broker.txt"
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
ExecStart=/bin/bash -c "java `cat conf/druid/historical/jvm.config | xargs` -cp "${DRUID_DIR}/conf/druid/_common:${DRUID_DIR}/conf/druid/_common/hadoop-xml:${DRUID_DIR}/conf/druid/historical:lib/*" org.apache.druid.cli.Main server historical > historical.txt"

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
ExecStart=/bin/bash -c "java `cat conf/druid/middleManager/jvm.config | xargs` -cp "${DRUID_DIR}/conf/druid/_common:${DRUID_DIR}/conf/druid/_common/hadoop-xml:${DRUID_DIR}/conf/druid/middleManager:lib/*" org.apache.druid.cli.Main server middleManager > middlemanager.txt"
[Install]
WantedBy=multi-user.target
EOF
  if [[ ${druid_overlord_port} != "" ]]; then
    sed -i -- "s/druid.plaintextPort=8090/druid.plaintextPort=${druid_overlord_port}/g" ${DRUID_DIR}/conf/druid/overlord/runtime.properties
  fi

  if [[ ${druid_coordinator_port} != "" ]]; then
    sed -i -- "s/druid.plaintextPort=8081/druid.plaintextPort=${druid_coordinator_port}/g" ${DRUID_DIR}/conf/druid/coordinator/runtime.properties
  fi

  # Restart zookeeper in order to avoid problems with connectivity
  ln -sf /usr/lib/zookeeper zk
  bash zk/bin/zkServer.sh restart
}

function start_druid_master(){
  systemctl daemon-reload
  systemctl enable druid-coordinator druid-broker druid-overlord
  systemctl start druid-coordinator druid-broker druid-overlord
  sleep 10
  for service in coordinator overlord broker; do
    systemctl status druid-${service} | grep 'active (running)' || err "${service} is not running"
  done

}

function start_druid_single_master(){
  systemctl daemon-reload
  systemctl enable druid-coordinator druid-broker druid-overlord druid-middle-manager druid-historical
  systemctl start druid-coordinator druid-broker druid-overlord druid-middle-manager druid-historical
  sleep 10
  for service in coordinator overlord broker historical middle-manager; do
    systemctl status druid-${service} | grep 'active (running)' || err "${service} is not running"
  done
}

function start_druid_worker(){
  systemctl daemon-reload
  systemctl enable druid-middle-manager druid-historical
  systemctl start  druid-middle-manager druid-historical
  sleep 10
  for service in historical middle-manager; do
    systemctl status druid-${service} | grep 'active (running)' || err "${service} is not running"
  done
}

function main() {
  local role
  local worker_count
  worker_count=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
  readonly enable_cloud_sql_metastore="$(/usr/share/google/get_metadata_value \
    attributes/enable-cloud-sql-hive-metastore || echo 'true')"
  role=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
  make_druid_home || err 'Cannot create Druid home directory'
  download_druid || err 'Druid download failed'
  prepare_environment_services || err 'Setting up mysql failed'
  configure_druid || err 'Druid configuration failed'
  # Install all druid services on single configuration
  if [[ "${worker_count}" == '0' ]]; then
    start_druid_single_master
  else
  # Isolate management services from middle-manager and historical
    if [[ ${role} == 'Master' ]]; then
      start_druid_master || err 'Starting Druid services on master failed'
    else
      start_druid_worker || err 'Starting Druid services on worker failed'
    fi
  fi
}

main