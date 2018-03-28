#!/bin/bash
#  Copyright 2015 Google, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
# This script installs Apache Tez (http://tez.apache.org) on a Google Cloud
# Dataproc cluster.

set -euxo pipefail

readonly TEZ_VERSION='0.7.0'
readonly PROTOBUF_VERSION='2.5.0'
readonly TEZ_HDFS_PATH='/apps/tez'
readonly TEZ_JARS='/usr/lib/tez'
readonly TEZ_CONF_DIR='/etc/tez/conf'
readonly HADOOP_CONF_DIR='/etc/hadoop/conf'
readonly HIVE_CONF_DIR='/etc/hive/conf'
readonly SPARK_CONF_DIR='/etc/spark/conf'

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
  return 1
}

function configure_master_node() {
  # Install Tez and YARN Application Timeline Server.
  # Install node/npm to run HTTP server for Tez UI.
  curl -sL 'https://deb.nodesource.com/setup_8.x' | bash - \
    || err 'Failed to setup node 8.'
  update_apt_get || err 'Unable to update packages lists.'
  apt-get install tez hadoop-yarn-timelineserver nodejs -y \
    || err 'Failed to install required packages.'

  # Copy to hdfs from one master only to avoid race
  if [[ "${HOSTNAME}" == "${master_hostname}" ]]; then
    # Stage Tez
    hadoop fs -mkdir -p ${TEZ_HDFS_PATH}
    hadoop fs -copyFromLocal ${TEZ_JARS}/* ${TEZ_HDFS_PATH}/ \
      || err 'Unable to copy tez jars to hdfs destination.'
  fi

  # Update the hadoop-env.sh
  {
    echo 'export TEZ_CONF_DIR=/etc/tez/'
    echo "export TEZ_JARS=${TEZ_JARS}"
    echo "HADOOP_CLASSPATH=\$HADOOP_CLASSPATH:${TEZ_CONF_DIR}:${TEZ_JARS}/*:${TEZ_JARS}/lib/*"
  } >> /etc/hadoop/conf/hadoop-env.sh

  # Configure YARN to enable the Application Timeline Server,
  # and configure the Tez UI to push logs to the ATS.
  # See https://tez.apache.org/tez-ui.html for more information.
  bdconfig set_property \
    --configuration_file "${HADOOP_CONF_DIR}/core-site.xml" \
    --name 'hadoop.http.filter.initializers' --value 'org.apache.hadoop.security.HttpCrossOriginFilterInitializer' \
    --clobber
  bdconfig set_property \
    --configuration_file "${HADOOP_CONF_DIR}/yarn-site.xml" \
    --name 'yarn.resourcemanager.webapp.cross-origin.enabled' --value 'true' \
    --clobber
  bdconfig set_property \
    --configuration_file "${HADOOP_CONF_DIR}/yarn-site.xml" \
    --name 'yarn.timeline-service.enabled' --value 'true' \
    --clobber
  bdconfig set_property \
    --configuration_file "${HADOOP_CONF_DIR}/yarn-site.xml" \
    --name 'yarn.timeline-service.hostname' --value "${HOSTNAME}" \
    --clobber
  bdconfig set_property \
    --configuration_file "${HADOOP_CONF_DIR}/yarn-site.xml" \
    --name 'yarn.timeline-service.http-cross-origin.enabled' --value 'true' \
    --clobber
  bdconfig set_property \
    --configuration_file "${HADOOP_CONF_DIR}/yarn-site.xml" \
    --name 'yarn.resourcemanager.system-metrics-publisher.enabled' --value 'true' \
    --clobber
  bdconfig set_property \
    --configuration_file "${TEZ_CONF_DIR}/tez-site.xml" \
    --name 'tez.history.logging.service.class' --value 'org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService' \
    --clobber
  bdconfig set_property \
    --configuration_file "${TEZ_CONF_DIR}/tez-site.xml" \
    --name 'tez.tez-ui.history-url.base' --value "http://${HOSTNAME}:9999/tez-ui/" \
    --clobber

  # Update hive to use tez as execution engine
  bdconfig set_property \
    --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
    --name 'hive.execution.engine' --value 'tez' \
    --clobber

  # Set up service for Tez UI on port 9999 at the path /tez-ui
  npm install -g http-server forever forever-service \
    || err 'Unable to install kafka libraries on master!'
  unzip ${TEZ_JARS}/tez-ui-*.war -d ${TEZ_JARS}/tez-ui \
    || err 'Unzipping tez-ui jars failed!'
  forever-service install tez-ui --script /usr/bin/http-server \
    -o "${TEZ_JARS}/ -p 9999" \
    || err 'Installation of tez-ui as a service failed!'

  # Restart daemons

  systemctl daemon-reload

  # Start tez ui
  systemctl enable tez-ui
  systemctl restart tez-ui
  systemctl status tez-ui  # Ensure it started successfully

  # Restart resource manager
  systemctl restart hadoop-yarn-resourcemanager
  systemctl status hadoop-yarn-resourcemanager  # Ensure it started successfully

  # Enable timeline server
  systemctl enable hadoop-yarn-timelineserver
  systemctl restart hadoop-yarn-timelineserver
  systemctl status hadoop-yarn-timelineserver  # Ensure it started successfully

  # Check hive-server2 status
  local hive_server2_status
  hive_server2_status=$(systemctl show --property=LoadState hive-server2.service \
    | sed -e 's/^.*=//')
  if [[ "${hive_server2_status}" == 'loaded' ]]; then
    # Restart hive server2 if it is installed/loaded
    systemctl restart hive-server2
    systemctl status hive-server2  # Ensure it started successfully
  else
    echo "Service hive-server2 is not loaded"
  fi
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  local master_hostname
  master_hostname="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"

  # Let spark continue using the existing hive configuration, as it will
  # not want to use hive on tez.
  cp ${HIVE_CONF_DIR}/* ${SPARK_CONF_DIR}/
  # Remove lines containing /etc/hive/conf from spark-env.sh
  sudo sed -i '\#CLASSPATH=.*/etc/hive/conf#d' /etc/spark/conf/spark-env.sh

  # Only run the installation on workers; verify zookeeper on master(s).
  if [[ "${role}" == 'Master' ]]; then
    configure_master_node
  fi

}

main
