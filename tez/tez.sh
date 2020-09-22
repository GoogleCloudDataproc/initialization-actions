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

readonly NOT_SUPPORTED_MESSAGE="Tez initialization action is not supported on Dataproc ${DATAPROC_VERSION}.
Tez is configured by default in Dataproc 1.3+"
[[ $DATAPROC_VERSION != 1.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

# Use Python from /usr/bin instead of /opt/conda.
export PATH=/usr/bin:$PATH

readonly TEZ_HDFS_PATH='/apps/tez'
readonly TEZ_JARS='/usr/lib/tez'
readonly TEZ_CONF_DIR='/etc/tez/conf'
readonly HADOOP_CONF_DIR='/etc/hadoop/conf'
readonly HIVE_CONF_DIR='/etc/hive/conf'
readonly SPARK_CONF_DIR='/etc/spark/conf'

function is_centos() {
  [[ "$(. /etc/os-release && echo "${ID}")" == 'centos' ]]
  return $?
}

function is_debian() {
  [[ "$(. /etc/os-release && echo "${ID}")" == 'debian' ]]
  return $?
}

function is_ubuntu() {
  [[ "$(. /etc/os-release && echo "${ID}")" == 'ubuntu' ]]
  return $?
}

function retry_command() {
  cmd="$1"
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function install_yum() {
  local pkgs="$*"
  retry_command "yum install -y $pkgs"
}

function install_apt_get() {
  local pkgs="$*"
  retry_command "apt-get install -y $pkgs"
}

function install_packages() {
  local pkgs="$*"
  if is_centos; then
    install_yum "$pkgs"
  else
    install_apt_get "$pkgs"
  fi
}

function update_repo() {
  if is_centos; then
    retry_command "yum -y update"
  else
    retry_command "apt-get update"
  fi
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  return 1
}

function configure_master_node() {
  update_repo || err 'Unable to update packages repository.'
  install_packages tez hadoop-yarn-timelineserver ||
    err 'Failed to install required packages.'

  # Copy to hdfs from one master only to avoid race
  if [[ "${HOSTNAME}" == "${master_hostname}" ]]; then
    # Stage Tez
    hadoop fs -mkdir -p ${TEZ_HDFS_PATH}
    hadoop fs -copyFromLocal ${TEZ_JARS}/* ${TEZ_HDFS_PATH}/ ||
      err 'Unable to copy tez jars to hdfs destination.'
  fi

  # Update the hadoop-env.sh
  {
    echo "export TEZ_CONF_DIR=${TEZ_CONF_DIR}"
    echo "export TEZ_JARS=${TEZ_JARS}"
    echo "HADOOP_CLASSPATH=\$HADOOP_CLASSPATH:${TEZ_CONF_DIR}:${TEZ_JARS}/*:${TEZ_JARS}/lib/*"
  } >>/etc/hadoop/conf/hadoop-env.sh

  # Configure YARN to enable the Application Timeline Server.
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

  # Configure ATS to serve Tez UI. Tez UI can be accessed at
  # http://clustername-m:8188/tez.
  TEZ_UI_WAR=$(ls /usr/lib/tez/tez-ui-*.war)
  bdconfig set_property \
    --configuration_file "${HADOOP_CONF_DIR}/yarn-site.xml" \
    --name 'yarn.timeline-service.ui-on-disk-path.tez' --value "${TEZ_UI_WAR}" \
    --clobber
  bdconfig set_property \
    --configuration_file "${HADOOP_CONF_DIR}/yarn-site.xml" \
    --name 'yarn.timeline-service.ui-web-path.tez' --value '/tez-ui' \
    --clobber
  bdconfig set_property \
    --configuration_file "${HADOOP_CONF_DIR}/yarn-site.xml" \
    --name 'yarn.timeline-service.ui-names' --value 'tez' \
    --clobber

  # Configure the Tez UI to push logs to the ATS.
  # See https://tez.apache.org/tez-ui.html for more information.
  bdconfig set_property \
    --configuration_file "${TEZ_CONF_DIR}/tez-site.xml" \
    --name 'tez.history.logging.service.class' --value 'org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService' \
    --clobber
  bdconfig set_property \
    --configuration_file "${TEZ_CONF_DIR}/tez-site.xml" \
    --name 'tez.tez-ui.history-url.base' --value "http://${HOSTNAME}:8188/tez-ui/" \
    --clobber

  # Update hive to use tez as execution engine
  bdconfig set_property \
    --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
    --name 'hive.execution.engine' --value 'tez' \
    --clobber

  # Restart daemons

  systemctl daemon-reload

  # Restart resource manager
  systemctl restart hadoop-yarn-resourcemanager
  systemctl status hadoop-yarn-resourcemanager # Ensure it started successfully

  # Enable timeline server
  systemctl enable hadoop-yarn-timelineserver
  systemctl restart hadoop-yarn-timelineserver
  systemctl status hadoop-yarn-timelineserver # Ensure it started successfully

  # Check hive-server2 status
  if (systemctl is-enabled --quiet hive-server2); then
    # Restart hive server2 if it is enabled
    systemctl restart hive-server2
    systemctl status hive-server2 # Ensure it started successfully
  else
    echo "Service hive-server2 is not enabled"
  fi
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  local master_hostname
  master_hostname="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"

  # Let Spark continue using the existing hive configuration, as it will
  # not want to use hive on tez.
  cp ${HIVE_CONF_DIR}/* ${SPARK_CONF_DIR}/
  # Remove lines containing /etc/hive/conf from spark-env.sh
  sudo sed -i '\#CLASSPATH=.*/etc/hive/conf#d' /etc/spark/conf/spark-env.sh

  # Only run the installation on workers; verify Zookeeper on master(s).
  if [[ "${role}" == 'Master' ]]; then
    configure_master_node
  fi
}

main
