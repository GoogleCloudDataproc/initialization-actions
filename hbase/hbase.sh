#!/bin/bash
#    Copyright 2018 Google, Inc.
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
#
# This initialization action installs Apache HBase on Dataproc Cluster.

set -euxo pipefail

readonly HBASE_HOME='/etc/hbase'
readonly CLUSTER_NAME="$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)"
readonly WORKER_COUNT="$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)"
readonly MASTER_ADDITIONAL="$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)"

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

function update_apt_get() {
  retry_command "apt-get update"
}

function install_apt_get() {
  pkgs="$@"
  retry_command "apt-get install -y $pkgs"
}

function configure_hbase() {
  local zookeeper_nodes="${CLUSTER_NAME}-m"
  local hbase_root_dir="$(/usr/share/google/get_metadata_value attributes/hbase-root-dir)"

  cat << EOF > hbase-site.xml.tmp
  <configuration>
    <property>
      <name>hbase.cluster.distributed</name>
      <value>true</value>
    </property>
    <property>
      <name>hbase.zookeeper.property.initLimit</name>
      <value>20</value>
    </property>
  </configuration>
EOF

  cat << EOF > /etc/systemd/system/hbase-master.service
[Unit]
Description=HBase Master
Wants=network-online.target
After=network-online.target hadoop-hdfs-namenode.service

[Service]
User=root
Group=root
Type=simple
EnvironmentFile=/etc/environment
Environment=HBASE_HOME=/etc/hbase
ExecStart=/usr/bin/hbase \
  --config ${HBASE_HOME}/conf/ \
  master start

[Install]
WantedBy=multi-user.target
EOF

  cat << EOF > /etc/systemd/system/hbase-regionserver.service
[Unit]
Description=HBase Regionserver
Wants=network-online.target
After=network-online.target hadoop-hdfs-datanode.service

[Service]
User=root
Group=root
Type=simple
EnvironmentFile=/etc/environment
Environment=HBASE_HOME=/etc/hbase
ExecStart=/usr/bin/hbase \
  --config ${HBASE_HOME}/conf/ \
  regionserver start

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload

  if [[ "${MASTER_ADDITIONAL}" != "" ]]; then
    # If true than init action is running on high availability dataproc cluster.
    # HBase will use zookeeper service pre-installed and running on master nodes.
    if [[ -z "${hbase_root_dir}" ]]; then
      hbase_root_dir="hdfs://${CLUSTER_NAME}-m-0:8020/hbase"
    fi
    bdconfig set_property \
      --configuration_file 'hbase-site.xml.tmp' \
      --name 'hbase.zookeeper.quorum' --value "${zookeeper_nodes}-0,${MASTER_ADDITIONAL}" \
      --clobber
    bdconfig set_property \
      --configuration_file 'hbase-site.xml.tmp' \
      --name 'hbase.rootdir' --value "${hbase_root_dir}" \
      --clobber
  else
    if [[ -z "${hbase_root_dir}" ]]; then
      hbase_root_dir="hdfs://${CLUSTER_NAME}-m:8020/hbase"
    fi
    for (( i=0; i<${WORKER_COUNT}; i++ ))
    do
      zookeeper_nodes="${zookeeper_nodes},${CLUSTER_NAME}-w-${i}"
    done
    bdconfig set_property \
      --configuration_file 'hbase-site.xml.tmp' \
      --name 'hbase.zookeeper.quorum' --value "${zookeeper_nodes}" \
      --clobber
    bdconfig set_property \
      --configuration_file 'hbase-site.xml.tmp' \
      --name 'hbase.rootdir' --value "${hbase_root_dir}" \
      --clobber
  fi

  bdconfig merge_configurations \
    --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
    --source_configuration_file hbase-site.xml.tmp \
    --clobber

  # On single node clusters we must also start regionserver on it and provide zookeeper-server.
  if [[ "${WORKER_COUNT}" -eq 0 ]]; then
    install_apt_get zookeeper-server || err 'Unable to install Zookeeper on single node cluster.'
    systemctl start hbase-regionserver
  fi
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

  update_apt_get || err 'Unable to update packages lists.'
  install_apt_get hbase || err 'Unable to install hbase.'

  configure_hbase

  if [[ "${role}" == 'Master' ]]; then
    systemctl start hbase-master
  else
    systemctl start hbase-regionserver
  fi
}

main
