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

readonly HBASE_FILE='hbase-1.3.2.1-bin'
readonly HBASE_LINK="https://www-us.apache.org/dist/hbase/1.3.2.1/${HBASE_FILE}.tar.gz"
readonly HBASE_SHA_SUM="${HBASE_LINK}.sha512"
readonly HBASE_HOME='/etc/hbase'
readonly CLUSTER_NAME="$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)"
readonly WORKER_COUNT="$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)"
readonly MASTER_ADDITIONAL="$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)"

function install_and_configure_hbase() {
  local zookeeper_nodes="${CLUSTER_NAME}-m"

  wget -q "${HBASE_LINK}" \
    && wget -q "${HBASE_SHA_SUM}" \
    && diff <(gpg --print-md SHA512 "${HBASE_FILE}.tar.gz") <(cat "${HBASE_FILE}.tar.gz.sha512") \
    && tar -xf "${HBASE_FILE}.tar.gz" \
    && rm -f "${HBASE_FILE}.tar.gz" "${HBASE_FILE}.tar.gz.sha512"

  if [[ -d "${HBASE_HOME}" ]]; then
    mv "${HBASE_HOME}" "${HBASE_HOME}_old"
  fi
  mv hbase-1.3.2.1 "${HBASE_HOME}"

  echo 'export PATH=/etc/hbase/bin:${PATH}' >> /etc/profile

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

  if [[ "${MASTER_ADDITIONAL}" != "" ]]; then
    # If true than init action is running on high availability dataproc cluster.
    # HBase will use zookeeper service pre-installed and running on master nodes.
    bdconfig set_property \
      --configuration_file 'hbase-site.xml.tmp' \
      --name 'hbase.zookeeper.quorum' --value "${zookeeper_nodes}-0,${MASTER_ADDITIONAL}" \
      --clobber
    bdconfig set_property \
      --configuration_file 'hbase-site.xml.tmp' \
      --name 'hbase.rootdir' --value "hdfs://${CLUSTER_NAME}-m-0:8020/hbase" \
      --clobber
  else
    # For non HA cluster we will use zookeper managed by HBase installed on
    # master and workers nodes.
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
      --name 'hbase.rootdir' --value "hdfs://${CLUSTER_NAME}-m:8020/hbase" \
      --clobber

    "${HBASE_HOME}/bin/hbase-daemon.sh" start zookeeper
  fi

  bdconfig merge_configurations \
    --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
    --source_configuration_file hbase-site.xml.tmp \
    --clobber
  
  # On single node clusters we must also start regionserver on it.
  if [[ "${WORKER_COUNT}" -eq 0 ]]; then
    "${HBASE_HOME}/bin/hbase-daemon.sh" start regionserver
  fi
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  
  install_and_configure_hbase

  if [[ "${role}" == 'Master' ]]; then
    "${HBASE_HOME}/bin/hbase-daemon.sh" start master
  else
    "${HBASE_HOME}/bin/hbase-daemon.sh" start regionserver
  fi
}

main