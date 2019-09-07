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

set -o errexit  # exit when a command fails - append "|| true" to allow a
                # command to fail
set -o nounset  # exit when attempting to use undeclared variables

# Show commands being run - useful to keep by default so that failures
# are easy to debug
set -x

# Variables for running this script
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly MASTER_FQDN="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"

SPARK_HOME=${SPARK_HOME:-"/usr/lib/spark"}
HIVE_HOME=${HIVE_HOME:-"/usr/lib/hive"}
HADOOP_HOME=${HADOOP_HOME:-"/usr/lib/hadoop"}
PRESTO_HOME=${PRESTO_HOME:-$(ls -d -- /presto-server*)}

# Script constants
ALLUXIO_VERSION=2.0.1
ALLUXIO_HOME=/opt/alluxio
ALLUXIO_SITE_PROPERTIES=${ALLUXIO_HOME}/conf/alluxio-site.properties
ALLUXIO_DOWNLOAD_URL=https://downloads.alluxio.io/downloads/files/${ALLUXIO_VERSION}/alluxio-${ALLUXIO_VERSION}-bin.tar.gz

# Downloads a file to the local machine from a remote HTTP(S) or GCS URI into the cwd
#
# Args:
#   $1: URI - the remote location to retrieve the file from
download_file() {
  local uri="$1"

  if [[ "${uri}" == gs://* ]]
  then
    sudo gsutil cp "${uri}" ./
  else
    # TODO Add metadata header tag to the wget for filtering out in download metrics.
    sudo wget -nv "${uri}"
  fi
}

# Appends a property KV pair to the alluxio-site.properties file
#
# Args:
#   $1: property name
#   $2: property value
append_alluxio_property() {
  local property="$1"
  local value="$2"

  echo "${property}=${value}" >> ${ALLUXIO_SITE_PROPERTIES}
}

# Calculates the default memory size as 1/3 of the total system memory
#
# Echo's the result to stdout. To store the return value in a variable use
# val=$(get_defaultmem_size)
get_default_mem_size() {
  local mem_div=3
  local phy_total=$(free -m | grep -oP '\d+' | head -n1)
  local mem_size=$(( ${phy_total} / ${mem_div} ))
  echo "${mem_size}MB"
}

# Download the Alluxio tarball and untar to ALLUXIO_HOME
function bootstrap_alluxio() {
  mkdir ${ALLUXIO_HOME}
  download_file ${ALLUXIO_DOWNLOAD_URL}
  local tarball_name=${ALLUXIO_DOWNLOAD_URL##*/}
  sudo tar -zxf ${tarball_name} -C ${ALLUXIO_HOME} --strip-components 1
  sudo ln -s ${ALLUXIO_HOME}/client/*client.jar ${ALLUXIO_HOME}/client/alluxio-client.jar

  # Download files to /opt/alluxio/conf
  local download_files_list=$(/usr/share/google/get_metadata_value attributes/download_files_list)
  local download_delimiter=";"
  IFS="${download_delimiter}" read -ra files_to_be_downloaded <<< "${download_files_list}"
  if [ "${#files_to_be_downloaded[@]}" -gt "0" ]; then
    for file in "${files_to_be_downloaded[@]}"; do
      local filename="$(basename ${file})"
      download_file "${file}"
      sudo mv "${filename}" "${ALLUXIO_HOME}/conf/${filename}"
    done
  fi

  # Configure client applications
  sudo mkdir -p ${SPARK_HOME}/jars/
  sudo ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" ${SPARK_HOME}/jars/alluxio-client.jar
  sudo mkdir -p ${HIVE_HOME}/lib/
  sudo ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" ${HIVE_HOME}/lib/alluxio-client.jar
  sudo mkdir -p ${HADOOP_HOME}/lib/
  sudo ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" ${HADOOP_HOME}/lib/alluxio-client.jar
  sudo mkdir -p ${PRESTO_HOME}/plugin/hive-hadoop2/
  sudo ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" ${PRESTO_HOME}/plugin/hive-hadoop2/alluxio-client.jar
  sudo systemctl restart hive-metastore
  sudo systemctl restart hive-server2
}

# Configure alluxio-site.properties
function configure_alluxio() {
  cp ${ALLUXIO_HOME}/conf/alluxio-site.properties.template ${ALLUXIO_SITE_PROPERTIES}

  append_alluxio_property alluxio.master.hostname "${MASTER_FQDN}"

  local root_ufs_uri=$(/usr/share/google/get_metadata_value attributes/root_ufs_uri)
  append_alluxio_property alluxio.master.mount.table.root.ufs "${root_ufs_uri}"

  local mem_size=$(get_default_mem_size)
  append_alluxio_property alluxio.worker.memory.size "${mem_size}"
  append_alluxio_property alluxio.worker.tieredstore.level0.alias "MEM"
  append_alluxio_property alluxio.worker.tieredstore.level0.dirs.path "/mnt/ramdisk"
  append_alluxio_property alluxio.worker.tieredstore.levels "1"

  append_alluxio_property alluxio.master.security.impersonation.root.users "*"
  append_alluxio_property alluxio.master.security.impersonation.root.groups "*"
  append_alluxio_property alluxio.master.security.impersonation.client.users "*"
  append_alluxio_property alluxio.master.security.impersonation.client.groups "*"
  append_alluxio_property alluxio.security.login.impersonation.username "none"
  append_alluxio_property alluxio.security.authorization.permission.enabled "false"

  local site_properties=$(/usr/share/google/get_metadata_value attributes/site_properties)
  local property_delimiter=";"
  if [[ "${site_properties}" ]]; then
    IFS="${property_delimiter}" read -ra conf <<< "${site_properties}"
    for property in "${conf[@]}"; do
      local key=${property%%"="*}
      local value=${property#*"="}
      append_alluxio_property "${key}" "${value}"
    done
  fi
}

# Start the Alluxio server process
function start_alluxio() {
  if [[ "${ROLE}" == "Master" ]]; then
    sudo ${ALLUXIO_HOME}/bin/alluxio formatMaster
    sudo ${ALLUXIO_HOME}/bin/alluxio-start.sh master
  else
    sleep 60 # TODO: Remove sleep after making AlluxioWorkerMonitor retry configurable
    sudo ${ALLUXIO_HOME}/bin/alluxio-mount.sh SudoMount local
    sudo ${ALLUXIO_HOME}/bin/alluxio formatWorker
    sudo ${ALLUXIO_HOME}/bin/alluxio-start.sh worker NoMount
  fi
}

function main() {
  bootstrap_alluxio
  configure_alluxio
  start_alluxio
}

main
