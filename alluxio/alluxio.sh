#!/bin/bash

#    Copyright 2019 Google LLC.
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

# Variables for running this script
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly MASTER_FQDN="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"

ALLUXIO_VERSION=$(/usr/share/google/get_metadata_value attributes/alluxio_version || echo "2.0.1")

SPARK_HOME=${SPARK_HOME:-"/usr/lib/spark"}
HIVE_HOME=${HIVE_HOME:-"/usr/lib/hive"}
HADOOP_HOME=${HADOOP_HOME:-"/usr/lib/hadoop"}

# Script constants
ALLUXIO_HOME=/opt/alluxio
ALLUXIO_SITE_PROPERTIES=${ALLUXIO_HOME}/conf/alluxio-site.properties
ALLUXIO_DOWNLOAD_URL=https://downloads.alluxio.io/downloads/files/${ALLUXIO_VERSION}/alluxio-${ALLUXIO_VERSION}-bin.tar.gz

# Downloads a file to the local machine from a remote HTTP(S) or GCS URI into the cwd
#
# Args:
#   $1: URI - the remote location to retrieve the file from
download_file() {
  local uri="$1"

  if [[ "${uri}" == gs://* ]]; then
    gsutil cp "${uri}" ./
  else
    # TODO Add metadata header tag to the wget for filtering out in download metrics.
    wget -nv --timeout=30 --tries=5 --retry-connrefused "${uri}"
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

  echo "${property}=${value}" >>${ALLUXIO_SITE_PROPERTIES}
}

# Calculates the default memory size as 1/3 of the total system memory
#
# Echo's the result to stdout. To store the return value in a variable use
# val=$(get_defaultmem_size)
get_default_mem_size() {
  local mem_div=3
  local phy_total
  phy_total=$(free -m | grep -oP '\d+' | head -n1)
  local mem_size
  mem_size=$((phy_total / mem_div))
  echo "${mem_size}MB"
}

# Download the Alluxio tarball and untar to ALLUXIO_HOME
function bootstrap_alluxio() {
  mkdir ${ALLUXIO_HOME}
  download_file "${ALLUXIO_DOWNLOAD_URL}"
  local tarball_name=${ALLUXIO_DOWNLOAD_URL##*/}
  tar -zxf "${tarball_name}" -C ${ALLUXIO_HOME} --strip-components 1
  ln -s "${ALLUXIO_HOME}/client/*client.jar" "${ALLUXIO_HOME}/client/alluxio-client.jar"

  # Download files to /opt/alluxio/conf
  local download_files_list
  download_files_list=$(/usr/share/google/get_metadata_value attributes/alluxio_download_files_list || true)
  local download_delimiter=";"
  IFS="${download_delimiter}" read -ra files_to_be_downloaded <<<"${download_files_list}"
  if [ "${#files_to_be_downloaded[@]}" -gt "0" ]; then
    for file in "${files_to_be_downloaded[@]}"; do
      local filename
      filename="$(basename "${file}")"
      download_file "${file}"
      mv "${filename}" "${ALLUXIO_HOME}/conf/${filename}"
    done
  fi

  # Configure client applications
  mkdir -p "${SPARK_HOME}/jars/"
  ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" "${SPARK_HOME}/jars/alluxio-client.jar"
  mkdir -p "${HIVE_HOME}/lib/"
  ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" "${HIVE_HOME}/lib/alluxio-client.jar"
  mkdir -p "${HADOOP_HOME}/lib/"
  ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" "${HADOOP_HOME}/lib/alluxio-client.jar"
  if [[ "${ROLE}" == "Master" ]]; then
    systemctl restart hive-metastore hive-server2
  fi

  # Optionally configure Presto
  # OK to fail in this section
  set +o errexit
  PRESTO_HOME=${PRESTO_HOME:-$(ls -d -- /presto-server-*)}
  if [[ -n $PRESTO_HOME ]]; then
    mkdir -p "${PRESTO_HOME}/plugin/hive-hadoop2/"
    ln -s "${ALLUXIO_HOME}/client/alluxio-client.jar" "${PRESTO_HOME}/plugin/hive-hadoop2/alluxio-client.jar"
    systemctl restart presto
  fi
  set -o errexit # errors not ok anymore
}

# Configure alluxio-site.properties
function configure_alluxio() {
  cp "${ALLUXIO_HOME}/conf/alluxio-site.properties.template" ${ALLUXIO_SITE_PROPERTIES}

  append_alluxio_property alluxio.master.hostname "${MASTER_FQDN}"

  local root_ufs_uri
  root_ufs_uri=$(/usr/share/google/get_metadata_value attributes/alluxio_root_ufs_uri)
  append_alluxio_property alluxio.master.mount.table.root.ufs "${root_ufs_uri}"

  local mem_size
  mem_size=$(get_default_mem_size)
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

  local site_properties
  site_properties=$(/usr/share/google/get_metadata_value attributes/alluxio_site_properties || true)
  local property_delimiter=";"
  if [[ "${site_properties}" ]]; then
    IFS="${property_delimiter}" read -ra conf <<<"${site_properties}"
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
    ${ALLUXIO_HOME}/bin/alluxio formatMaster
    ${ALLUXIO_HOME}/bin/alluxio-start.sh master
  else
    sleep 60 # TODO: Remove sleep after making AlluxioWorkerMonitor retry configurable
    ${ALLUXIO_HOME}/bin/alluxio-mount.sh SudoMount local
    ${ALLUXIO_HOME}/bin/alluxio formatWorker
    ${ALLUXIO_HOME}/bin/alluxio-start.sh worker NoMount
  fi
}

function main() {
  bootstrap_alluxio
  configure_alluxio
  start_alluxio
}

main
