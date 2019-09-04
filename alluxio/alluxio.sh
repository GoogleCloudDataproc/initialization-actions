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

# Variables for running this script
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly MASTER_FQDN="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"

# The following properties must be manually configured
## BEGIN CONFIGURATION ##
ALLUXIO_VERSION=2.0.1
## END CONFIGURATION ##

# Script constants
ALLUXIO_HOME=/opt/alluxio
ALLUXIO_SITE_PROPERTIES=${ALLUXIO_HOME}/conf/alluxio-site.properties
ALLUXIO_DOWNLOAD_URL=https://downloads.alluxio.io/downloads/files/${ALLUXIO_VERSION}/alluxio-${ALLUXIO_VERSION}-bin.tar.gz

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
  sudo wget ${ALLUXIO_DOWNLOAD_URL}
  local tarball_name=${ALLUXIO_DOWNLOAD_URL##*/}
  sudo tar -zxf ${tarball_name} -C ${ALLUXIO_HOME} --strip-components 1
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

  local delimited_properties=$(/usr/share/google/get_metadata_value attributes/delimited_properties)
  if [[ "${delimited_properties}" ]]; then
    IFS="${property_delimiter}" read -ra conf <<< "${delimited_properties}"
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
