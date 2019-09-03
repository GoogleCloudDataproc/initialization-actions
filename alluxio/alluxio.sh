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
ALLUXIO_VERSION=1.8.1
HADOOP_VERSION=2.8
## END CONFIGURATION ##

function bootstrap_alluxio() {
  sudo apt-get install jq

  # Download Alluxio
  cd /opt
  VERSION=alluxio-${ALLUXIO_VERSION}-hadoop-${HADOOP_VERSION}
  sudo wget http://downloads.alluxio.org/downloads/files/${ALLUXIO_VERSION}/${VERSION}-bin.tar.gz
  sudo tar -zxf ${VERSION}-bin.tar.gz
  sudo chown -R hadoop:hadoop ${VERSION}
  cd ${VERSION}
}

function configure_alluxio() {
  sudo chown -R hadoop:hadoop .

  sudo ./bin/alluxio bootstrapConf "${MASTER_FQDN}"

  cp conf/alluxio-site.properties.template conf/alluxio-site.properties
  echo "alluxio.master.security.impersonation.root.users=*" >> ./conf/alluxio-site.properties
  echo "alluxio.master.security.impersonation.root.groups=*" >> ./conf/alluxio-site.properties
  echo "alluxio.master.security.impersonation.client.users=*" >> ./conf/alluxio-site.properties
  echo "alluxio.master.security.impersonation.client.groups=*" >> ./conf/alluxio-site.properties
  echo "alluxio.security.login.impersonation.username=none" >> ./conf/alluxio-site.properties
  echo "alluxio.security.authorization.permission.enabled=false" >> ./conf/alluxio-site.properties
  echo "alluxio.user.block.size.bytes.default=128MB" >> ./conf/alluxio-site.properties
}

function start_alluxio() {
  if [[ "${ROLE}" == "Master" ]]; then
    sudo ./bin/alluxio format
    sudo ./bin/alluxio-start.sh master
  else
    sudo ./bin/alluxio format
    sudo ./bin/alluxio-start.sh worker Mount
  fi
}

function main() {
  bootstrap_alluxio
  configure_alluxio
  start_alluxio
}

main
