#!/bin/bash
#    Copyright 2019 Google, Inc.
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

# This script installs CMAK formerly only as Kafka Manager
# on a Dataproc cluster.
#
# Kafka Manager server will be running on the first master node (port 9000 by default).

set -euxo pipefail

readonly KAFKA_MANAGER_HTTP_PORT="$(/usr/share/google/get_metadata_value attributes/kafka-manager-http-port || echo 9000)"
readonly KAFKA_MANAGER_GIT_URI=https://github.com/yahoo/CMAK.git
readonly KAFKA_MANAGER_GIT_DIR=/tmp/kafka-manager
readonly KAFKA_MANAGER_VERSION="cmak-3.0.0.7"
readonly KAFKA_MANAGER_HOME="/opt/kafka-manager-${KAFKA_MANAGER_VERSION}"
readonly ZOOKEEPER_CONFIG=/etc/zookeeper/conf/zoo.cfg

function install_packages(){
   apt-get update
   apt-get install -yq apt-transport-https curl gnupg
}

function add_sources(){
   echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list
   echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list

   curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" |
      gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
   chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
}

function install_sbt(){
   apt-get update
   apt-get install -yq sbt
}

function build_cmak(){
   mkdir -p "${KAFKA_MANAGER_GIT_DIR}"
   cd "${KAFKA_MANAGER_GIT_DIR}"
   git clone "${KAFKA_MANAGER_GIT_URI}"
   cd "${KAFKA_MANAGER_GIT_DIR}"/CMAK
   sbt clean dist
}

function install_cmak(){
   cp "${KAFKA_MANAGER_GIT_DIR}"/CMAK/target/universal/"${KAFKA_MANAGER_VERSION}".zip /opt
   cd /opt
   unzip "${KAFKA_MANAGER_VERSION}".zip
}

function get_zookeeper_list() {
   local zookeeper_client_port=$(grep 'clientPort' "${ZOOKEEPER_CONFIG}" |
      tail -n 1 |
      cut -d '=' -f 2)
   local zookeeper_list=$(grep '^server.' "${ZOOKEEPER_CONFIG}" |
      tac |
      sort -u -t '=' -k1,1 |
      cut -d '=' -f 2 |
      cut -d ':' -f 1 |
      sed "s/$/:${zookeeper_client_port}/" |
      xargs echo |
      sed "s/ /,/g")
  echo "${zookeeper_list}"
}

function configure_and_start_cmak(){
   local cluster_name=$(hostname | sed 's/\(.*\)-m-0$/\1/g')
   local zkhosts="$(get_zookeeper_list)"

   cd /opt/"${KAFKA_MANAGER_VERSION}"
   sed -i 's/cmak.zkhosts="kafka-manager-zookeeper:2181"/cmak.zkhosts=\"'"${zkhosts}"'\"/g' conf/application.conf

   echo "Starting Kafka Manager server on ${HOSTNAME}:${KAFKA_MANAGER_HTTP_PORT}."
   ./bin/cmak -Dconfig.file=conf/application.conf -Dhttp.port="${KAFKA_MANAGER_HTTP_PORT}" &
}

function main(){
   local java_major_version=$(java -version 2>&1 | grep -oP 'version "?(1\.)?\K\d+' || true)
   if [[ ${java_major_version} -lt 11 ]]; then
      echo "Error: Java 11 or higher is required for CMAK" >&2
      echo "CMAK has not been installed" >&2
      exit 1
   else
      # Run Kafka Manager on the first master node.
      if [[ "${HOSTNAME}" == *-m || "${HOSTNAME}" == *-m-0 ]]; then
         install_packages
         add_sources
         install_sbt
         build_cmak
         install_cmak
         configure_and_start_cmak
      fi
   fi
}

main
echo "All Done"
