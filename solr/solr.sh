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
#
# This script installs Apache Solr (http://lucene.apache.org/solr/) on a Google Cloud
# Dataproc cluster.

set -euxo pipefail

readonly SOLR_VERSION='7.6.0'
readonly SOLR_DOWNLOAD_LINK="http://archive.apache.org/dist/lucene/solr/${SOLR_VERSION}/solr-${SOLR_VERSION}.tgz"
readonly MASTER_ADDITIONAL="$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)"
readonly CLUSTER_NAME="$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)"
readonly NODE_NAME="$(/usr/share/google/get_metadata_value name)"

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function install_and_configure_solr() {
  local solr_home_dir
  local zookeeper_nodes
  zookeeper_nodes="$(grep '^server\.' /etc/zookeeper/conf/zoo.cfg \
    | uniq | cut -d '=' -f 2 | cut -d ':' -f 1 | xargs echo | sed "s/ /,/g")"

  cd tmp && wget -q "${SOLR_DOWNLOAD_LINK}" && wget -q "${SOLR_DOWNLOAD_LINK}.sha512"
  diff <(sha512sum solr-${SOLR_VERSION}.tgz | awk {'print $1'}) \
    <(cat solr-${SOLR_VERSION}.tgz.sha512 | awk {'print $1'}) \
    || err 'Verification of downloaded solr archive failed.'

  tar -xf "solr-${SOLR_VERSION}.tgz" && pushd "solr-${SOLR_VERSION}/bin" \
    && ./install_solr_service.sh "/tmp/solr-${SOLR_VERSION}.tgz" -n && popd

  rm -rf "/tmp/solr-${SOLR_VERSION}.tgz" "/tmp/solr-${SOLR_VERSION}"

  sed -i "s/^#SOLR_HOST=\"192.168.1.1\"/SOLR_HOST=\"${NODE_NAME}\"/" \
    /etc/default/solr.in.sh
  mkdir -p /var/log/solr && chown solr:solr /var/log/solr
  sed -i 's/^SOLR_LOGS_DIR="\/var\/solr\/logs"/SOLR_LOGS_DIR="\/var\/log\/solr"/' \
    /etc/default/solr.in.sh

  # Enable SolrCloud setup in HA mode.
  if [[ "${MASTER_ADDITIONAL}" != "" ]]; then
    sed -i "s/^#ZK_HOST=\"\"/ZK_HOST=\"${zookeeper_nodes}\/solr\"/" \
      /etc/default/solr.in.sh
    /opt/solr/bin/solr zk mkroot /solr -z "${NODE_NAME}:2181" || echo 'Node already exists for /solr.'
  fi

  # Enable hdfs as default backend storage.
  if [[ "${MASTER_ADDITIONAL}" != "" ]]; then
    solr_home_dir="hdfs://${CLUSTER_NAME}-m-0:8020/solr"
  else
    solr_home_dir="hdfs://${CLUSTER_NAME}-m:8020/solr"
  fi
  cat << EOF >> /etc/default/solr.in.sh
SOLR_OPTS="\${SOLR_OPTS} -Dsolr.directoryFactory=HdfsDirectoryFactory -Dsolr.lock.type=hdfs \
 -Dsolr.hdfs.home=${solr_home_dir}"
EOF
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

  if [[ "${role}" == 'Master' ]]; then
    install_and_configure_solr
    systemctl start solr || err 'Unable to start solr service.'
  fi
}

main