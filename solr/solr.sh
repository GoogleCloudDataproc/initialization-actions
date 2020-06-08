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

readonly NOT_SUPPORTED_MESSAGE="Solr initialization action is not supported on Dataproc 2.0+.
Use Solr Component instead: https://cloud.google.com/dataproc/docs/concepts/components/solr"
[[ $DATAPROC_VERSION = 2.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

readonly MASTER_ADDITIONAL="$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)"
readonly CLUSTER_NAME="$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)"
readonly NODE_NAME="$(/usr/share/google/get_metadata_value name)"
readonly SOLR_CONF_FILE='/etc/default/solr'

function retry_apt_command() {
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
  retry_apt_command "apt-get update"
}

function install_apt_get() {
  pkgs="$@"
  retry_apt_command "apt-get install -y $pkgs"
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function install_and_configure_solr() {
  local solr_home_dir
  local zookeeper_nodes
  zookeeper_nodes="$(grep '^server\.' /etc/zookeeper/conf/zoo.cfg |
    uniq | cut -d '=' -f 2 | cut -d ':' -f 1 | xargs echo | sed "s/ /,/g")"

  # Install deb packages from GS
  update_apt_get
  install_apt_get solr

  sed -i "s/^#SOLR_HOST=\"192.168.1.1\"/SOLR_HOST=\"${NODE_NAME}\"/" "${SOLR_CONF_FILE}"
  sed -i 's/^#SOLR_LOGS_DIR=/SOLR_LOGS_DIR=/' "${SOLR_CONF_FILE}"
  chown -R solr:solr /usr/lib/solr/server/solr

  # Enable SolrCloud setup in HA mode.
  if [[ "${MASTER_ADDITIONAL}" != "" ]]; then
    sed -i "s/^#ZK_HOST=\"\"/ZK_HOST=\"${zookeeper_nodes}\/solr\"/" \
      "${SOLR_CONF_FILE}"
    /usr/lib/solr/bin/solr zk mkroot /solr -z "${NODE_NAME}:2181" || echo 'Node already exists for /solr.'
  fi

  # Enable hdfs as default backend storage.
  if [[ "${MASTER_ADDITIONAL}" != "" ]]; then
    solr_home_dir="hdfs://${CLUSTER_NAME}-m-0:8020/solr"
  else
    solr_home_dir="hdfs://${CLUSTER_NAME}-m:8020/solr"
  fi
  cat <<EOF >>"${SOLR_CONF_FILE}"
SOLR_OPTS="\${SOLR_OPTS} -Dsolr.directoryFactory=HdfsDirectoryFactory -Dsolr.lock.type=hdfs \
 -Dsolr.hdfs.home=${solr_home_dir}"
EOF

  cat <<EOF >/etc/systemd/system/solr.service
[Unit]
Description=Apache SOLR
ConditionPathExists=/usr/lib/solr/bin
After=syslog.target network.target remote-fs.target nss-lookup.target systemd-journald-dev-log.socket
Before=multi-user.target
Conflicts=shutdown.target

[Service]
User=solr
LimitNOFILE=1048576
LimitNPROC=1048576
Environment=SOLR_INCLUDE=/etc/default/solr
Environment=RUNAS=solr
Environment=SOLR_INSTALL_DIR=/usr/lib/solr

Restart=on-failure
RestartSec=5

ExecStart=/usr/lib/solr/bin/solr start -f
ExecStop=/usr/lib/solr/bin/solr stop
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

  # Remove default init.d file and use solr as systemd service
  rm -f /etc/init.d/solr-server && update-rc.d -f solr-server remove
  systemctl daemon-reload || err 'Unable to reload daemons.'
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
