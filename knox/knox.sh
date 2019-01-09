#!/bin/bash
# Copyright 2016 Google, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This init script installs knox on each master node in the cluster

set -xe

export GATEWAY_HOME="/usr/lib/knox"

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function prepare_helper(){
  cd ${GATEWAY_HOME}
  cat << 'EOF' > bin/knox_helper.sh
#!/usr/bin/expect -f

set timeout 1800
set cmd [lindex $argv 0]

spawn {*}$cmd
expect {
    "Enter master secret:" {
        exp_send "pass\r"
        exp_continue
     }
     "Enter master secret again:" {
        exp_send "pass\r"
        exp_continue
     }
     eof
}
EOF
}

function create_master(){
  /usr/bin/expect ${GATEWAY_HOME}/bin/knox_helper.sh "${GATEWAY_HOME}/bin/knoxcli.sh create-master --force"
  sudo chmod -R u+w ${GATEWAY_HOME}/data/security
  sudo chown -R knox:sudo ${GATEWAY_HOME}/data/security
}

function set_webhdfs_address(){
  local host
  local port
  local master
  master="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
  host=$(hdfs getconf -confKey dfs.namenode.http-address)
  port="$(echo ${host} | sed -e 's,^.*:,:,g' -e 's,.*:\([0-9]*\).*,\1,g' -e 's,[^0-9],,g')"
  sed -i -- "s/localhost:50070/${master}:${port}/g" ${GATEWAY_HOME}/conf/topologies/sandbox.xml
}

function start_services(){
  sudo su - knox -p knox -c "${GATEWAY_HOME}/bin/ldap.sh start"
  sudo su - knox -p knox -c "${GATEWAY_HOME}/bin/gateway.sh start"
}

function install_dependencies(){
  yes | apt-get install knox || err "Failed to install Knox"
  yes | apt-get install expect || err "Failed to install expect"
}

function add_user(){
  useradd knox
  echo "knox:knox"|chpasswd
}

function grant_knox_folder_permissions(){
  chmod -R u+w ${GATEWAY_HOME}
  chown -R knox:sudo ${GATEWAY_HOME}
}

function configure_ha(){
  local master
  master="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"

  if [[ "${HOSTNAME}" == ${master} ]];then
    hdfs dfs -mkdir -p /user/$(whoami)/
    hadoop fs -put ${GATEWAY_HOME}/data/security/keystores/ /user/$(whoami)/
  else
    until [ "$(hdfs dfs -ls /user/$(whoami)/keystores || echo $?)" != "0" ]; do
       hdfs dfs -get -f /user/$(whoami)/keystores ${GATEWAY_HOME}/data/security/
    done
  fi
}

function start_knox(){
  local master_additional
  master_additional="$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)"

  add_user
  install_dependencies
  grant_knox_folder_permissions
  prepare_helper
  create_master

  # Share keystores if dataproc HA config
  if [[ "${master_additional}" != "" ]];then
    configure_ha
  fi

  set_webhdfs_address
  start_services
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  if [[ "${role}" == 'Master' ]]; then
    start_knox
  fi
}

main
