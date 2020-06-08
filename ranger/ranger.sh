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
# This initialization action installs Apache Ranger on Dataproc Cluster.

set -euxo pipefail

readonly NOT_SUPPORTED_MESSAGE="Ranger initialization action is not supported on Dataproc 2.0+.
Use Ranger Component instead: https://cloud.google.com/dataproc/docs/concepts/components/ranger"
[[ $DATAPROC_VERSION = 2.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

# Use Python from /usr/bin instead of /opt/conda.
export PATH=/usr/bin:$PATH

readonly NODE_NAME=$(/usr/share/google/get_metadata_value name)
readonly RANGER_ADMIN_PORT=$(/usr/share/google/get_metadata_value attributes/ranger-port || echo '6080')
readonly RANGER_ADMIN_PASS=$(/usr/share/google/get_metadata_value attributes/default-admin-password)
readonly RANGER_INSTALL_DIR='/usr/lib/ranger'
readonly SOLR_HOME='/usr/lib/solr'
readonly MASTER_ADDITIONAL=$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)
readonly CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  return 1
}

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
  local pkgs="$*"
  retry_apt_command "apt-get install -y $pkgs"
}

function configure_admin() {
  sed -i 's/^db_root_password=/db_root_password=root-password/' \
    "${RANGER_INSTALL_DIR}/ranger-admin/install.properties"
  sed -i 's/^db_password=/db_password=rangerpass/' \
    "${RANGER_INSTALL_DIR}/ranger-admin/install.properties"
  sed -i "s/^rangerAdmin_password=/rangerAdmin_password=${RANGER_ADMIN_PASS}/" \
    "${RANGER_INSTALL_DIR}/ranger-admin/install.properties"
  sed -i 's/^audit_solr_user=/audit_solr_user=solr/' \
    "${RANGER_INSTALL_DIR}/ranger-admin/install.properties"
  bdconfig set_property \
    --configuration_file "${RANGER_INSTALL_DIR}/ranger-admin/ews/webapp/WEB-INF/classes/conf.dist/ranger-admin-site.xml" \
    --name 'ranger.service.http.port' --value "${RANGER_ADMIN_PORT}" \
    --clobber
  mysql -u root -proot-password -e "CREATE USER 'rangeradmin'@'localhost' IDENTIFIED BY 'rangerpass';"
  mysql -u root -proot-password -e "CREATE DATABASE ranger;"
  mysql -u root -proot-password -e "GRANT ALL PRIVILEGES ON ranger.* TO 'rangeradmin'@'localhost';"

  if [[ "${MASTER_ADDITIONAL}" != "" ]]; then
    sed -i "s/^audit_solr_zookeepers=/audit_solr_zookeepers=${CLUSTER_NAME}-m-0:2181,${CLUSTER_NAME}-m-1:2181,${CLUSTER_NAME}-m-2:2181\/solr/" \
      "${RANGER_INSTALL_DIR}/ranger-admin/install.properties"
    sed -i 's/^audit_solr_urls=/audit_solr_urls=none/' \
      "${RANGER_INSTALL_DIR}/ranger-admin/install.properties"
    runuser -l solr -s /bin/bash -c "${SOLR_HOME}/bin/solr create_collection -c ranger_audits -d ${RANGER_INSTALL_DIR}/ranger-admin/contrib/solr_for_audit_setup/conf -shards 1 -replicationFactor 3"
  else
    sed -i 's/^audit_solr_urls=/audit_solr_urls=http:\/\/localhost:8983\/solr\/ranger_audits/' \
      "${RANGER_INSTALL_DIR}/ranger-admin/install.properties"
    runuser -l solr -s /bin/bash -c "${SOLR_HOME}/bin/solr create_core -c ranger_audits -d ${RANGER_INSTALL_DIR}/ranger-admin/contrib/solr_for_audit_setup/conf -shards 1 -replicationFactor 1"
  fi
}

function run_ranger_admin() {
  configure_admin
  pushd "${RANGER_INSTALL_DIR}/ranger-admin" && ./set_globals.sh && ./setup.sh
  ranger-admin start
  popd
}

function add_usersync_plugin() {
  mkdir -p /var/log/ranger-usersync && chown ranger /var/log/ranger-usersync &&
    chgrp ranger /var/log/ranger-usersync

  sed -i 's/^logdir=logs/logdir=\/var\/log\/ranger-usersync/' \
    "${RANGER_INSTALL_DIR}/ranger-usersync/install.properties"
  sed -i "s/^POLICY_MGR_URL =/POLICY_MGR_URL = http:\/\/localhost:${RANGER_ADMIN_PORT}/" \
    "${RANGER_INSTALL_DIR}/ranger-usersync/install.properties"

  pushd "${RANGER_INSTALL_DIR}/ranger-usersync" && ./setup.sh
  ranger-usersync start
  popd
}

#######################################
# Configure Policy Manager URL, Solar Auditing and Service name
# Arguments:
#  plugin_name - name of ranger plugin
#  service_name - service that will use plugin
#######################################
function apply_common_plugin_configuration() {
  local plugin_name="${1}"
  local service_name="${2}"
  sed -i "s/^POLICY_MGR_URL=/POLICY_MGR_URL=http:\/\/localhost:${RANGER_ADMIN_PORT}/" \
    "${RANGER_INSTALL_DIR}/${plugin_name}/install.properties"
  sed -i "s/^REPOSITORY_NAME=/REPOSITORY_NAME=${service_name}/" \
    "${RANGER_INSTALL_DIR}/${plugin_name}/install.properties"
  sed -i 's/^XAAUDIT.SOLR.ENABLE=false/XAAUDIT.SOLR.ENABLE=true/' \
    "${RANGER_INSTALL_DIR}/${plugin_name}/install.properties"
  sed -i 's/^XAAUDIT.SOLR.URL=NONE/XAAUDIT.SOLR.URL=http:\/\/localhost:8983\/solr\/ranger_audits/' \
    "${RANGER_INSTALL_DIR}/${plugin_name}/install.properties"
  sed -i 's/^XAAUDIT.SOLR.USER=NONE/XAAUDIT.SOLR.USER=solr/' \
    "${RANGER_INSTALL_DIR}/${plugin_name}/install.properties"
  if [[ "${MASTER_ADDITIONAL}" != "" ]]; then
    sed -i "s/^XAAUDIT.SOLR.ZOOKEEPER=NONE/XAAUDIT.SOLR.ZOOKEEPER=${CLUSTER_NAME}-m-0:2181,${CLUSTER_NAME}-m-1:2181,${CLUSTER_NAME}-m-2:2181\/solr/" \
      "${RANGER_INSTALL_DIR}/${plugin_name}/install.properties"
  fi
}

function create_symlinks_to_hadoop_conf() {
  if [[ ! -d "${RANGER_INSTALL_DIR}/hadoop/etc" ]]; then
    mkdir -p "${RANGER_INSTALL_DIR}/hadoop/etc" "${RANGER_INSTALL_DIR}/hadoop/share/hadoop/hdfs/"
    ln -s /etc/hadoop/conf "${RANGER_INSTALL_DIR}/hadoop/etc/hadoop"
    ln -s /usr/lib/hadoop-hdfs/lib "${RANGER_INSTALL_DIR}/hadoop/share/hadoop/hdfs/"
  fi
}

function add_hdfs_plugin() {
  apply_common_plugin_configuration "ranger-hdfs-plugin" "hadoop-dataproc"

  pushd ranger-hdfs-plugin && ./enable-hdfs-plugin.sh && popd

  if [[ "${NODE_NAME}" =~ ^.*(-m|-m-0)$ ]]; then
    systemctl stop hadoop-hdfs-namenode.service
    systemctl start hadoop-hdfs-namenode.service

    # Notify a cluster that plugin installed on master.
    until hadoop fs -touchz /tmp/ranger-hdfs-plugin-ready &>/dev/null; do
      sleep 10
    done

    cat <<EOF >service-hdfs.json
{
    "configs": {
        "username": "admin",
        "password": "${RANGER_ADMIN_PASS}",
        "hadoop.security.authentication": "Simple",
        "hadoop.security.authorization": "No",
        "fs.default.name": "hdfs://${NODE_NAME}:8020"
        },
    "description": "Hadoop hdfs service",
    "isEnabled": true,
    "name": "hadoop-dataproc",
    "type": "hdfs",
    "version": 1
}
EOF
    curl --user "admin:${RANGER_ADMIN_PASS}" -H "Content-Type: application/json" \
      -X POST -d @service-hdfs.json "http://localhost:${RANGER_ADMIN_PORT}/service/public/v2/api/service"
  elif [[ "${NODE_NAME}" =~ ^.*(-m-1)$ ]]; then
    # Waiting until HDFS plugin will be configured on m-0
    until hadoop fs -ls /tmp/ranger-hdfs-plugin-ready &>/dev/null; do
      sleep 10
    done
    systemctl stop hadoop-hdfs-namenode.service
    systemctl start hadoop-hdfs-namenode.service
  fi
}

function add_hive_plugin() {
  apply_common_plugin_configuration "ranger-hive-plugin" "hive-dataproc"
  mkdir -p hive &&
    ln -s /etc/hive/conf hive &&
    ln -s /usr/lib/hive/lib hive
  pushd ranger-hive-plugin && ./enable-hive-plugin.sh && popd

  if [[ "${NODE_NAME}" =~ ^.*(-m|-m-0)$ ]]; then
    systemctl stop hive-server2.service
    systemctl start hive-server2.service

    # Notify cluster that hive plugin is installed on master.
    until hadoop fs -touchz /tmp/ranger-hive-plugin-ready &>/dev/null; do
      sleep 10
    done

    cat <<EOF >service-hive.json
{
    "configs": {
        "username": "admin",
        "password": "${RANGER_ADMIN_PASS}",
        "jdbc.driverClassName" : "org.apache.hive.jdbc.HiveDriver",
        "jdbc.url" : "jdbc:mysql://localhost"
        },
    "description": "Hadoop HIVE service",
    "isEnabled": true,
    "name": "hive-dataproc",
    "type": "hive",
    "version": 1
}
EOF
    curl --user "admin:${RANGER_ADMIN_PASS}" -H "Content-Type: application/json" \
      -X POST -d @service-hive.json "http://localhost:${RANGER_ADMIN_PORT}/service/public/v2/api/service"
  elif [[ "${NODE_NAME}" =~ ^.*(-m-1|-m-2)$ ]]; then
    # Waiting until hive plugin will be configured on m-0
    until hadoop fs -ls /tmp/ranger-hive-plugin-ready &>/dev/null; do
      sleep 10
    done
    systemctl stop hive-server2.service
    systemctl start hive-server2.service
  fi
}

function add_yarn_plugin() {
  apply_common_plugin_configuration "ranger-yarn-plugin" "yarn-dataproc"
  pushd ranger-yarn-plugin && ./enable-yarn-plugin.sh && popd

  if [[ "${NODE_NAME}" =~ ^.*(-m|-m-0)$ ]]; then
    systemctl stop hadoop-yarn-resourcemanager.service
    systemctl start hadoop-yarn-resourcemanager.service

    # Notify cluster that yarn plugin is installed on master.
    until hadoop fs -touchz /tmp/ranger-yarn-plugin-ready &>/dev/null; do
      sleep 10
    done

    cat <<EOF >service-yarn.json
{
    "configs": {
        "username": "admin",
        "password": "${RANGER_ADMIN_PASS}",
        "yarn.url": "http://${NODE_NAME}:8088"
        },
    "description": "Hadoop YARN service",
    "isEnabled": true,
    "name": "yarn-dataproc",
    "type": "yarn",
    "version": 1
}
EOF
    curl --user "admin:${RANGER_ADMIN_PASS}" -H "Content-Type: application/json" \
      -X POST -d @service-yarn.json "http://localhost:${RANGER_ADMIN_PORT}/service/public/v2/api/service"
  elif [[ "${NODE_NAME}" =~ ^.*(-m-1|-m-2)$ ]]; then
    # Waiting until yarn plugin will be configured on m-0
    until hadoop fs -ls /tmp/ranger-yarn-plugin-ready &>/dev/null; do
      sleep 10
    done
    systemctl stop hadoop-yarn-resourcemanager.service
    systemctl start hadoop-yarn-resourcemanager.service
  fi
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  if [[ -z "${RANGER_ADMIN_PASS}" ]]; then
    err 'Ranger admin password not set. Please use metadata flag - default-password'
  fi
  mkdir -p "${RANGER_INSTALL_DIR}" && cd "${RANGER_INSTALL_DIR}"

  update_apt_get
  install_apt_get ranger

  if [[ "${role}" == 'Master' && "${NODE_NAME}" =~ ^.*(-m|-m-0)$ ]]; then
    run_ranger_admin
    add_usersync_plugin
    create_symlinks_to_hadoop_conf
    add_hdfs_plugin
    add_hive_plugin
    add_yarn_plugin
  fi

  # In HA clusters Namenode is installed also on m-1. We also need to install and configure
  # ranger hdfs plugin on m-1.
  if [[ "${role}" == 'Master' && "${NODE_NAME}" =~ ^.*(-m-1)$ ]]; then
    create_symlinks_to_hadoop_conf
    add_hdfs_plugin
  fi

  # In HA clusters ResourceManager is installed also on m-1 and m-2.
  # We also need to install and configure ranger yarn plugin on additional masters.
  if [[ "${role}" == 'Master' && "${NODE_NAME}" =~ ^.*(-m-1|-m-2)$ ]]; then
    create_symlinks_to_hadoop_conf
    add_hive_plugin
    add_yarn_plugin
  fi
}

main
