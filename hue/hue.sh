#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This script installs Hue on a master node within a Google Cloud Dataproc cluster.

set -euxo pipefail

# Determine the role of this node
MASTER_HOSTNAME=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly MASTER_HOSTNAME

function random_string() {
  tr </dev/urandom -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  exit 1
}

function retry_apt_command() {
  local cmd="$1"
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

function install_hue_and_configure() {
  local old_hdfs_url='## webhdfs_url\=http:\/\/localhost:50070'
  local new_hdfs_url
  new_hdfs_url="webhdfs_url\=http:\/\/$(hdfs getconf -confKey dfs.namenode.http-address)"
  local hue_password='hue-password'
  local old_mysql_settings='## engine=sqlite3(\s+)## host=(\s+)## port=(\s+)## user=(\s+)## password='
  local new_mysql_settings="engine=mysql\$1host=127.0.0.1\$2port=3306\$3user=hue\$4password=${hue_password}"
  local hadoop_conf_dir='/etc/hadoop/conf'

  # Install Hue
  retry_apt_command "apt-get install -t $(lsb_release -sc)-backports -y hue" ||
    err "Failed to install Hue"

  # Stop Hue
  systemctl stop hue || err "Hue stop action not performed"

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/core-site.xml" \
    --name 'hadoop.proxyuser.hue.hosts' --value '*' \
    --create_if_absent \
    --clobber ||
    err 'Unable to set hadoop.proxyuser.hue.hosts'

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/core-site.xml" \
    --name 'hadoop.proxyuser.hue.groups' --value '*' \
    --create_if_absent \
    --clobber ||
    err 'Unable to set hadoop.proxyuser.hue.groups'

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/hdfs-site.xml" \
    --name 'dfs.webhdfs.enabled' --value 'true' \
    --create_if_absent \
    --clobber ||
    err 'Unable to set dfs.webhdfs.enabled'

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/hdfs-site.xml" \
    --name 'hadoop.proxyuser.hue.hosts' --value '*' \
    --create_if_absent \
    --clobber ||
    err 'Unable to set hadoop.proxyuser.hue.hosts'

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/hdfs-site.xml" \
    --name 'hadoop.proxyuser.hue.groups' --value '*' \
    --create_if_absent \
    --clobber ||
    err 'Unable to set hadoop.proxyuser.hue.groups'

  cat >>/etc/hue/conf/hue.ini <<'EOF'

# Defaults to ${HADOOP_MR1_HOME} or /usr/lib/hadoop-0.20-mapreduce
hadoop_mapred_home=/usr/lib/hadoop-mapreduce
EOF

  # If Oozie installed, add Hue as proxy user for Oozie
  if [[ -f /etc/oozie/conf/oozie-site.xml ]]; then
    bdconfig set_property \
      --configuration_file '/etc/oozie/conf/oozie-site.xml' \
      --name 'oozie.service.ProxyUserService.proxyuser.hue.hosts' --value '*' \
      --create_if_absent \
      --clobber ||
      err 'Unable to set hadoop.proxyuser.hue.groups'

    bdconfig set_property \
      --configuration_file '/etc/oozie/conf/oozie-site.xml' \
      --name 'oozie.service.ProxyUserService.proxyuser.hue.groups' --value '*' \
      --create_if_absent \
      --clobber ||
      err 'Unable to set hadoop.proxyuser.hue.groups'

    systemctl restart oozie || err "Unable to restart oozie"
  else
    echo "Oozie not installed, skipped configuring Hue as an user proxy"
  fi

  # Uncomment supported interpreters
  local interpreters=(
    hive sql spark pyspark r jar py text markdown pig java spark2 mapreduce solr druid impala
    sqoop1 distcp shell presto
  )
  for interpreter in "${interpreters[@]}"; do
    sed -i "/^ *# \[\[\[${interpreter}\]\]\]/,/^ *$/s/^\( *\)# \?/\1/" /etc/hue/conf/hue.ini
  done

  # Configure webhdfs_url
  sed -i "s/${old_hdfs_url}/${new_hdfs_url}/" /etc/hue/conf/hue.ini

  # Replace localhost in all values with FQDN
  sed -i "s/\(.*=.*\)localhost/\1$(hostname --fqdn)/" /etc/hue/conf/hue.ini

  # Uncomment necessary config keys
  local config_keys=(
    default_from_email django_server_email resourcemanager_host resourcemanager_api_url
    proxy_api_url history_server_api_url spark_history_server_url hive_server_host livy_server_url
    sql_server_host server_url host_ports rest_url oozie_url whitelist hbase_clusters solr_url
    ensemble
  )
  for config_key in "${config_keys[@]}"; do
    sed -i "s/#[# ]*\(${config_key} \?=\)/\1/" /etc/hue/conf/hue.ini
  done

  # Uncomment Sentry configuration
  sed -i "s/#[# ]*\(hostname \?= \?$(hostname --fqdn)\)/\1/" /etc/hue/conf/hue.ini

  # Comment out any duplicate resourcemanager_api_url fields after the first one
  sed -i '0,/resourcemanager_api_url/! s/resourcemanager_api_url/## resourcemanager_api_url/' \
    /etc/hue/conf/hue.ini

  # Make hive warehouse directory
  local warehause_dir="/user/hive/warehouse"
  hdfs dfs -mkdir "${warehause_dir}" || hdfs dfs -stat "${warehause_dir}"

  bdconfig set_property \
    --configuration_file '/etc/hue/conf/hive-site.xml' \
    --create_if_absent \
    --name 'hive.metastore.warehouse.dir' --value '/user/hive/warehouse' \
    --clobber

  # Configure Desktop Database to use mysql
  perl -i -0777 -pe "s/${old_mysql_settings}/${new_mysql_settings}/" /etc/hue/conf/hue.ini

  # Comment out sqlite3 configuration
  sed -i 's/engine=sqlite3/## engine=sqlite3/' /etc/hue/conf/hue.ini

  # Set database name to 'hue'
  sed -i 's/name=\/var\/lib\/hue\/desktop.db/name=hue/' /etc/hue/conf/hue.ini

  # Disable logging and set random secret key
  set +x
  sed -i "s/secret_key=.*/secret_key=$(random_string)/" /etc/hue/conf/hue.ini
  set -x

  # Create a database, give 'hue' user permissions
  mysql -u root -proot-password -e "
      CREATE DATABASE hue;
      CREATE USER 'hue'@'localhost' IDENTIFIED BY '${hue_password}';
      GRANT ALL PRIVILEGES ON hue.* TO 'hue'@'localhost';" ||
    err "Unable to create database"

  # Hue creates all needed tables
  /usr/lib/hue/build/env/bin/hue syncdb --noinput
  /usr/lib/hue/build/env/bin/hue migrate

  # Restart servers
  systemctl restart hadoop-hdfs-namenode hadoop-yarn-resourcemanager

  # Start Hue
  systemctl start hue
}

# Only run on the master node ("0"-master in HA mode) of the cluster
if [[ "${HOSTNAME}" == "${MASTER_HOSTNAME}" ]]; then
  update_apt_get || err "Unable to update apt-get"
  install_hue_and_configure || err "Hue install process failed"
fi
