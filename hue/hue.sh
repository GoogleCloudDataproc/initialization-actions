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
# This script installs HUE on a master node within a Google Cloud Dataproc cluster.

set -x -e

function random_string()
{
    cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w ${1:-32} | head -n 1
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
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

function install_hue_and_configure() {
  local old_hdfs_url='## webhdfs_url\=http:\/\/localhost:50070'
  local new_hdfs_url='webhdfs_url\=http:\/\/'"$(hdfs getconf -confKey dfs.namenode.http-address)"
  local old_mysql_settings='## engine=sqlite3(\s+)## host=(\s+)## port=(\s+)## user=(\s+)## password='
  local new_mysql_settings='engine=mysql$1host=127.0.0.1$2port=3306$3user=hue$4password=hue-password'
  local mysql_password='root-password'
  local hue_password='hue-password'
  local random=$(random_string)
  local hadoop_conf_dir='/etc/hadoop/conf'
  # Install hue
  retry_apt_command "apt-get install -t $(lsb_release -sc)-backports -y hue" || err "Failed to install hue"

  # Stop hue
  systemctl stop hue || err "Hue stop action not performed"

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/core-site.xml" \
    --name 'hadoop.proxyuser.hue.hosts' --value '*' \
    --create_if_absent \
    --clobber \
    || err 'Unable to set hadoop.proxyuser.hue.hosts'

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/core-site.xml" \
    --name 'hadoop.proxyuser.hue.groups' --value '*' \
    --create_if_absent \
    --clobber \
    || err 'Unable to set hadoop.proxyuser.hue.groups'

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/hdfs-site.xml" \
    --name 'dfs.webhdfs.enabled' --value 'true' \
    --create_if_absent \
    --clobber \
    || err 'Unable to set dfs.webhdfs.enabled'

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/hdfs-site.xml" \
    --name 'hadoop.proxyuser.hue.hosts' --value '*' \
    --create_if_absent \
    --clobber \
    || err 'Unable to set hadoop.proxyuser.hue.hosts'

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/hdfs-site.xml" \
    --name 'hadoop.proxyuser.hue.groups' --value '*' \
    --create_if_absent \
    --clobber \
    || err 'Unable to set hadoop.proxyuser.hue.groups'


  cat >> /etc/hue/conf/hue.ini <<EOF
# Defaults to ${HADOOP_MR1_HOME} or /usr/lib/hadoop-0.20-mapreduce
hadoop_mapred_home=/usr/lib/hadoop-mapreduce
EOF

  # If oozie installed, add hue as proxy user for oozie
  if [[ -f /etc/oozie/conf/oozie-site.xml ]];
  then
    bdconfig set_property \
      --configuration_file '/etc/oozie/conf/oozie-site.xml' \
      --name 'oozie.service.ProxyUserService.proxyuser.hue.hosts' --value '*' \
      --create_if_absent \
      --clobber \
      || err 'Unable to set hadoop.proxyuser.hue.groups'

    bdconfig set_property \
      --configuration_file '/etc/oozie/conf/oozie-site.xml' \
      --name 'oozie.service.ProxyUserService.proxyuser.hue.groups' --value '*' \
      --create_if_absent \
      --clobber \
      || err 'Unable to set hadoop.proxyuser.hue.groups'

    systemctl restart oozie || err "Unable to restart oozie"
  else
    echo "oozie not installed, skipped configuring hue as an user proxy"
  fi

  # Fix webhdfs_url
  sed -i 's/'"${old_hdfs_url}"'/'"${new_hdfs_url}"'/' /etc/hue/conf/hue.ini
  # Uncomment every line containing localhost, replacing localhost with the FQDN
  sed -i "s/#*\([^#]*=.*\)localhost/\1$(hostname --fqdn)/" /etc/hue/conf/hue.ini

  # Comment out any duplicate resourcemanager_api_url fields after the first one
  sed -i '0,/resourcemanager_api_url/! s/resourcemanager_api_url/## resourcemanager_api_url/' \
    /etc/hue/conf/hue.ini

  # Make hive warehouse directory
  hdfs dfs -mkdir /user/hive/warehouse || err "Unable to create folder"

  bdconfig set_property \
      --configuration_file '/etc/hue/conf/hive-site.xml' \
      --create_if_absent \
      --clobber \
      --name 'hive.metastore.warehouse.dir' --value '/user/hive/warehouse'

  # Configure Desktop Database to use mysql
  perl -i -0777 -pe 's/'"${old_mysql_settings}"'/'"${new_mysql_settings}"'/' /etc/hue/conf/hue.ini

  # Comment out sqlite3 configuration
  sed -i 's/engine=sqlite3/## engine=sqlite3/' /etc/hue/conf/hue.ini

  # Set database name to hue
  sed -i 's/name=\/var\/lib\/hue\/desktop.db/name=hue/' /etc/hue/conf/hue.ini

  # Set random secret key
  sed -i 's/'"secret_key=.*"'/'"secret_key=${random}"'/' /etc/hue/conf/hue.ini

  # Create database, give hue user permissions
  mysql -u root -proot-password -e " \
    CREATE DATABASE hue; \
    CREATE USER 'hue'@'localhost' IDENTIFIED BY '${hue_password}'; \
    GRANT ALL PRIVILEGES ON hue.* TO 'hue'@'localhost';" \
    || err "Unable to create database"

  # Restart mysql
  systemctl restart mysql || err "Unable to restart mysql"

  # Hue creates all needed tables
  /usr/lib/hue/build/env/bin/hue syncdb --noinput || err "Database sync failed"
  /usr/lib/hue/build/env/bin/hue migrate || err "Data migration failed"

  # Restart servers
  systemctl restart hadoop-hdfs-namenode || err "Unable to restart hadoop-hdfs-namenode"
  systemctl restart hadoop-yarn-resourcemanager || err "Unable to restart hadoop-yarn-resourcemanager"
  systemctl restart hue || err "Unable to restart hue"
  systemctl restart mysql || err "Unable to restart mysql"
}

function main() {
  # Determine the role of this node
  local role=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
  # Only run on the master node of the cluster
  if [[ "${role}" == 'Master' ]]; then
    update_apt_get || err "Unable to update apt-get"
    install_hue_and_configure || err "Hue install process failed"
  else
    return 0 || err "Hue can be installed only on master node - skipped for worker node"
  fi
}

main
