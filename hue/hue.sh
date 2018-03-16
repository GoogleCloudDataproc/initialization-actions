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

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function install_hue_and_configure() {
  local old_hdfs_url='## webhdfs_url\=http:\/\/localhost:50070'
  local new_hdfs_url='webhdfs_url\=http:\/\/'"$(hdfs getconf -confKey  dfs.namenode.http-address)"
  local old_mysql_settings='## engine=sqlite3(\s+)## host=(\s+)## port=(\s+)## user=(\s+)## password='
  local new_mysql_settings='engine=mysql$1host=127.0.0.1$2port=3306$3user=hue$4password=hue-password'
  local mysql_password='root-password'
  local hue_password='hue-password'
  # Install hue
  apt-get install -t jessie-backports hue -y || err "Failed to install hue"

  # Stop hue
  systemctl stop hue || err "Hue stop action not performed"

  bdconfig set_property \
    --configuration_file 'core-site-patch.xml' \
    --name 'hadoop.proxyuser.hue.hosts' --value '*' \
    --clobber \
    || err 'Unable to set hadoop.proxyuser.hue.hosts'

  bdconfig set_property \
    --configuration_file 'core-site-patch.xml' \
    --name 'hadoop.proxyuser.hue.groups' --value '*' \
    --clobber \
    || err 'Unable to set hadoop.proxyuser.hue.groups'

  sed -i '/<\/configuration>/e cat core-site-patch.xml' \
    /etc/hadoop/conf/core-site.xml

  bdconfig set_property \
    --configuration_file 'hdfs-site-patch.xml' \
    --name 'dfs.webhdfs.enabled' --value 'true' \
    --clobber \
    || err 'Unable to set dfs.webhdfs.enabled'

  bdconfig set_property \
    --configuration_file 'hdfs-site-patch.xml' \
    --name 'hadoop.proxyuser.hue.hosts' --value '*' \
    --clobber \
    || err 'Unable to set hadoop.proxyuser.hue.hosts'

  bdconfig set_property \
    --configuration_file 'hdfs-site-patch.xml' \
    --name 'hadoop.proxyuser.hue.groups' --value '*' \
    --clobber \
    || err 'nable to set hadoop.proxyuser.hue.groups'

  sed -i '/<\/configuration>/e cat hdfs-site-patch.xml' \
    /etc/hadoop/conf/hdfs-site.xml

  cat >> /etc/hue/conf/hue.ini <<EOF
# Defaults to ${HADOOP_MR1_HOME} or /usr/lib/hadoop-0.20-mapreduce
hadoop_mapred_home=/usr/lib/hadoop-mapreduce
EOF

  # If oozie installed, add hue as proxy user for oozie
  if [[ -f /etc/oozie/conf/oozie-site.xml ]];
  then
    bdconfig set_property \
      --configuration_file 'oozie-site-patch.xml' \
      --name 'oozie.service.ProxyUserService.proxyuser.hue.hosts' --value '*' \
      --clobber \
      || err 'Unable to set hadoop.proxyuser.hue.groups'

    bdconfig set_property \
      --configuration_file 'oozie-site-patch.xml' \
      --name 'oozie.service.ProxyUserService.proxyuser.hue.groups' --value '*' \
      --clobber \
      || err 'Unable to set hadoop.proxyuser.hue.groups'

    sed -i '/<\/configuration>/e cat oozie-site-patch.xml' \
      /etc/oozie/conf/oozie-site.xml

    rm oozie-site-patch.xml
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

  # Clean up temporary fles
  rm -rf hdfs-site-patch.xml core-site-patch.xml hue-patch.ini

  # Make hive warehouse directory
  if [[ ! -d /user/hive/warehouse ]]; then
    hdfs dfs -mkdir /user/hive/warehouse || err "Unable to create folder"
  fi

  # Configure Desktop Database to use mysql
  sed -i 's/'"${old_mysql_settings}"'/'"${new_mysql_settings}"'/' /etc/hue/conf/hue.ini

  # Comment out sqlite3 configuration
  sed -i 's/engine=sqlite3/## engine=sqlite3/' /etc/hue/conf/hue.ini

  # Set database name to hue
  sed -i 's/name=\/var\/lib\/hue\/desktop.db/name=hue/' /etc/hue/conf/hue.ini

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