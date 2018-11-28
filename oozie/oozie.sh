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
# Initialization action for installing Apache Oozie on a Google Cloud
# Dataproc cluster. This script will install and configure Oozie to run on the
# master node of a Dataproc cluster. The version of Oozie which is installed
# comes from the BigTop repository.
#
# You can find more information about Oozie at http://oozie.apache.org/
# For more information in init actions and Google Cloud Dataproc see the Cloud
# Dataproc documentation at https://cloud.google.com/dataproc/init-actions
#
# This script should run in under a few minutes

set -x -e

# Use Python from /usr/bin instead of /opt/conda.
export PATH=/usr/bin:$PATH

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

function main() {
  # Determine the role of this node
  local role=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
  # Only run on the master node of the cluster
  if [[ "${role}" == 'Master' ]]; then
    install_oozie
  fi
}

function install_oozie(){
  local master_node=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
  local node_name=${HOSTNAME}

  # Upgrade the repository and install Oozie
  update_apt_get || err 'Failed to update apt-get'
  install_apt_get oozie oozie-client || err 'Unable to install oozie-client'

  if [[ ${node_name} == ${master_node} ]]; then
    # The ext library is needed to enable the Oozie web console
    wget http://archive.cloudera.com/gplextras/misc/ext-2.2.zip || err 'Unable to download ext-2.2.zip'
    unzip ext-2.2.zip
    ln -s ext-2.2 /var/lib/oozie/ext-2.2 || err 'Unable to create symbolic link'
   # Install share lib
  install -d /usr/lib/oozie
  tar -xvzf /usr/lib/oozie/oozie-sharelib.tar.gz -C /usr/lib/oozie

  # Workaround to issue where jackson 1.8 and 1.9 jars are found on the classpath, causing
  # AbstractMethodError at runtime. We know hadoop/lib has matching vesions of jackson.
  rm -f /usr/lib/oozie/share/lib/hive2/jackson-*
  cp /usr/lib/hadoop/lib/jackson-* /usr/lib/oozie/share/lib/hive2/

  hadoop fs -mkdir -p /user/oozie/
  hadoop fs -put -f /usr/lib/oozie/share /user/oozie/
    # Clean up temporary fles
   rm -rf ext-2.2 ext-2.2.zip share oozie-sharelib.tar.gz
   fi

  # Create the Oozie database
  sudo -u oozie /usr/lib/oozie/bin/ooziedb.sh create -run
  # Hadoop must allow impersonation for Oozie to work properly

  bdconfig set_property \
      --configuration_file "/etc/hadoop/conf/core-site.xml" \
      --name 'hadoop.proxyuser.oozie.hosts' --value '*' \
      --clobber

  bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/core-site.xml" \
    --name 'hadoop.proxyuser.oozie.groups' --value '*' \
    --clobber


  # Detect if current node configuration is HA and then set oozie servers
  local additional_nodes=$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional | sed 's/,/\n/g' | wc -l)
  if [[ ${additional_nodes} -ge 2 ]]; then
    echo 'Starting configuration for HA'
    # List of servers is used for proper zookeeper configuration. It is needed to replace original ports range with specific one
    local servers=$(cat /usr/lib/zookeeper/conf/zoo.cfg \
      | grep 'server.' \
      | sed 's/server.//g' \
      | sed 's/:2888:3888//g' \
      | cut -d'=' -f2- \
      | sed 's/\n/,/g' \
      | head -n 3 \
      | sed 's/$/:2181,/g' \
      | xargs -L3 \
      | sed 's/.$//g')

    bdconfig set_property \
      --configuration_file "/etc/oozie/conf/oozie-site.xml" \
      --name 'oozie.services.ext' --value \
        'org.apache.oozie.service.ZKLocksService,
        org.apache.oozie.service.ZKXLogStreamingService,
        org.apache.oozie.service.ZKJobsConcurrencyService,
        org.apache.oozie.service.ZKUUIDService' \
      --clobber

    bdconfig set_property \
      --configuration_file "/etc/oozie/conf/oozie-site.xml" \
      --name 'oozie.zookeeper.connection.string' --value "${servers}" \
      --clobber

  fi

  /usr/lib/zookeeper/bin/zkServer.sh restart
  # HDFS and YARN must be cycled; restart to clean things up
  for service in hadoop-hdfs-namenode hadoop-hdfs-secondarynamenode hadoop-yarn-resourcemanager oozie; do
    if [[ $(systemctl list-unit-files | grep ${service}) != '' ]] && \
      [[ $(systemctl is-enabled ${service}) == 'enabled' ]]; then
      systemctl restart ${service}
    fi
  done
}

main
