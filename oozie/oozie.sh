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

set -euxo pipefail

# Use Python from /usr/bin instead of /opt/conda.
export PATH=/usr/bin:$PATH

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

function min_version() {
  echo -e "$1\n$2" | sort -r -t'.' -n -k1,1 -k2,2 -k3,3 | tail -n1
}

function install_oozie() {
  local master_node
  master_node=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

  # Upgrade the repository and install Oozie
  retry_apt_command "apt-get update"
  retry_apt_command "apt-get install -q -y oozie oozie-client"

  # For Oozie, remove Log4j 2 jar not compatible with Log4j 1 that was brought by Hive 2
  find /usr/lib/oozie/lib -name "log4j-1.2-api*.jar" -delete

  # Delete redundant Slf4j backend implementation
  find /usr/lib/oozie/lib -name "slf4j-simple*.jar" -delete
  find /usr/lib/oozie/lib -name "log4j-slf4j-impl*.jar" -delete

  # Redirect Log4j2 logging to Slf4j backend
  local log4j2_version
  log4j2_version=$(
    find /usr/lib/oozie/lib -name "log4j-core*-2.*.jar" | cut -d '/' -f 6 | cut -d '-' -f 3
  )
  log4j2_version=${log4j2_version/.jar/}
  if [[ -n ${log4j2_version} ]]; then
    local log4j2_to_slf4j=log4j-to-slf4j-${log4j2_version}.jar
    local log4j2_to_slf4j_url=https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-to-slf4j/${log4j2_version}/${log4j2_to_slf4j}
    wget -nv --timeout=30 --tries=5 --retry-connrefused "${log4j2_to_slf4j_url}" -P /usr/lib/oozie/lib
  fi

  # Delete old versions of Jetty jars brought in by dependencies
  find /usr/lib/oozie/ -name "jetty*-6.*.jar" -delete

  local oozie_version
  oozie_version=$(oozie version 2>&1 |
    sed -n 's/.*Oozie[^:]\+:[[:blank:]]\+\([0-9]\+\.[0-9]\.[0-9]\+\+\).*/\1/p' | head -n1)
  if [[ $(min_version '5.0.0' "${oozie_version}") == 5.0.0 ]]; then
    find /usr/lib/oozie/ -name "jetty*-7.*.jar" -delete
  fi

  if [[ "${HOSTNAME}" == "${master_node}" ]]; then
    local tmp_dir
    tmp_dir=$(mktemp -d -t oozie-install-XXXX)

    # The ext library is needed to enable the Oozie web console
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      http://archive.cloudera.com/gplextras/misc/ext-2.2.zip -P "${tmp_dir}"
    unzip -q "${tmp_dir}/ext-2.2.zip" -d /var/lib/oozie

    # Install share lib
    tar -xzf /usr/lib/oozie/oozie-sharelib.tar.gz -C "${tmp_dir}"

    if [[ $(min_version '5.0.0' "${oozie_version}") != 5.0.0 ]]; then
      # Workaround to issue where jackson 1.8 and 1.9 jars are found on the classpath, causing
      # AbstractMethodError at runtime. We know hadoop/lib has matching vesions of jackson.
      rm -f "${tmp_dir}"/share/lib/hive2/jackson-*
      cp /usr/lib/hadoop/lib/jackson-* "${tmp_dir}/share/lib/hive2/"
    fi

    hadoop fs -mkdir -p /user/oozie/
    hadoop fs -put -f "${tmp_dir}/share" /user/oozie/

    # Clean up temporary fles
    rm -rf "${tmp_dir}"
  fi

  # Create the Oozie database
  sudo -u oozie /usr/lib/oozie/bin/ooziedb.sh create -run

  # Set hostname to allow connection from other hosts (not only localhost)
  bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.http.hostname' --value "${HOSTNAME}" \
    --clobber

  # Hadoop must allow impersonation for Oozie to work properly
  bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/core-site.xml" \
    --name 'hadoop.proxyuser.oozie.hosts' --value '*' \
    --clobber

  bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/core-site.xml" \
    --name 'hadoop.proxyuser.oozie.groups' --value '*' \
    --clobber
    
  bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.HadoopAccessorService.supported.filesystems' --value 'hdfs,gs' \
    --clobber

  bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/core-site.xml" \
    --name 'fs.AbstractFileSystem.gs.impl' \
    --value 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS' \
    --clobber
  
  local gcs_connector_dir="/usr/local/share/google/dataproc/lib"
  if [[ ! -d $gcs_connector_dir ]]; then
    gcs_connector_dir="/usr/lib/hadoop/lib"
  fi  
  cp "${gcs_connector_dir}/gcs-connector.jar" /usr/lib/oozie/lib/
  
  # Detect if current node configuration is HA and then set oozie servers
  local additional_nodes
  additional_nodes=$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional |
    sed 's/,/\n/g' | wc -l)
  if [[ ${additional_nodes} -ge 2 ]]; then
    echo 'Starting configuration for HA'
    # List of servers is used for proper zookeeper configuration.
    # It is needed to replace original ports range with specific one
    local servers
    servers=$(grep 'server\.' /usr/lib/zookeeper/conf/zoo.cfg |
      sed 's/server.//g' |
      sed 's/:2888:3888//g' |
      cut -d'=' -f2- |
      sed 's/\n/,/g' |
      head -n 3 |
      sed 's/$/:2181,/g' |
      xargs -L3 |
      sed 's/.$//g')

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
    if [[ $(systemctl list-unit-files | grep ${service}) != '' ]] &&
      [[ $(systemctl is-enabled ${service}) == 'enabled' ]]; then
      systemctl restart ${service}
    fi
  done

  # Leave a safe mode - HDFS will enter a safe mode because of Name Node restart
  if [[ "${HOSTNAME}" == "${master_node}" ]]; then
    hadoop dfsadmin -safemode leave
  fi
}

function main() {
  # Determine the role of this node
  local role
  role=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
  # Only run on the master node of the cluster
  if [[ "${role}" == 'Master' ]]; then
    install_oozie
  fi
}

main
