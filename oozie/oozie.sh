#!bin/bash
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
export DATAPROC_IMAGE=$(/usr/share/google/get_metadata_value image)

function retry_command() {
  local cmd="$1"
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep $((i * 5 ))
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
  if [[ "$DATAPROC_IMAGE" == *"cen"* ]]; then
    retry_command "dnf -y -q install oozie oozie-client"

    # Setup symlinks for hadoop jar dependencies on centos
    ln -sf /usr/lib/hadoop/hadoop-common.jar /usr/lib/oozie/lib/
    ln -sf /usr/lib/hadoop/hadoop-auth.jar /usr/lib/oozie/lib/
    ln -sf /usr/lib/hadoop/hadoop-annotations.jar /usr/lib/oozie/lib/

    ln -sf /usr/lib/hadoop-hdfs/hadoop-hdfs-client.jar /usr/lib/oozie/lib/

    ln -sf /usr/lib/hadoop-yarn/hadoop-yarn-common.jar /usr/lib/oozie/lib/
    ln -sf /usr/lib/hadoop-yarn/hadoop-yarn-client.jar /usr/lib/oozie/lib/
    ln -sf /usr/lib/hadoop-yarn/hadoop-yarn-server-common.jar /usr/lib/oozie/lib/
    ln -sf /usr/lib/hadoop-yarn/hadoop-yarn-api.jar /usr/lib/oozie/lib/

    ln -sf /usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-jobclient.jar /usr/lib/oozie/lib/
    ln -sf /usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-app.jar /usr/lib/oozie/lib/
    ln -sf /usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-common.jar /usr/lib/oozie/lib/
    ln -sf /usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-core.jar /usr/lib/oozie/lib/
    ln -sf /usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-shuffle.jar /usr/lib/oozie/lib/
  else
    retry_command "apt-get update"
    retry_command "apt-get install -q -y oozie oozie-client"
  fi

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

    # These jars are required to run the spark example in cluster mode
    hadoop fs -put -f ${tmp_dir}/share/lib/hive/hadoop-common-3.2.2.jar /user/oozie/share/lib/spark
    hadoop fs -put -f ${tmp_dir}/share/lib/hive/woodstox-core-5.0.3.jar /user/oozie/share/lib/spark
    hadoop fs -put -f ${tmp_dir}/share/lib/hive/stax-api-1.0.1.jar /user/oozie/share/lib/spark
    hadoop fs -put -f ${tmp_dir}/share/lib/hive/stax2-api-3.1.4.jar /user/oozie/share/lib/spark
    hadoop fs -put -f ${tmp_dir}/share/lib/hive/commons-collections4-4.1.jar /user/oozie/share/lib/spark
    hadoop fs -put -f ${tmp_dir}/share/lib/hive/commons-collections4-4.1.jar /user/oozie/share/lib/spark
    hadoop fs -put -f ${tmp_dir}/share/lib/hive/commons-collections-3.2.2.jar /user/oozie/share/lib/spark
    hadoop fs -put -f ${tmp_dir}/share/lib/hive/commons-collections-3.2.2.jar /user/oozie/share/lib/spark
    hadoop fs -put -f ${tmp_dir}/share/lib/hive/commons-*.jar /user/oozie/share/lib/spark
    hadoop fs -put -f ${tmp_dir}/share/lib/hive/htrace-core4-4.1.0-incubating.jar /user/oozie/share/lib/spark
    hadoop fs -put -f ${tmp_dir}/share/lib/hive/hadoop*.jar /user/oozie/share/lib/spark
    hadoop fs -put -f /usr/lib/spark/jars/hadoop*.jar /user/oozie/share/lib/spark
    hadoop fs -put -f /usr/lib/spark/jars/spark-hadoop-cloud_2.12-3.1.2.jar /user/oozie/share/lib/spark
    hadoop fs -put -f /usr/lib/spark/jars/hadoop-cloud-storage-3.2.2.jar /user/oozie/share/lib/spark
    hadoop fs -put -f /usr/local/share/google/dataproc/lib/gcs-connector.jar /user/oozie/share/lib/spark
    hadoop fs -put -f /usr/lib/spark/jars/re2j-1.1.jar /user/oozie/share/lib/spark

    # Update properties for database
    /usr/bin/mysqladmin -u root --password=root-password create oozie
    /usr/bin/mysql -u root --password=root-password <<EOM
CREATE USER 'oozie'@'%' IDENTIFIED BY 'oozie-password';
GRANT ALL PRIVILEGES ON *.* TO 'oozie'@'%';
FLUSH PRIVILEGES;
EOM

    # Clean up temporary fles
    rm -rf "${tmp_dir}"
  fi

  # Link the MySQL JDBC driver to the Oozie library directory
  ln -s /usr/share/java/mysql.jar /usr/lib/oozie/lib/mysql.jar

  # Set JDBC properties
  mysql_host=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

  bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.JPAService.jdbc.driver' --value "com.mysql.cj.jdbc.Driver" \
    --clobber

  bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.JPAService.jdbc.url' --value "jdbc:mysql://${mysql_host}/oozie" \
    --clobber

  bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.JPAService.jdbc.username' --value "oozie" \
    --clobber

  bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.JPAService.jdbc.password' --value "oozie-password" \
    --clobber

  bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.JPAService.pool.max.active.conn' --value "20" \
    --clobber

  # Create the Oozie database
  retry_command "sudo -u oozie /usr/lib/oozie/bin/ooziedb.sh create -run"

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
    hdfs dfsadmin -safemode leave
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
